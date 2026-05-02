#!/usr/bin/env python3
"""
XCP-ng Interactive Admin Utility

Provides quick access to common hypervisor operations including:
- VM lifecycle (start, stop, reboot, snapshot)
- Disk management (resize VDI)
- Network management (add/remove VIFs)
- Bulk operations (migrate, shutdown all)
- Housekeeping (delete old snapshots, find orphans)

Uses XenAPI over HTTPS for direct communication with XCP-ng hosts.

Features operation logging for audit trail and debugging.
"""

import sys
import ssl
import json
import socket
import logging
import argparse
from pathlib import Path
import subprocess
from datetime import datetime
from getpass import getpass
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import XenAPI
except ImportError:
    print("Error: XenAPI module not found.")
    print("Install with: pip install XenAPI")
    sys.exit(1)


# ============================================================================
# CONFIGURATION AND LOGGING
# ============================================================================

_config = {
    'hosts': [],  # List of XCP-ng hosts
    'default_host': None,
    'logging': {
        'enabled': False,
        'log_file': 'xcp_admin.log',
        'log_level': 'INFO',
        'log_operations': True
    }
}

# Session options (reset each run)
_session = {
    'warning_level': 'full',  # 'full', 'confirm', or 'none'
}

# Persistent marker file for disclaimer acceptance
DISCLAIMER_ACCEPTED_FILE = Path.home() / '.xcp_admin_accepted'

op_logger = None


def load_config(config_file='xcp_admin_config.json'):
    """Load configuration from JSON file."""
    global _config
    config_path = Path(config_file)

    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                loaded_config = json.load(f)
                _config.update(loaded_config)
                print(f"✓ Loaded configuration from {config_file}")
                return True
        except Exception as e:
            print(f"⚠ Warning: Could not load config file {config_file}: {e}")
            print("  Using default configuration.")
    return False


def setup_logger(enabled=None, log_file=None, log_level=None):
    """Set up operation logging."""
    global op_logger, _config

    if enabled is not None:
        _config['logging']['enabled'] = enabled
    if log_file is not None:
        _config['logging']['log_file'] = log_file
    if log_level is not None:
        _config['logging']['log_level'] = log_level

    if not _config['logging']['enabled']:
        op_logger = None
        return

    op_logger = logging.getLogger('xcp_operations')
    op_logger.setLevel(getattr(logging, _config['logging']['log_level']))
    op_logger.handlers.clear()

    log_path = Path(_config['logging']['log_file'])
    file_handler = logging.FileHandler(log_path, mode='a')
    file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    op_logger.addHandler(file_handler)

    op_logger.info("=" * 80)
    op_logger.info("XCP-NG ADMIN SESSION STARTED")
    op_logger.info("=" * 80)

    print(f"✓ Operation logging enabled: {log_path}")


def log_operation(operation, details="", level="INFO"):
    """Log a high-level operation."""
    if op_logger:
        op_logger.log(getattr(logging, level), f"{operation} | {details}")


def load_server_config(config_file='xcpng.config'):
    """Load server list from config file.

    Config file format:
    [Servers]
    <ip>  <name>  <password>

    Returns list of dicts: [{'address': '...', 'name': '...', 'password': '...'}]
    """
    servers = []
    config_path = Path(config_file)

    if not config_path.exists():
        return servers

    try:
        with open(config_path, 'r') as f:
            in_servers = False
            for line in f:
                line = line.strip()
                if line == '[Servers]':
                    in_servers = True
                    continue
                if line.startswith('[') and in_servers:
                    break
                if in_servers and line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 3:
                        servers.append({
                            'address': parts[0],
                            'name': parts[1],
                            'password': parts[2]
                        })
    except Exception as e:
        print(f"⚠ Warning: Could not load server config: {e}")

    return servers


# ============================================================================
# SQLITE CACHE
# ============================================================================

import sqlite3
import time

DB_FILE = 'xcp_cache.db'

SCHEMA = """
-- Infrastructure metadata
CREATE TABLE IF NOT EXISTS sync_info (
    host_address TEXT PRIMARY KEY,
    host_name TEXT,
    last_sync TIMESTAMP,
    sync_duration_ms INTEGER
);

-- Core objects
CREATE TABLE IF NOT EXISTS hosts (
    uuid TEXT PRIMARY KEY,
    host_address TEXT,
    name_label TEXT,
    hostname TEXT,
    address TEXT,
    enabled INTEGER,
    software_version TEXT,
    cpu_info TEXT,
    memory_total INTEGER,
    memory_free INTEGER,
    metrics_live INTEGER,
    FOREIGN KEY (host_address) REFERENCES sync_info(host_address)
);

CREATE TABLE IF NOT EXISTS vms (
    uuid TEXT PRIMARY KEY,
    host_address TEXT,
    name_label TEXT,
    name_description TEXT,
    power_state TEXT,
    vcpus_max INTEGER,
    memory_static_max INTEGER,
    is_template INTEGER,
    is_control_domain INTEGER,
    is_snapshot INTEGER,
    snapshot_of_uuid TEXT,
    snapshot_time TEXT,
    resident_on_uuid TEXT,
    FOREIGN KEY (host_address) REFERENCES sync_info(host_address)
);

CREATE TABLE IF NOT EXISTS vdis (
    uuid TEXT PRIMARY KEY,
    host_address TEXT,
    name_label TEXT,
    virtual_size INTEGER,
    physical_utilisation INTEGER,
    sr_uuid TEXT,
    FOREIGN KEY (host_address) REFERENCES sync_info(host_address)
);

CREATE TABLE IF NOT EXISTS vbds (
    uuid TEXT PRIMARY KEY,
    host_address TEXT,
    vm_uuid TEXT,
    vdi_uuid TEXT,
    device TEXT,
    bootable INTEGER,
    type TEXT,
    FOREIGN KEY (host_address) REFERENCES sync_info(host_address)
);

CREATE TABLE IF NOT EXISTS networks (
    uuid TEXT PRIMARY KEY,
    host_address TEXT,
    name_label TEXT,
    bridge TEXT,
    FOREIGN KEY (host_address) REFERENCES sync_info(host_address)
);

CREATE TABLE IF NOT EXISTS vifs (
    uuid TEXT PRIMARY KEY,
    host_address TEXT,
    vm_uuid TEXT,
    network_uuid TEXT,
    device TEXT,
    mac TEXT,
    FOREIGN KEY (host_address) REFERENCES sync_info(host_address)
);

CREATE TABLE IF NOT EXISTS srs (
    uuid TEXT PRIMARY KEY,
    host_address TEXT,
    name_label TEXT,
    name_description TEXT,
    type TEXT,
    content_type TEXT,
    physical_size INTEGER,
    physical_utilisation INTEGER,
    shared INTEGER,
    device_config TEXT,
    sm_config TEXT,
    other_config TEXT,
    FOREIGN KEY (host_address) REFERENCES sync_info(host_address)
);

CREATE TABLE IF NOT EXISTS pbds (
    uuid TEXT PRIMARY KEY,
    host_address TEXT,
    sr_uuid TEXT,
    device_config TEXT,
    currently_attached INTEGER,
    FOREIGN KEY (host_address) REFERENCES sync_info(host_address)
);
"""


def init_database(db_path=DB_FILE):
    """Create database schema if not exists."""
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    # Migrate: add new SR columns if they don't exist
    cursor = conn.execute("PRAGMA table_info(srs)")
    columns = {row[1] for row in cursor.fetchall()}
    migrations = [
        ('name_description', 'TEXT'),
        ('content_type', 'TEXT'),
        ('device_config', 'TEXT'),
        ('sm_config', 'TEXT'),
        ('other_config', 'TEXT'),
    ]
    for col_name, col_type in migrations:
        if col_name not in columns:
            conn.execute(f"ALTER TABLE srs ADD COLUMN {col_name} {col_type}")
    conn.commit()
    conn.close()


def get_db(db_path=DB_FILE):
    """Get SQLite connection with row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def clear_host_data(db, host_address):
    """Delete all cached data for a host."""
    tables = ['hosts', 'vms', 'vdis', 'vbds', 'networks', 'vifs', 'srs', 'pbds']
    for table in tables:
        db.execute(f"DELETE FROM {table} WHERE host_address = ?", (host_address,))
    db.commit()


def sync_host_to_cache(api_conn, host_address, host_name):
    """Full sync from single XenAPI host to SQLite.

    Fetches all infrastructure data from the XenAPI and stores it in SQLite.
    Uses INSERT OR REPLACE to handle objects that may exist across multiple hosts.
    """
    start_time = time.time()
    db = get_db()

    try:
        print(f"  Syncing data from {host_name}...")

        # Clear existing data for this host
        clear_host_data(db, host_address)

        api = api_conn.api

        # Sync hosts
        print("    - Hosts...", end=" ", flush=True)
        host_records = api.host.get_all_records()
        for ref, rec in host_records.items():
            metrics_ref = rec.get('metrics', 'OpaqueRef:NULL')
            memory_total = memory_free = metrics_live = 0
            if metrics_ref != 'OpaqueRef:NULL':
                try:
                    metrics = api.host_metrics.get_record(metrics_ref)
                    memory_total = int(metrics.get('memory_total', 0))
                    memory_free = int(metrics.get('memory_free', 0))
                    metrics_live = 1 if metrics.get('live', False) else 0
                except:
                    pass
            db.execute("""
                INSERT OR REPLACE INTO hosts (uuid, host_address, name_label, hostname,
                    address, enabled, software_version, cpu_info, memory_total,
                    memory_free, metrics_live)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (rec['uuid'], host_address, rec.get('name_label', ''),
                  rec.get('hostname', ''), rec.get('address', ''),
                  1 if rec.get('enabled', False) else 0,
                  json.dumps(rec.get('software_version', {})),
                  json.dumps(rec.get('cpu_info', {})),
                  memory_total, memory_free, metrics_live))
        print(f"{len(host_records)} found")

        # Sync VMs
        print("    - VMs...", end=" ", flush=True)
        vm_records = api.VM.get_all_records()
        for ref, rec in vm_records.items():
            resident_on_uuid = None
            resident_ref = rec.get('resident_on', 'OpaqueRef:NULL')
            if resident_ref != 'OpaqueRef:NULL' and resident_ref in host_records:
                resident_on_uuid = host_records[resident_ref]['uuid']
            snapshot_of_uuid = None
            snapshot_of_ref = rec.get('snapshot_of', 'OpaqueRef:NULL')
            if snapshot_of_ref != 'OpaqueRef:NULL':
                try:
                    snapshot_of_uuid = api.VM.get_uuid(snapshot_of_ref)
                except:
                    pass
            snapshot_time = rec.get('snapshot_time')
            if hasattr(snapshot_time, 'value'):
                snapshot_time = str(snapshot_time.value)
            else:
                snapshot_time = str(snapshot_time) if snapshot_time else None
            db.execute("""
                INSERT OR REPLACE INTO vms (uuid, host_address, name_label, name_description,
                    power_state, vcpus_max, memory_static_max, is_template, is_control_domain,
                    is_snapshot, snapshot_of_uuid, snapshot_time, resident_on_uuid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (rec['uuid'], host_address, rec.get('name_label', ''),
                  rec.get('name_description', ''), rec.get('power_state', ''),
                  int(rec.get('VCPUs_max', 0)), int(rec.get('memory_static_max', 0)),
                  1 if rec.get('is_a_template', False) else 0,
                  1 if rec.get('is_control_domain', False) else 0,
                  1 if rec.get('is_a_snapshot', False) else 0,
                  snapshot_of_uuid, snapshot_time, resident_on_uuid))
        print(f"{len(vm_records)} found")

        # Sync SRs
        print("    - Storage repositories...", end=" ", flush=True)
        sr_records = api.SR.get_all_records()
        for ref, rec in sr_records.items():
            db.execute("""
                INSERT OR REPLACE INTO srs (uuid, host_address, name_label, name_description,
                    type, content_type, physical_size, physical_utilisation, shared,
                    device_config, sm_config, other_config)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (rec['uuid'], host_address, rec.get('name_label', ''),
                  rec.get('name_description', ''), rec.get('type', ''),
                  rec.get('content_type', ''), int(rec.get('physical_size', 0)),
                  int(rec.get('physical_utilisation', 0)),
                  1 if rec.get('shared', False) else 0,
                  json.dumps(rec.get('device_config', {})),
                  json.dumps(rec.get('sm_config', {})),
                  json.dumps(rec.get('other_config', {}))))
        print(f"{len(sr_records)} found")

        # Sync PBDs (Physical Block Devices - contain mount info for SRs)
        print("    - Physical block devices...", end=" ", flush=True)
        pbd_records = api.PBD.get_all_records()
        for ref, rec in pbd_records.items():
            sr_uuid = None
            sr_ref = rec.get('SR', 'OpaqueRef:NULL')
            if sr_ref != 'OpaqueRef:NULL' and sr_ref in sr_records:
                sr_uuid = sr_records[sr_ref]['uuid']
            db.execute("""
                INSERT OR REPLACE INTO pbds (uuid, host_address, sr_uuid,
                    device_config, currently_attached)
                VALUES (?, ?, ?, ?, ?)
            """, (rec['uuid'], host_address, sr_uuid,
                  json.dumps(rec.get('device_config', {})),
                  1 if rec.get('currently_attached', False) else 0))
        print(f"{len(pbd_records)} found")

        # Sync VDIs
        print("    - Virtual disks...", end=" ", flush=True)
        vdi_records = api.VDI.get_all_records()
        for ref, rec in vdi_records.items():
            sr_uuid = None
            sr_ref = rec.get('SR', 'OpaqueRef:NULL')
            if sr_ref != 'OpaqueRef:NULL' and sr_ref in sr_records:
                sr_uuid = sr_records[sr_ref]['uuid']
            db.execute("""
                INSERT OR REPLACE INTO vdis (uuid, host_address, name_label,
                    virtual_size, physical_utilisation, sr_uuid)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (rec['uuid'], host_address, rec.get('name_label', ''),
                  int(rec.get('virtual_size', 0)),
                  int(rec.get('physical_utilisation', 0)), sr_uuid))
        print(f"{len(vdi_records)} found")

        # Sync VBDs
        print("    - Virtual block devices...", end=" ", flush=True)
        vbd_records = api.VBD.get_all_records()
        for ref, rec in vbd_records.items():
            vm_uuid = None
            vm_ref = rec.get('VM', 'OpaqueRef:NULL')
            if vm_ref != 'OpaqueRef:NULL' and vm_ref in vm_records:
                vm_uuid = vm_records[vm_ref]['uuid']
            vdi_uuid = None
            vdi_ref = rec.get('VDI', 'OpaqueRef:NULL')
            if vdi_ref != 'OpaqueRef:NULL' and vdi_ref in vdi_records:
                vdi_uuid = vdi_records[vdi_ref]['uuid']
            db.execute("""
                INSERT OR REPLACE INTO vbds (uuid, host_address, vm_uuid, vdi_uuid,
                    device, bootable, type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (rec['uuid'], host_address, vm_uuid, vdi_uuid,
                  rec.get('device', ''), 1 if rec.get('bootable', False) else 0,
                  rec.get('type', '')))
        print(f"{len(vbd_records)} found")

        # Sync Networks
        print("    - Networks...", end=" ", flush=True)
        net_records = api.network.get_all_records()
        for ref, rec in net_records.items():
            db.execute("""
                INSERT OR REPLACE INTO networks (uuid, host_address, name_label, bridge)
                VALUES (?, ?, ?, ?)
            """, (rec['uuid'], host_address, rec.get('name_label', ''),
                  rec.get('bridge', '')))
        print(f"{len(net_records)} found")

        # Sync VIFs
        print("    - Virtual interfaces...", end=" ", flush=True)
        vif_records = api.VIF.get_all_records()
        for ref, rec in vif_records.items():
            vm_uuid = None
            vm_ref = rec.get('VM', 'OpaqueRef:NULL')
            if vm_ref != 'OpaqueRef:NULL' and vm_ref in vm_records:
                vm_uuid = vm_records[vm_ref]['uuid']
            network_uuid = None
            net_ref = rec.get('network', 'OpaqueRef:NULL')
            if net_ref != 'OpaqueRef:NULL' and net_ref in net_records:
                network_uuid = net_records[net_ref]['uuid']
            db.execute("""
                INSERT OR REPLACE INTO vifs (uuid, host_address, vm_uuid, network_uuid,
                    device, mac)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (rec['uuid'], host_address, vm_uuid, network_uuid,
                  rec.get('device', ''), rec.get('MAC', '')))
        print(f"{len(vif_records)} found")

        # Update sync info
        duration_ms = int((time.time() - start_time) * 1000)
        db.execute("""
            INSERT OR REPLACE INTO sync_info (host_address, host_name, last_sync, sync_duration_ms)
            VALUES (?, ?, datetime('now'), ?)
        """, (host_address, host_name, duration_ms))

        db.commit()
        print(f"  Sync complete ({duration_ms}ms)")
        log_operation("CACHE_SYNC", f"Synced {host_name} ({host_address}) in {duration_ms}ms")

    finally:
        db.close()


# ============================================================================
# XENAPI CONNECTION
# ============================================================================

class XCPConnection:
    """Manages connection to XCP-ng host."""

    def __init__(self, host, username, password, host_name=None):
        self.host = host
        self.host_name = host_name or host
        self.username = username
        self.session = None
        self._connect(password)

    def _connect(self, password):
        """Establish connection to XCP-ng host."""
        # Disable SSL verification for self-signed certs (common in home labs)
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        url = f"https://{self.host}"
        self.session = XenAPI.Session(url, ignore_ssl=True)
        self.session.login_with_password(self.username, password, "1.0", "xcp-admin-tool")
        log_operation("CONNECT", f"Connected to {self.host}")

    @property
    def api(self):
        """Return the XenAPI interface."""
        return self.session.xenapi

    def sync_to_cache(self):
        """Sync this host's data to SQLite cache."""
        sync_host_to_cache(self, self.host, self.host_name)

    def close(self):
        """Close the session."""
        if self.session:
            try:
                self.session.xenapi.session.logout()
                log_operation("DISCONNECT", f"Disconnected from {self.host}")
            except:
                pass


def check_host_reachable(address, port=443, timeout=2):
    """Check if a host is reachable on given port."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((address, port))
        sock.close()
        return result == 0
    except:
        return False


def check_servers_status(servers, timeout=2):
    """Check reachability of all servers in parallel. Returns dict of address -> bool."""
    status = {}
    with ThreadPoolExecutor(max_workers=len(servers)) as executor:
        futures = {executor.submit(check_host_reachable, s['address'], 443, timeout): s['address']
                   for s in servers}
        for future in as_completed(futures):
            address = futures[future]
            try:
                status[address] = future.result()
            except:
                status[address] = False
    return status


def select_server(servers):
    """Interactive server selection from config.

    Returns selected server dict or None to exit.
    """
    if not servers:
        return None

    status = None  # Cache status check

    while True:
        warning_level = _session.get('warning_level', 'full')

        print(f"\n{'─' * 60}")
        if warning_level == 'none':
            print("  Select Server  [NO CONFIRMATIONS]")
        else:
            print("  Select Server")
        print('─' * 60)

        # Check server availability in parallel (only on first display)
        if status is None:
            print("  Checking server status...", end='', flush=True)
            status = check_servers_status(servers)
            print("\r" + " " * 35 + "\r", end='')  # Clear the checking message

        print(f"  {'#':<4} {'Name':<15} {'Address':<18} {'Status'}")
        print('  ' + '-' * 50)

        for i, s in enumerate(servers, 1):
            online = status.get(s['address'], False)
            status_str = "online" if online else "OFFLINE"
            print(f"  {i:<4} {s['name']:<15} {s['address']:<18} {status_str}")

        print(f"  [o] Options [warning: {warning_level}]")
        print(f"  [x] Exit")
        print('─' * 60)
        choice = input("Select: ").strip()

        if choice.lower() == 'x':
            return None
        elif choice.lower() == 'o':
            options_menu()
            continue

        try:
            idx = int(choice) - 1
            if 0 <= idx < len(servers):
                return servers[idx]
        except ValueError:
            # Try matching by name
            for server in servers:
                if choice.lower() in server['name'].lower():
                    return server

        print(f"Invalid selection: {choice}")


# ============================================================================
# DISPLAY HELPERS
# ============================================================================

def format_size(bytes_val):
    """Format bytes as human-readable size."""
    try:
        bytes_val = int(bytes_val)
    except (ValueError, TypeError):
        return "Unknown"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} PB"


def format_power_state(state):
    """Format power state with indicator."""
    indicators = {
        'Running': '🟢',
        'Halted': '⚫',
        'Suspended': '🟡',
        'Paused': '🟠'
    }
    return f"{indicators.get(state, '❓')} {state}"


def display_menu(title, options, show_quit=True, show_exit=True):
    """Display a menu and get user choice.

    Navigation convention:
        [q] Quit - go back/up one level
        [x] Exit - exit program completely
    """
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print('─' * 60)

    for key, label in options:
        print(f"  [{key}] {label}")

    if show_quit:
        print(f"  [q] Quit")
    if show_exit:
        print(f"  [x] Exit")

    print('─' * 60)
    return input("Select: ").strip().lower()


# ============================================================================
# VM OPERATIONS
# ============================================================================

def list_vms(conn, include_templates=False, include_control=False, include_snapshots=False):
    """
    Get list of VMs with basic info.
    Returns list of dicts with vm_ref, name, power_state, etc.
    """
    vms = []
    all_vms = conn.api.VM.get_all_records()

    for ref, rec in all_vms.items():
        # Skip templates unless requested
        if rec['is_a_template'] and not include_templates:
            continue
        # Skip control domains (dom0) unless requested
        if rec['is_control_domain'] and not include_control:
            continue
        # Skip snapshots unless requested
        if rec.get('is_a_snapshot', False) and not include_snapshots:
            continue

        vms.append({
            'ref': ref,
            'uuid': rec['uuid'],
            'name': rec['name_label'],
            'power_state': rec['power_state'],
            'vcpus': rec['VCPUs_max'],
            'memory': rec['memory_static_max'],
            'description': rec['name_description'],
            'snapshots': len(rec.get('snapshots', []))
        })

    # Sort by name
    vms.sort(key=lambda x: x['name'].lower())
    return vms


def display_vm_list(vms):
    """Display formatted VM list."""
    print(f"\n{'#':<4} {'Name':<25} {'State':<15} {'vCPUs':<6} {'Mem':<8} {'Snaps':<5}")
    print('─' * 72)

    for i, vm in enumerate(vms, 1):
        state = format_power_state(vm['power_state'])
        mem = format_size(vm['memory'])
        snaps = vm.get('snapshots', 0)
        snap_str = str(snaps) if snaps > 0 else '-'
        print(f"{i:<4} {vm['name']:<25} {state:<15} {vm['vcpus']:<6} {mem:<8} {snap_str:<5}")


def select_vm(conn):
    """Interactive VM selection.

    Returns:
        (vm_ref, vm_info) - selected VM
        (None, None) - user chose quit (go back)
        ('exit', None) - user chose exit program
    """
    vms = list_vms(conn)

    if not vms:
        print("\nNo VMs found on this host.")
        return None, None

    print(f"\n{'─' * 60}")
    print("  Select VM")
    print('─' * 60)
    display_vm_list(vms)

    print(f"\n  [q] Quit    [x] Exit")
    print('─' * 60)
    choice = input("Select VM: ").strip()

    if choice.lower() == 'q':
        return None, None
    if choice.lower() == 'x':
        return 'exit', None

    # Try as number first
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(vms):
            return vms[idx]['ref'], vms[idx]
    except ValueError:
        pass

    # Try as name (partial match)
    for vm in vms:
        if choice.lower() in vm['name'].lower():
            return vm['ref'], vm

    print(f"\nVM not found: {choice}")
    return None, None


def vm_start(conn, vm_ref, vm_info, dry_run=False):
    """Start a VM."""
    if vm_info['power_state'] == 'Running':
        print(f"\n⚠ VM '{vm_info['name']}' is already running.")
        return False
    
    if dry_run:
        print(f"\n[DRY RUN] Would start VM: {vm_info['name']}")
        return True
    
    print(f"\nStarting VM: {vm_info['name']}...")
    conn.api.VM.start(vm_ref, False, False)  # start_paused=False, force=False
    log_operation("VM_START", f"Started VM: {vm_info['name']} ({vm_info['uuid']})")
    print(f"✓ VM '{vm_info['name']}' started.")
    return True


def vm_shutdown(conn, vm_ref, vm_info, force=False, dry_run=False):
    """Shutdown a VM (clean or forced)."""
    if vm_info['power_state'] != 'Running':
        print(f"\n⚠ VM '{vm_info['name']}' is not running (state: {vm_info['power_state']}).")
        return False
    
    action = "force shutdown" if force else "clean shutdown"
    
    if dry_run:
        print(f"\n[DRY RUN] Would {action} VM: {vm_info['name']}")
        return True
    
    print(f"\nPerforming {action} on VM: {vm_info['name']}...")
    
    if force:
        conn.api.VM.hard_shutdown(vm_ref)
    else:
        conn.api.VM.clean_shutdown(vm_ref)
    
    log_operation("VM_SHUTDOWN", f"{action.title()}: {vm_info['name']} ({vm_info['uuid']})")
    print(f"✓ VM '{vm_info['name']}' shut down.")
    return True


def vm_reboot(conn, vm_ref, vm_info, force=False, dry_run=False):
    """Reboot a VM (clean or forced)."""
    if vm_info['power_state'] != 'Running':
        print(f"\n⚠ VM '{vm_info['name']}' is not running.")
        return False
    
    action = "force reboot" if force else "clean reboot"
    
    if dry_run:
        print(f"\n[DRY RUN] Would {action} VM: {vm_info['name']}")
        return True
    
    print(f"\nPerforming {action} on VM: {vm_info['name']}...")
    
    if force:
        conn.api.VM.hard_reboot(vm_ref)
    else:
        conn.api.VM.clean_reboot(vm_ref)
    
    log_operation("VM_REBOOT", f"{action.title()}: {vm_info['name']} ({vm_info['uuid']})")
    print(f"✓ VM '{vm_info['name']}' rebooting.")
    return True


def vm_snapshot(conn, vm_ref, vm_info, dry_run=False):
    """Create a snapshot of a VM."""
    # Generate snapshot name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    default_name = f"{vm_info['name']}-snap-{timestamp}"
    
    print(f"\nSnapshot name [{default_name}]:")
    snap_name = input("Name: ").strip() or default_name
    
    if dry_run:
        print(f"\n[DRY RUN] Would create snapshot '{snap_name}' of VM: {vm_info['name']}")
        return True
    
    print(f"\nCreating snapshot '{snap_name}'...")
    snap_ref = conn.api.VM.snapshot(vm_ref, snap_name)
    snap_uuid = conn.api.VM.get_uuid(snap_ref)
    
    log_operation("VM_SNAPSHOT", f"Created snapshot '{snap_name}' ({snap_uuid}) of {vm_info['name']}")
    print(f"✓ Snapshot created: {snap_name}")
    print(f"  UUID: {snap_uuid}")
    return True


def list_snapshots(conn, vm_ref, vm_info):
    """List snapshots of a VM."""
    snapshots = conn.api.VM.get_snapshots(vm_ref)
    
    if not snapshots:
        print(f"\n  No snapshots found for '{vm_info['name']}'")
        return []
    
    snap_list = []
    for snap_ref in snapshots:
        rec = conn.api.VM.get_record(snap_ref)
        snap_list.append({
            'ref': snap_ref,
            'uuid': rec['uuid'],
            'name': rec['name_label'],
            'snapshot_time': rec['snapshot_time'].value if hasattr(rec['snapshot_time'], 'value') else str(rec['snapshot_time'])
        })
    
    # Sort by time
    snap_list.sort(key=lambda x: x['snapshot_time'], reverse=True)
    
    print(f"\n  Snapshots for '{vm_info['name']}':")
    print(f"  {'#':<4} {'Name':<35} {'Created':<20}")
    print('  ' + '─' * 65)
    
    for i, snap in enumerate(snap_list, 1):
        print(f"  {i:<4} {snap['name']:<35} {snap['snapshot_time']:<20}")
    
    return snap_list


def vm_revert_snapshot(conn, vm_ref, vm_info, dry_run=False):
    """Revert VM to a snapshot."""
    snapshots = list_snapshots(conn, vm_ref, vm_info)
    
    if not snapshots:
        return False
    
    print("\nEnter snapshot number to revert to (or 'c' to cancel):")
    choice = input("Select: ").strip()
    
    if choice.lower() == 'c':
        print("Cancelled.")
        return False
    
    try:
        idx = int(choice) - 1
        if not (0 <= idx < len(snapshots)):
            print("❌ Invalid selection.")
            return False
    except ValueError:
        print("❌ Invalid selection.")
        return False
    
    snap = snapshots[idx]
    
    if dry_run:
        print(f"\n[DRY RUN] Would revert '{vm_info['name']}' to snapshot '{snap['name']}'")
        return True

    # Confirm (unless warning_level is 'none')
    warning_level = _session.get('warning_level', 'full')
    if warning_level != 'none':
        print(f"\n⚠ WARNING: This will revert '{vm_info['name']}' to snapshot '{snap['name']}'")
        print("  All changes since the snapshot will be lost!")
        confirm = input("Type 'yes' to confirm: ").strip()
        if confirm.lower() != 'yes':
            print("Cancelled.")
            return False

    print(f"\nReverting to snapshot '{snap['name']}'...")
    conn.api.VM.revert(snap['ref'])
    
    log_operation("VM_REVERT", f"Reverted {vm_info['name']} to snapshot {snap['name']} ({snap['uuid']})")
    print(f"✓ VM reverted to snapshot '{snap['name']}'")
    print("  Note: VM is now in halted state. Start it when ready.")
    return True


def vm_delete_snapshot(conn, vm_ref, vm_info, dry_run=False):
    """Delete a snapshot."""
    snapshots = list_snapshots(conn, vm_ref, vm_info)
    
    if not snapshots:
        return False
    
    print("\nEnter snapshot number to delete (or 'c' to cancel):")
    choice = input("Select: ").strip()
    
    if choice.lower() == 'c':
        print("Cancelled.")
        return False
    
    try:
        idx = int(choice) - 1
        if not (0 <= idx < len(snapshots)):
            print("❌ Invalid selection.")
            return False
    except ValueError:
        print("❌ Invalid selection.")
        return False
    
    snap = snapshots[idx]
    
    if dry_run:
        print(f"\n[DRY RUN] Would delete snapshot '{snap['name']}'")
        return True

    # Confirm (unless warning_level is 'none')
    warning_level = _session.get('warning_level', 'full')
    if warning_level != 'none':
        print(f"\n⚠ Delete snapshot '{snap['name']}'?")
        confirm = input("Type 'yes' to confirm: ").strip()
        if confirm.lower() != 'yes':
            print("Cancelled.")
            return False

    print(f"\nDeleting snapshot '{snap['name']}'...")
    conn.api.VM.destroy(snap['ref'])
    
    log_operation("SNAPSHOT_DELETE", f"Deleted snapshot {snap['name']} ({snap['uuid']})")
    print(f"✓ Snapshot deleted.")
    return True


# ============================================================================
# DISK OPERATIONS
# ============================================================================

def list_vm_disks(conn, vm_ref, vm_info):
    """List disks attached to a VM."""
    vbds = conn.api.VM.get_VBDs(vm_ref)
    
    disks = []
    for vbd_ref in vbds:
        vbd_rec = conn.api.VBD.get_record(vbd_ref)
        
        # Skip CD drives
        if vbd_rec['type'] == 'CD':
            continue
        
        vdi_ref = vbd_rec['VDI']
        if vdi_ref == 'OpaqueRef:NULL':
            continue
        
        vdi_rec = conn.api.VDI.get_record(vdi_ref)
        
        disks.append({
            'vbd_ref': vbd_ref,
            'vdi_ref': vdi_ref,
            'vdi_uuid': vdi_rec['uuid'],
            'name': vdi_rec['name_label'],
            'virtual_size': vdi_rec['virtual_size'],
            'physical_size': vdi_rec['physical_utilisation'],
            'device': vbd_rec['device'],
            'bootable': vbd_rec['bootable']
        })
    
    return disks


def display_vm_disks(disks):
    """Display disk information."""
    print(f"\n  {'#':<4} {'Device':<8} {'Name':<25} {'Size':<12} {'Used':<12} {'Boot'}")
    print('  ' + '─' * 75)
    
    for i, disk in enumerate(disks, 1):
        size = format_size(disk['virtual_size'])
        used = format_size(disk['physical_size'])
        boot = '✓' if disk['bootable'] else ''
        print(f"  {i:<4} {disk['device']:<8} {disk['name']:<25} {size:<12} {used:<12} {boot}")


def resize_vdi(conn, vm_ref, vm_info, dry_run=False):
    """Resize a VDI (expand only)."""
    disks = list_vm_disks(conn, vm_ref, vm_info)
    
    if not disks:
        print(f"\n❌ No disks found for '{vm_info['name']}'")
        return False
    
    print(f"\n  Disks for '{vm_info['name']}':")
    display_vm_disks(disks)
    
    print("\nEnter disk number to resize (or 'c' to cancel):")
    choice = input("Select: ").strip()
    
    if choice.lower() == 'c':
        return False
    
    try:
        idx = int(choice) - 1
        if not (0 <= idx < len(disks)):
            print("❌ Invalid selection.")
            return False
    except ValueError:
        print("❌ Invalid selection.")
        return False
    
    disk = disks[idx]
    current_size = int(disk['virtual_size'])
    
    print(f"\n  Current size: {format_size(current_size)} ({current_size} bytes)")
    print("\n  Common sizes:")
    print("    60GB  = 64426606592")
    print("    80GB  = 85899345920")
    print("    100GB = 107374182400")
    print("    120GB = 128849018880")
    print("    150GB = 161061273600")
    print("    200GB = 214748364800")
    
    print("\nEnter new size in bytes (or GB with 'G' suffix, e.g., '100G'):")
    size_input = input("New size: ").strip()
    
    if not size_input:
        print("Cancelled.")
        return False
    
    # Parse size
    try:
        if size_input.upper().endswith('G'):
            new_size = int(float(size_input[:-1]) * 1024 * 1024 * 1024)
        elif size_input.upper().endswith('T'):
            new_size = int(float(size_input[:-1]) * 1024 * 1024 * 1024 * 1024)
        else:
            new_size = int(size_input)
    except ValueError:
        print("❌ Invalid size format.")
        return False
    
    if new_size <= current_size:
        print(f"❌ New size must be larger than current size ({format_size(current_size)})")
        return False
    
    if dry_run:
        print(f"\n[DRY RUN] Would resize disk from {format_size(current_size)} to {format_size(new_size)}")
        return True

    # Warn about VM state (unless warning_level is 'none')
    warning_level = _session.get('warning_level', 'full')
    if warning_level != 'none' and vm_info['power_state'] == 'Running':
        print("\n⚠ WARNING: VM is running. It's safer to shut it down first.")
        confirm = input("Continue anyway? (yes/no): ").strip()
        if confirm.lower() != 'yes':
            print("Cancelled.")
            return False

    print(f"\nResizing disk from {format_size(current_size)} to {format_size(new_size)}...")
    conn.api.VDI.resize(disk['vdi_ref'], str(new_size))
    
    log_operation("VDI_RESIZE", f"Resized VDI {disk['vdi_uuid']} from {format_size(current_size)} to {format_size(new_size)}")
    print(f"✓ Disk resized to {format_size(new_size)}")
    print("\n  Next steps inside the VM:")
    print("    sudo growpart /dev/xvda 1    # Expand partition")
    print("    sudo resize2fs /dev/xvda1    # Expand filesystem")
    return True


# ============================================================================
# NETWORK OPERATIONS
# ============================================================================

def list_vm_networks(conn, vm_ref, vm_info):
    """List network interfaces attached to a VM."""
    vifs = conn.api.VM.get_VIFs(vm_ref)
    
    interfaces = []
    for vif_ref in vifs:
        vif_rec = conn.api.VIF.get_record(vif_ref)
        net_ref = vif_rec['network']
        net_rec = conn.api.network.get_record(net_ref)
        
        interfaces.append({
            'vif_ref': vif_ref,
            'device': vif_rec['device'],
            'mac': vif_rec['MAC'],
            'network_name': net_rec['name_label'],
            'network_uuid': net_rec['uuid'],
            'network_ref': net_ref
        })
    
    interfaces.sort(key=lambda x: x['device'])
    return interfaces


def display_vm_networks(interfaces):
    """Display network interface information."""
    if not interfaces:
        print("  No network interfaces attached.")
        return
    
    print(f"\n  {'#':<4} {'Device':<8} {'MAC':<20} {'Network'}")
    print('  ' + '─' * 60)
    
    for i, iface in enumerate(interfaces, 1):
        print(f"  {i:<4} {iface['device']:<8} {iface['mac']:<20} {iface['network_name']}")


def list_available_networks(conn):
    """List all available networks."""
    networks = conn.api.network.get_all_records()
    
    net_list = []
    for ref, rec in networks.items():
        net_list.append({
            'ref': ref,
            'uuid': rec['uuid'],
            'name': rec['name_label'],
            'bridge': rec['bridge']
        })
    
    net_list.sort(key=lambda x: x['name'].lower())
    return net_list


def add_vif(conn, vm_ref, vm_info, dry_run=False):
    """Add a network interface to a VM."""
    print(f"\n  Current interfaces for '{vm_info['name']}':")
    current_vifs = list_vm_networks(conn, vm_ref, vm_info)
    display_vm_networks(current_vifs)
    
    # Determine next device number
    used_devices = {int(v['device']) for v in current_vifs}
    next_device = 0
    while next_device in used_devices:
        next_device += 1
    
    # List available networks
    networks = list_available_networks(conn)
    
    print(f"\n  Available networks:")
    print(f"  {'#':<4} {'Name':<30} {'Bridge'}")
    print('  ' + '─' * 50)
    
    for i, net in enumerate(networks, 1):
        print(f"  {i:<4} {net['name']:<30} {net['bridge']}")
    
    print("\nEnter network number to attach (or 'c' to cancel):")
    choice = input("Select: ").strip()
    
    if choice.lower() == 'c':
        return False
    
    try:
        idx = int(choice) - 1
        if not (0 <= idx < len(networks)):
            print("❌ Invalid selection.")
            return False
    except ValueError:
        print("❌ Invalid selection.")
        return False
    
    net = networks[idx]
    
    if dry_run:
        print(f"\n[DRY RUN] Would add VIF (device {next_device}) connected to '{net['name']}'")
        return True
    
    print(f"\nAdding interface to network '{net['name']}'...")
    
    # Create VIF
    vif_record = {
        'VM': vm_ref,
        'network': net['ref'],
        'device': str(next_device),
        'MAC': '',  # Auto-generate
        'MTU': '1500',
        'other_config': {},
        'qos_algorithm_type': '',
        'qos_algorithm_params': {}
    }
    
    vif_ref = conn.api.VIF.create(vif_record)
    vif_uuid = conn.api.VIF.get_uuid(vif_ref)
    
    log_operation("VIF_CREATE", f"Created VIF {vif_uuid} on {vm_info['name']} -> {net['name']}")
    print(f"✓ Network interface added (device {next_device})")
    print(f"  Network: {net['name']}")
    
    if vm_info['power_state'] == 'Running':
        print("\n  Note: You may need to configure the interface inside the VM.")
    
    return True


def remove_vif(conn, vm_ref, vm_info, dry_run=False):
    """Remove a network interface from a VM."""
    interfaces = list_vm_networks(conn, vm_ref, vm_info)
    
    if not interfaces:
        print(f"\n❌ No network interfaces on '{vm_info['name']}'")
        return False
    
    print(f"\n  Network interfaces for '{vm_info['name']}':")
    display_vm_networks(interfaces)
    
    print("\nEnter interface number to remove (or 'c' to cancel):")
    choice = input("Select: ").strip()
    
    if choice.lower() == 'c':
        return False
    
    try:
        idx = int(choice) - 1
        if not (0 <= idx < len(interfaces)):
            print("❌ Invalid selection.")
            return False
    except ValueError:
        print("❌ Invalid selection.")
        return False
    
    iface = interfaces[idx]
    
    if dry_run:
        print(f"\n[DRY RUN] Would remove VIF device {iface['device']} ({iface['network_name']})")
        return True

    # Confirm (unless warning_level is 'none')
    warning_level = _session.get('warning_level', 'full')
    if warning_level != 'none':
        print(f"\n⚠ Remove interface {iface['device']} ({iface['network_name']})?")
        confirm = input("Type 'yes' to confirm: ").strip()
        if confirm.lower() != 'yes':
            print("Cancelled.")
            return False

    print(f"\nRemoving interface...")
    conn.api.VIF.destroy(iface['vif_ref'])
    
    log_operation("VIF_REMOVE", f"Removed VIF device {iface['device']} from {vm_info['name']}")
    print(f"✓ Interface removed.")
    return True


# ============================================================================
# HOST OVERVIEW
# ============================================================================

def get_host_overview(host_address):
    """Get host summary from cache.

    Returns dict with host info, VM stats, and storage stats.
    """
    db = get_db()

    host = db.execute(
        "SELECT * FROM hosts WHERE host_address = ?", (host_address,)
    ).fetchone()

    sync_info = db.execute(
        "SELECT * FROM sync_info WHERE host_address = ?", (host_address,)
    ).fetchone()

    vm_stats = db.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN power_state='Running' THEN 1 ELSE 0 END) as running,
            SUM(CASE WHEN power_state='Halted' THEN 1 ELSE 0 END) as halted,
            SUM(memory_static_max) as memory_allocated,
            SUM(vcpus_max) as vcpus_allocated
        FROM vms
        WHERE host_address = ? AND is_template=0 AND is_control_domain=0 AND is_snapshot=0
    """, (host_address,)).fetchone()

    template_count = db.execute("""
        SELECT COUNT(*) as count FROM vms
        WHERE host_address = ? AND is_template=1
    """, (host_address,)).fetchone()

    snapshot_count = db.execute("""
        SELECT COUNT(*) as count FROM vms
        WHERE host_address = ? AND is_snapshot=1
    """, (host_address,)).fetchone()

    sr_stats = db.execute("""
        SELECT COUNT(*) as count, SUM(physical_size) as total, SUM(physical_utilisation) as used
        FROM srs WHERE host_address = ?
    """, (host_address,)).fetchone()

    network_count = db.execute(
        "SELECT COUNT(*) as count FROM networks WHERE host_address = ?", (host_address,)
    ).fetchone()

    db.close()

    return {
        'host': dict(host) if host else None,
        'sync_info': dict(sync_info) if sync_info else None,
        'vms': dict(vm_stats) if vm_stats else None,
        'templates': template_count['count'] if template_count else 0,
        'snapshots': snapshot_count['count'] if snapshot_count else 0,
        'storage': dict(sr_stats) if sr_stats else None,
        'networks': network_count['count'] if network_count else 0
    }


def display_host_overview(conn):
    """Display compact host information screen."""
    data = get_host_overview(conn.host)

    if not data['host']:
        print("\n  No cached data for this host. Run refresh first.")
        return

    host = data['host']
    vms = data['vms']
    storage = data['storage']

    # Parse JSON fields
    sw = json.loads(host.get('software_version', '{}'))
    cpu = json.loads(host.get('cpu_info', '{}'))

    # Header
    print(f"\n{'=' * 64}")
    print(f"  HOST: {host['name_label']} ({host['address']})")
    print(f"{'=' * 64}")

    # Software
    print(f"  XCP-ng {sw.get('product_version', '?')} | Xen {sw.get('xen', '?')} | Linux {sw.get('linux', '?')}")

    # CPU - truncate model name if too long
    cpu_model = cpu.get('modelname', 'Unknown')
    if len(cpu_model) > 45:
        cpu_model = cpu_model[:42] + '...'
    print(f"  CPU:      {cpu_model} ({cpu.get('cpu_count', '?')} cores)")

    # Memory
    mem_total = format_size(host['memory_total'])
    mem_free = format_size(host['memory_free'])
    mem_alloc = format_size(vms['memory_allocated']) if vms and vms['memory_allocated'] else '0 B'
    print(f"  Memory:   {mem_free} free / {mem_total} total ({mem_alloc} allocated)")

    # VMs
    if vms:
        running = vms['running'] or 0
        halted = vms['halted'] or 0
        total = vms['total'] or 0
        vcpus = vms['vcpus_allocated'] or 0
        print(f"  VMs:      {running} running, {halted} halted ({total} total, {vcpus} vCPUs)")
        print(f"            {data['templates']} templates, {data['snapshots']} snapshots")

    # Storage
    if storage and storage['count']:
        sr_total = format_size(storage['total']) if storage['total'] else '0 B'
        sr_used = format_size(storage['used']) if storage['used'] else '0 B'
        print(f"  Storage:  {sr_used} / {sr_total} across {storage['count']} repositories")

    # Networks
    print(f"  Networks: {data['networks']}")

    # Cache info
    if data['sync_info']:
        print(f"  Cached:   {data['sync_info']['last_sync']} ({data['sync_info']['sync_duration_ms']}ms)")


# ============================================================================
# STORAGE OVERVIEW
# ============================================================================

def get_storage_repositories(host_address):
    """Get all SRs from cache for a host."""
    db = get_db()
    rows = db.execute("""
        SELECT * FROM srs WHERE host_address = ? ORDER BY name_label
    """, (host_address,)).fetchall()
    db.close()
    return [dict(row) for row in rows]


def display_storage_list(srs):
    """Display compact SR list."""
    print(f"\n  {'#':<3} {'Name':<25} {'Type':<10} {'Used':<10} {'Total':<10} {'Shared'}")
    print('  ' + '-' * 70)

    for i, sr in enumerate(srs, 1):
        used = format_size(sr['physical_utilisation'])
        total = format_size(sr['physical_size']) if sr['physical_size'] else '-'
        shared = 'Yes' if sr['shared'] else 'No'
        name = sr['name_label'][:24] if len(sr['name_label']) > 24 else sr['name_label']
        print(f"  {i:<3} {name:<25} {sr['type']:<10} {used:<10} {total:<10} {shared}")


def get_sr_pbd_config(sr_uuid, host_address):
    """Get PBD device_config for an SR (contains NFS mount info, etc.)."""
    db = get_db()
    pbd = db.execute("""
        SELECT device_config, currently_attached FROM pbds
        WHERE sr_uuid = ? AND host_address = ?
    """, (sr_uuid, host_address)).fetchone()
    db.close()
    if pbd:
        return json.loads(pbd['device_config'] or '{}'), pbd['currently_attached']
    return {}, False


def display_sr_details(sr):
    """Display detailed SR information."""
    print(f"\n{'=' * 64}")
    print(f"  SR: {sr['name_label']}")
    print(f"{'=' * 64}")

    print(f"  UUID:        {sr['uuid']}")
    print(f"  Type:        {sr['type']}")
    print(f"  Description: {sr['name_description'] or '(none)'}")
    print(f"  Content:     {sr['content_type'] or '(none)'}")
    print(f"  Shared:      {'Yes' if sr['shared'] else 'No'}")

    used = format_size(sr['physical_utilisation'])
    total = format_size(sr['physical_size']) if sr['physical_size'] else 'Unknown'
    print(f"  Used:        {used}")
    print(f"  Total:       {total}")

    # Get PBD device_config (contains actual mount info for NFS, iSCSI, etc.)
    pbd_config, attached = get_sr_pbd_config(sr['uuid'], sr['host_address'])
    if pbd_config:
        print(f"\n  Connection ({('attached' if attached else 'detached')}):")
        for key, val in pbd_config.items():
            print(f"    {key}: {val}")

    # SR device config (usually empty for NFS/ISO, info is in PBD)
    device_config = json.loads(sr.get('device_config', '{}') or '{}')
    if device_config:
        print(f"\n  SR Device Config:")
        for key, val in device_config.items():
            print(f"    {key}: {val}")

    # SM config
    sm_config = json.loads(sr.get('sm_config', '{}') or '{}')
    if sm_config:
        print(f"\n  SM Config:")
        for key, val in sm_config.items():
            print(f"    {key}: {val}")


def sr_details_workflow(sr, conn):
    """SR details view with edit option."""
    # Types that support connection editing
    editable_types = ['nfs', 'iso', 'smb', 'cifs', 'iscsi', 'lvmoiscsi']

    while True:
        display_sr_details(sr)

        # Show options
        pbd_config, attached = get_sr_pbd_config(sr['uuid'], sr['host_address'])
        if sr['type'] in editable_types and pbd_config:
            print(f"\n  [e] Edit connection    [q] Back")
        else:
            print(f"\n  [q] Back")

        choice = input("\nSelect: ").strip().lower()

        if choice == 'q' or choice == '':
            return
        elif choice == 'e' and sr['type'] in editable_types and pbd_config:
            edit_sr_connection(sr, conn)
            # Refresh SR data after edit
            return


def edit_sr_connection(sr, conn):
    """Show commands to edit an SR's connection parameters (NFS path, etc.)."""
    pbd_config, attached = get_sr_pbd_config(sr['uuid'], sr['host_address'])

    if not pbd_config:
        print("\n  No connection parameters found for this SR.")
        input("\nPress Enter to continue...")
        return

    # Allow editing the config
    edited_config = dict(pbd_config)

    while True:
        print(f"\n{'=' * 64}")
        print(f"  Edit SR Connection: {sr['name_label']}")
        print(f"{'=' * 64}")
        print(f"  Status: {'Attached' if attached else 'Detached'}")

        print(f"\n  Connection parameters:")
        keys = list(edited_config.keys())
        for i, key in enumerate(keys, 1):
            orig = pbd_config.get(key, '')
            curr = edited_config[key]
            if orig != curr:
                print(f"    [{i}] {key}: {curr}  (was: {orig})")
            else:
                print(f"    [{i}] {key}: {curr}")

        has_changes = edited_config != pbd_config
        print()
        if has_changes:
            print(f"  [c] Apply changes")
        print(f"  [q] Back")

        choice = input("\nSelect: ").strip().lower()

        if choice == 'q':
            return

        # Check if editing a parameter
        try:
            param_idx = int(choice) - 1
            if 0 <= param_idx < len(keys):
                key = keys[param_idx]
                print(f"\n  Current {key}: {edited_config[key]}")
                new_val = input(f"  New {key} (Enter to keep): ").strip()
                if new_val:
                    edited_config[key] = new_val
                continue
        except ValueError:
            pass

        if choice == 'c':
            result = _show_pbd_update_commands(sr, pbd_config, edited_config, attached, conn)
            if result == 'applied':
                return  # Go back after successful apply


def _show_pbd_update_commands(sr, old_config, new_config, attached, conn):
    """Show xe commands to update PBD configuration. Returns 'applied' if executed."""
    # Check if anything changed
    if old_config == new_config:
        print("\n  No changes made.")
        input("\nPress Enter to continue...")
        return None

    dc_params = ' '.join(f"device-config:{k}=\"{v}\"" for k, v in new_config.items())

    while True:
        print(f"\n{'─' * 64}")
        print("  XE Commands to Update SR Connection")
        print(f"{'─' * 64}")
        print(f"  # SR: {sr['name_label']}")
        print(f"  # Changes:")
        for key in new_config:
            if old_config.get(key) != new_config[key]:
                print(f"  #   {key}: {old_config.get(key, '')} -> {new_config[key]}")

        print()

        # Method: Destroy and recreate PBD
        print("  # Step 1: Get the PBD UUID for this SR")
        print(f"  PBD=$(xe pbd-list sr-uuid={sr['uuid']} --minimal)")
        print()

        if attached:
            print("  # Step 2: Detach the PBD")
            print("  xe pbd-unplug uuid=$PBD")
            print()

        print("  # Step 3: Destroy the old PBD")
        print("  xe pbd-destroy uuid=$PBD")
        print()

        print("  # Step 4: Get host UUID and create new PBD")
        print("  HOST=$(xe host-list --minimal)")
        print(f"  xe pbd-create sr-uuid={sr['uuid']} host-uuid=$HOST {dc_params}")
        print()

        print("  # Step 5: Plug the new PBD")
        print(f"  NEW_PBD=$(xe pbd-list sr-uuid={sr['uuid']} --minimal)")
        print("  xe pbd-plug uuid=$NEW_PBD")

        print(f"\n{'─' * 64}")
        print("  [r] Run via SSH    [q] Back")

        choice = input("\nSelect: ").strip().lower()

        if choice == 'q' or choice == '':
            return None
        elif choice == 'r':
            warning_level = _session.get('warning_level', 'full')
            if warning_level != 'none':
                confirm = input("\n  Apply changes via SSH? Type 'yes' to confirm: ").strip()
                if confirm.lower() != 'yes':
                    print("  Cancelled.")
                    continue
            success = _apply_pbd_update_ssh(sr, new_config, attached, conn)
            if success:
                return 'applied'


def _apply_pbd_update_ssh(sr, new_config, attached, conn):
    """Apply PBD update via SSH. Returns True on success."""
    # Get server password from config
    servers = load_server_config()
    server = next((s for s in servers if s['address'] == conn.host), None)

    if not server:
        print(f"\n  Error: Could not find credentials for {conn.host}")
        input("\nPress Enter to continue...")
        return False

    host = conn.host
    password = server['password']
    dc_params = ' '.join(f"device-config:{k}='{v}'" for k, v in new_config.items())

    # Build the command script - more robust version
    script = f'''
set -e
PBD=$(xe pbd-list sr-uuid={sr['uuid']} --minimal)
if [ -z "$PBD" ]; then
    echo "Error: No PBD found for this SR"
    exit 1
fi
echo "PBD UUID: $PBD"

# Check if currently attached
ATTACHED=$(xe pbd-param-get uuid=$PBD param-name=currently-attached 2>/dev/null || echo "false")
if [ "$ATTACHED" = "true" ]; then
    echo "Detaching PBD..."
    if ! xe pbd-unplug uuid=$PBD 2>&1; then
        echo ""
        echo "Failed to detach. The NFS mount may be busy."
        echo "Try: ssh root@$(hostname) 'fuser -m /run/sr-mount/{sr['uuid']}'"
        echo "Or wait a moment and retry."
        exit 1
    fi
else
    echo "PBD already detached"
fi

echo "Destroying old PBD..."
xe pbd-destroy uuid=$PBD
HOST=$(xe host-list --minimal)
echo "Creating new PBD on host $HOST..."
xe pbd-create sr-uuid={sr['uuid']} host-uuid=$HOST {dc_params}
NEW_PBD=$(xe pbd-list sr-uuid={sr['uuid']} --minimal)
echo "Plugging new PBD: $NEW_PBD"
xe pbd-plug uuid=$NEW_PBD
echo "Done!"
'''

    print(f"\n  Connecting to {conn.host_name}...")

    try:
        # Use sshpass if available, otherwise try ssh with key auth
        result = subprocess.run(
            ['sshpass', '-p', password, 'ssh', '-o', 'StrictHostKeyChecking=no',
             f'root@{host}', 'bash -s'],
            input=script,
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            print(f"\n  {result.stdout.strip()}")
            print("\n  ✓ SR connection updated successfully!")
            print("  Refreshing cache...")
            conn.sync_to_cache()
            input("\nPress Enter to continue...")
            return True
        else:
            # Show both stdout and stderr - script errors often go to stdout
            output = result.stdout.strip()
            error_msg = result.stderr.strip()
            if output:
                print(f"\n  Output:\n  {output.replace(chr(10), chr(10) + '  ')}")
            if error_msg:
                print(f"\n  Error: {error_msg}")
            combined = output + error_msg
            if 'VDI is in use' in combined or 'in use by some other operation' in combined:
                print("\n  Tip: Use [4] Host Operations > Eject all ISOs to unmount ISOs first.")
            input("\nPress Enter to continue...")
            return False

    except FileNotFoundError:
        # sshpass not installed, try without password (key auth)
        try:
            result = subprocess.run(
                ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'BatchMode=yes',
                 f'root@{host}', 'bash -s'],
                input=script,
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                print(f"\n  {result.stdout.strip()}")
                print("\n  ✓ SR connection updated successfully!")
                print("  Refreshing cache...")
                conn.sync_to_cache()
                input("\nPress Enter to continue...")
                return True
            else:
                error_msg = result.stderr.strip()
                print(f"\n  Error: {error_msg}")
                if 'VDI is in use' in error_msg or 'in use by some other operation' in error_msg:
                    print("\n  Tip: Use [4] Host Operations > Eject all ISOs to unmount ISOs first.")
                print("\n  Note: Install 'sshpass' for password auth: sudo apt install sshpass")
                input("\nPress Enter to continue...")
                return False

        except Exception as e:
            print(f"\n  SSH Error: {e}")
            print("  Note: Install 'sshpass' for password auth: sudo apt install sshpass")
            input("\nPress Enter to continue...")
            return False

    except subprocess.TimeoutExpired:
        print("\n  Error: SSH command timed out")
        input("\nPress Enter to continue...")
        return False

    except Exception as e:
        print(f"\n  Error: {e}")
        input("\nPress Enter to continue...")
        return False


def get_sr_create_command(sr):
    """Generate xe command to recreate this SR on another host."""
    # Get device_config from PBD (has actual mount info) and merge with SR config
    pbd_config, _ = get_sr_pbd_config(sr['uuid'], sr['host_address'])
    sr_config = json.loads(sr.get('device_config', '{}') or '{}')
    device_config = {**sr_config, **pbd_config}  # PBD takes precedence

    sm_config = json.loads(sr.get('sm_config', '{}') or '{}')

    sr_type = sr['type']
    name = sr['name_label']
    desc = sr['name_description'] or ''

    # Build device-config params
    dc_params = ' '.join(f"device-config:{k}=\"{v}\"" for k, v in device_config.items())
    sm_params = ' '.join(f"sm-config:{k}=\"{v}\"" for k, v in sm_config.items())

    # Base command
    cmd = f"xe sr-create name-label=\"{name}\""
    if desc:
        cmd += f" name-description=\"{desc}\""
    cmd += f" type={sr_type} shared={'true' if sr['shared'] else 'false'}"
    if sr['content_type']:
        cmd += f" content-type={sr['content_type']}"
    if dc_params:
        cmd += f" {dc_params}"
    if sm_params:
        cmd += f" {sm_params}"

    return cmd


def get_sr_create_script(sr, target_servers=None):
    """Generate a shell script to create this SR on multiple servers."""
    # Get device_config from PBD (has actual mount info) and merge with SR config
    pbd_config, _ = get_sr_pbd_config(sr['uuid'], sr['host_address'])
    sr_config = json.loads(sr.get('device_config', '{}') or '{}')
    device_config = {**sr_config, **pbd_config}  # PBD takes precedence

    lines = ["#!/bin/bash", "# Auto-generated script to create SR on XCP-ng hosts", ""]
    lines.append(f"SR_NAME=\"{sr['name_label']}\"")
    lines.append(f"SR_DESC=\"{sr['name_description'] or ''}\"")
    lines.append(f"SR_TYPE=\"{sr['type']}\"")
    lines.append(f"SR_SHARED=\"{'true' if sr['shared'] else 'false'}\"")

    if sr['content_type']:
        lines.append(f"SR_CONTENT=\"{sr['content_type']}\"")

    # Add device config as variables
    for key, val in device_config.items():
        var_name = f"DC_{key.upper().replace('-', '_').replace(':', '_')}"
        lines.append(f"{var_name}=\"{val}\"")

    lines.append("")
    lines.append("# Target hosts (edit as needed)")

    if target_servers:
        lines.append(f"HOSTS=({' '.join(target_servers)})")
    else:
        lines.append("HOSTS=(xcpng01 xcpng02 xcpng03)")

    lines.append("")
    lines.append("for HOST in \"${HOSTS[@]}\"; do")
    lines.append("    echo \"Creating SR on $HOST...\"")

    # Build xe command
    cmd_parts = ["    xe sr-create"]
    cmd_parts.append("        name-label=\"$SR_NAME\"")
    cmd_parts.append("        name-description=\"$SR_DESC\"")
    cmd_parts.append("        type=$SR_TYPE")
    cmd_parts.append("        shared=$SR_SHARED")

    if sr['content_type']:
        cmd_parts.append("        content-type=$SR_CONTENT")

    for key, val in device_config.items():
        var_name = f"DC_{key.upper().replace('-', '_').replace(':', '_')}"
        cmd_parts.append(f"        device-config:{key}=\"${var_name}\"")

    lines.append(" \\\n".join(cmd_parts))
    lines.append("done")

    return "\n".join(lines)


def storage_workflow(conn):
    """Storage operations workflow. Returns 'exit' if user wants to exit program."""
    try:
        conn.sync_to_cache()
    except Exception as e:
        print(f"Warning: Cache refresh failed: {e}")
        print("  Cached data may be stale; proceed with caution.")
    while True:
        srs = get_storage_repositories(conn.host)

        if not srs:
            print("\n  No storage repositories found in cache.")
            print("  Try refreshing the cache from the main menu.")
            input("\nPress Enter to continue...")
            return

        print(f"\n{'─' * 64}")
        print(f"  Storage Repositories - {conn.host_name}")
        print('─' * 64)

        display_storage_list(srs)

        print(f"\n  [#] View SR details    [c] Copy SR config")
        print(f"  [q] Quit               [x] Exit")
        print('─' * 64)
        choice = input("Select: ").strip().lower()

        if choice == 'x':
            return 'exit'
        elif choice == 'q':
            return  # Back to main menu
        elif choice == 'c':
            # Copy SR config - only show remote/shared storage types
            copyable_types = ['nfs', 'iso', 'smb', 'cifs', 'iscsi', 'lvmoiscsi', 'gfs2']
            copyable_srs = [sr for sr in srs if sr['type'] in copyable_types]

            if not copyable_srs:
                print("\n  No remote storage repositories to copy.")
                input("\nPress Enter to continue...")
                continue

            print(f"\n  Remote Storage Repositories:")
            for i, sr in enumerate(copyable_srs, 1):
                print(f"    [{i}] {sr['name_label']} ({sr['type']})")

            sr_choice = input("\n  Select SR to copy: ").strip()
            try:
                idx = int(sr_choice) - 1
                if 0 <= idx < len(copyable_srs):
                    show_sr_copy_options(copyable_srs[idx], conn)
                else:
                    print("  Invalid selection.")
            except ValueError:
                print("  Invalid selection.")
        else:
            # Try as SR number for details
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(srs):
                    sr_details_workflow(srs[idx], conn)
                else:
                    print("Invalid selection.")
            except ValueError:
                print(f"Invalid choice: '{choice}'")


def show_sr_copy_options(sr, conn):
    """Show options for copying SR configuration to other hosts."""
    # Only show copy options for types that make sense to copy
    copyable_types = ['nfs', 'iso', 'smb', 'cifs', 'iscsi', 'lvmoiscsi', 'gfs2']

    if sr['type'] not in copyable_types:
        print(f"\n  SR type '{sr['type']}' is typically local storage and cannot be")
        print("  meaningfully copied to another host.")
        input("\nPress Enter to continue...")
        return

    # Get device_config from PBD (actual connection info) merged with SR config
    pbd_config, _ = get_sr_pbd_config(sr['uuid'], sr['host_address'])
    sr_config = json.loads(sr.get('device_config', '{}') or '{}')
    device_config = {**sr_config, **pbd_config}

    # Allow editing the config
    edited_config = dict(device_config)

    # Get other servers
    servers = load_server_config()
    other_servers = [s for s in servers if s['address'] != conn.host]

    while True:
        print(f"\n{'=' * 64}")
        print(f"  Copy SR: {sr['name_label']}")
        print(f"{'=' * 64}")
        print(f"  Type: {sr['type']}")

        # Show current connection config
        if edited_config:
            print(f"\n  Connection parameters:")
            keys = list(edited_config.keys())
            for i, key in enumerate(keys, 1):
                print(f"    [{i}] {key}: {edited_config[key]}")
        else:
            print(f"\n  No connection parameters found.")

        print()
        print(f"  [r] Create on target host")
        print(f"  [c] Show xe command")
        print(f"  [q] Back")

        choice = input("\nSelect: ").strip().lower()

        if choice == 'q':
            return

        # Check if editing a parameter
        try:
            param_idx = int(choice) - 1
            if 0 <= param_idx < len(edited_config):
                key = keys[param_idx]
                print(f"\n  Current {key}: {edited_config[key]}")
                new_val = input(f"  New {key} (Enter to keep): ").strip()
                if new_val:
                    edited_config[key] = new_val
                continue
        except ValueError:
            pass

        if choice == 'r':
            _create_sr_on_target(sr, edited_config, other_servers, conn)

        elif choice == 'c':
            print(f"\n{'─' * 64}")
            print("  XE Command (run on target host):")
            print(f"{'─' * 64}")
            print(f"\n{_get_sr_create_command_with_config(sr, edited_config)}\n")
            input("Press Enter to continue...")


def _create_sr_on_target(sr, device_config, other_servers, conn):
    """Create SR on a target host via SSH."""
    if not other_servers:
        print("\n  No other servers configured.")
        input("\nPress Enter to continue...")
        return

    # Check server status
    print("\n  Checking server status...", end='', flush=True)
    status = check_servers_status(other_servers)
    print("\r" + " " * 35 + "\r", end='')

    print(f"\n  Select target host:")
    for i, s in enumerate(other_servers, 1):
        online = status.get(s['address'], False)
        status_str = "" if online else " (OFFLINE)"
        print(f"    [{i}] {s['name']}{status_str}")

    choice = input("\n  Target: ").strip()

    try:
        idx = int(choice) - 1
        if 0 <= idx < len(other_servers):
            target = other_servers[idx]
        else:
            print("  Invalid selection.")
            return
    except ValueError:
        print("  Invalid selection.")
        return

    # Check if online
    if not status.get(target['address'], False):
        print(f"\n  {target['name']} is offline.")
        input("\nPress Enter to continue...")
        return

    # Build the xe command
    xe_cmd = _get_sr_create_command_with_config(sr, device_config)

    print(f"\n  Creating SR on {target['name']}...")
    print(f"  Command: {xe_cmd[:60]}..." if len(xe_cmd) > 60 else f"  Command: {xe_cmd}")

    warning_level = _session.get('warning_level', 'full')
    if warning_level != 'none':
        confirm = input(f"\n  Proceed? Type 'yes' to confirm: ").strip()
        if confirm.lower() != 'yes':
            print("  Cancelled.")
            return

    try:
        result = subprocess.run(
            ['sshpass', '-p', target['password'], 'ssh', '-o', 'StrictHostKeyChecking=no',
             f'root@{target["address"]}', xe_cmd],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            sr_uuid = result.stdout.strip()
            print(f"\n  ✓ SR created successfully!")
            print(f"  UUID: {sr_uuid}")
        else:
            print(f"\n  Error: {result.stderr.strip()}")
            if result.stdout.strip():
                print(f"  Output: {result.stdout.strip()}")

    except FileNotFoundError:
        print("\n  Error: sshpass not installed. Run: sudo apt install sshpass")
    except subprocess.TimeoutExpired:
        print("\n  Error: SSH command timed out")
    except Exception as e:
        print(f"\n  Error: {e}")

    input("\nPress Enter to continue...")


def _get_sr_create_command_with_config(sr, device_config):
    """Generate xe command with provided device_config."""
    sm_config = json.loads(sr.get('sm_config', '{}') or '{}')

    sr_type = sr['type']
    name = sr['name_label']
    desc = sr['name_description'] or ''

    # Build device-config params
    dc_params = ' '.join(f"device-config:{k}=\"{v}\"" for k, v in device_config.items())
    sm_params = ' '.join(f"sm-config:{k}=\"{v}\"" for k, v in sm_config.items())

    # Base command
    cmd = f"xe sr-create name-label=\"{name}\""
    if desc:
        cmd += f" name-description=\"{desc}\""
    cmd += f" type={sr_type} shared={'true' if sr['shared'] else 'false'}"
    if sr['content_type']:
        cmd += f" content-type={sr['content_type']}"
    if dc_params:
        cmd += f" {dc_params}"
    if sm_params:
        cmd += f" {sm_params}"

    return cmd


def _get_sr_create_script_with_config(sr, device_config, target_servers=None):
    """Generate a shell script with provided device_config."""
    lines = ["#!/bin/bash", "# Auto-generated script to create SR on XCP-ng hosts", ""]
    lines.append(f"SR_NAME=\"{sr['name_label']}\"")
    lines.append(f"SR_DESC=\"{sr['name_description'] or ''}\"")
    lines.append(f"SR_TYPE=\"{sr['type']}\"")
    lines.append(f"SR_SHARED=\"{'true' if sr['shared'] else 'false'}\"")

    if sr['content_type']:
        lines.append(f"SR_CONTENT=\"{sr['content_type']}\"")

    # Add device config as variables
    for key, val in device_config.items():
        var_name = f"DC_{key.upper().replace('-', '_').replace(':', '_')}"
        lines.append(f"{var_name}=\"{val}\"")

    lines.append("")
    lines.append("# Target hosts (edit as needed)")

    if target_servers:
        lines.append(f"HOSTS=({' '.join(target_servers)})")
    else:
        lines.append("HOSTS=(xcpng01 xcpng02 xcpng03)")

    lines.append("")
    lines.append("for HOST in \"${HOSTS[@]}\"; do")
    lines.append("    echo \"Creating SR on $HOST...\"")

    # Build xe command
    cmd_parts = ["    ssh root@$HOST xe sr-create"]
    cmd_parts.append("        name-label=\"$SR_NAME\"")
    cmd_parts.append("        name-description=\"$SR_DESC\"")
    cmd_parts.append("        type=$SR_TYPE")
    cmd_parts.append("        shared=$SR_SHARED")

    if sr['content_type']:
        cmd_parts.append("        content-type=$SR_CONTENT")

    for key in device_config.keys():
        var_name = f"DC_{key.upper().replace('-', '_').replace(':', '_')}"
        cmd_parts.append(f"        device-config:{key}=\"${var_name}\"")

    lines.append(" \\\n".join(cmd_parts))
    lines.append("done")

    return "\n".join(lines)


# ============================================================================
# VM CLONE
# ============================================================================

def vm_clone(conn, vm_ref, vm_info, dry_run=False):
    """Clone a VM."""
    print(f"\nCloning VM: {vm_info['name']}")
    
    # Suggest name
    default_name = f"{vm_info['name']}-clone"
    print(f"\nNew VM name [{default_name}]:")
    new_name = input("Name: ").strip() or default_name
    
    if dry_run:
        print(f"\n[DRY RUN] Would clone '{vm_info['name']}' to '{new_name}'")
        return True
    
    # VM must be halted for clone
    if vm_info['power_state'] != 'Halted':
        print(f"\n⚠ VM must be halted to clone. Current state: {vm_info['power_state']}")
        return False
    
    print(f"\nCloning '{vm_info['name']}' to '{new_name}'...")
    print("(This may take a while for large disks)")
    
    clone_ref = conn.api.VM.clone(vm_ref, new_name)
    clone_uuid = conn.api.VM.get_uuid(clone_ref)
    
    log_operation("VM_CLONE", f"Cloned {vm_info['name']} to {new_name} ({clone_uuid})")
    print(f"✓ Clone created: {new_name}")
    print(f"  UUID: {clone_uuid}")
    print("\n  Remember to update inside the clone:")
    print("    - Hostname")
    print("    - Machine ID")
    print("    - SSH host keys")
    print("    - Static IP (if applicable)")
    return True


def vm_delete(conn, vm_ref, vm_info, dry_run=False):
    """Delete a VM and its associated disks."""
    print(f"\nDelete VM: {vm_info['name']}")

    # VM must be halted
    if vm_info['power_state'] != 'Halted':
        print(f"\nVM must be halted to delete. Current state: {vm_info['power_state']}")
        return False

    # Get associated disks
    disks = list_vm_disks(conn, vm_ref, vm_info)

    if disks:
        print(f"\n  Associated disks ({len(disks)}):")
        for disk in disks:
            print(f"    - {disk['name']} ({format_size(disk['virtual_size'])})")

    if dry_run:
        print(f"\n[DRY RUN] Would delete VM '{vm_info['name']}' and {len(disks)} disk(s)")
        return True

    # Double confirmation for destructive operation (unless warning_level is 'none')
    warning_level = _session.get('warning_level', 'full')
    if warning_level != 'none':
        print(f"\n⚠ WARNING: This will permanently delete:")
        print(f"    - VM: {vm_info['name']}")
        print(f"    - {len(disks)} attached disk(s)")
        print(f"\n  This action CANNOT be undone!")

        if warning_level == 'full':
            print(f"\nType the VM name to confirm deletion:")
            confirm_name = input("VM name: ").strip()
            if confirm_name != vm_info['name']:
                print("Name does not match. Cancelled.")
                return False
        else:  # 'confirm'
            confirm = input("\nDelete? Type 'yes' to confirm: ").strip()
            if confirm.lower() != 'yes':
                print("Cancelled.")
                return False

    print(f"\nDeleting VM '{vm_info['name']}'...")

    # Delete associated VDIs first
    for disk in disks:
        try:
            print(f"  Deleting disk: {disk['name']}...")
            conn.api.VDI.destroy(disk['vdi_ref'])
        except Exception as e:
            print(f"  Warning: Could not delete disk {disk['name']}: {e}")

    # Delete the VM
    conn.api.VM.destroy(vm_ref)

    log_operation("VM_DELETE", f"Deleted VM: {vm_info['name']} ({vm_info['uuid']}) with {len(disks)} disks")
    print(f"✓ VM '{vm_info['name']}' deleted.")
    return 'deleted'  # Special return to signal VM no longer exists


def display_vm_overview(conn, vm_ref, vm_info):
    """Display comprehensive VM information."""
    api = conn.api

    # Fetch full VM record
    try:
        rec = api.VM.get_record(vm_ref)
    except Exception as e:
        print(f"\nError fetching VM details: {e}")
        return

    print(f"\n{'=' * 64}")
    print(f"  VM: {rec['name_label']}")
    print(f"{'=' * 64}")

    # Basic info
    print(f"  UUID:        {rec['uuid']}")
    print(f"  State:       {format_power_state(rec['power_state'])}")
    if rec.get('name_description'):
        desc = rec['name_description']
        if len(desc) > 50:
            desc = desc[:47] + '...'
        print(f"  Description: {desc}")

    # Compute resources
    print(f"\n  vCPUs:       {rec['VCPUs_max']} max, {rec['VCPUs_at_startup']} at startup")
    mem_max = format_size(rec['memory_static_max'])
    mem_min = format_size(rec['memory_static_min'])
    mem_dyn_max = format_size(rec['memory_dynamic_max'])
    mem_dyn_min = format_size(rec['memory_dynamic_min'])
    print(f"  Memory:      {mem_max} (static max)")
    print(f"               {mem_dyn_min} - {mem_dyn_max} (dynamic range)")

    # Boot info
    boot_order = rec.get('HVM_boot_params', {}).get('order', 'cd')
    print(f"  Boot order:  {boot_order}")

    # Host residence
    resident_ref = rec.get('resident_on', 'OpaqueRef:NULL')
    if resident_ref != 'OpaqueRef:NULL':
        try:
            host_name = api.host.get_name_label(resident_ref)
            print(f"  Running on:  {host_name}")
        except:
            pass

    # Disks
    disks = list_vm_disks(conn, vm_ref, vm_info)
    if disks:
        total_disk = sum(int(d['virtual_size']) for d in disks)
        print(f"\n  Disks:       {len(disks)} ({format_size(total_disk)} total)")
        for d in disks:
            boot = ' [boot]' if d['bootable'] else ''
            print(f"               {d['device']}: {d['name'][:30]} ({format_size(d['virtual_size'])}){boot}")

    # Network interfaces
    interfaces = list_vm_networks(conn, vm_ref, vm_info)
    if interfaces:
        print(f"\n  Networks:    {len(interfaces)} interface(s)")
        for iface in interfaces:
            print(f"               eth{iface['device']}: {iface['mac']} -> {iface['network_name']}")

    # Snapshots
    snapshots = rec.get('snapshots', [])
    if snapshots:
        print(f"\n  Snapshots:   {len(snapshots)}")
        for snap_ref in snapshots[:5]:  # Show first 5
            try:
                snap_name = api.VM.get_name_label(snap_ref)
                snap_time = api.VM.get_snapshot_time(snap_ref)
                if hasattr(snap_time, 'value'):
                    snap_time = str(snap_time.value)[:19]
                print(f"               - {snap_name} ({snap_time})")
            except:
                pass
        if len(snapshots) > 5:
            print(f"               ... and {len(snapshots) - 5} more")

    # Guest metrics (if running)
    if rec['power_state'] == 'Running':
        guest_metrics_ref = rec.get('guest_metrics', 'OpaqueRef:NULL')
        if guest_metrics_ref != 'OpaqueRef:NULL':
            try:
                gm = api.VM_guest_metrics.get_record(guest_metrics_ref)
                os_version = gm.get('os_version', {})
                if os_version:
                    os_name = os_version.get('name', os_version.get('distro', 'Unknown'))
                    print(f"\n  Guest OS:    {os_name}")
                networks = gm.get('networks', {})
                if networks:
                    print(f"  Guest IPs:")
                    for key, ip in sorted(networks.items()):
                        if '/ip' in key:
                            print(f"               {key}: {ip}")
            except:
                pass

    # Other info
    start_time = rec.get('start_time')
    if start_time and rec['power_state'] == 'Running':
        if hasattr(start_time, 'value'):
            start_time = str(start_time.value)[:19]
        print(f"\n  Started:     {start_time}")

    # Platform info
    platform = rec.get('platform', {})
    if platform.get('cores-per-socket'):
        print(f"  Topology:    {platform.get('cores-per-socket')} cores/socket")


# ============================================================================
# MENUS AND WORKFLOWS
# ============================================================================

def vm_operations_menu(vm_info):
    """Display VM operations menu."""
    options = [
        ('1', 'VM Overview'),
        ('2', 'Start'),
        ('3', 'Shutdown (clean)'),
        ('4', 'Shutdown (force)'),
        ('5', 'Reboot'),
        ('6', 'Create snapshot'),
        ('7', 'Revert to snapshot'),
        ('8', 'Delete snapshot'),
        ('9', 'Resize disk'),
        ('10', 'Add network interface'),
        ('11', 'Remove network interface'),
        ('12', 'Clone VM'),
        ('13', 'Delete VM'),
    ]

    title = f"VM: {vm_info['name']} ({format_power_state(vm_info['power_state'])})"
    return display_menu(title, options, show_quit=True, show_exit=True)


def run_with_dry_run(operation_func, conn, vm_ref, vm_info, **kwargs):
    """Run an operation with optional dry-run, respecting warning level."""
    warning_level = _session.get('warning_level', 'full')

    if warning_level == 'none':
        # No warnings - immediate execution
        return operation_func(conn, vm_ref, vm_info, dry_run=False, **kwargs)

    elif warning_level == 'confirm':
        # Simple confirmation only
        print(f"\nExecute operation on '{vm_info['name']}'? [y/N]:")
        proceed = input("Confirm: ").strip().lower()
        if proceed not in ('y', 'yes'):
            print("Cancelled.")
            return False
        return operation_func(conn, vm_ref, vm_info, dry_run=False, **kwargs)

    else:  # 'full' - default
        # Full dry-run workflow
        print("\nPreview operation first (dry-run)? [Y/n]:")
        dry_run = input("Dry run? ").strip().lower()

        if dry_run in ('', 'y', 'yes'):
            operation_func(conn, vm_ref, vm_info, dry_run=True, **kwargs)

            print("\nProceed with actual operation? [y/N]:")
            proceed = input("Execute? ").strip().lower()

            if proceed not in ('y', 'yes'):
                print("Cancelled.")
                return False

        return operation_func(conn, vm_ref, vm_info, dry_run=False, **kwargs)


def vm_workflow(conn):
    """Main VM operations workflow. Returns 'exit' if user wants to exit program."""
    while True:
        vm_ref, vm_info = select_vm(conn)

        if vm_ref == 'exit':
            return 'exit'
        if vm_ref is None:
            return  # Quit back to main menu

        while True:
            # Refresh VM info
            try:
                vm_info = {
                    'ref': vm_ref,
                    'uuid': conn.api.VM.get_uuid(vm_ref),
                    'name': conn.api.VM.get_name_label(vm_ref),
                    'power_state': conn.api.VM.get_power_state(vm_ref),
                    'vcpus': conn.api.VM.get_VCPUs_max(vm_ref),
                    'memory': conn.api.VM.get_memory_static_max(vm_ref)
                }
            except Exception:
                # VM may have been deleted
                print("\nVM no longer exists.")
                break

            choice = vm_operations_menu(vm_info)

            if choice == 'x':
                return 'exit'
            elif choice == 'q':
                break  # Back to VM selection
            elif choice == '1':
                display_vm_overview(conn, vm_ref, vm_info)
                input("\nPress Enter to continue...")
            elif choice == '2':
                vm_start(conn, vm_ref, vm_info)
            elif choice == '3':
                run_with_dry_run(vm_shutdown, conn, vm_ref, vm_info, force=False)
            elif choice == '4':
                run_with_dry_run(vm_shutdown, conn, vm_ref, vm_info, force=True)
            elif choice == '5':
                run_with_dry_run(vm_reboot, conn, vm_ref, vm_info)
            elif choice == '6':
                run_with_dry_run(vm_snapshot, conn, vm_ref, vm_info)
            elif choice == '7':
                run_with_dry_run(vm_revert_snapshot, conn, vm_ref, vm_info)
            elif choice == '8':
                run_with_dry_run(vm_delete_snapshot, conn, vm_ref, vm_info)
            elif choice == '9':
                run_with_dry_run(resize_vdi, conn, vm_ref, vm_info)
            elif choice == '10':
                run_with_dry_run(add_vif, conn, vm_ref, vm_info)
            elif choice == '11':
                run_with_dry_run(remove_vif, conn, vm_ref, vm_info)
            elif choice == '12':
                run_with_dry_run(vm_clone, conn, vm_ref, vm_info)
            elif choice == '13':
                result = run_with_dry_run(vm_delete, conn, vm_ref, vm_info)
                if result == 'deleted':
                    # VM no longer exists, go back to VM selection
                    break
            else:
                print(f"\nInvalid choice: '{choice}'")


# ============================================================================
# HOST OPERATIONS
# ============================================================================

def host_operations_workflow(conn):
    """Host-level operations workflow. Returns 'exit' if user wants to exit program."""
    try:
        conn.sync_to_cache()
    except Exception as e:
        print(f"Warning: Cache refresh failed: {e}")
        print("  Cached data may be stale; proceed with caution.")
    while True:
        print(f"\n{'─' * 64}")
        print(f"  Host Operations - {conn.host_name}")
        print('─' * 64)

        options = [
            ('1', 'Eject all ISOs from VMs'),
            ('2', 'Reboot hypervisor'),
        ]

        for key, label in options:
            print(f"  [{key}] {label}")

        print(f"\n  [q] Quit               [x] Exit")
        print('─' * 64)
        choice = input("Select: ").strip().lower()

        if choice == 'x':
            return 'exit'
        elif choice == 'q':
            return  # Back to main menu
        elif choice == '1':
            eject_all_isos(conn)
        elif choice == '2':
            reboot_hypervisor(conn)
        else:
            print(f"Invalid choice: '{choice}'")


def eject_all_isos(conn):
    """Eject all mounted ISOs from VMs on this host."""
    # Get server password
    servers = load_server_config()
    server = next((s for s in servers if s['address'] == conn.host), None)

    if not server:
        print(f"\n  Error: Could not find credentials for {conn.host}")
        input("\nPress Enter to continue...")
        return

    print(f"\n  Scanning for mounted ISOs...")

    # First, find all mounted ISOs via SSH
    scan_script = '''
xe vbd-list type=CD empty=false --minimal 2>/dev/null | tr ',' '\n' | while read vbd; do
    if [ -n "$vbd" ]; then
        vm_uuid=$(xe vbd-param-get uuid=$vbd param-name=vm-uuid 2>/dev/null)
        vm_name=$(xe vm-param-get uuid=$vm_uuid param-name=name-label 2>/dev/null)
        vdi_uuid=$(xe vbd-param-get uuid=$vbd param-name=vdi-uuid 2>/dev/null)
        vdi_name=$(xe vdi-param-get uuid=$vdi_uuid param-name=name-label 2>/dev/null)
        echo "$vbd|$vm_name|$vdi_name"
    fi
done
'''

    try:
        result = subprocess.run(
            ['sshpass', '-p', server['password'], 'ssh', '-o', 'StrictHostKeyChecking=no',
             f'root@{conn.host}', 'bash -s'],
            input=scan_script,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            print(f"\n  Error scanning: {result.stderr.strip()}")
            input("\nPress Enter to continue...")
            return

        mounted = []
        for line in result.stdout.strip().split('\n'):
            if '|' in line:
                parts = line.split('|')
                if len(parts) >= 3:
                    mounted.append({
                        'vbd': parts[0],
                        'vm': parts[1],
                        'iso': parts[2]
                    })

        if not mounted:
            print("\n  No ISOs currently mounted.")
            input("\nPress Enter to continue...")
            return

        print(f"\n  Found {len(mounted)} mounted ISO(s):")
        for m in mounted:
            print(f"    - {m['vm']}: {m['iso']}")

        warning_level = _session.get('warning_level', 'full')
        if warning_level != 'none':
            confirm = input(f"\n  Eject all {len(mounted)} ISOs? Type 'yes' to confirm: ").strip()
            if confirm.lower() != 'yes':
                print("  Cancelled.")
                input("\nPress Enter to continue...")
                return

        # Eject all
        vbd_list = ' '.join(m['vbd'] for m in mounted)
        eject_script = f'''
for vbd in {vbd_list}; do
    echo "Ejecting $vbd..."
    xe vbd-eject uuid=$vbd 2>&1 || echo "  Failed to eject $vbd"
done
echo "Done!"
'''

        print(f"\n  Ejecting ISOs...")
        result = subprocess.run(
            ['sshpass', '-p', server['password'], 'ssh', '-o', 'StrictHostKeyChecking=no',
             f'root@{conn.host}', 'bash -s'],
            input=eject_script,
            capture_output=True,
            text=True,
            timeout=60
        )

        print(f"\n  {result.stdout.strip()}")
        if result.stderr.strip():
            print(f"  {result.stderr.strip()}")

        print("\n  ✓ ISO eject complete")
        input("\nPress Enter to continue...")

    except FileNotFoundError:
        print("\n  Error: sshpass not installed. Run: sudo apt install sshpass")
        input("\nPress Enter to continue...")
    except subprocess.TimeoutExpired:
        print("\n  Error: SSH command timed out")
        input("\nPress Enter to continue...")
    except Exception as e:
        print(f"\n  Error: {e}")
        input("\nPress Enter to continue...")


def reboot_hypervisor(conn):
    """Reboot the hypervisor - only if all VMs are halted."""
    # Get server password
    servers = load_server_config()
    server = next((s for s in servers if s['address'] == conn.host), None)

    if not server:
        print(f"\n  Error: Could not find credentials for {conn.host}")
        input("\nPress Enter to continue...")
        return

    # Check for running VMs from cache
    db = get_db()
    running_vms = db.execute("""
        SELECT name_label, power_state FROM vms
        WHERE host_address = ?
          AND is_template = 0
          AND is_control_domain = 0
          AND is_snapshot = 0
          AND power_state != 'Halted'
        ORDER BY name_label
    """, (conn.host,)).fetchall()
    db.close()

    if running_vms:
        print(f"\n  Cannot reboot: {len(running_vms)} VM(s) still running:")
        for vm in running_vms[:10]:  # Show first 10
            print(f"    - {vm['name_label']} ({vm['power_state']})")
        if len(running_vms) > 10:
            print(f"    ... and {len(running_vms) - 10} more")
        print("\n  Shutdown all VMs first, then retry.")
        input("\nPress Enter to continue...")
        return

    # All VMs halted - confirm reboot (unless warning_level is 'none')
    warning_level = _session.get('warning_level', 'full')
    if warning_level != 'none':
        print(f"\n  All VMs are halted on {conn.host_name}.")
        confirm = input(f"  Reboot {conn.host_name}? Type 'yes' to confirm: ").strip()
        if confirm.lower() != 'yes':
            print("  Cancelled.")
            input("\nPress Enter to continue...")
            return

    print(f"\n  Rebooting {conn.host_name}...")

    try:
        result = subprocess.run(
            ['sshpass', '-p', server['password'], 'ssh', '-o', 'StrictHostKeyChecking=no',
             f'root@{conn.host}', 'reboot'],
            capture_output=True,
            text=True,
            timeout=10
        )

        print(f"\n  ✓ Reboot command sent to {conn.host_name}")
        print("  The host will be unavailable for a few minutes.")
        input("\nPress Enter to continue...")

    except subprocess.TimeoutExpired:
        # This is expected - reboot kills the SSH connection
        print(f"\n  ✓ Reboot command sent to {conn.host_name}")
        print("  The host will be unavailable for a few minutes.")
        input("\nPress Enter to continue...")
    except FileNotFoundError:
        print("\n  Error: sshpass not installed. Run: sudo apt install sshpass")
        input("\nPress Enter to continue...")
    except Exception as e:
        print(f"\n  Error: {e}")
        input("\nPress Enter to continue...")


def main_menu(host_name=None):
    """Display main menu."""
    warning_level = _session.get('warning_level', 'full')
    title = f"XCP-ng Admin Tool - {host_name}" if host_name else "XCP-ng Admin Tool"
    if warning_level == 'none':
        title += "  [NO CONFIRMATIONS]"
    options = [
        ('1', 'VM Operations'),
        ('2', 'Host Overview'),
        ('3', 'Storage Overview'),
        ('4', 'Host Operations'),
        ('o', f'Options [warning: {warning_level}]'),
        ('r', 'Refresh cache'),
    ]
    return display_menu(title, options, show_quit=True, show_exit=True)


def options_menu():
    """Session options menu."""
    while True:
        warning_level = _session.get('warning_level', 'full')
        disclaimer_disabled = DISCLAIMER_ACCEPTED_FILE.exists()

        print(f"\n{'─' * 64}")
        print(f"  Session Options")
        print('─' * 64)

        print(f"\n  Warning Level: [{warning_level}]")
        print(f"    full    - Dry-run preview, confirm execution")
        print(f"    confirm - Simple yes/no confirmation only")
        print(f"    none    - No confirmations, immediate execution")

        print(f"\n  Startup Disclaimer: [{'disabled' if disclaimer_disabled else 'enabled'}]")

        print(f"\n  [1] Set warning level")
        print(f"  [2] Toggle startup disclaimer")
        print(f"  [q] Back")
        print('─' * 64)

        choice = input("Select: ").strip().lower()

        if choice == 'q' or choice == '':
            return
        elif choice == '1':
            print(f"\n  Current: {warning_level}")
            print("  Options: full, confirm, none")
            new_level = input("  New level: ").strip().lower()
            if new_level in ('full', 'confirm', 'none'):
                _session['warning_level'] = new_level
                print(f"  ✓ Warning level set to '{new_level}'")
            elif new_level:
                print(f"  Invalid option: '{new_level}'")
        elif choice == '2':
            if disclaimer_disabled:
                # Re-enable disclaimer
                try:
                    DISCLAIMER_ACCEPTED_FILE.unlink()
                    print("  ✓ Startup disclaimer enabled (will show on next run)")
                except Exception as e:
                    print(f"  Error: {e}")
            else:
                # Disable disclaimer
                try:
                    DISCLAIMER_ACCEPTED_FILE.write_text(
                        f"Disclaimer accepted on {datetime.now().isoformat()}\n"
                    )
                    print("  ✓ Startup disclaimer disabled for future runs")
                except Exception as e:
                    print(f"  Error: {e}")


# ============================================================================
# MAIN
# ============================================================================

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='XCP-ng Interactive Admin Utility',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --host xcpng01
  %(prog)s --host xcpng01 --log
  %(prog)s --config my_config.json
        """
    )
    
    parser.add_argument(
        '--host',
        type=str,
        help='XCP-ng host to connect to'
    )
    
    parser.add_argument(
        '--user',
        type=str,
        default='root',
        help='Username (default: root)'
    )
    
    parser.add_argument(
        '--log',
        action='store_true',
        help='Enable operation logging'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        default=None,
        help='Path to log file (default: xcp_admin.log)'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='xcp_admin_config.json',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--create-config',
        action='store_true',
        help='Create a default configuration file and exit'
    )
    
    return parser.parse_args()


def create_default_config(filename='xcp_admin_config.json'):
    """Create a default configuration file."""
    default_config = {
        'hosts': [
            {'name': 'xcpng01', 'address': 'xcpng01.local'},
            {'name': 'xcpng02', 'address': 'xcpng02.local'}
        ],
        'default_host': 'xcpng01',
        'default_user': 'root',
        'logging': {
            'enabled': True,
            'log_file': 'xcp_admin.log',
            'log_level': 'INFO',
            'log_operations': True
        }
    }
    
    try:
        with open(filename, 'w') as f:
            json.dump(default_config, f, indent=4)
        print(f"✓ Created default configuration file: {filename}")
        return True
    except Exception as e:
        print(f"❌ Error creating config file: {e}")
        return False


def show_disclaimer():
    """Display legal disclaimer and require acceptance."""
    # Check if previously accepted and disabled
    if DISCLAIMER_ACCEPTED_FILE.exists():
        return

    print("\n" + "=" * 70)
    print("  XCP-NG ADMIN TOOL - PRE-ALPHA SOFTWARE")
    print("=" * 70)
    print("""
  IMPORTANT NOTICE - PLEASE READ CAREFULLY

  This software is provided "AS IS" in PRE-ALPHA state, without warranty
  of any kind, express or implied, including but not limited to the
  warranties of merchantability, fitness for a particular purpose, and
  noninfringement.

  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
  CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
  TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN CONNECTION WITH THE
  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

  This tool performs administrative operations on XCP-ng hypervisors
  including but not limited to: starting/stopping VMs, modifying storage,
  rebooting hosts, and deleting resources. Improper use may result in
  DATA LOSS, SERVICE INTERRUPTION, or other IRREVERSIBLE DAMAGE.

  BY CONTINUING, YOU ACKNOWLEDGE THAT:
  - You understand this is experimental, pre-alpha software
  - You accept all risks associated with its use
  - You will not hold the authors liable for any damages
  - You have appropriate backups and recovery procedures in place
""")
    print("=" * 70)

    response = input("\n  Type 'I ACCEPT' to continue: ").strip()
    if response != 'I ACCEPT':
        print("\n  You must accept the terms to continue. Exiting.")
        sys.exit(0)
    print()


def main():
    """Main entry point."""
    args = parse_arguments()

    if args.create_config:
        create_default_config(args.config)
        return

    # Show disclaimer and require acceptance
    show_disclaimer()

    print("=" * 60)
    print("  XCP-NG ADMIN TOOL")
    print("=" * 60)

    # Initialize SQLite database
    init_database()

    # Load config
    if Path(args.config).exists():
        load_config(args.config)

    # Setup logging
    if args.log or _config['logging']['enabled']:
        setup_logger(enabled=True, log_file=args.log_file)

    # Load servers from xcpng.config
    servers = load_server_config()
    user = args.user or _config.get('default_user', 'root')

    # If --host specified, use single-server mode (no server selection loop)
    if args.host:
        host = args.host
        host_name = args.host
        password = None
        # Check if this host is in our config for the password
        for s in servers:
            if s['address'] == args.host or s['name'] == args.host:
                host = s['address']
                host_name = s['name']
                password = s['password']
                break
        if not password:
            print(f"\nConnecting to {host} as {user}")
            password = getpass("Password: ")

        conn = _connect_and_sync(host, user, password, host_name)
        if not conn:
            return
        try:
            _run_main_menu(conn)
        finally:
            conn.close()
            print("\n✓ Disconnected. Goodbye!")
        return

    # Server selection loop
    while True:
        if servers:
            server = select_server(servers)
            if not server:
                print("\nGoodbye!")
                return
            host = server['address']
            host_name = server['name']
            password = server['password']
        else:
            # No config - prompt for everything
            print("\nNo xcpng.config found.")
            print("Enter XCP-ng host address (or 'q' to quit):")
            host = input("Host: ").strip()
            if not host or host.lower() == 'q':
                print("\nGoodbye!")
                return
            host_name = host
            print(f"\nConnecting to {host} as {user}")
            password = getpass("Password: ")

        conn = _connect_and_sync(host, user, password, host_name)
        if not conn:
            # Connection failed, let user try another server
            input("Press Enter to continue...")
            continue

        try:
            switch_server = _run_main_menu(conn)
        finally:
            conn.close()
            print(f"\n✓ Disconnected from {host_name}")

        if not switch_server:
            # User chose quit, not switch server
            print("\nGoodbye!")
            return


def _connect_and_sync(host, user, password, host_name):
    """Connect to a host and sync cache. Returns connection or None on failure."""
    print(f"\nConnecting to {host_name} ({host})...")
    try:
        conn = XCPConnection(host, user, password, host_name=host_name)
        print(f"✓ Connected to {host_name}")
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

    print("\nSyncing infrastructure data to cache...")
    try:
        conn.sync_to_cache()
    except Exception as e:
        print(f"Warning: Cache sync failed: {e}")
        print("  Some features may show stale data.")

    return conn


def _run_main_menu(conn):
    """Run the main menu loop. Returns True if user wants to switch servers, False to exit."""
    while True:
        choice = main_menu(host_name=conn.host_name)

        if choice == 'x':
            return False  # Exit program
        elif choice == 'q':
            return True   # Quit to server selection
        elif choice == '1':
            result = vm_workflow(conn)
            if result == 'exit':
                return False
        elif choice == '2':
            display_host_overview(conn)
            input("Press Enter to continue...")
        elif choice == '3':
            result = storage_workflow(conn)
            if result == 'exit':
                return False
        elif choice == '4':
            result = host_operations_workflow(conn)
            if result == 'exit':
                return False
        elif choice == 'o':
            options_menu()
        elif choice == 'r':
            print("\nRefreshing cache from host...")
            try:
                conn.sync_to_cache()
            except Exception as e:
                print(f"Refresh failed: {e}")
        else:
            print(f"\nInvalid choice: '{choice}'")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nCancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
