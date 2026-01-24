# XCP-ng Admin Tool

**Version:** 1.0  
**Status:** Initial Release  
**Author:** Joel  
**Date:** January 2025

---

## Overview

A Python-based interactive CLI tool for managing XCP-ng hypervisors. Designed to provide quick access to frequently needed operations that are either cumbersome in Xen Orchestra or require manual `xe` command construction.

## Problem Statement

Day-to-day XCP-ng administration often involves repetitive tasks:

- Starting/stopping VMs by name (without hunting for UUIDs)
- Creating pre-maintenance snapshots
- Resizing disks after storage expansion
- Adding/removing network interfaces on cloned VMs
- Quick status checks across VMs

Xen Orchestra provides a full GUI but can be slower for these quick operations. The `xe` CLI is powerful but requires remembering UUIDs and command syntax. This tool bridges the gap with an interactive menu-driven interface.

## Target User

- Home lab administrators managing XCP-ng clusters
- IT staff performing routine VM maintenance
- Developers needing quick VM lifecycle control

## Architecture

```
┌─────────────────┐         HTTPS/443         ┌─────────────────┐
│  Ubuntu VM      │ ───────────────────────── │  XCP-ng Host    │
│  (xcp_admin.py) │        XenAPI             │  (Pool Master)  │
└─────────────────┘                           └─────────────────┘
```

- **Connection:** XenAPI over HTTPS (same API Xen Orchestra uses)
- **Authentication:** Username/password (typically root)
- **No agent required:** Communicates directly with XAPI daemon

## Version 1.0 Features

### VM Lifecycle

| Operation | Description |
|-----------|-------------|
| Start | Power on a halted VM |
| Shutdown (clean) | Guest-initiated graceful shutdown |
| Shutdown (force) | Hard power off |
| Reboot | Clean reboot via guest tools |

### Snapshot Management

| Operation | Description |
|-----------|-------------|
| Create snapshot | Auto-generates timestamped name |
| List snapshots | Shows all snapshots with creation time |
| Revert to snapshot | Restores VM state (VM halts after revert) |
| Delete snapshot | Removes snapshot and associated VDIs |

### Disk Management

| Operation | Description |
|-----------|-------------|
| View disks | Lists VDIs with size and utilization |
| Resize VDI | Expand disk (accepts bytes or `100G` format) |

**Note:** Resize is expand-only. Shrinking requires guest-side operations first.

### Network Management

| Operation | Description |
|-----------|-------------|
| View interfaces | Lists VIFs with MAC and network name |
| Add VIF | Attach VM to a network (auto-assigns device number) |
| Remove VIF | Detach network interface |

### VM Cloning

| Operation | Description |
|-----------|-------------|
| Clone VM | Full copy of halted VM with new name |

Outputs reminder checklist for post-clone identity changes (hostname, machine-id, SSH keys, static IP).

## Installation

### Requirements

- Python 3.8+
- Network access to XCP-ng host(s) on port 443
- XCP-ng credentials (typically root)

### Setup

```bash
# Install XenAPI library
pip install XenAPI

# Download the tool
# (copy xcp_admin.py to your preferred location)

# Optional: Create configuration file
python xcp_admin.py --create-config
```

## Usage

### Basic Usage

```bash
# Connect to a specific host
python xcp_admin.py --host xcpng01

# Connect with logging enabled
python xcp_admin.py --host xcpng01 --log

# Use a config file
python xcp_admin.py --config /path/to/config.json
```

### Interactive Flow

```
1. Launch tool → Enter host/password
2. Main Menu → Select "VM Operations"
3. VM List → Select VM by number or name
4. VM Menu → Choose operation
5. Dry Run → Preview action (optional)
6. Execute → Confirm and run
```

### Command-Line Arguments

| Argument | Description |
|----------|-------------|
| `--host` | XCP-ng host address |
| `--user` | Username (default: root) |
| `--log` | Enable operation logging |
| `--log-file` | Custom log file path |
| `--config` | Path to config file |
| `--create-config` | Generate default config and exit |

### Configuration File

```json
{
    "hosts": [
        {"name": "xcpng01", "address": "xcpng01.local"},
        {"name": "xcpng02", "address": "xcpng02.local"}
    ],
    "default_host": "xcpng01",
    "default_user": "root",
    "logging": {
        "enabled": true,
        "log_file": "xcp_admin.log",
        "log_level": "INFO"
    }
}
```

## Safety Features

- **Dry-run mode:** Preview all destructive operations before execution
- **Confirmation prompts:** Type "yes" to confirm snapshots reverts, deletions, etc.
- **Operation logging:** Audit trail of all actions with timestamps
- **State validation:** Checks VM power state before operations (e.g., can't start a running VM)

## Limitations (v1.0)

- Single-host connection (no pool-wide view)
- No VM creation (use XO or `xe` for initial provisioning)
- No storage repository management
- No host maintenance mode operations
- No live migration
- VDI resize is expand-only

## Roadmap

### Version 1.1 (Planned)

- [ ] Host Overview (CPU/memory utilization)
- [ ] Storage Overview (SR usage, orphaned VDIs)
- [ ] Batch snapshot cleanup (delete older than X days)
- [ ] Quick actions menu (skip VM selection for common tasks)

### Version 1.2 (Planned)

- [ ] Pool-aware mode (connect to master, see all hosts)
- [ ] Bulk operations (shutdown all VMs on host)
- [ ] VM migration between hosts
- [ ] Template management

### Future Considerations

- [ ] Host maintenance workflow (evacuate → maintenance → return)
- [ ] Scheduled snapshot automation
- [ ] Integration with backup solutions
- [ ] Web UI wrapper (Flask/FastAPI)

## Related Files

| File | Purpose |
|------|---------|
| `xcp_admin.py` | Main application |
| `xcp_admin_config.json` | Configuration (auto-generated) |
| `xcp_admin.log` | Operation log (when enabled) |
| `clone-identity.sh` | Post-clone Ubuntu identity reset script |

## License

Internal tool for personal/home lab use.
