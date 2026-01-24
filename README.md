# xe-xpress

**Status:** Pre-Alpha
**Author:** Joel
**Date:** January 2025

---

## Disclaimer

This software is provided "AS IS" in PRE-ALPHA state, without warranty of any kind. The authors are not liable for any damages resulting from its use. See the startup disclaimer for full terms.

---

## Overview

A Python-based interactive CLI tool for managing XCP-ng hypervisors. Designed to provide quick access to frequently needed operations that are either cumbersome in Xen Orchestra or require manual `xe` command construction.

## Features

### Multi-Server Management
- Configure multiple XCP-ng hosts in `xcpng.config`
- Server status checking (online/offline) at startup
- Switch between servers without restarting

### VM Operations
| Operation | Description |
|-----------|-------------|
| VM Overview | Detailed VM info (CPU, memory, disks, networks) |
| Start | Power on a halted VM |
| Shutdown (clean) | Guest-initiated graceful shutdown |
| Shutdown (force) | Hard power off |
| Reboot | Clean reboot via guest tools |
| Create snapshot | Auto-generates timestamped name |
| Revert to snapshot | Restores VM state |
| Delete snapshot | Removes snapshot and associated VDIs |
| Resize disk | Expand VDI (accepts `100G` format) |
| Add network interface | Attach VM to a network |
| Remove network interface | Detach VIF |
| Clone VM | Full copy with new name |
| Delete VM | Remove VM and associated disks |

### Host Overview
- Host version and CPU info
- Memory total and allocated
- VM count (running/total)
- Storage summary

### Storage Overview
- List all storage repositories
- View SR details with connection info
- Copy SR config to other hosts (NFS, SMB, iSCSI)
- Edit SR connection parameters via SSH

### Host Operations
- Eject all ISOs from VMs
- Reboot hypervisor (requires all VMs halted)

### Session Options
Configurable warning levels:
- **full** (default): Dry-run preview, type VM name for destructive ops
- **confirm**: Simple yes/no confirmations
- **none**: No confirmations, immediate execution (like `xe` CLI)

### Infrastructure Cache
- SQLite database caches host/VM/storage data locally
- Fast queries without repeated API calls
- Manual refresh available

## Installation

### Requirements
- Python 3.8+
- Network access to XCP-ng host(s) on port 443
- `sshpass` for SSH operations: `sudo apt install sshpass`

### Setup

```bash
# Clone the repository
git clone https://github.com/radiolithic/xe-xpress.git
cd xe-xpress

# Install dependencies
pip install -r requirements.txt

# Create server configuration
cp sample_xcpng.config xcpng.config
chmod 600 xcpng.config

# Edit with your server details (tab-separated)
nano xcpng.config
```

### Server Configuration

Edit `xcpng.config`:
```
[Servers]
192.168.1.21	xcpng01	your_password
192.168.1.22	xcpng02	your_password
```

Format: `IP<tab>NAME<tab>PASSWORD`

## Usage

```bash
# Interactive mode (recommended)
python xcp_admin.py

# Connect to specific host
python xcp_admin.py --host xcpng01

# Enable operation logging
python xcp_admin.py --log
```

### Interactive Flow

```
1. Accept disclaimer (first run)
2. Select server from list
3. Main menu: VM Operations, Host Overview, Storage, etc.
4. Perform operations with confirmation prompts
```

### Command-Line Arguments

| Argument | Description |
|----------|-------------|
| `--host` | XCP-ng host address (bypass server selection) |
| `--user` | Username (default: root) |
| `--log` | Enable operation logging |
| `--log-file` | Custom log file path |
| `--config` | Path to JSON config file |

## Files

| File | Purpose |
|------|---------|
| `xcp_admin.py` | Main application |
| `xcpng.config` | Server credentials (git-ignored) |
| `sample_xcpng.config` | Template for server config |
| `xcp_cache.db` | SQLite infrastructure cache |
| `requirements.txt` | Python dependencies |
| `clone-identity.sh` | Post-clone Ubuntu identity reset |

## Architecture

```
┌─────────────────┐                         ┌─────────────────┐
│  xcp_admin.py   │ ──── HTTPS/XenAPI ───── │  XCP-ng Host    │
│                 │ ──── SSH (sshpass) ──── │                 │
└────────┬────────┘                         └─────────────────┘
         │
    ┌────┴────┐
    │ SQLite  │  (local cache)
    │  Cache  │
    └─────────┘
```

## License

Pre-alpha software for personal/home lab use. Use at your own risk.
