#!/bin/bash
#
# clone-identity.sh - Reconfigure a cloned Ubuntu VM's identity
# Usage: sudo ./clone-identity.sh <new-hostname>
#

set -e

# Check for root
if [[ $EUID -ne 0 ]]; then
    echo "Error: This script must be run as root (use sudo)"
    exit 1
fi

# Check for hostname argument
if [[ -z "$1" ]]; then
    echo "Usage: $0 <new-hostname>"
    echo "Example: $0 omadub04"
    exit 1
fi

NEW_HOSTNAME="$1"
OLD_HOSTNAME=$(hostname)

echo "=== Ubuntu VM Clone Identity Reset ==="
echo "Old hostname: $OLD_HOSTNAME"
echo "New hostname: $NEW_HOSTNAME"
echo ""

# 1. Set new hostname
echo "[1/4] Setting hostname to '$NEW_HOSTNAME'..."
hostnamectl set-hostname "$NEW_HOSTNAME"

# 2. Update /etc/hosts
echo "[2/4] Updating /etc/hosts..."
if grep -q "$OLD_HOSTNAME" /etc/hosts; then
    sed -i "s/$OLD_HOSTNAME/$NEW_HOSTNAME/g" /etc/hosts
    echo "  - Replaced '$OLD_HOSTNAME' with '$NEW_HOSTNAME' in /etc/hosts"
else
    # Ensure there's an entry for the new hostname
    if ! grep -q "$NEW_HOSTNAME" /etc/hosts; then
        echo "127.0.1.1   $NEW_HOSTNAME" >> /etc/hosts
        echo "  - Added entry for '$NEW_HOSTNAME' to /etc/hosts"
    fi
fi

# 3. Regenerate machine-id
echo "[3/4] Regenerating machine-id..."
rm -f /etc/machine-id
systemd-machine-id-setup
rm -f /var/lib/dbus/machine-id 2>/dev/null || true
ln -sf /etc/machine-id /var/lib/dbus/machine-id
echo "  - New machine-id: $(cat /etc/machine-id)"

# 4. Regenerate SSH host keys
echo "[4/4] Regenerating SSH host keys..."
rm -f /etc/ssh/ssh_host_*
dpkg-reconfigure openssh-server
echo "  - SSH host keys regenerated"

echo ""
echo "=== Identity reset complete ==="
echo ""

# Check for static IP in netplan
NETPLAN_FILES=$(ls /etc/netplan/*.yaml 2>/dev/null || true)
if [[ -n "$NETPLAN_FILES" ]]; then
    echo "NOTE: Found netplan config(s):"
    for f in $NETPLAN_FILES; do
        echo "  - $f"
        if grep -q "addresses:" "$f"; then
            echo "    ^ Contains static IP - you may need to edit this manually"
        fi
    done
    echo ""
fi

# Prompt for reboot
read -p "Reboot now? [y/N] " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Rebooting..."
    reboot
else
    echo "Remember to reboot for all changes to take effect."
fi
