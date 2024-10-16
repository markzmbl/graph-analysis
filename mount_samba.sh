#!/bin/bash

# Create mount point if it doesn't exist
if [ ! -d "$MOUNT_POINT" ]; then
    mkdir -p "$MOUNT_POINT"
fi

# Mount the Samba share
# Check if the mount was successful
if mount_smbfs "smb://$USERNAME:$PASSWORD@$SMB_SHARE" "$MOUNT_POINT"
then
    echo "Samba share mounted successfully."
else
    echo "Failed to mount Samba share."
    exit 1
fi