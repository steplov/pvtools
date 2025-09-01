# pvtools

Proxmox node disk backup/restore helper for ZFS + LVM-thin storage with Proxmox Backup Server.

Manages backup and restore of dynamically created disks on Proxmox nodes, particularly useful for disks created by [Proxmox CSI plugin](https://github.com/sergelogvinov/proxmox-csi-plugin) but works with any Proxmox storage.

## Usage

### Backup

```bash
pvtools backup <SUBCOMMAND> [OPTIONS]
```

**Subcommands:**
- `run` — Run backup
- `list-archives` — Show which volumes would be backed up

**Options (for `backup run`):**
- `--target <repo>` — Target PBS repository from config
- `--dry-run` — Show plan without executing

**Examples:**
```bash
# Run backup to repository "nas"
pvtools backup run --target nas

# Dry run backup
pvtools backup run --dry-run

# Show which archives would be created
pvtools backup list-archives --target nas
```

### Restore

```bash
pvtools restore <SUBCOMMAND> [OPTIONS]
```

**Subcommands:**
- `list-snapshots` — Show available PBS snapshots
- `list-archives` — Show archives inside a snapshot
- `run` — Restore one or more archives

**Options (for `restore run`):**
- `--source <repo>` — Source PBS repository
- `--snapshot <epoch|latest>` — Snapshot timestamp or `latest`
- `--archive <archive>` — Restore specific archive (can be repeated)
- `--all` — Restore all archives in snapshot
- `--force` — Overwrite existing volumes
- `--dry-run` — Show what would be restored

**Examples:**
```bash
# List snapshots in repo "nas"
pvtools restore list-snapshots --source nas

# List archives inside the latest snapshot
pvtools restore list-archives --source nas --snapshot latest

# Restore all archives from latest snapshot
pvtools restore run --source nas --snapshot latest --all

# Restore specific archive from snapshot at given time
pvtools restore run --source nas --snapshot 1735689600 --archive vm-9999-disk-data.raw

# Dry run restore plan
pvtools restore run --source nas --snapshot latest --all --dry-run
```

### Configuration

```bash
# Validate configuration
pvtools --check-config

# Print parsed configuration
pvtools --print-config
```

## Configuration

Create `config.toml`:

```toml
[pbs]
# PBS authentication
keyfile       = "./enc.key"           # Optional encryption key
password_file = "./token"             # PBS token file
ns            = "pv"                  # PBS namespace
backup_id     = ""                    # Defaults to <hostname>-k8s-pv

# Disk filtering
pv_prefixes   = ["vm-9999-", "vm-7777-"]  # Include disks with these prefixes
pv_exclude_re = "plex-transcode"          # Exclude disks matching regex

# PBS repositories
[pbs.repos]
nas     = "root@pam!pve@10.10.0.24:nas-store"
s3      = "root@pam!pve@10.10.0.24:s3-store"
offsite = "root@pam!pve@203.0.113.5:offsite-store"

# ZFS configuration
[zfs]
pools = ["tank"]

[zfs.restore]
dest_root = "tank"

# LVM-thin configuration  
[lvmthin]
vgs = ["pve"]

[lvmthin.restore]
vg = "pve"
thinpool = "data"
```

## Requirements

- `proxmox-backup-client` in PATH
- ZFS and/or LVM-thin tools (`zfs`, `lvcreate`, etc.)
- Appropriate permissions for volume operations

## How It Works

1. **Discovery**: Scans ZFS pools and LVM volume groups for datasets/volumes matching `pv_prefixes`
2. **Filtering**: Applies `pv_exclude_re` to filter out unwanted disks
3. **Snapshot**: Creates temporary snapshots of matching volumes
4. **Backup**: Streams snapshots to Proxmox Backup Server via `proxmox-backup-client`
5. **Cleanup**: Removes temporary snapshots after successful backup

For restore operations, the process reverses: downloads from PBS and creates new volumes in the target storage.

## Use Cases

- Backup/restore of Proxmox CSI plugin dynamically created disks
- General Proxmox node storage management
- Automated backup of VM disks stored on ZFS/LVM-thin
- Disaster recovery for Proxmox storage backends
