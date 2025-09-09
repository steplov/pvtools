# pvtools

![Build Status](https://github.com/steplov/pvtools/workflows/CI/badge.svg)
![Latest Release](https://img.shields.io/github/v/release/steplov/pvtools)
![Downloads](https://img.shields.io/github/downloads/steplov/pvtools/total)

## What is pvtools?

pvtools is a command-line utility that simplifies backup and restore operations for Proxmox virtual machine disks stored on ZFS and LVM-thin storage backends. It integrates seamlessly with Proxmox Backup Server (PBS) and is particularly valuable for managing dynamically created volumes in Kubernetes environments using the Proxmox CSI plugin.

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
# =========================
# PBS (Proxmox Backup Server)
# =========================
[pbs]
# Optional client-side encryption key (PEM). Relative paths resolve from this file's dir.
keyfile       = "./enc.key"

# Token/secret file. File content = secret (no trailing newline).
password_file = "./token"

# Optional PBS namespace. Empty = PBS root.
ns            = "pv"

# Backup group. Empty -> "<hostname>-backup".
backup_id     = ""

[pbs.repos]
# Repository aliases. Use these names on CLI and in [backup.target].repo.
# Alias rules: [A-Za-z0-9_-], len 1..32.
nas     = "root@pam!pve@10.10.0.24:nas-store"
s3      = "root@pam!pve@10.10.0.24:s3-store"
offsite = "root@pam!pve@203.0.113.5:offsite-store"

# =========================
# BACKUP
# =========================
# Global PV discovery filters.
# If pv_prefixes is empty → allow all.
# pv_exclude_re is a Rust regex; if set → exclude matching PV names.
[backup]
pv_prefixes   = ["vm-9999-", "vm-7777-"]
pv_exclude_re = "tmp$"

# Default repository alias for backups. CLI --target overrides this.
[backup.target]
repo = "nas"

# Discovery sources used when scanning for PVs to back up.
[backup.sources.zfs]
pools = ["tank"]          # ZFS pools to scan

[backup.sources.lvmthin]
vgs = ["pve"]             # LVM VGs with thinpools to scan

# =========================
# RESTORE
# =========================

# 1) Restore targets — where data will be restored.
#    This is a MAP of named targets (NOT an array). One table per target.
#    Each has a type and type-specific fields.

[restore.targets.zfs_pv]
type = "zfs"              # Required. Provider type name.
root = "tank"             # ZFS root: results in /dev/zvol/tank/<leaf> or a file under its mountpoint.

[restore.targets.lvm_pve]
type = "lvmthin"          # Required. Provider type name.
vg = "pve"                # LVM volume group
thinpool = "data"         # LVM thinpool (required)

# 2) Routing rules — pick a target based on the archive's SOURCE provider and optional filename regex.
#    First match wins, top to bottom. Keys with dots MUST be quoted in TOML.
#    If no regex is given, the rule is a wildcard for that provider.

[[restore.rules]]
"match.provider" = "zfs"
target = "zfs_pv"         # route all zfs-provider archives here (wildcard rule)

[[restore.rules]]
"match.provider"      = "lvmthin"
"match.archive_regex" = 'vm-7777-.*'   # only LVM-thin archives matching this regex go to lvm_pve
target = "lvm_pve"

# 3) Default/fallback. Used if nothing matched.
#    Actual resolution order:
#      a) first rule match (above),
#      b) else: first defined target of the same provider type ("zfs" or "lvmthin"),
#      c) else: default_target (cross-type restore is allowed).
[restore]
default_target = "zfs_pv"
```

## Requirements

- `proxmox-backup-client` in PATH
- ZFS and/or LVM-thin tools (`zfs`, `lvcreate`, etc.)
- Appropriate permissions for volume operations
