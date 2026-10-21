#!/usr/bin/env python3
"""Bound backup disk use before pg_dump can exhaust the application volume."""
from pathlib import Path
import shutil
import sys

GIB = 1024 ** 3

def required_bytes(root: Path) -> int:
    dumps = [p.stat().st_size for p in root.glob('*/database.dump') if p.is_file()]
    artifacts = [p.stat().st_size for p in root.glob('*/artifacts.tar.gz') if p.is_file()]
    # Allow growth over the largest retained backup, plus free operating space.
    return max(10 * GIB, int((max(dumps, default=0) + max(artifacts, default=0)) * 1.3) + 5 * GIB)

def main():
    root = Path('/var/backups/agentos')
    required = required_bytes(root)
    free = shutil.disk_usage(root).free
    if free < required:
        print(f'BACKUP_CAPACITY: free={free} required={required}; backup not started, existing backups preserved.', file=sys.stderr)
        return 1
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
