#!/bin/sh
set -eu
umask 077
target=/var/backups/agentos/$(date -u +%Y%m%d-%H%M%S)
mkdir -p "$target"
runuser -u postgres -- /usr/lib/postgresql/17/bin/pg_dump -p 15482 -Fc agentos > "$target/database.dump"
tar -C /var/lib/agentos -czf "$target/artifacts.tar.gz" .
printf '%s\n' "$target" > /var/backups/agentos/LATEST
# Keep seven completed daily backups; preserve the original migration backup.
find /var/backups/agentos -mindepth 2 -maxdepth 2 -type f -mtime +7 -name database.dump -delete
find /var/backups/agentos -mindepth 2 -maxdepth 2 -type f -mtime +7 -name artifacts.tar.gz -delete
find /var/backups/agentos -mindepth 1 -maxdepth 1 -type d -empty -delete
