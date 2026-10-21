#!/bin/sh
set -eu
umask 077
python3 "$(dirname "$0")/backup-preflight.py"
target=/var/backups/agentos/$(date -u +%Y%m%d-%H%M%S)
mkdir -p "$target"
cleanup_partial() {
  rm -f -- "$target/database.dump.partial" "$target/artifacts.tar.gz.partial"
}
trap cleanup_partial EXIT
trap 'exit 1' HUP INT TERM
runuser -u postgres -- /usr/lib/postgresql/17/bin/pg_dump -p 15482 -Fc agentos > "$target/database.dump.partial"
mv -- "$target/database.dump.partial" "$target/database.dump"
tar -C /var/lib/agentos -czf "$target/artifacts.tar.gz.partial" .
mv -- "$target/artifacts.tar.gz.partial" "$target/artifacts.tar.gz"
printf '%s\n' "$target" > /var/backups/agentos/LATEST
# Keep seven completed daily backups; preserve the original migration backup.
find /var/backups/agentos -mindepth 2 -maxdepth 2 -type f -mtime +7 -name database.dump -delete
find /var/backups/agentos -mindepth 2 -maxdepth 2 -type f -mtime +7 -name artifacts.tar.gz -delete
find /var/backups/agentos -mindepth 1 -maxdepth 1 -type d -empty -delete
