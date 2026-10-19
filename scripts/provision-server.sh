#!/bin/sh
set -eu
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=l
install -d -m 0755 /usr/share/postgresql-common/pgdg /etc/postgresql-common
if ! command -v /usr/lib/postgresql/17/bin/pg_dump >/dev/null 2>&1; then
  curl --fail --silent --show-error https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
  printf '%s\n' 'Types: deb' 'URIs: https://apt.postgresql.org/pub/repos/apt' 'Suites: noble-pgdg' 'Components: main' 'Signed-By: /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc' > /etc/apt/sources.list.d/agentos-pgdg.sources
  test -f /etc/postgresql-common/createcluster.conf || printf 'create_main_cluster = false\n' > /etc/postgresql-common/createcluster.conf
  apt-get update -qq
  apt-get install -y -qq postgresql-17
fi
if ! test -d /etc/postgresql/17/agentos; then
  pg_createcluster 17 agentos --port=15482 --start -- --encoding=UTF8 --auth-local=peer --auth-host=scram-sha-256
fi
install -d -m 0750 /opt/agentos/releases /var/lib/agentos /var/backups/agentos /etc/agentos
pg_lsclusters
