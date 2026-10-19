#!/bin/sh
set -eu
install -m 0644 /opt/agentos/current/scripts/agentos-cycles.service /etc/systemd/system/agentos-cycles.service
mkdir -p /etc/systemd/system/agentos-workers.service.d
cat > /etc/systemd/system/agentos-workers.service.d/cycles.conf <<'EOF'
[Service]
Environment=AGENTOS_CYCLES_DEDICATED=1
EOF
systemctl daemon-reload
systemctl enable --now agentos-cycles.service
sh /opt/agentos/current/scripts/install-maintenance.sh
