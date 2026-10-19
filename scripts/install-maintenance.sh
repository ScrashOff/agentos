#!/bin/sh
set -eu
cat > /etc/systemd/system/agentos-rollout.service <<'EOF'
[Unit]
Description=Apply AgentOS worker update when the persisted queue is idle
[Service]
Type=oneshot
WorkingDirectory=/opt/agentos/current
EnvironmentFile=/etc/agentos/server.env
Environment=PATH=/opt/deepseek-harness/node/bin:/usr/bin:/bin
ExecStart=/opt/deepseek-harness/node/bin/node --import tsx scripts/rollout-workers.ts
EOF
cat > /etc/systemd/system/agentos-rollout.timer <<'EOF'
[Unit]
Description=Check pending AgentOS worker update
[Timer]
OnBootSec=60
OnUnitActiveSec=30
[Install]
WantedBy=timers.target
EOF
cat > /etc/systemd/system/agentos-backup.service <<'EOF'
[Unit]
Description=Back up isolated AgentOS database and artifacts
[Service]
Type=oneshot
UMask=0077
ExecStart=/bin/sh /opt/agentos/current/scripts/backup-server.sh
EOF
cat > /etc/systemd/system/agentos-backup.timer <<'EOF'
[Unit]
Description=Daily AgentOS backup
[Timer]
OnCalendar=*-*-* 04:20:00
Persistent=true
[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now agentos-rollout.timer agentos-backup.timer
