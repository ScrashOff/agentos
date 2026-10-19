#!/bin/sh
set -eu
for role in api workers; do
cat > /etc/systemd/system/agentos-$role.service <<EOF
[Unit]
Description=AgentOS $role (isolated control plane)
After=network-online.target postgresql@17-agentos.service
Wants=network-online.target
[Service]
Type=simple
WorkingDirectory=/opt/agentos/current
EnvironmentFile=/etc/agentos/server.env
Environment=AGENTOS_ROLE=$role
Environment=PATH=/opt/deepseek-harness/node/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ExecStart=/opt/deepseek-harness/node/bin/node --import tsx server/index.ts
Restart=on-failure
RestartSec=5
TimeoutStopSec=120
UMask=0077
[Install]
WantedBy=multi-user.target
EOF
done
cat > /etc/systemd/system/agentos-opencode.service <<'EOF'
[Unit]
Description=AgentOS OpenCode queue consumer
After=network-online.target postgresql@17-agentos.service
[Service]
WorkingDirectory=/opt/agentos/current
EnvironmentFile=/etc/agentos/server.env
Environment=PATH=/opt/deepseek-harness/node/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ExecStart=/opt/deepseek-harness/node/bin/node --import tsx scripts/opencode-batch.ts serve
Restart=on-failure
RestartSec=5
TimeoutStopSec=3600
UMask=0077
[Install]
WantedBy=multi-user.target
EOF
install -d -m 0755 /opt/agentos-control /opt/agentos-project-deps/agentos
cp /opt/agentos/current/scripts/agentos-release-action.py /opt/agentos-control/
cp -a /opt/agentos/current/node_modules /opt/agentos-project-deps/agentos/
chmod 700 /etc/agentos/ssh
cat > /etc/agentos/actions.json <<'EOF'
{
 "agentos.validate":{"label":"Validar nova versão do AgentOS","command":"/usr/bin/python3","args":["/opt/agentos-control/agentos-release-action.py","validate"],"effect":"read","timeoutSeconds":1900,"projects":["agentos"]},
 "agentos.publish":{"label":"Publicar versão validada do AgentOS","command":"/usr/bin/python3","args":["/opt/agentos-control/agentos-release-action.py","publish"],"effect":"write","timeoutSeconds":120,"projects":["agentos"]}
}
EOF
chmod 600 /etc/agentos/actions.json
systemctl daemon-reload
