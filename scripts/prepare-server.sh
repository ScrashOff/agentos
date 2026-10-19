#!/bin/sh
set -eu
export PATH=/opt/deepseek-harness/node/bin:$PATH
release=/opt/agentos/releases/20261006-cycles
mkdir -p "$release"
tar -xzf /tmp/agentos-release.tar.gz -C "$release"
cd "$release"
npm ci --ignore-scripts --no-audit --no-fund
install -d -m 0700 /etc/agentos/ssh
if ! test -f /etc/agentos/ssh/id_ed25519; then
  ssh-keygen -t ed25519 -N '' -f /etc/agentos/ssh/id_ed25519 -C agentos-local-control >/dev/null
  printf 'from="127.0.0.1",no-agent-forwarding,no-port-forwarding,no-X11-forwarding,no-pty ' >> /root/.ssh/authorized_keys
  cat /etc/agentos/ssh/id_ed25519.pub >> /root/.ssh/authorized_keys
fi
touch /root/.ssh/config
if ! grep -q '^Host agentos-local$' /root/.ssh/config; then
  printf '\nHost agentos-local\n HostName 127.0.0.1\n User root\n IdentityFile /etc/agentos/ssh/id_ed25519\n IdentitiesOnly yes\n StrictHostKeyChecking accept-new\n' >> /root/.ssh/config
fi
ssh -oBatchMode=yes agentos-local /opt/deepseek-harness/node/bin/node --version
echo 'AgentOS release and local bridge ready'
