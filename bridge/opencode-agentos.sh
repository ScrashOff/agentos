#!/bin/sh
# Dedicated profile; does not read or modify the production Harness credentials.
export HOME=/var/lib/agentos-bridge/opencode
export XDG_CONFIG_HOME="$HOME/.config"
export XDG_DATA_HOME="$HOME/.local/share"
export XDG_STATE_HOME="$HOME/.local/state"
export XDG_CACHE_HOME="$HOME/.cache"
export OPENCODE_DISABLE_AUTOUPDATE=true
export PATH=/opt/deepseek-harness/node/bin:/usr/local/bin:/usr/bin:/bin
exec /opt/agentos-opencode/node_modules/opencode-linux-x64/bin/opencode "$@"
