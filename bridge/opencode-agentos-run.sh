#!/bin/sh
# For automation with the prompt in argv; stdin must reach EOF before run starts.
if [ "$#" -eq 0 ]; then
  echo 'Usage: /opt/agentos-opencode/run [options] "prompt"' >&2
  exit 2
fi
exec /opt/agentos-opencode/opencode run "$@" </dev/null
