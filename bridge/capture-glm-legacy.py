"""Inventory existing isolated OpenCode gateway owners without reading secrets."""
import json
import pathlib
import re
import sys
import os

root = pathlib.Path('/proc')
sockets = {}
for line in (root / 'net/unix').read_text().splitlines()[1:]:
    fields = line.split()
    if len(fields) >= 8:
        sockets[fields[6]] = fields[7]
rows = []
for process in root.iterdir():
    if not process.name.isdigit():
        continue
    try:
        args = (process / 'cmdline').read_bytes().split(b'\0')
        if b'/opt/agentos-bridge/opencode-executor.mjs' not in args:
            continue
        stat = (process / 'stat').read_text().rsplit(')', 1)[1].split()
        run_ids = set()
        for fd in (process / 'fd').iterdir():
            try:
                link = str(fd.readlink())
                match = re.fullmatch(r'socket:\[(\d+)\]', link)
                if match:
                    path = sockets.get(match[1], '')
                    run = re.search(r'/worker-runs/([0-9a-f-]{36})/', path)
                    if run:
                        run_ids.add(run[1])
            except FileNotFoundError:
                pass
        if len(run_ids) != 1:
            raise RuntimeError('GATEWAY_IDENTITY_UNCERTAIN_' + process.name)
        rows.append({'runId': run_ids.pop(), 'pid': int(process.name), 'startTime': stat[19]})
    except FileNotFoundError:
        continue
result = {'reservations': rows}
if len(sys.argv) == 3 and sys.argv[1] == '--output':
    output = pathlib.Path(sys.argv[2])
    if output.parent != pathlib.Path('/var/lib/agentos-bridge'):
        raise RuntimeError('INVALID_RESERVATION_PATH')
    temporary = output.with_suffix('.tmp')
    temporary.write_text(json.dumps(result))
    os.chmod(temporary, 0o600)
    temporary.replace(output)
json.dump(result, sys.stdout)
print()
