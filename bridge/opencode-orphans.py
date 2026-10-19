"""Recover only expired shell tools whose command exited and left a test server.

Read the isolated runtime's tool table; never change the database or inspect
credentials. Age alone cannot authorize a signal. Every remaining process must
be a known runtime process or an orphaned server in an owned fixture directory.
"""
import json
import math
import os
import pathlib
import re
import signal
import shlex
import hashlib
import sqlite3
import stat as stat_mode
import subprocess
import sys
import time

UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
FIXTURE_DIRS = ('/workspace/data/repair-candidate', '/workspace/data/repair-before', '/workspace/data/accepted-source')


def process_cwd(root):
    cwd = os.readlink(root / 'cwd')
    info = (root / 'cwd').stat()
    return {'cwd': cwd, 'cwdDevice': info.st_dev, 'cwdInode': info.st_ino,
            'cwdDeleted': cwd.endswith(' (deleted)') and info.st_nlink == 0
            and stat_mode.S_ISDIR(info.st_mode)}


def owned_fixture_cwd(process):
    cwd = process.get('cwd', '')
    if cwd.endswith(' (deleted)'):
        # The kernel, not a filename convention or age, must attest that this
        # is the same unlinked directory. A literal suffix grants nothing.
        if (process.get('cwdDeleted') is not True
                or type(process.get('cwdDevice')) is not int
                or type(process.get('cwdInode')) is not int
                or process['cwdInode'] <= 0):
            return False
        cwd = cwd[:-len(' (deleted)')]
    return any(cwd == d or cwd.startswith(d + '/') for d in FIXTURE_DIRS)


def expired_tool(part, now_ms):
    state = part.get('state', {})
    start = state.get('time', {}).get('start')
    timeout = state.get('input', {}).get('timeout')
    if (part.get('type') != 'tool' or part.get('tool') != 'bash'
            or state.get('status') != 'running'
            or isinstance(start, bool) or not isinstance(start, (int, float))
            or isinstance(timeout, bool) or not isinstance(timeout, (int, float))
            or not math.isfinite(start) or not math.isfinite(timeout)
            or not 1000 <= timeout <= 3600000
            or now_ms < start + timeout + 30000):
        return None
    return {'startedAt': start, 'timeoutMs': timeout}


def orphan_servers(processes):
    initializers = {p['pid'] for p in processes
                    if p['exe'] == 'bwrap' and p.get('namespacePid') == 1}
    if len(initializers) != 1:
        return []
    found = []
    for p in processes:
        exe, args = p['exe'], p.get('args', [])
        if exe == 'bwrap':
            continue
        if exe == 'python3' and args and pathlib.PurePosixPath(args[0]).name == 'slot-runner.py':
            continue
        if exe == 'opencode' and args and args[0] == 'run':
            continue
        if exe == 'node' and args and pathlib.PurePosixPath(args[0]).name == 'opencode-sandbox.mjs':
            continue
        if (exe == 'node' and args and pathlib.PurePosixPath(args[0]).name == 'server.mjs'
                and p['parent'] in initializers and p.get('state') == 'S'
                and owned_fixture_cwd(p)):
            identity = {k: p[k] for k in ('cwdDevice', 'cwdInode')} if p.get('cwdDeleted') else {}
            found.append({'pid': p['pid'], 'startTicks': p['startTicks'], 'parent': p['parent'], **identity})
            continue
        # A shell, running test, browser, non-orphan server, or any unknown child
        # means there can still be real work; never infer its death from age.
        return []
    return sorted(found, key=lambda p: p['pid'])


def bounded_process_argv(raw):
    # Never truncate identities: an omitted fourth argument changes the command.
    if not raw or len(raw) > 1048576 or not raw.endswith(b'\0'):
        return None
    fields = raw[:-1].split(b'\0')
    if not fields[0] or len(fields) > 512:
        return None
    try:
        return [field.decode('utf-8', errors='strict') for field in fields]
    except UnicodeError:
        return None


def literal_timeout_command(command):
    """Recognize one bounded literal timeout invocation; never execute shell text.

    A legacy exit echo or the exact test-server cleanup grammar may follow it.
    Neither cleanup nor command substitutions are evaluated by this inspector.
    The wrapper must already be adopted by namespace init, so no live shell or
    subsequent command is inferred safe from parsing alone.
    """
    if not isinstance(command, str) or not command.strip() or len(command) > 65536 or '\0' in command:
        return None
    final = command.strip().splitlines()[-1]
    invocation, separator, cleanup = final.partition(';')
    if separator and not (re.fullmatch(r'\s*echo\s+[A-Z_]+=["\']?\$\?["\']?\s*', cleanup)
            or re.fullmatch(r'\s*kill\s+\$\(cat\s+(?:/tmp/opencode/|/tmp/)[A-Za-z0-9_-][A-Za-z0-9_.-]{0,100}\.pid\)\s+2>/dev/null\s*', cleanup)):
        return None
    # Shell expansion/control operators are forbidden in the invocation, even
    # when quoting might make them literal. shlex is only a tokenizer here.
    if re.search(r'[^A-Za-z0-9_./:=+\- \t\'\"]', invocation):
        return None
    try:
        args = shlex.split(invocation, posix=True)
    except ValueError:
        return None
    if args and args[0] == 'PLAYWRIGHT_BROWSERS_PATH=/opt/browsers':
        args = args[1:]
    if (not 4 <= len(args) <= 20 or args[0] != 'timeout' or args[2] != 'node'
            or not re.fullmatch(r'[1-9][0-9]{0,3}s?', args[1])
            or not re.fullmatch(r'(?:/tmp/opencode/|/workspace/qa-protocol/|qa-protocol/)[A-Za-z0-9_-][A-Za-z0-9_.-]{0,100}\.mjs', args[3])
            or any(not re.fullmatch(r'[A-Za-z0-9_./:=+\-]{1,256}', arg) for arg in args[4:])):
        return None
    return int(args[1].rstrip('s')) * 1000, args[1:], args[3:]


def expired_timeout_processes(processes, part, now_ms, boot_ms, ticks_per_second):
    """A GNU timeout may send TERM while Playwright's cleanup never returns.

    Require the current bash tool's explicit deadline AND its exact final
    timeout command. Only that adopted command subtree can be force-stopped;
    runtime processes and separately started fixture servers stay untouched.
    """
    if not expired_tool(part, now_ms):
        return []
    command = part.get('state', {}).get('input', {}).get('command')
    parsed = literal_timeout_command(command)
    if not parsed:
        return []
    duration, wrapper_args, node_args = parsed
    if duration > part['state']['input']['timeout'] or duration > 3600000:
        return []
    initializers = {p['pid'] for p in processes if p['exe'] == 'bwrap' and p.get('namespacePid') == 1}
    if len(initializers) != 1:
        return []
    wrappers = [p for p in processes if p['exe'] == 'timeout' and p.get('parent') in initializers
                and p.get('args') == wrapper_args
                and (p.get('cwd') == '/workspace' or
                     (p.get('cwd') == '/tmp/opencode' and node_args[0].startswith('/tmp/opencode/')))]
    if len(wrappers) != 1:
        return []
    wrapper = wrappers[0]
    started = boot_ms + int(wrapper['startTicks']) * 1000 / ticks_per_second
    if started < part['state']['time']['start'] - 100 or now_ms < started + duration + 30000:
        return []
    if not any(p['exe'] == 'node' and p.get('parent') == wrapper['pid'] and p.get('args') == node_args and p.get('cwd') == wrapper.get('cwd') for p in processes):
        return []
    depths = {wrapper['pid']: 0}
    while True:
        extra = {p['pid']: depths[p['parent']] + 1 for p in processes
                 if p.get('parent') in depths and p['pid'] not in depths}
        if not extra:
            break
        depths.update(extra)
    remainder = [p for p in processes if p['pid'] not in depths]
    for p in remainder:
        exe, args = p['exe'], p.get('args', [])
        if exe == 'bwrap' or (exe == 'python3' and args and pathlib.PurePosixPath(args[0]).name == 'slot-runner.py'):
            continue
        if (exe == 'opencode' and args and args[0] == 'run') or (exe == 'node' and args and pathlib.PurePosixPath(args[0]).name == 'opencode-sandbox.mjs'):
            continue
        if (exe == 'node' and args == ['server.mjs'] and p.get('parent') in initializers and p.get('state') == 'S'
                and any(p.get('cwd') == d or p.get('cwd', '').startswith(d + '/') for d in FIXTURE_DIRS)):
            continue
        return []
    selected = [p for p in processes if p['pid'] in depths]
    return [{'pid': p['pid'], 'startTicks': p['startTicks'], 'parent': p['parent']}
            for p in sorted(selected, key=lambda p: (-depths[p['pid']], p['pid']))]


def inspect(run_id):
    if not UUID.fullmatch(run_id):
        return None
    home = pathlib.Path('/var/lib/agentos-bridge/worker-runs') / run_id / 'home'
    db = home / '.local/share/opencode/opencode.db'
    if not db.is_file() or db.resolve() != db or home.resolve() != home:
        return None
    with sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=0.1) as connection:
        rows = connection.execute("SELECT id,data FROM part WHERE json_extract(data,'$.type')='tool' "
                                  "AND json_extract(data,'$.tool')='bash' "
                                  "AND json_extract(data,'$.state.status')='running' "
                                  "ORDER BY time_created DESC LIMIT 2").fetchall()
    if len(rows) != 1:
        return None
    part = json.loads(rows[0][1])
    now_ms = time.time() * 1000
    tool = expired_tool(part, now_ms)
    if not tool:
        return None
    unit = 'agentos-run-' + run_id + '.service'
    props = subprocess.check_output(['systemctl', 'show', unit, '-p', 'ControlGroup', '-p', 'ActiveState'],
                                    text=True, timeout=4)
    props = dict(line.split('=', 1) for line in props.strip().splitlines())
    if props.get('ActiveState') != 'active' or props.get('ControlGroup') != '/system.slice/' + unit:
        return None
    cgroup = pathlib.Path('/sys/fs/cgroup') / props['ControlGroup'].lstrip('/')
    # Nested cgroups could hide active commands from the direct process list.
    if list(cgroup.glob('*/cgroup.procs')):
        return None
    processes = []
    for pid in cgroup.joinpath('cgroup.procs').read_text().splitlines():
        root = pathlib.Path('/proc') / pid
        with root.joinpath('cmdline').open('rb') as command_file:
            argv = bounded_process_argv(command_file.read(1048577))
        if argv is None:
            return None
        stat = root.joinpath('stat').read_text().split(') ', 1)[1].split()
        status = root.joinpath('status').read_text()
        ns = re.search(r'^NSpid:\s+(.*)$', status, re.M)
        processes.append({'pid': int(pid), 'exe': pathlib.PurePosixPath(argv[0]).name,
                          'args': argv[1:],
                          'state': stat[0], 'parent': int(stat[1]), 'startTicks': stat[19],
                          'namespacePid': int(ns[1].split()[-1]) if ns else None,
                          **process_cwd(root)})
    servers = orphan_servers(processes)
    if servers:
        return {'runId': run_id, 'unit': unit, 'toolPartId': rows[0][0], **tool, 'servers': servers}
    # LXC may virtualize /proc/uptime to container age while proc start ticks
    # still use the kernel clock. CLOCK_BOOTTIME shares their actual basis.
    boot_ms = now_ms - time.clock_gettime(time.CLOCK_BOOTTIME) * 1000
    expired = expired_timeout_processes(processes, part, now_ms, boot_ms, os.sysconf('SC_CLK_TCK'))
    if expired:
        return {'runId': run_id, 'unit': unit, 'toolPartId': rows[0][0], **tool, 'expiredProcesses': expired,
                'commandSha256': hashlib.sha256(part['state']['input']['command'].encode()).hexdigest()}
    return None


def recover(expected):
    # A second observation from the caller is followed by a fresh kernel/runtime
    # inspection here. A changed command, process set or PID incarnation refuses.
    current = inspect(expected.get('runId', ''))
    if not current or current != expected:
        return {'action': 'no-op'}
    if current.get('expiredProcesses'):
        # Acquire and check every kernel handle before any signal. Children are
        # ordered before parents; no runtime or other tool is in this subtree.
        handles = []
        try:
            for child in current['expiredProcesses']:
                fd = os.pidfd_open(child['pid'])
                handles.append(fd)
                root = pathlib.Path('/proc') / str(child['pid'])
                stat = root.joinpath('stat').read_text().split(') ', 1)[1].split()
                if stat[19] != child['startTicks'] or int(stat[1]) != child['parent'] or current['unit'] not in root.joinpath('cgroup').read_text():
                    return {'action': 'no-op'}
            terminated = []
            for child, fd in zip(current['expiredProcesses'], handles):
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                    terminated.append(child['pid'])
                except ProcessLookupError:
                    pass  # A descendant may exit as an earlier child closes.
            return {'action': 'terminated' if terminated else 'no-op', 'pids': terminated, 'runId': current['runId'],
                    'toolPartId': current['toolPartId'], 'timeoutMs': current['timeoutMs'],
                    'reason': 'EXPIRED_TOOL_TIMEOUT_CONFIRMED', 'filesUntouched': True, 'runtimeUntouched': True}
        except OSError:
            return {'action': 'no-op'}
        finally:
            for fd in handles:
                os.close(fd)
    handles = []
    try:
        # Check every orphan before signalling any of them. A replaced PID,
        # directory or cgroup cannot turn an old proof into a partial cleanup.
        for server in current['servers']:
            fd = os.pidfd_open(server['pid'])
            handles.append(fd)
            root = pathlib.Path('/proc') / str(server['pid'])
            stat = root.joinpath('stat').read_text().split(') ', 1)[1].split()
            if stat[19] != server['startTicks'] or int(stat[1]) != server['parent']:
                return {'action': 'no-op'}
            if current.get('unit') and current['unit'] not in root.joinpath('cgroup').read_text():
                return {'action': 'no-op'}
            if 'cwdInode' in server:
                cwd = process_cwd(root)
                if not cwd['cwdDeleted'] or any(cwd[k] != server[k] for k in ('cwdDevice', 'cwdInode')):
                    return {'action': 'no-op'}
        terminated = []
        for server, fd in zip(current['servers'], handles):
            try:
                signal.pidfd_send_signal(fd, signal.SIGTERM)
                terminated.append(server['pid'])
            except ProcessLookupError:
                pass
        return {'action': 'terminated' if terminated else 'no-op', 'pids': terminated,
                'runId': current['runId'], 'toolPartId': current['toolPartId'],
                'timeoutMs': current['timeoutMs'], 'reason': 'EXPIRED_TOOL_ORPHAN_CONFIRMED',
                'filesUntouched': True, 'runtimeUntouched': True}
    except OSError:
        return {'action': 'no-op'}
    finally:
        for fd in handles:
            os.close(fd)


if __name__ == '__main__':
    try:
        if len(sys.argv) == 3 and sys.argv[1] == 'inspect':
            result = inspect(sys.argv[2])
        elif len(sys.argv) == 2 and sys.argv[1] == 'recover':
            raw = sys.stdin.read(8193)
            result = recover(json.loads(raw)) if len(raw) <= 8192 else None
        else:
            result = None
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError, AttributeError, KeyError):
        # Missing platform features/unstable observations never grant permission.
        result = None
    print(json.dumps(result))
