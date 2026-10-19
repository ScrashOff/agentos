import importlib.util
import pathlib
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

root = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('orphans', root / 'bridge/opencode-orphans.py')
orphans = importlib.util.module_from_spec(spec)
spec.loader.exec_module(orphans)


class OrphanProof(unittest.TestCase):
    def setUp(self):
        self.part = {'type': 'tool', 'tool': 'bash', 'state': {'status': 'running',
                     'time': {'start': 1000}, 'input': {'timeout': 90000}}}
        self.processes = [
            {'pid': 10, 'exe': 'python3', 'args': ['slot-runner.py']},
            {'pid': 11, 'exe': 'bwrap', 'namespacePid': 11},
            {'pid': 12, 'exe': 'bwrap', 'namespacePid': 1},
            {'pid': 13, 'exe': 'node', 'args': ['opencode-sandbox.mjs']},
            {'pid': 14, 'exe': 'opencode', 'args': ['run']},
            {'pid': 20, 'exe': 'node', 'args': ['server.mjs'], 'parent': 12,
             'state': 'S', 'startTicks': '80', 'cwd': '/workspace/data/repair-candidate'},
        ]

    def test_explicit_deadline_plus_grace_is_required(self):
        self.assertIsNone(orphans.expired_tool(self.part, 120999))
        self.assertEqual(orphans.expired_tool(self.part, 121000)['timeoutMs'], 90000)
        self.part['state']['input'].pop('timeout')
        self.assertIsNone(orphans.expired_tool(self.part, 999999))

    def test_invalid_times_or_completed_commands_are_not_expired(self):
        for value in (True, None, float('nan'), float('inf')):
            self.part['state']['time']['start'] = value
            self.assertIsNone(orphans.expired_tool(self.part, 999999))
        self.part['state']['time']['start'] = 1000
        self.part['state']['status'] = 'completed'
        self.assertIsNone(orphans.expired_tool(self.part, 999999))

    def test_only_owned_orphan_fixture_is_selected(self):
        self.assertEqual(orphans.orphan_servers(self.processes), [{'pid': 20, 'startTicks': '80', 'parent': 12}])

    def test_deleted_fixture_requires_confirmed_kernel_directory_identity(self):
        server = {**self.processes[-1], 'cwd': '/workspace/data/accepted-source (deleted)',
                  'cwdDeleted': True, 'cwdDevice': 202, 'cwdInode': 1234}
        self.assertEqual(orphans.orphan_servers(self.processes[:-1] + [server]),
                         [{'pid': 20, 'startTicks': '80', 'parent': 12, 'cwdDevice': 202, 'cwdInode': 1234}])

    def test_deleted_suffix_does_not_authorize_foreign_or_unverified_directory(self):
        server = {**self.processes[-1], 'cwd': '/workspace/data/accepted-source (deleted)',
                  'cwdDeleted': True, 'cwdDevice': 202, 'cwdInode': 1234}
        for change in ({'cwdDeleted': False}, {'cwdDeleted': None}, {'cwdInode': None},
                       {'cwdDevice': True}, {'cwd': '/opt/production (deleted)'},
                       {'cwd': '/workspace/data/accepted-source-other (deleted)'},
                       {'args': ['analysis.mjs']}, {'parent': 14}):
            self.assertEqual(orphans.orphan_servers(self.processes[:-1] + [{**server, **change}]), [])

    def test_changed_deleted_directory_identity_prevents_all_signals(self):
        expected = {'runId': 'run', 'servers': [{'pid': 20, 'startTicks': '80', 'parent': 12,
                     'cwdDevice': 202, 'cwdInode': 1234}]}
        with patch.object(orphans, 'inspect', return_value={**expected, 'servers': [
                {**expected['servers'][0], 'cwdInode': 1235}]}), \
                patch.object(orphans.os, 'pidfd_open', create=True) as opened:
            self.assertEqual(orphans.recover(expected), {'action': 'no-op'})
            opened.assert_not_called()

    @unittest.skipUnless(pathlib.Path('/proc/self/cwd').exists(), 'Linux proc directory required')
    def test_kernel_marks_unlinked_cwd_but_not_a_literal_deleted_suffix(self):
        with tempfile.TemporaryDirectory() as folder:
            cwd = pathlib.Path(folder) / 'fixture'
            cwd.mkdir()
            child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], cwd=cwd)
            try:
                before = orphans.process_cwd(pathlib.Path('/proc') / str(child.pid))
                self.assertFalse(before['cwdDeleted'])
                cwd.rmdir()
                after = orphans.process_cwd(pathlib.Path('/proc') / str(child.pid))
                self.assertTrue(after['cwdDeleted'])
                self.assertEqual(after['cwdInode'], before['cwdInode'])
                self.assertEqual(after['cwdDevice'], before['cwdDevice'])
                # An actually deleted directory outside the registered fixture
                # roots is still not an owned fixture that can be recovered.
                server = {**self.processes[-1], **after}
                self.assertEqual(orphans.orphan_servers(self.processes[:-1] + [server]), [])
            finally:
                child.terminate()
                child.wait(timeout=5)
            named = pathlib.Path(folder) / 'fixture (deleted)'
            named.mkdir()
            child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], cwd=named)
            try:
                self.assertFalse(orphans.process_cwd(pathlib.Path('/proc') / str(child.pid))['cwdDeleted'])
            finally:
                child.terminate()
                child.wait(timeout=5)

    def test_live_shell_test_browser_or_other_command_prevents_recovery(self):
        for exe, args in [('bash', ['-c']), ('node', ['test.mjs']), ('chrome', []), ('python3', ['analysis.py'])]:
            self.assertEqual(orphans.orphan_servers(self.processes + [{'pid': 30, 'exe': exe, 'args': args}]), [])

    def test_active_parent_foreign_workspace_or_running_server_is_not_orphan(self):
        for change in ({'parent': 30}, {'cwd': '/workspace'}, {'cwd': '/workspace/data/repair-candidate-other'},
                       {'cwd': '/opt/production'}, {'state': 'R'}):
            self.assertEqual(orphans.orphan_servers(self.processes[:-1] + [{**self.processes[-1], **change}]), [])

    def test_missing_or_ambiguous_namespace_init_refuses(self):
        self.assertEqual(orphans.orphan_servers(self.processes[3:]), [])
        self.assertEqual(orphans.orphan_servers(self.processes + [{'pid': 99, 'exe': 'bwrap', 'namespacePid': 1}]), [])

    def timeout_tree(self):
        self.part['state']['input']['command'] = "cat > /tmp/opencode/probe7.mjs <<'EOF'\n// fixture\nEOF\ntimeout 80 node /tmp/opencode/probe7.mjs; echo EXIT=$?"
        return self.processes + [
            {'pid': 30, 'exe': 'timeout', 'args': ['80', 'node', '/tmp/opencode/probe7.mjs'], 'parent': 12, 'startTicks': '110', 'cwd': '/workspace'},
            {'pid': 31, 'exe': 'node', 'args': ['/tmp/opencode/probe7.mjs'], 'parent': 30, 'startTicks': '111', 'cwd': '/workspace'},
            {'pid': 32, 'exe': 'chrome-headless-shell', 'args': [], 'parent': 31, 'startTicks': '112', 'state': 'R'},
        ]

    def test_expired_explicit_timeout_selects_only_its_subtree_not_agent_or_fixture(self):
        found = orphans.expired_timeout_processes(self.timeout_tree(), self.part, 122000, 0, 100)
        self.assertEqual([p['pid'] for p in found], [32, 31, 30])

    def test_expired_timeout_requires_both_deadlines_and_exact_current_command(self):
        tree = self.timeout_tree()
        self.assertEqual(orphans.expired_timeout_processes(tree, self.part, 120999, 0, 100), [])
        self.assertEqual(orphans.expired_timeout_processes(tree, self.part, 122000, 115000, 100), [])
        for command in ('timeout 80 node /tmp/opencode/other.mjs', 'timeout 80 node /opt/production/job.mjs',
                        'timeout 80 node /tmp/opencode/../../other.mjs', 'timeout 800 node /tmp/opencode/probe7.mjs',
                        'timeout 80 node /tmp/opencode/probe7.mjs; node another.mjs'):
            self.part['state']['input']['command'] = command
            self.assertEqual(orphans.expired_timeout_processes(tree, self.part, 999999, 0, 100), [])

    def literal_tree(self):
        tree = self.timeout_tree()
        self.part['state']['input']['command'] = "PLAYWRIGHT_BROWSERS_PATH=/opt/browsers timeout 80 node /tmp/opencode/probe7.mjs 18790; kill $(cat /tmp/probe7.pid) 2>/dev/null"
        tree[6]['args'].append('18790')
        tree[7]['args'].append('18790')
        tree[6]['cwd'] = tree[7]['cwd'] = '/tmp/opencode'
        return tree

    def test_real_literal_argument_env_and_cleanup_select_only_expired_tree(self):
        tree = self.literal_tree()
        with patch.object(orphans.subprocess, 'check_output') as shell:
            found = orphans.expired_timeout_processes(tree, self.part, 122000, 0, 100)
        self.assertEqual([p['pid'] for p in found], [32, 31, 30])
        shell.assert_not_called()  # Cleanup cat/kill is parsed, never evaluated.

    def test_literal_parser_accepts_bounded_quoted_argv_not_shell_expansion(self):
        parsed = orphans.literal_timeout_command("timeout 80s node '/tmp/opencode/probe7.mjs' '18790' --mode=qa")
        self.assertEqual(parsed, (80000, ['80s', 'node', '/tmp/opencode/probe7.mjs', '18790', '--mode=qa'],
                                  ['/tmp/opencode/probe7.mjs', '18790', '--mode=qa']))
        for argument in ('$PORT', '$(cat /tmp/pid)', '`id`', '*.json', 'a|b', 'x&&true',
                         '>/tmp/out', '${PORT}', 'a\\ b', "'a b'", '"$PORT"', "'unterminated"):
            with self.subTest(argument=argument):
                self.assertIsNone(orphans.literal_timeout_command('timeout 80 node /tmp/opencode/probe7.mjs ' + argument))

    def test_unknown_environment_cleanup_and_oversized_literals_refuse(self):
        for command in ('OTHER=value timeout 80 node /tmp/opencode/probe7.mjs',
                        'PLAYWRIGHT_BROWSERS_PATH=/other timeout 80 node /tmp/opencode/probe7.mjs',
                        'timeout 80 node /tmp/opencode/probe7.mjs; kill $(sh /tmp/script.pid) 2>/dev/null',
                        'timeout 80 node /tmp/opencode/probe7.mjs; kill $(cat /opt/prod.pid) 2>/dev/null',
                        'timeout 80 node /tmp/opencode/probe7.mjs; kill $(cat /tmp/../prod.pid) 2>/dev/null',
                        'timeout 80 node /tmp/opencode/probe7.mjs; kill $(cat /tmp/probe.pid) 2>/dev/null; true',
                        'timeout 80 node /tmp/opencode/probe7.mjs; echo EXIT=$?; node other.mjs',
                        'timeout 80 node /tmp/opencode/probe7.mjs ' + 'x'*257,
                        'timeout 80 node /tmp/opencode/probe7.mjs ' + 'x '*17,
                        'x'*65537, '', ' ', None, 'timeout 80 node /tmp/opencode/probe7.mjs\0'):
            with self.subTest(command=str(command)[:90]):
                self.assertIsNone(orphans.literal_timeout_command(command))

    def test_complete_wrapper_and_node_arguments_must_match_current_tool(self):
        for index, value in ((6, ['80', 'node', '/tmp/opencode/probe7.mjs']),
                             (6, ['80', 'node', '/tmp/opencode/probe7.mjs', '18791']),
                             (6, ['80', 'node', '/tmp/opencode/probe7.mjs', '18790', '--extra']),
                             (7, ['/tmp/opencode/probe7.mjs']),
                             (7, ['/tmp/opencode/probe7.mjs', '18791']),
                             (7, ['/tmp/opencode/probe7.mjs', '18790', ''])):
            tree = self.literal_tree(); tree[index]['args'] = value
            self.assertEqual(orphans.expired_timeout_processes(tree, self.part, 122000, 0, 100), [])

    def test_cwd_pair_and_absolute_tmp_script_are_required(self):
        for index, cwd in ((6, '/tmp'), (6, '/tmp/opencode/child'), (6, '/other'),
                           (7, '/workspace'), (7, '/other'), (7, None)):
            tree = self.literal_tree(); tree[index]['cwd'] = cwd
            self.assertEqual(orphans.expired_timeout_processes(tree, self.part, 122000, 0, 100), [])
        tree = self.literal_tree()
        self.part['state']['input']['command'] = 'timeout 80 node qa-protocol/probe7.mjs 18790'
        tree[6]['args'][2] = tree[7]['args'][0] = 'qa-protocol/probe7.mjs'
        self.assertEqual(orphans.expired_timeout_processes(tree, self.part, 122000, 0, 100), [])

    def test_complete_proc_argv_preserves_empty_and_late_arguments_and_fails_closed(self):
        self.assertEqual(orphans.bounded_process_argv(b'/usr/bin/timeout\0' + b'80\0node\0/tmp/opencode/test.mjs\0' + b'18790\0\0'),
                         ['/usr/bin/timeout', '80', 'node', '/tmp/opencode/test.mjs', '18790', ''])
        for raw in (b'', b'\0', b'node', b'node\0\xff\0', b'n\0' * 513, b'x' * 1048576 + b'\0'):
            self.assertIsNone(orphans.bounded_process_argv(raw))

    def test_literal_tree_retains_deadline_adoption_unknown_process_and_incarnation_guards(self):
        tree = self.literal_tree()
        self.assertEqual(orphans.expired_timeout_processes(tree, self.part, 120999, 0, 100), [])
        for change in ({'parent': 14}, {'startTicks': '1'}):
            altered = [dict(p) for p in tree]; altered[6].update(change)
            self.assertEqual(orphans.expired_timeout_processes(altered, self.part, 122000, 0, 100), [])
        self.assertEqual(orphans.expired_timeout_processes(tree + [{'pid':99,'exe':'bash','args':['-c'],'parent':12}], self.part, 122000, 0, 100), [])

    def test_container_age_is_not_the_kernel_process_start_clock(self):
        tree = self.timeout_tree()
        # Same current tool/tree: a 14-day uptime translation would pretend
        # the helper was born in the future and keep this expired tool stuck.
        self.assertEqual(orphans.expired_timeout_processes(tree, self.part, 122000, 1264508000, 100), [])
        self.assertEqual([p['pid'] for p in orphans.expired_timeout_processes(tree, self.part, 122000, 0, 100)], [32, 31, 30])

    def test_unrelated_or_ambiguous_commands_prevent_timeout_cleanup(self):
        tree = self.timeout_tree()
        for extra in ({'pid': 99, 'exe': 'node', 'args': ['unrelated.mjs'], 'parent': 12},
                      {**tree[-3], 'pid': 99}):
            self.assertEqual(orphans.expired_timeout_processes(tree + [extra], self.part, 122000, 0, 100), [])
        for change in ({'parent': 14}, {'cwd': '/other'}, {'startTicks': '1'}):
            altered = tree[:6] + [{**tree[6], **change}] + tree[7:]
            self.assertEqual(orphans.expired_timeout_processes(altered, self.part, 122000, 0, 100), [])

    def test_changed_expired_tool_subtree_prevents_all_signals(self):
        expected = {'runId': 'run', 'expiredProcesses': [{'pid': 30, 'startTicks': '80', 'parent': 12}]}
        with patch.object(orphans, 'inspect', return_value={**expected, 'expiredProcesses': [{'pid': 30, 'startTicks': '81', 'parent': 12}]}), \
                patch.object(orphans.os, 'pidfd_open', create=True) as opened:
            self.assertEqual(orphans.recover(expected), {'action': 'no-op'})
            opened.assert_not_called()

    def test_changed_process_incarnation_prevents_all_signals(self):
        expected = {'runId': 'run', 'servers': [{'pid': 20, 'startTicks': '80'}]}
        with patch.object(orphans, 'inspect', return_value={**expected, 'servers': [{'pid': 20, 'startTicks': '81'}]}), \
                patch.object(orphans.os, 'pidfd_open', create=True) as opened:
            self.assertEqual(orphans.recover(expected), {'action': 'no-op'})
            opened.assert_not_called()

    def test_missing_runtime_or_new_command_prevents_all_signals(self):
        expected = {'runId': 'run'}
        with patch.object(orphans, 'inspect', return_value=None), \
                patch.object(orphans.os, 'pidfd_open', create=True) as opened:
            self.assertEqual(orphans.recover(expected), {'action': 'no-op'})
            opened.assert_not_called()

    @unittest.skipUnless(hasattr(os, 'pidfd_open') and hasattr(orphans.signal, 'pidfd_send_signal'), 'Linux pidfd required')
    def test_expired_helper_ignoring_term_is_killed_without_touching_other_process_or_files(self):
        child = subprocess.Popen([sys.executable, '-c', 'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)'])
        control = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
        try:
            with tempfile.TemporaryDirectory() as folder:
                evidence = pathlib.Path(folder) / 'implementation.patch'
                evidence.write_text('Exact preserved bytes')
                stat = pathlib.Path('/proc').joinpath(str(child.pid), 'stat').read_text().split(') ', 1)[1].split()
                group = pathlib.Path('/proc/self/cgroup').read_text().strip()
                proof = {'runId': 'run', 'unit': group, 'toolPartId': 'tool', 'timeoutMs': 90000,
                         'expiredProcesses': [{'pid': child.pid, 'startTicks': stat[19], 'parent': os.getpid()}]}
                with patch.object(orphans, 'inspect', return_value=proof):
                    result = orphans.recover(proof)
                self.assertEqual(result['reason'], 'EXPIRED_TOOL_TIMEOUT_CONFIRMED')
                self.assertEqual(result['pids'], [child.pid])
                child.wait(timeout=5)
                self.assertEqual(child.returncode, -orphans.signal.SIGKILL)
                self.assertIsNone(control.poll())
                self.assertEqual(evidence.read_text(), 'Exact preserved bytes')
        finally:
            for process in (child, control):
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)

    @unittest.skipUnless(hasattr(os, 'pidfd_open') and hasattr(orphans.signal, 'pidfd_send_signal'), 'Linux pidfd required')
    def test_one_changed_child_prevents_signals_to_the_entire_expired_subtree(self):
        children = [subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']) for _ in range(2)]
        try:
            refs = []
            for child in children:
                stat = pathlib.Path('/proc').joinpath(str(child.pid), 'stat').read_text().split(') ', 1)[1].split()
                refs.append({'pid': child.pid, 'startTicks': stat[19], 'parent': os.getpid()})
            refs[-1]['startTicks'] = str(int(refs[-1]['startTicks']) + 1)
            proof = {'runId': 'run', 'unit': pathlib.Path('/proc/self/cgroup').read_text().strip(), 'expiredProcesses': refs}
            with patch.object(orphans, 'inspect', return_value=proof):
                self.assertEqual(orphans.recover(proof), {'action': 'no-op'})
            self.assertTrue(all(child.poll() is None for child in children))
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate()
                    child.wait(timeout=5)

    @unittest.skipUnless(hasattr(os, 'pidfd_open') and hasattr(orphans.signal, 'pidfd_send_signal'), 'Linux pidfd required')
    def test_signals_only_verified_child_and_preserves_files(self):
        with tempfile.TemporaryDirectory() as folder:
            evidence = pathlib.Path(folder) / 'implementation.patch'
            evidence.write_text('Preserved implementation bytes')
            child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
            try:
                stat = pathlib.Path('/proc').joinpath(str(child.pid), 'stat').read_text().split(') ', 1)[1].split()
                proof = {'runId': 'run', 'toolPartId': 'tool', 'timeoutMs': 90000,
                         'servers': [{'pid': child.pid, 'startTicks': stat[19], 'parent': os.getpid()}]}
                with patch.object(orphans, 'inspect', return_value=proof):
                    result = orphans.recover(proof)
                self.assertEqual(result['pids'], [child.pid])
                child.wait(timeout=5)
                self.assertEqual(evidence.read_text(), 'Preserved implementation bytes')
                self.assertTrue(result['filesUntouched'])
                self.assertTrue(result['runtimeUntouched'])
            finally:
                if child.poll() is None:
                    child.terminate()
                    child.wait(timeout=5)


unittest.main()
