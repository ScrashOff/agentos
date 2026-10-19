import importlib.util, pathlib, tempfile, unittest
from types import SimpleNamespace
from unittest.mock import patch

BROKER = pathlib.Path(__file__).parents[2] / 'scripts' / 'agentos-published-rebase.py'
SPEC = importlib.util.spec_from_file_location('published_rebase_contract_target', BROKER)
TARGET = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TARGET)

def make_case():
    goal_id = '11111111-1111-4111-8111-111111111111'
    task_id = '22222222-2222-4222-8222-222222222222'
    run_id = '33333333-3333-4333-8333-333333333333'
    digest = lambda c: c * 64
    files = {'server/index.ts': TARGET.sha(b'new published source')}
    published = {
        'publishedBaseline': '/opt/agentos/releases/managed-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
        'publishedBaselineDigest': digest('b'), 'sourceManifestDigest': TARGET.sha(TARGET.canonical(files)),
        'sourceHashes': files, 'docsGitByteHashes': {}, 'publicationProofRef': '/var/lib/agentos-build/pub.json',
        'publicationProofDigest': digest('c'), 'validationProofRef': '/var/lib/agentos-build/validation.json',
        'validationProofDigest': digest('d'), 'registeredReceiptPath': '/var/lib/agentos/releases/receipt.json',
        'registeredReceiptSha': digest('e'), 'registeredActionSha': digest('f'),
    }
    baseline = {'version': 1, 'release': published['publishedBaseline'],
        'releaseDigest': published['publishedBaselineDigest'], 'sourceWorkspace': 'agentos/src' + digest('b')[:16],
        'sourceManifestHashes': files, 'sourceManifestDigest': published['sourceManifestDigest'],
        'generatedReleaseHashes': {}, 'sealedReleaseExtraHashes': {}, 'workspaceManifestDigest': TARGET.sha(TARGET.canonical(files)),
        'publicationProofRef': published['publicationProofRef'], 'publicationProofDigest': published['publicationProofDigest'],
        'validationProofRef': published['validationProofRef'], 'validationProofDigest': published['validationProofDigest'],
        'registeredReceiptRef': published['registeredReceiptPath'], 'registeredReceiptDigest': published['registeredReceiptSha'],
        'registeredActionDigest': published['registeredActionSha']}
    req = {'version': 1, 'actionId': '44444444-4444-4444-8444-444444444444', 'project': 'agentos', 'goalId': goal_id,
        'frameworkAuthorization': {'kind': 'registered_agentos_source', 'project': 'agentos', 'goalId': goal_id},
        'taskId': task_id, 'expectedAttempt': 2, 'taskState': 'failed', 'stage': 'integrate',
        'runId': run_id, 'runFence': '55555555-5555-4555-8555-555555555555',
        'contextVersion': '66666666-6666-4666-8666-666666666666', 'runState': 'submitted',
        'currentWorkspace': 'agentos/o-source', 'priorSourceWorkspace': 'agentos/srcold', 'baseWorkspace': 'agentos/base',
        'workKind': None, 'maxAttemptsBefore': 2,
        'criteriaDigest': TARGET.sha(TARGET.canonical([{'id': 'ready'}])), 'dependencyIds': [],
        'publishedBaseline': baseline, 'sourceWorkspace': baseline['sourceWorkspace'],
        'publishedBaselineDigest': baseline['releaseDigest'], 'sourceManifestDigest': baseline['sourceManifestDigest'],
        'workspaceManifestDigest': baseline['workspaceManifestDigest'],
        'baselineWorkspace': 'agentos/pub' + baseline['releaseDigest'],
        'targetWorkspace': 'agentos/o-rb-44444444444444448444444444444444', 'candidateDigest': '7' * 64,
        'candidateArtifactId': '77777777-7777-4777-8777-777777777777', 'candidateArtifactDigest': '8' * 64,
        'candidateArtifactSize': 12, 'dependencyManifest': [], 'dependencyDigest': TARGET.sha(TARGET.canonical([])),
        'reason': 'Rebuild from the protected current publication while retaining the accepted inputs.'}
    payload = dict(req)
    payload.pop('state', None)
    for field in ('startedAt', 'prepared', 'preparedAt', 'errorCode', 'updatedAt', 'appliedAt'):
        payload.pop(field, None)
    req['requestDigest'] = TARGET.sha(TARGET.canonical(payload))
    authority = {'request': req, 'goal': {'id': goal_id, 'project': 'agentos', 'state': 'active'},
        'task': {'id': task_id, 'goalId': goal_id, 'state': 'failed', 'attempt': 2, 'stage': 'integrate',
          'workspace': 'agentos/o-source', 'sourceWorkspace': 'agentos/srcold', 'baseWorkspace': 'agentos/base',
          'workKind': None, 'maxAttempts': 2, 'criteria': [{'id': 'ready'}]},
        'run': {'id': run_id, 'fence': req['runFence'], 'contextId': req['contextVersion'], 'attempt': 2,
          'state': 'submitted', 'workspace': 'agentos/o-source'}, 'managerAccepted': True, 'dependencies': [], 'registry': None}
    return authority, req['actionId'], published, files

class PublishedRebaseAuthority(unittest.TestCase):
    def test_registered_alias_requires_complete_physical_release_inventory(self):
        source = {'server/index.ts': '1' * 64}
        generated = {'dist/assets/app.js': '2' * 64, 'bridge/__pycache__/workspace_compose.cpython312.pyc': '3' * 64}
        physical = {**source, **generated}
        self.assertTrue(TARGET.registered_inventory_matches(physical, source, generated))
        self.assertFalse(TARGET.registered_inventory_matches(source, source, generated))
        self.assertFalse(TARGET.registered_inventory_matches({**physical, 'dist/unknown.js': '4' * 64}, source, generated))
        self.assertFalse(TARGET.registered_inventory_matches({**source, 'bridge/__pycache__/workspace_compose.cpython312.pyc': '0' * 64}, source, generated))

    def test_only_source_manifest_enters_baseline_digest_or_copy_map(self):
        _, _, published, files = make_case()
        generated = {'dist/assets/app.js': '2' * 64, 'bridge/__pycache__/workspace_compose.cpython312.pyc': '3' * 64}
        with patch.object(TARGET, 'protected_release_outputs', return_value={**files, **generated}):
            observed_generated, copied, workspace_digest = TARGET.complete_workspace_manifest(published, files)
        self.assertEqual(observed_generated, generated)
        self.assertEqual(copied, files)
        self.assertEqual(workspace_digest, TARGET.sha(TARGET.canonical(files)))
        baseline = make_case()[0]['request']['publishedBaseline']
        self.assertEqual(baseline['generatedReleaseHashes'], {})
        self.assertEqual(baseline['sealedReleaseExtraHashes'], {})
        self.assertNotIn('dist/assets/app.js', copied)
        self.assertNotIn('bridge/__pycache__/workspace_compose.cpython312.pyc', copied)

    def test_canonical_serialization_matches_typescript_utf8_bytes_for_unicode(self):
        value = {'a': 'a\u00e7\u00e3o \U0001F600'}
        self.assertEqual(TARGET.canonical(value), bytes.fromhex('7b2261223a2261c3a7c3a36f20f09f9880227d'))

    def test_valid_current_published_baseline_and_unchanged_contract(self):
        authority, action_id, published, files = make_case()
        with patch.object(TARGET, 'complete_workspace_manifest',
            side_effect=lambda _published, source: ({}, source, TARGET.sha(TARGET.canonical(source)))):
            self.assertEqual(TARGET.verify_authority(authority, action_id, published, files), authority['request'])

    def test_refuses_project_goal_or_task_drift(self):
        for location, key, value, code in [
            ('goal', 'project', 'other-project', 'REBASE_AUTHORITY_INVALID'),
            ('task', 'state', 'accepted', 'REBASE_BINDING_STALE'),
            ('task', 'attempt', 3, 'REBASE_BINDING_STALE'),
            ('task', 'maxAttempts', 99, 'REBASE_BINDING_STALE'),
            ('task', 'criteria', [{'id': 'changed'}], 'REBASE_CRITERIA_CHANGED'),
        ]:
            with self.subTest(location=location, key=key):
                authority, action_id, published, files = make_case()
                authority[location][key] = value
                with patch.object(TARGET, 'complete_workspace_manifest',
                    side_effect=lambda _published, source: ({}, source, TARGET.sha(TARGET.canonical(source)))):
                    with self.assertRaisesRegex(ValueError, code):
                        TARGET.verify_authority(authority, action_id, published, files)

    def test_unicode_reason_keeps_request_digest_bound_to_typescript_bytes(self):
        authority, action_id, published, files = make_case()
        req = authority['request']
        req['reason'] = 'Rebase da publica\u00e7\u00e3o; crit\u00e9rios e seguran\u00e7a \U0001F9EA\U0001F642.'
        payload = dict(req)
        payload.pop('requestDigest', None)
        req['requestDigest'] = TARGET.sha(TARGET.canonical(payload))
        with patch.object(TARGET, 'complete_workspace_manifest',
            side_effect=lambda _published, source: ({}, source, TARGET.sha(TARGET.canonical(source)))):
            self.assertEqual(TARGET.verify_authority(authority, action_id, published, files)['reason'], req['reason'])

    def test_refuses_unaccepted_or_changed_dependency_manifest(self):
        authority, action_id, published, files = make_case()
        authority['dependencies'] = [{'taskId': '99999999-9999-4999-8999-999999999999',
          'goalId': authority['goal']['id'], 'state': 'submitted', 'runId': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'}]
        with self.assertRaisesRegex(ValueError, 'REBASE_DEPENDENCIES_CHANGED'):
            TARGET.verify_authority(authority, action_id, published, files)

    def test_refuses_publication_or_candidate_workspace_substitution(self):
        for change in ('publishedBaselineDigest', 'currentWorkspace'):
            with self.subTest(change=change):
                authority, action_id, published, files = make_case()
                if change == 'publishedBaselineDigest':
                    authority['request'][change] = '9' * 64
                else:
                    authority['request'][change] = 'agentos/o-foreign'
                req = authority['request']
                payload = dict(req); payload.pop('requestDigest')
                req['requestDigest'] = TARGET.sha(TARGET.canonical(payload))
                with patch.object(TARGET, 'complete_workspace_manifest',
                    side_effect=lambda _published, source: ({}, source, TARGET.sha(TARGET.canonical(source)))):
                    with self.assertRaisesRegex(ValueError, 'REBASE_PUBLISHED_BASE_CHANGED|REBASE_BINDING_STALE'):
                        TARGET.verify_authority(authority, action_id, published, files)

    def test_source_change_proof_allows_only_distinct_published_manifest(self):
        _, _, published, files = make_case()
        with patch.object(TARGET, 'protected_release_outputs', return_value={}):
            with self.assertRaisesRegex(ValueError, 'REBASE_ALREADY_CURRENT'):
                TARGET.source_change_proof('agentos/srcold', dict(files), published, files, TARGET.sha(TARGET.canonical(files)))
            changed = {'server/index.ts': '9' * 64}
            proof = TARGET.source_change_proof('agentos/srcold', changed, published, files, TARGET.sha(TARGET.canonical(files)))
        self.assertIs(proof['sourceChanged'], True)
        self.assertEqual(proof['priorSourceManifestDigest'], TARGET.sha(TARGET.canonical(changed)))
        self.assertEqual(proof['priorWorkspaceManifestDigest'], proof['priorSourceManifestDigest'])
        self.assertEqual(len(proof['sourceChangeDigest']), 64)

    def test_generated_dist_delta_cannot_satisfy_source_change(self):
        _, _, published, files = make_case()
        prior = {**files, 'dist/old.js': '9' * 64}
        with patch.object(TARGET, 'protected_release_outputs', return_value={}):
            with self.assertRaisesRegex(ValueError, 'REBASE_PRIOR_SOURCE_MANIFEST_INVALID'):
                TARGET.source_change_proof('harness-improvement-20261013/source', prior, published, files,
                    TARGET.sha(TARGET.canonical(files)))

    def test_same_source_with_sealed_physical_extras_is_not_a_source_change(self):
        _, _, published, files = make_case()
        source = {f'src/file-{index}.ts': TARGET.sha(str(index).encode()) for index in range(665)}
        source.update({'server/index.ts': '1' * 64, 'dist/index.html': '2' * 64})
        extras = {'dist/assets/app.js': '3' * 64,
            'bridge/__pycache__/workspace_compose.cpython312.pyc': '4' * 64}
        prior = {**source, 'dist/assets/app.js': '9' * 64,
            'bridge/__pycache__/workspace_compose.cpython312.pyc': '8' * 64}
        self.assertEqual(len(source), 667)
        with patch.object(TARGET, 'protected_release_outputs', return_value={**source, **extras}):
            with self.assertRaisesRegex(ValueError, 'REBASE_ALREADY_CURRENT'):
                TARGET.source_change_proof('agentos/srcold', prior, published, source,
                    TARGET.sha(TARGET.canonical(source)))

    def test_dist_path_in_source_manifest_remains_part_of_source_identity(self):
        _, _, published, _ = make_case()
        source = {f'src/file-{index}.ts': TARGET.sha(str(index).encode()) for index in range(665)}
        source.update({'server/index.ts': '1' * 64, 'dist/index.html': '2' * 64})
        prior = dict(source, **{'dist/index.html': '9' * 64})
        physical = {**source, 'dist/assets/app.js': '3' * 64,
            'bridge/__pycache__/workspace_compose.cpython312.pyc': '4' * 64}
        self.assertEqual(len(source), 667)
        with patch.object(TARGET, 'protected_release_outputs', return_value=physical):
            proof = TARGET.source_change_proof('agentos/srcold', prior, published, source,
                TARGET.sha(TARGET.canonical(source)))
        self.assertIs(proof['sourceChanged'], True)
        self.assertEqual(proof['priorSourceManifestDigest'], TARGET.sha(TARGET.canonical(prior)))

    def test_unknown_prior_only_source_path_is_refused(self):
        _, _, published, files = make_case()
        prior = {**files, 'new/unmanifested.ts': '9' * 64}
        with patch.object(TARGET, 'protected_release_outputs', return_value=files):
            with self.assertRaisesRegex(ValueError, 'REBASE_PRIOR_SOURCE_MANIFEST_INVALID'):
                TARGET.source_change_proof('agentos/srcold', prior, published, files,
                    TARGET.sha(TARGET.canonical(files)))

    def test_prior_source_namespace_is_confined_to_authorized_project(self):
        self.assertEqual(TARGET.validate_prior_source_workspace('harness-improvement-20261013', 'harness-improvement-20261013/src-old'),
            'harness-improvement-20261013/src-old')
        for foreign in ('other-project/src-old', '/tmp/source', 'agentos/src0123456789abcdef'):
            with self.subTest(foreign=foreign), self.assertRaisesRegex(ValueError, 'REBASE_SOURCE_WORKSPACE_INVALID'):
                TARGET.validate_prior_source_workspace('harness-improvement-20261013', foreign)

    def test_lost_prepare_reply_is_recovered_from_read_only_receipt_without_replay(self):
        authority, action_id, published, files = make_case()
        req = authority['request']
        req['state'] = 'uncertain'
        prior_bytes = b'previous published source'
        prior = {'server/index.ts': prior_bytes}
        prior_hashes = {name: TARGET.sha(value) for name, value in prior.items()}
        workspace_digest = req['workspaceManifestDigest']
        with patch.object(TARGET, 'protected_release_outputs', return_value={}):
            source_change = TARGET.source_change_proof(req['priorSourceWorkspace'], prior_hashes, published, files, workspace_digest)
        target_digest = 'a' * 64
        receipt = {'version': 1, 'actionId': action_id, 'requestDigest': req['requestDigest'],
            'project': req['project'], 'goalId': req['goalId'], 'taskId': req['taskId'], 'runId': req['runId'],
            'runFence': req['runFence'], 'publishedBaselineDigest': req['publishedBaselineDigest'],
            'sourceManifestDigest': req['sourceManifestDigest'], 'workspaceManifestDigest': workspace_digest,
            'baselineWorkspace': req['baselineWorkspace'], 'targetWorkspace': req['targetWorkspace'],
            'sourceChanged': True, **source_change, 'targetBaseDigest': target_digest, 'state': 'prepared'}

        class Manager:
            inspect_calls = 0
            def workspace(self, workspace): return workspace
            def sources(self, workspace):
                if workspace == req['baselineWorkspace']: return {'server/index.ts': b'new published source'}
                if workspace == req['priorSourceWorkspace']: return prior
                raise AssertionError('unexpected workspace read')
            def inspect(self, value):
                self.inspect_calls += 1
                return {'baseDigest': target_digest, 'candidateDigest': target_digest}
        manager = Manager()

        with tempfile.TemporaryDirectory() as temp:
            state_root = pathlib.Path(temp) / 'state'
            state_root.mkdir(mode=0o700)
            action_dir = state_root / action_id
            action_dir.mkdir(mode=0o700)
            recorded_request = dict(req, state='preparing')
            (action_dir / 'request.json').write_bytes(TARGET.canonical(recorded_request))
            (action_dir / 'prepared.json').write_bytes(TARGET.canonical(receipt))
            for path in (action_dir / 'request.json', action_dir / 'prepared.json'):
                path.chmod(0o600)
            original_stat = pathlib.Path.stat
            def receipt_stat(path, *args, **kwargs):
                info = original_stat(path, *args, **kwargs)
                try: path.relative_to(state_root)
                except ValueError: return info
                return SimpleNamespace(st_uid=0, st_mode=info.st_mode, st_size=info.st_size)
            with patch.object(TARGET, 'STATE', state_root), \
                 patch.object(TARGET.os, 'geteuid', return_value=0), \
                 patch.object(pathlib.Path, 'stat', receipt_stat), \
                 patch.object(TARGET, 'read_authority', return_value=authority), \
                 patch.object(TARGET, 'protected_manifest', return_value=(None, published, files)), \
                 patch.object(TARGET, 'inspect_baseline', return_value={'baseline': req['publishedBaseline']}), \
                 patch.object(TARGET, 'protected_release_outputs', return_value={}), \
                 patch.object(TARGET, 'complete_workspace_manifest', return_value=({}, files, workspace_digest)), \
                 patch.object(TARGET, 'load_module', return_value=manager), \
                 patch.object(TARGET, 'prepare', side_effect=AssertionError('receipt recovery must not replay prepare')):
                observed = TARGET.read_prepared_receipt(action_id)

        self.assertEqual(observed, receipt)
        self.assertEqual(manager.inspect_calls, 1)

    def test_receipt_recovery_keeps_nonroot_refusal(self):
        _, action_id, _, _ = make_case()
        with patch.object(TARGET.os, 'geteuid', return_value=1001):
            with self.assertRaisesRegex(ValueError, 'REBASE_PRIVILEGE_REQUIRED'):
                TARGET.read_prepared_receipt(action_id)

if __name__ == '__main__': unittest.main()
