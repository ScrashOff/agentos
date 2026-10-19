import importlib.util
import json
import pathlib
import tempfile
import unittest
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('verification_source_binding', ROOT / 'scripts/agentos-verification-source-binding.py')
binding = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(binding)


class VerificationSourceBindingContract(unittest.TestCase):
    def fixture(self, temp):
        workspace_root = pathlib.Path(temp) / 'workspaces'
        source = 'dept/o-parent'
        source_path = workspace_root / source
        source_path.mkdir(parents=True)
        (source_path / '.agentos-snapshot.json').write_text(json.dumps({
            'workspace': source, 'sourceWorkspace': 'dept/pubbaseline'
        }))
        (source_path / 'implementation.json').write_text('{"readyForValidation":true}\n')

        # workspace-manager.inspect compares the candidate with its immutable
        # snapshot baseline. Build a real minimal baseline and lock directory.
        data = workspace_root.parent / '.verification-binding-test-data'
        manager_path = ROOT / 'bridge/workspace-manager.py'
        manager_spec = importlib.util.spec_from_file_location('verification_binding_test_manager', manager_path)
        manager = importlib.util.module_from_spec(manager_spec)
        manager_spec.loader.exec_module(manager)
        manager.ROOT = workspace_root
        manager.DATA = data
        baseline = data / 'workspace-baselines' / manager.digest_key(source)
        baseline.mkdir(parents=True)
        (baseline / 'implementation.json').write_bytes((source_path / 'implementation.json').read_bytes())
        observed = manager.inspect({'workspace': source})
        request = {
            'version': 1, 'actionId': str(uuid.uuid4()), 'requestDigest': 'a' * 64,
            'taskId': str(uuid.uuid4()), 'goalId': str(uuid.uuid4()), 'project': 'dept',
            'parent': {'taskId': str(uuid.uuid4()), 'runId': str(uuid.uuid4()), 'fence': str(uuid.uuid4()),
                'attempt': 2, 'candidateArtifactId': str(uuid.uuid4()), 'candidateArtifactDigest': 'b' * 64,
                'candidateDigest': observed['candidateDigest'], 'baselineWorkspace': 'dept/pubbaseline',
                'rebaseActionId': str(uuid.uuid4()), 'rebaseReceiptDigest': 'c' * 64},
            'sourceWorkspace': source, 'targetWorkspace': 'dept/o-vb-' + uuid.uuid4().hex,
            'baseWorkspace': 'dept/base'
        }
        return request, workspace_root, manager_path

    def test_real_filesystem_source_proof_and_receipt_revalidation(self):
        with tempfile.TemporaryDirectory() as temp:
            request, workspace_root, manager_path = self.fixture(temp)
            proof = binding.physical_proof(request, str(workspace_root), manager_path)
            self.assertEqual(proof['sourceManifestDigest'], request['parent']['candidateDigest'])
            self.assertTrue(proof['sourceChanged'])
            recorded_request = json.loads(binding.canonical(request))
            stored = {**proof, 'requestDigest': request['requestDigest']}
            self.assertEqual(binding.verify_receipt_values(recorded_request, stored, request, proof), stored)

            source_file = workspace_root / request['sourceWorkspace'] / 'implementation.json'
            source_file.write_text('{"readyForValidation":false}\n')
            with self.assertRaisesRegex(ValueError, 'VERIFICATION_BINDING_SOURCE_CHANGED'):
                binding.physical_proof(request, str(workspace_root), manager_path)

    def test_receipt_rejects_changed_request_or_source_and_prepare_never_replays(self):
        proof_path = pathlib.Path(tempfile.gettempdir()) / ('verification-binding-' + uuid.uuid4().hex)
        try:
            proof_path.write_text('{}')
            with self.assertRaisesRegex(ValueError, 'VERIFICATION_BINDING_PREPARE_ALREADY_RECORDED'):
                binding.assert_unprepared(proof_path)
        finally:
            proof_path.unlink(missing_ok=True)
        request = {'requestDigest': 'd' * 64, 'taskId': str(uuid.uuid4())}
        stored = {'requestDigest': request['requestDigest'], 'sourceManifestDigest': 'e' * 64}
        with self.assertRaisesRegex(ValueError, 'VERIFICATION_BINDING_RECEIPT_CHANGED'):
            binding.verify_receipt_values({**request, 'taskId': str(uuid.uuid4())}, stored, request, stored)
        with self.assertRaisesRegex(ValueError, 'VERIFICATION_BINDING_SOURCE_CHANGED'):
            binding.verify_receipt_values(request, stored, request, {**stored, 'sourceManifestDigest': 'f' * 64})

    def test_source_busy_is_preprepare_and_can_retry_without_a_partial_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            request, workspace_root, manager_path = self.fixture(temp)
            state = pathlib.Path(temp) / 'receipt'
            state.mkdir()
            def busy(_request): raise ValueError('SOURCE_BUSY')
            with self.assertRaisesRegex(ValueError, 'SOURCE_BUSY'):
                binding.prepare_receipt_files(request, state, busy)
            self.assertFalse((state / 'request.json').exists())
            self.assertFalse((state / 'prepared.json').exists())

            calls = []
            proof = binding.prepare_receipt_files(request, state,
                lambda value: (calls.append(value), binding.physical_proof(value, str(workspace_root), manager_path))[1])
            self.assertEqual(len(calls), 1)
            self.assertTrue((state / 'request.json').is_file())
            self.assertTrue((state / 'prepared.json').is_file())
            with self.assertRaisesRegex(ValueError, 'VERIFICATION_BINDING_PREPARE_ALREADY_RECORDED'):
                binding.prepare_receipt_files(request, state,
                    lambda _value: self.fail('prepare must not replay after a durable proof'))
            self.assertEqual(json.loads((state / 'prepared.json').read_text()), proof)


if __name__ == '__main__':
    unittest.main()
