import test from 'node:test';
import assert from 'node:assert/strict';
import { preserveRuntimeArtifactClaims } from '../bridge/runtime-artifacts.mjs';

test('preserves worker bytes separately from canonical runtime artifact names', () => {
  const claims = [
    { name: 'candidate.json', digest: 'claimed-manifest', base64: 'not canonical' },
    { name: 'changes.patch', digest: 'claimed-patch', base64: 'unchanged original bytes' },
    { name: 'protocol-report.json', digest: 'negative-test', base64: 'failed cases' },
  ];
  assert.deepEqual(preserveRuntimeArtifactClaims(claims), [
    { originalName: 'candidate.json', name: 'worker-candidate.json', digest: 'claimed-manifest' },
    { originalName: 'changes.patch', name: 'worker-changes.patch', digest: 'claimed-patch' },
  ]);
  assert.deepEqual(claims.map(a => a.name), ['worker-candidate.json', 'worker-changes.patch', 'protocol-report.json']);
  assert.equal(claims[1].base64, 'unchanged original bytes');
  assert.equal(claims[2].base64, 'failed cases');
  assert.deepEqual(preserveRuntimeArtifactClaims(claims), []);
});

test('refuses alias collisions without mutating or discarding evidence', () => {
  const claims = [{ name: 'candidate.json' }, { name: 'changes.patch' }, { name: 'worker-changes.patch' }];
  assert.throws(() => preserveRuntimeArtifactClaims(claims), { code: 'ARTIFACT_NAME_COLLISION' });
  assert.deepEqual(claims.map(a => a.name), ['candidate.json', 'changes.patch', 'worker-changes.patch']);
});
