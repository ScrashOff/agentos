import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { materializeSourceInputs } from '../bridge/source-inputs.mjs';

const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
const input = (name, content) => {
  const bytes = Buffer.from(content);
  return { id: randomUUID(), name, digest: hash(bytes), base64: bytes.toString('base64') };
};
function fixture(
  source = 'import assert from "node:assert";\nassert.equal(1, 1);\n',
  path = 'qa-protocol/native.test.mjs',
) {
  const newline = source.endsWith('\n');
  const lines = (newline ? source.slice(0, -1) : source).split('\n');
  return [
    input('candidate.json', JSON.stringify({ files: [{ path, sha256: hash(source) }] })),
    input(
      'changes.patch',
      `--- /dev/null\n+++ b/${path}\n@@ -0,0 +1,${lines.length} @@\n${lines.map((v) => '+' + v + '\n').join('')}${newline ? '' : '\\ No newline at end of file\n'}`,
    ),
  ];
}
test('recovers exact bytes with stable identities and provenance without mutating supplied artifacts', () => {
  for (const source of ['line1\nline2\n', 'line1\nline2', 'line1\r\nline2\r\n']) {
    const inputs = fixture(source);
    const original = structuredClone(inputs);
    const result = materializeSourceInputs(inputs);
    assert.equal(result.telemetry.materialized, 1);
    assert.equal(Buffer.from(result.inputs[2].base64, 'base64').toString(), source);
    assert.deepEqual(inputs, original);
    assert.deepEqual(materializeSourceInputs(inputs), result);
    const index = JSON.parse(Buffer.from(result.inputs[3].base64, 'base64'));
    assert.equal(index.sources[0].candidateInputId, inputs[0].id);
    assert.equal(index.sources[0].patchInputId, inputs[1].id);
    assert.equal(index.sources[0].sha256, hash(source));
    assert.match(result.instruction, /not acceptance/);
  }
});
test('rejects wrong manifest hashes, traversal, incomplete additions and existing-file deltas', () => {
  const originals = fixture('safe\n');
  const patch = Buffer.from(originals[1].base64, 'base64').toString();
  for (const bad of [
    [originals[0], input('changes.patch', patch.replace('+safe', '+changed'))],
    fixture('unsafe\n', 'qa-protocol/../secret.mjs'),
    [originals[0], input('changes.patch', patch.replace('+1,1', '+1,2'))],
    [
      originals[0],
      input('changes.patch', patch.replace('--- /dev/null', '--- a/qa-protocol/native.test.mjs')),
    ],
    [originals[0], input('changes.patch', patch + '@@ -1,1 +2,1 @@\n+extra\n')],
  ])
    assert.equal(materializeSourceInputs(bad).telemetry.materialized, 0);
});
test('preserves different supplied versions and avoids duplicating standalone sources', () => {
  const result = materializeSourceInputs([...fixture('old\n'), ...fixture('new\n')]);
  assert.equal(result.telemetry.materialized, 2);
  assert.notEqual(result.inputs[4].id, result.inputs[5].id);
  assert.equal(result.inputs[4].name, result.inputs[5].name);
  const provided = fixture('known\n');
  assert.equal(
    materializeSourceInputs([...provided, input('native.test.mjs', 'known\n')]).telemetry
      .materialized,
    0,
  );
});
test('bounds optional source expansion and refuses corrupt original inputs', () => {
  const inputs = fixture('payload\n');
  for (const bounds of [{ files: 0 }, { fileBytes: 1 }, { extraBytes: 1 }, { totalBytes: 1 }]) {
    assert.deepEqual(materializeSourceInputs(inputs, bounds).inputs, inputs);
  }
  assert.throws(() => materializeSourceInputs([{ ...inputs[0], digest: '0'.repeat(64) }]), {
    code: 'INPUT_DIGEST',
  });
});
