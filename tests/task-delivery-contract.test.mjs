import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { taskDeliveryContract, validateTaskDeliveryContract, museTaskDeliveryInstruction } from '../bridge/task-delivery-contract.mjs';
import { musePrompt } from '../bridge/muse-runner.mjs';
import { collectArtifacts, parseArtifactResult } from '../bridge/artifact-collector.mjs';

const criteria = [
  { type: 'artifact', name: 'change.diff' }, { type: 'artifact', name: 'manifest.json' },
  { type: 'json_field', name: 'verification.json', field: 'passed', equals: true },
];
test('criteria names are deduplicated, including multiple fields in one JSON artifact', () => {
  const contract = taskDeliveryContract([...criteria, { type: 'json_field', name: 'verification.json', field: 'cases' }]);
  assert.deepEqual(contract.artifacts.map(a => [a.name, a.path]), [
    ['change.diff', 'evidence/change.diff'], ['manifest.json', 'evidence/manifest.json'], ['verification.json', 'evidence/verification.json'],
  ]);
  assert.deepEqual(validateTaskDeliveryContract(contract), contract);
});
test('reviews and controller artifacts never become worker-produced files', () => {
  const contract = taskDeliveryContract([
    ...criteria, ...['candidate.json', 'changes.patch', 'runtime-protocol-check.json', 'runtime-plan-check.json'].map(name => ({ type: 'artifact', name })),
    { type: 'review', name: 'human-review.json' },
  ]);
  assert.deepEqual(contract, taskDeliveryContract(criteria));
});
test('unsafe, malformed, reserved, excessive or duplicate wire entries are rejected', () => {
  for (const name of ['../manifest.json', 'evidence/manifest.json', '', 'bad\nname.json'])
    assert.throws(() => taskDeliveryContract([{ type: 'artifact', name }]), { code: 'TASK_DELIVERY_CONTRACT' });
  assert.throws(() => taskDeliveryContract(Array(31).fill(criteria[0])), { code: 'TASK_DELIVERY_CONTRACT' });
  const contract = taskDeliveryContract(criteria);
  for (const value of [null, { version: 2, artifacts: [] },
    { version: 1, artifacts: [{ ...contract.artifacts[0], path: 'change.diff' }] },
    { version: 1, artifacts: [{ ...contract.artifacts[0], path: '../change.diff' }] },
    { version: 1, artifacts: [contract.artifacts[0], contract.artifacts[0]] },
    { version: 1, artifacts: [{ name: 'candidate.json', path: 'evidence/candidate.json', mime: 'application/json' }] },
    { version: 1, artifacts: [{ ...contract.artifacts[0], mime: 'application/json' }] },
  ]) assert.throws(() => validateTaskDeliveryContract(value), { code: 'TASK_DELIVERY_CONTRACT' });
});
test('the actual Muse prompt ends with every required filename after its generic report example', () => {
  const deliveryContract = taskDeliveryContract(criteria);
  const prompt = musePrompt({ prompt: 'Existing task and criteria.', readOnlySource: true, deliveryContract }, '\nAccepted source instructions.');
  const finalContract = prompt.slice(prompt.lastIndexOf('TASK_DELIVERY_CONTRACT_V1'));
  for (const item of deliveryContract.artifacts) assert.ok(finalContract.includes(JSON.stringify(item)));
  assert.ok(prompt.indexOf('TASK_DELIVERY_CONTRACT_V1') > prompt.indexOf('"name":"report.json"'));
  assert.match(finalContract, /does not replace the required entries/);
  assert.match(finalContract, /never at the root of \/workspace/);
  assert.match(finalContract, /Preserve actual failed or inconclusive/);
  assert.match(finalContract, /Preserve existing source files/);
  assert.match(prompt, /Source files are read-only/);
  assert.ok(prompt.endsWith('Independent acceptance remains with the controller.'));
});
test('legacy callers without a contract retain the original final schema', () => {
  assert.equal(museTaskDeliveryInstruction(), '');
  const prompt = musePrompt({ prompt: 'Legacy task' });
  assert.ok(!prompt.includes('TASK_DELIVERY_CONTRACT_V1'));
  assert.match(prompt, /Final response MUST be JSON/);
});
test('the proposed names and paths pass the existing collector while preserving a true negative verdict', async t => {
  const workspace = await mkdtemp(path.join(os.tmpdir(), 'agentos-delivery-contract-'));
  t.after(() => rm(workspace, { recursive: true, force: true }));
  await mkdir(path.join(workspace, 'evidence'));
  const contract = taskDeliveryContract(criteria);
  const contents = { 'change.diff': 'actual patch', 'manifest.json': '{"candidate":"real"}', 'verification.json': '{"passed":false,"limitations":["actual failed test"]}' };
  for (const item of contract.artifacts) await writeFile(path.join(workspace, item.path), contents[item.name]);
  await writeFile(path.join(workspace, 'verification.json'), '{"passed":true,"stale":true}');
  const value = parseArtifactResult(JSON.stringify({ summary: 'Actual negative delivery', artifacts: contract.artifacts }));
  const artifacts = await collectArtifacts(value, workspace);
  assert.deepEqual(artifacts.map(a => a.name), ['change.diff', 'manifest.json', 'verification.json']);
  const verification = JSON.parse(Buffer.from(artifacts.find(a => a.name === 'verification.json').base64, 'base64'));
  assert.equal(verification.passed, false);
  assert.ok(!verification.stale);
  assert.equal((await collectArtifacts({ artifacts: [] }, workspace)).length, 0);
});
