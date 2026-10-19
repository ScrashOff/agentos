import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createOrphanCheck } from '../bridge/opencode-orphan-watch.mjs';

const proof = { runId: 'run', toolPartId: 'tool', timeoutMs: 90000,
  servers: [{ pid: 20, startTicks: '80', parent: 1 }] };
test('requires two identical kernel/tool observations before recovering only the fixture', async () => {
  const recovered = [], emitted = [];
  const check = createOrphanCheck({ runId: 'run', canRecover: () => true,
    inspect: async () => proof, recover: async (p) => { recovered.push(p); return { action: 'terminated', pids: [20] }; },
    emit: (...event) => emitted.push(event) });
  await check();
  assert.equal(recovered.length, 0);
  await check();
  assert.deepEqual(recovered, [proof]);
  assert.equal(emitted[0][0], 'OPENCODE_ORPHAN_FIXTURE_RECOVERED');
  await check();
  assert.equal(recovered.length, 1);
});
test('model activity or a newly active command discards old recovery evidence', async () => {
  let allowed = true, observation = proof, signals = 0;
  const check = createOrphanCheck({ runId: 'run', canRecover: () => allowed,
    inspect: async () => observation, recover: async () => { signals++; } });
  await check(); allowed = false; await check(); allowed = true; await check();
  assert.equal(signals, 0);
  observation = null; await check(); observation = proof; await check();
  assert.equal(signals, 0);
  observation = { ...proof, servers: [{ pid: 20, startTicks: '81', parent: 1 }] };
  await check(); assert.equal(signals, 0);
});
test('model activity that starts during inspection prevents a signal', async () => {
  let allowed = true;
  const check = createOrphanCheck({ runId: 'run', canRecover: () => allowed,
    inspect: async () => { allowed = false; return proof; }, recover: async () => assert.fail('Unexpected signal') });
  await check();
});
test('reports explicit expired tool recovery separately from orphan fixture cleanup', async () => {
  const emitted = [];
  const check = createOrphanCheck({ runId: 'run', canRecover: () => true,
    inspect: async () => ({ ...proof, expiredProcesses: proof.servers }),
    recover: async () => ({ action: 'terminated', reason: 'EXPIRED_TOOL_TIMEOUT_CONFIRMED', pids: [20] }),
    emit: (...event) => emitted.push(event) });
  await check(); await check();
  assert.equal(emitted[0][0], 'OPENCODE_EXPIRED_TOOL_RECOVERED');
});
test('Python inspector refuses live commands, wrong namespaces and unrelated servers', () => {
  const p = spawnSync(process.platform === 'win32' ? 'python' : 'python3',
    [fileURLToPath(new URL('./fixtures/opencode_orphans_test.py', import.meta.url))], { encoding: 'utf8' });
  assert.equal(p.status, 0, p.stdout + p.stderr);
});
