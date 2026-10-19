import { beforeAll, beforeEach, afterAll, expect, it } from 'vitest';
import { randomUUID, createHash } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { createGoal, schedule, startRun, writeArtifact, completeRun, validateTask } from '../server/service.js';
import { glmRepairCriteria } from '../server/glm-repair.js';

const sha = (bytes: string) => createHash('sha256').update(bytes).digest('hex');
const patch = '--- a/module.js\n+++ b/module.js\n@@ -1 +1 @@\n-before\n+after\n';
const hashes = [{ path: 'module.js', before: sha('before\n'), after: sha('after\n') }];
beforeAll(async () => { await migrate(); });
beforeEach(async () => { await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE'); await seedRegistry(); });
afterAll(async () => { await pool.end(); });

async function fixture(options: { legacy?: boolean; sourceHashes?: unknown; duplicate?: boolean; corrupt?: boolean } = {}) {
  const criteria = options.legacy ? glmRepairCriteria.filter(c => c.type !== 'repair_source_proof') : glmRepairCriteria;
  const goal = await createGoal({ title: 'Repair delivery is not final QA', project: 'repair-proof-test', idempotencyKey: randomUUID(), tasks: [
    { key: 'repair', title: 'Deliver a bounded repair', agentId: 'system.validation', accountId: 'local-validation', criteria },
    { key: 'qa', title: 'Independently verify integration', agentId: 'system.validation', accountId: 'local-validation', dependencies: ['repair'],
      criteria: [{ id: 'final', type: 'artifact', name: 'qa.json', label: 'Independent QA' }] },
  ] }, true);
  const [runId] = await schedule(), run = await startRun(runId, 'repair-proof-test');
  if (!run) throw Error('TEST_RUN_NOT_STARTED');
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [run.task_id])).rows[0];
  const patchArtifact = await writeArtifact(run.id, run.fence, 'implementation.patch', patch, 'text/plain');
  const report = JSON.stringify({ fixVerified: true,
    ...(Object.hasOwn(options, 'sourceHashes') ? { sourceHashes: options.sourceHashes } : {}),
  });
  const reportArtifact = await writeArtifact(run.id, run.fence, 'repair-report.json', report);
  await writeArtifact(run.id, run.fence, 'repair-tests.log', 'Preserved focal output; not final QA.', 'text/plain');
  if (options.duplicate) await writeArtifact(run.id, run.fence, 'implementation.patch', patch, 'text/plain');
  await completeRun(run.id, run.fence, 'Submitted repair package', {});
  if (options.corrupt) await writeFile(path.join(config.dataDir, 'artifacts', reportArtifact.id), report + ' ');
  return { goal, task, run, patchArtifact, reportArtifact, report };
}

it('accepts a new structurally valid package with exact artifact digests while independent QA remains unaccepted', async () => {
  const f = await fixture({ sourceHashes: hashes });
  const result = await validateTask(f.task.id);
  expect(result.passed).toBe(true);
  expect(result.checks.find(c => c.type === 'repair_source_proof')?.evidence).toMatchObject({ version: 1,
    patchArtifactId: f.patchArtifact.id, reportArtifactId: f.reportArtifact.id,
    patchSha256: sha(patch), reportSha256: sha(f.report), applicabilityVerified: false, qaVerified: false });
  const tasks = (await pool.query('SELECT key,state,criteria FROM tasks WHERE goal_id=$1 ORDER BY key', [f.goal.id])).rows;
  expect(tasks.find(t => t.key === 'repair').state).toBe('accepted');
  expect(tasks.find(t => t.key === 'qa').state).not.toBe('accepted');
  expect(tasks.find(t => t.key === 'repair').criteria).toEqual(f.task.criteria);
});

it.each(['missing', 'wrong_list', 'duplicate_name', 'corrupt_bytes'])(
  'rejects a new repair package with %s before accepting its producer', async mode => {
    const f = await fixture({ ...(mode !== 'missing' ? { sourceHashes: mode === 'wrong_list' ? { 'module.js': hashes[0] } : hashes } : {}),
      duplicate: mode === 'duplicate_name', corrupt: mode === 'corrupt_bytes' });
    const result = await validateTask(f.task.id);
    expect(result.passed).toBe(false);
    expect(result.checks.find(c => c.type === 'repair_source_proof')).toMatchObject({ passed: false });
    expect((await pool.query('SELECT state,criteria,attempt_count FROM tasks WHERE id=$1', [f.task.id])).rows[0])
      .toMatchObject({ state: 'submitted', criteria: f.task.criteria, attempt_count: f.task.attempt_count });
    expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1 AND passed', [f.task.id])).rowCount).toBe(0);
  },
);

it('keeps legacy persisted criteria and their existing acceptance semantics unchanged', async () => {
  const f = await fixture({ legacy: true });
  expect(f.task.criteria.some((c: any) => c.type === 'repair_source_proof')).toBe(false);
  expect((await validateTask(f.task.id)).passed).toBe(true);
  const after = (await pool.query('SELECT state,criteria FROM tasks WHERE id=$1', [f.task.id])).rows[0];
  expect(after).toEqual({ state: 'accepted', criteria: f.task.criteria });
});
