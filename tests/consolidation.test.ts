import { beforeAll, beforeEach, afterAll, it, expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { consolidateGoals, resumeConsolidatedRun } from '../server/consolidation.js';
import { schedule } from '../server/service.js';

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'exclusive-workstreams:%'");
  await seedRegistry();
});
afterAll(async () => {
  await pool.query("DELETE FROM settings WHERE id LIKE 'exclusive-workstreams:%'");
  await pool.end();
});
async function goal() {
  const id = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,state,idempotency_key) VALUES($1,'Test','Test','consolidation-test','paused',$2)",
    [id, 'test-' + id],
  );
  return id;
}
async function task(goalId: string, key: string, base = 'game-a') {
  const id = randomUUID();
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,criteria,execution,resources) VALUES($1,$2,$3,$3,'test','system.validation','local-validation','[]',$4,'[]')",
    [
      id,
      goalId,
      key,
      JSON.stringify({
        kind: 'harness',
        workspace: 'consolidation-test/' + key,
        baseWorkspace: 'consolidation-test/' + base,
      }),
    ],
  );
  return id;
}
async function run(taskId: string, state = 'running') {
  const id = randomUUID(),
    fence = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,account_id,runtime,agent_version,state) VALUES($1,$2,1,$3,'local-validation','harness',1,$4)",
    [id, taskId, fence, state],
  );
  await pool.query('UPDATE tasks SET state=$2,attempt_count=1 WHERE id=$1', [taskId, state]);
  return { id, fence };
}
it('merges task ownership and preserves dependencies, attempts and artifacts; repeat is idempotent', async () => {
  const a = await goal(),
    b = await goal(),
    t = await task(a, 'implementation'),
    u = await task(b, 'protocol'),
    r = await run(u);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [t, u]);
  await pool.query(
    "INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'proof.json','application/json','abc',2)",
    [randomUUID(), r.id],
  );
  expect(await consolidateGoals(a, b)).toMatchObject({ movedTasks: 1, activeRuns: 1 });
  expect((await pool.query('SELECT goal_id FROM tasks WHERE id=$1', [u])).rows[0].goal_id).toBe(a);
  expect((await pool.query('SELECT * FROM dependencies')).rowCount).toBe(1);
  expect((await pool.query('SELECT * FROM artifacts')).rowCount).toBe(1);
  expect(
    (await pool.query('SELECT state,merged_into FROM goals WHERE id=$1', [b])).rows[0],
  ).toEqual({ state: 'cancelled', merged_into: a });
  expect(
    (await pool.query('SELECT resource FROM leases WHERE run_id=$1', [r.id])).rows[0].resource,
  ).toBe('workstream:consolidation-test/game-a');
  expect(await consolidateGoals(a, b)).toMatchObject({ alreadyMerged: true });
});
it('refuses a merge while two live or uncertain attempts own one game', async () => {
  const a = await goal(),
    b = await goal();
  await run(await task(a, 'first'));
  await run(await task(b, 'second'), 'reconciling');
  await expect(consolidateGoals(a, b)).rejects.toThrow('OVERLAPPING_RUNS_REMAIN');
  expect((await pool.query('SELECT state FROM goals WHERE id=$1', [b])).rows[0].state).toBe(
    'paused',
  );
});
it('future tasks inherit exclusivity and concurrent admissions across goals admit exactly one', async () => {
  const a = await goal(),
    b = await goal();
  await task(a, 'baseline');
  await consolidateGoals(a, b);
  const c = await goal(),
    t = await task(a, 'later'),
    u = await task(c, 'other');
  expect(
    (await pool.query('SELECT resources FROM tasks WHERE id=$1', [t])).rows[0].resources,
  ).toContain('workstream:consolidation-test/game-a');
  const results = await Promise.allSettled([run(t), run(u)]);
  expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
  expect(results.filter((r) => r.status === 'rejected')).toHaveLength(1);
  await expect(run(await task(c, 'independent', 'game-b'))).resolves.toBeTruthy();
});
it('scheduler queues another game but waits for the occupied game', async () => {
  const a = await goal(),
    b = await goal(),
    owner = await task(a, 'owner');
  await run(owner);
  await consolidateGoals(a, b);
  const waiting = await task(a, 'waiting'),
    other = await task(a, 'other', 'game-b');
  // Builtin fixtures keep logical baseWorkspace while avoiding remote account dispatch.
  await pool.query(
    "UPDATE tasks SET execution=jsonb_build_object('baseWorkspace',execution->>'baseWorkspace') WHERE id=ANY($1::uuid[])",
    [[waiting, other]],
  );
  config.remoteExecution = true;
  config.bridgeHost = 'test-only';
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [a]);
  // Clear execution only for runtime configuration checks; preserve policy resources.
  await pool.query('UPDATE tasks SET execution=NULL WHERE id=ANY($1::uuid[])', [[waiting, other]]);
  await schedule();
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [waiting])).rows[0].state).toBe(
    'waiting_resource',
  );
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [other])).rows[0].state).toBe(
    'queued',
  );
});
it('resumes only after fresh stop proof, keeping a cancelled attempt and immutable checkpoint', async () => {
  const a = await goal(),
    t = await task(a, 'paused'),
    r = await run(t, 'cancelled');
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [
    t,
    JSON.stringify({ runId: r.id }),
  ]);
  const proof = {
    runId: r.id,
    workspace: 'consolidation-test/paused',
    checkedAt: new Date().toISOString(),
    candidate: { patch: 'example diff' },
    process: {
      unit: 'agentos-run-' + r.id,
      stopped: true,
      activeState: 'inactive',
      mainPid: 0,
      populated: false,
    },
  };
  await expect(
    resumeConsolidatedRun(r.id, r.fence, {
      ...proof,
      process: { ...proof.process, populated: true },
    }),
  ).rejects.toThrow('INVALID_STOP_PROOF');
  const result = await resumeConsolidatedRun(r.id, r.fence, proof);
  expect(result.artifacts).toHaveLength(2);
  expect(await resumeConsolidatedRun(r.id, r.fence, proof)).toEqual(result);
  expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1', [r.id])).rowCount).toBe(2);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [r.id])).rows[0].state).toBe(
    'cancelled',
  );
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [t])).rows[0].state).toBe(
    'waiting_resource',
  );
});
