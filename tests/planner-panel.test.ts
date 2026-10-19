import { beforeAll, beforeEach, afterAll, it, expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import { migrate, pool, type DB } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installPlanningDemand } from '../server/planning-demand.js';
import { plannerPanel, readPlannerPanel } from '../server/planner-panel.js';
import { hash } from '../server/memory.js';
import { installTaskExecutionMetadata } from '../server/task-execution-metadata.js';

beforeAll(async () => { await migrate(); await installTaskExecutionMetadata(); await installPlanningDemand(); });
beforeEach(async () => { await pool.query('TRUNCATE goals,agents,accounts,events CASCADE'); await seedRegistry(); });
afterAll(async () => { await pool.end(); });
async function goal() {
  const id = randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Scope pilot','Deliver results','planner-panel',$1::text)", [id]);
  await pool.query('INSERT INTO settings VALUES($1,$2)', ['workforce:' + id, JSON.stringify({ enabled: true, glmTarget: 12, plannerMaxActive: 2 })]);
  return id;
}
async function task(goalId: string, name: string, options: Record<string, any> = {}) {
  const id = randomUUID(), runId = options.runState ? randomUUID() : null;
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,stage,execution,attempt_count,reason)
    VALUES($1::uuid,$2,$1::text,$3,repeat('PRIVATE-PROMPT-',1000),'research.requirements','[]',$4,$5,$6,$7,$8)`,
  [id, goalId, name, options.state ?? 'ready', options.stage ?? 'work', JSON.stringify({
    kind: 'opencode', baseWorkspace: options.scope ?? 'panel/alpha', workspace: 'panel/' + id,
    workKind: options.kind, inputs: [{ id:randomUUID(),name:'private.txt',
      digest:hash(Buffer.from('PRIVATE-INPUT-BYTES')),base64:Buffer.from('PRIVATE-INPUT-BYTES').toString('base64') }],
  }), runId ? 1 : 0, options.reason ?? null]);
  if (runId) await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,model,heartbeat_at)
    VALUES($1,$2,1,$3,$4,'opencode',1,'glm-5.3-flash',now())`, [runId, id, options.runState, randomUUID()]);
  if (options.passed !== undefined) await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,$4,'[]','test')`, [randomUUID(), id, runId, options.passed]);
  return { id, runId };
}
async function request(goalId: string, scope: string, taskId: string | null, state: string, extra: Record<string, any> = {}) {
  const id = randomUUID();
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,task_id,receipt)
    VALUES($1,$2,$3,'glm',$4,$5,'READY_RESERVE_DEFICIT',$6,$7,$8)`,
  [id, goalId, scope, randomUUID().replaceAll('-', '').repeat(2), state, JSON.stringify({ private: 'PRIVATE-SNAPSHOT' }), taskId, JSON.stringify(extra)]);
  return id;
}

it('reports actual workers, dependency-ready tasks and accepted evidence without loading private context', async () => {
  const id = await goal();
  const active = await task(id, 'Implement candidate', { state: 'running', runState: 'running' });
  await task(id, 'Ready isolated work');
  const dependent = await task(id, 'Waiting on producer');
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [dependent.id, active.id]);
  await task(id, 'Confirm remote stop', { state: 'recovery_blocked', runState: 'reconciling', kind: 'repair' });
  const accepted = await task(id, 'Useful diagnostic', { state: 'accepted', runState: 'submitted', passed: true, kind: 'diagnostic' });
  const planner = await task(id, 'Scope planning', { state: 'running', runState: 'running', kind: 'planning' });
  await request(id, 'panel/alpha', planner.id, 'running');
  const other = await goal(); await task(other, 'Foreign scope', { scope: 'other/private', state: 'running', runState: 'running' });
  const before = (await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n;
  const panel = await plannerPanel(id);
  expect(panel.maxActive).toBe(2);
  expect(panel.scopes).toHaveLength(1);
  expect(panel.scopes[0]).toMatchObject({ scope: 'panel/alpha', planner: { taskId: planner.id, state: 'running' },
    nextDelivery: { taskId: active.id }, workers: { running: 1, ready: 1, queued: 0, confirming: 1 },
    lastAdvance: { taskId: accepted.id, kind: 'auxiliary' } });
  const output = JSON.stringify(panel);
  for (const secret of ['PRIVATE-PROMPT', 'PRIVATE-INPUT-BYTES', 'PRIVATE-SNAPSHOT', 'Foreign scope']) expect(output).not.toContain(secret);
  expect(Buffer.byteLength(output)).toBeLessThan(5000);
  expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(before);
});

it('does not count heartbeat, a submitted plan or a stale acceptance as delivery progress', async () => {
  const id = await goal();
  await task(id, 'Planner returned', { state: 'accepted', runState: 'submitted', passed: true, kind: 'planning' });
  const old = await task(id, 'Candidate being revised', { state: 'accepted', runState: 'submitted', passed: true });
  await pool.query("UPDATE tasks SET attempt_count=2 WHERE id=$1", [old.id]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version)
    VALUES($1,$2,2,'failed',$3,'opencode',1)`, [randomUUID(), old.id, randomUUID()]);
  await task(id, 'Live heartbeat', { state: 'running', runState: 'running', kind: 'diagnostic' });
  const panel = await plannerPanel(id);
  expect(panel.scopes[0].lastAdvance).toBeNull();
  expect(panel.scopes[0].workers.running).toBe(1);
});

it('keeps an uncertain owner visible ahead of a newer pending request and exposes a concise blocker', async () => {
  const id = await goal();
  const planner = await task(id, 'Uncertain planner', { state: 'recovery_blocked', runState: 'reconciling', kind: 'planning' });
  await request(id, 'panel/alpha', planner.id, 'awaiting_supervision', { code: 'SUPERVISOR_LOST' });
  await request(id, 'panel/alpha', null, 'pending');
  const delivery = await task(id, 'Integrate existing patches', { state: 'blocked_dependencies', stage: 'integrate', reason: 'Awaiting patch consumption proof.' });
  const panel = await plannerPanel(id);
  expect(panel.scopes[0]).toMatchObject({ planner: { taskId: planner.id, state: 'reconciling', occupied: true },
    nextDelivery: { taskId: delivery.id }, blocker: { taskId: delivery.id, reason: 'Awaiting patch consumption proof.' },
    workers: { running: 0, confirming: 0 } });
  expect((await pool.query('SELECT count(*)::int n FROM planning_requests WHERE goal_id=$1', [id])).rows[0].n).toBe(2);
});

it('scopes reads to an existing goal and preserves a paused objective', async () => {
  await expect(plannerPanel(randomUUID())).rejects.toMatchObject({ code: 'NOT_FOUND' });
  const id = await goal(); await task(id, 'Not dispatched');
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [id]);
  const panel = await plannerPanel(id);
  expect(panel.goalState).toBe('paused');
  expect(panel.scopes[0].planner.state).toBe('idle');
  expect((await pool.query('SELECT state FROM goals WHERE id=$1', [id])).rows[0].state).toBe('paused');
  expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(0);
});

function observeProjection(db: DB) {
  const batches: number[] = [];
  const observed = { query: async (text: string, values?: any[]) => {
    if (text.includes('COALESCE(metadata.base_workspace,metadata.workspace) scope')) batches.push(1);
    expect(text).not.toContain('jsonb_to_record(t.execution)');
    return db.query(text, values);
  } } as DB;
  return { observed, batches };
}
async function measuredPanel(goalId: string) {
  const db = await pool.connect(), { observed, batches } = observeProjection(db);
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const panel = await readPlannerPanel(observed, goalId);
    await db.query('COMMIT');
    return { panel, batches };
  } catch (error) { await db.query('ROLLBACK'); throw error; }
  finally { db.release(); }
}

it('reads persisted metadata once on both cold and warm reads without reopening execution payloads', async () => {
  const id = await goal();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,stage,execution)
    SELECT item.id,$1,item.id::text,'Scoped item','Original instructions','research.requirements','[]','ready','work',
      jsonb_build_object('kind','opencode','baseWorkspace','panel/bulk','workspace','panel/'||item.id::text)
    FROM (SELECT gen_random_uuid() id FROM generate_series(1,130)) item`, [id]);
  const cold = await measuredPanel(id), warm = await measuredPanel(id);
  expect(cold.batches).toEqual([1]);
  expect(warm.batches).toEqual([1]);
  expect(warm.panel.scopes[0].workers.ready).toBe(130);
  const changed = (await pool.query('SELECT id FROM tasks WHERE goal_id=$1 ORDER BY id LIMIT 1', [id])).rows[0].id;
  await pool.query("UPDATE tasks SET execution=execution||'{\"baseWorkspace\":\"panel/updated\",\"workKind\":\"diagnostic\"}'::jsonb WHERE id=$1", [changed]);
  const updated = await measuredPanel(id);
  expect(updated.batches).toEqual([1]);
  expect(updated.panel.scopes.find(scope => scope.scope === 'panel/bulk')?.workers.ready).toBe(129);
  expect(updated.panel.scopes.find(scope => scope.scope === 'panel/updated')?.workers.ready).toBe(1);
});

it('does not use another goal or a recreated task row as scope authority', async () => {
  const first = await goal(), second = await goal();
  const original = await task(first, 'Original scope');
  await measuredPanel(first);
  await pool.query('DELETE FROM tasks WHERE id=$1', [original.id]);
  expect((await measuredPanel(first)).panel.scopes).toEqual([]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,stage,execution)
    VALUES($1::uuid,$2,$1::text,'Recreated task','New instructions','research.requirements','[]','ready','work',
      '{"kind":"opencode","baseWorkspace":"panel/new-goal","workspace":"panel/recreated"}')`, [original.id, second]);
  const recreated = await measuredPanel(second);
  expect(recreated.batches).toEqual([1]);
  expect(recreated.panel.scopes.map(scope => scope.scope)).toEqual(['panel/new-goal']);
  expect((await measuredPanel(first)).panel.scopes).toEqual([]);
});

it('uses metadata from each concurrent repeatable-read snapshot', async () => {
  const id = await goal(), current = await task(id, 'Snapshot scope');
  const older = await pool.connect(), newer = await pool.connect();
  try {
    await older.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await older.query('SELECT id FROM tasks WHERE id=$1', [current.id]);
    await pool.query("UPDATE tasks SET execution=execution||'{\"baseWorkspace\":\"panel/new-version\"}'::jsonb WHERE id=$1", [current.id]);
    await newer.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const latest = await readPlannerPanel(newer, id);
    expect(latest.scopes.map(scope => scope.scope)).toEqual(['panel/new-version']);
    const historical = await readPlannerPanel(older, id);
    expect(historical.scopes.map(scope => scope.scope)).toEqual(['panel/alpha']);
    const again = await readPlannerPanel(newer, id);
    expect(again.scopes.map(scope => scope.scope)).toEqual(['panel/new-version']);
    await older.query('COMMIT'); await newer.query('COMMIT');
  } finally {
    await older.query('ROLLBACK'); await newer.query('ROLLBACK');
    older.release(); newer.release();
  }
});
