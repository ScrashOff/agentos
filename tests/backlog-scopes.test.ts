import { beforeAll, beforeEach, afterAll, it, expect } from 'vitest';
import { randomUUID, randomBytes } from 'node:crypto';
import { migrate, pool } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installTaskExecutionMetadata } from '../server/task-execution-metadata.js';
import { readBacklogScopes } from '../server/backlog-scopes.js';

beforeAll(async () => { await migrate(); await installTaskExecutionMetadata(); });
beforeEach(async () => { await pool.query('TRUNCATE goals,agents,accounts,events CASCADE'); await seedRegistry(); });
afterAll(async () => { await pool.end(); });
async function goal() {
  const id = randomUUID();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Scopes','Scopes','backlog-scopes-test',$1::text)`, [id]);
  return id;
}
async function task(goalId: string, execution: unknown) {
  const id = randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,stage,execution)
    VALUES($1::uuid,$2::uuid,$1::text,'Task','Instruction','research.requirements','[]','ready','work',$3)`,
  [id, goalId, execution === undefined ? null : JSON.stringify(execution)]);
  return id;
}
const normalized = (values: Array<string | null>) => values.map(value => JSON.stringify(value)).sort();

it('equals legacy DISTINCT for SQL null, JSON null, missing/empty/scalar/structured keys and nonobjects', async () => {
  const id = await goal();
  for (const value of [undefined, null, {}, { baseWorkspace: null }, { baseWorkspace: '' },
    { baseWorkspace: 'project/game' }, { baseWorkspace: 'project/game' },
    { baseWorkspace: 12 }, { baseWorkspace: false }, { baseWorkspace: ['a', 'b'] },
    { baseWorkspace: { nested: 'value' } }, 'scalar', 8, true, []]) await task(id, value);
  await task(await goal(), { baseWorkspace: 'foreign/scope' });
  const legacy = (await pool.query("SELECT DISTINCT execution->>'baseWorkspace' workspace FROM tasks WHERE goal_id=$1 AND execution IS NOT NULL", [id])).rows.map(row => row.workspace);
  const actual = await readBacklogScopes(pool, id);
  expect(normalized(actual)).toEqual(normalized(legacy));
  expect(actual).toContain(null);
  expect(actual).toContain('');
  expect(actual).not.toContain('foreign/scope');
  expect(actual.filter(value => value === 'project/game')).toHaveLength(1);
});
it('excludes SQL-null-only tasks while including JSON null as the legacy null workspace', async () => {
  const id = await goal();
  await task(id, undefined);
  expect(await readBacklogScopes(pool, id)).toEqual([]);
  await task(id, null);
  expect(await readBacklogScopes(pool, id)).toEqual([null]);
});
it('does not return embedded execution inputs or read tasks.execution', async () => {
  const id = await goal();
  await task(id, { kind: 'opencode', baseWorkspace: 'project/large',
    inputs: [{ base64: randomBytes(1024 * 1024).toString('base64') }] });
  const statements: string[] = [];
  const readOnly = { query: async (sql: string, args: any[]) => {
    statements.push(sql);
    return pool.query(sql, args);
  } };
  expect(await readBacklogScopes(readOnly as any, id)).toEqual(['project/large']);
  expect(statements).toHaveLength(1);
  expect(statements[0]).not.toMatch(/\bt\.execution\b|\bt\.\*|instruction_snapshot/);
});
it.each(['missing', 'obsolete', 'wrong-goal'] as const)('fails closed on %s metadata, including a SQL-null execution task', async mode => {
  const id = await goal(), other = await goal(), taskId = await task(id, undefined);
  if (mode === 'missing') await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1', [taskId]);
  if (mode === 'obsolete') await pool.query('UPDATE task_execution_metadata SET version=0 WHERE task_id=$1', [taskId]);
  if (mode === 'wrong-goal') await pool.query('UPDATE task_execution_metadata SET goal_id=$2 WHERE task_id=$1', [taskId, other]);
  await expect(readBacklogScopes(pool, id)).rejects.toMatchObject({ code: 'TASK_EXECUTION_METADATA_MISSING' });
});
it('sees execution updates atomically and rollback retains the previous scope', async () => {
  const id = await goal(), taskId = await task(id, { baseWorkspace: 'project/old' }), db = await pool.connect();
  try {
    await db.query('BEGIN');
    await db.query("UPDATE tasks SET execution=jsonb_build_object('baseWorkspace','project/new') WHERE id=$1", [taskId]);
    expect(await readBacklogScopes(db, id)).toEqual(['project/new']);
    expect(await readBacklogScopes(pool, id)).toEqual(['project/old']);
    await db.query('ROLLBACK');
    expect(await readBacklogScopes(pool, id)).toEqual(['project/old']);
    await pool.query("UPDATE tasks SET execution=jsonb_build_object('baseWorkspace','project/new') WHERE id=$1", [taskId]);
    expect(await readBacklogScopes(pool, id)).toEqual(['project/new']);
  } finally { await db.query('ROLLBACK'); db.release(); }
});
it('tracks a moved task and excludes its scope from the original goal', async () => {
  const first = await goal(), second = await goal(), taskId = await task(first, { baseWorkspace: 'project/moved' });
  await pool.query('UPDATE tasks SET goal_id=$2 WHERE id=$1', [taskId, second]);
  expect(await readBacklogScopes(pool, first)).toEqual([]);
  expect(await readBacklogScopes(pool, second)).toEqual(['project/moved']);
});
