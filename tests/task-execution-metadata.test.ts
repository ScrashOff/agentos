import { beforeAll, beforeEach, afterAll, it, expect } from 'vitest';
import { randomUUID, randomBytes, createHash } from 'node:crypto';
import { migrate, pool } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installTaskExecutionMetadata, backfillTaskExecutionMetadataBatch,
  requireTaskExecutionMetadataReady, taskExecutionMetadataReceipt } from '../server/task-execution-metadata.js';
import { readObjectiveFrontierTasks } from '../server/work-frontier.js';
import { collectOperationProgress } from '../server/operation-progress.js';
import { scopeSourceRevision, installPlanningDemand } from '../server/planning-demand.js';
import { readObservedRunCandidates } from '../server/observations.js';

beforeAll(async () => { await migrate(); await installTaskExecutionMetadata(); await installPlanningDemand(); });
beforeEach(async () => { await pool.query('TRUNCATE goals,agents,accounts,events CASCADE'); await seedRegistry(); });
afterAll(async () => { await pool.end(); });
async function goal() {
  const id = randomUUID();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Metadata','Metadata','metadata-test',$1::text)`, [id]);
  return id;
}
async function task(goalId: string, execution: any = { kind: 'opencode', baseWorkspace: 'metadata/alpha', workspace: 'metadata/isolated', workKind: 'diagnostic' }, key?: string) {
  const id = randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,stage,execution)
    VALUES($1,$2,$3,'Task','Original instruction','research.requirements','[]','ready','work',$4)`,
    [id, goalId, key ?? id, execution === undefined ? null : JSON.stringify(execution)]);
  return id;
}
const read = async (id: string) => (await pool.query('SELECT * FROM task_execution_metadata WHERE task_id=$1', [id])).rows[0];

it('derives scalar control and ordered digests without persisting the input bytes or changing tasks', async () => {
  const goalId = await goal(), bytes = randomBytes(1024 * 1024), digest = createHash('sha256').update(bytes).digest('hex');
  const execution = { kind: 'opencode', baseWorkspace: 'metadata/alpha', workspace: 'metadata/isolated', workKind: 'repair',
    readOnlySource: false, backlogContext: { sourceTaskId: randomUUID(), sourceBytes: bytes.toString('base64') },
    inputs: [{ id: randomUUID(), name: 'source.bin', digest, base64: bytes.toString('base64') }] };
  const id = await task(goalId, execution), projected = await read(id);
  expect(projected).toMatchObject({ version: 1, goal_id: goalId, execution_type: 'object', execution_present: true,
    base_workspace: execution.baseWorkspace, work_kind: 'repair', input_digests: [{ digest }], inputs_valid: true });
  expect(projected.control.backlogContext).toEqual({ sourceTaskId: execution.backlogContext.sourceTaskId });
  expect(JSON.stringify(projected).length).toBeLessThan(1600);
  expect((await pool.query('SELECT execution,description FROM tasks WHERE id=$1', [id])).rows[0])
    .toEqual({ execution, description: 'Original instruction' });
  const frontier = await readObjectiveFrontierTasks(pool, goalId);
  expect(frontier[0].execution).toEqual({ baseWorkspace: execution.baseWorkspace, workspace: execution.workspace, workKind: 'repair' });
  const progress = await collectOperationProgress(pool, goalId, async () => { throw Error('UNEXPECTED_ARTIFACT_READ'); });
  expect(progress.inputDigests).toEqual([digest]);
});

it('matches the old frontier projection for null, missing keys, strings and JSON values', async () => {
  const goalId = await goal();
  for (const execution of [{}, { baseWorkspace: null, workspace: 'metadata/fallback' },
    { baseWorkspace: 12, workspace: false, workKind: { nested: 'value' } },
    { baseWorkspace: 'metadata/alpha', workspace: 'metadata/isolated', workKind: 'planning' }]) await task(goalId, execution);
  const nullId = await task(goalId);
  await pool.query('UPDATE tasks SET execution=NULL WHERE id=$1', [nullId]);
  const legacy = (await pool.query(`SELECT t.id,CASE WHEN t.execution IS NULL THEN NULL ELSE jsonb_build_object(
    'baseWorkspace',x."baseWorkspace",'workspace',x.workspace,'workKind',x."workKind") END execution
    FROM tasks t LEFT JOIN LATERAL jsonb_to_record(t.execution) AS x("baseWorkspace" text,workspace text,"workKind" text) ON true
    WHERE t.goal_id=$1 ORDER BY t.id`, [goalId])).rows;
  const actual = (await readObjectiveFrontierTasks(pool, goalId)).map(({ id, execution }) => ({ id, execution })).sort((a, b) => a.id.localeCompare(b.id));
  expect(actual).toEqual(legacy);
  expect((await read(nullId)).execution_present).toBe(false);
});

it('tracks update, transaction rollback, goal move and delete atomically', async () => {
  const first = await goal(), second = await goal(), id = await task(first), db = await pool.connect();
  try {
    await db.query('BEGIN');
    await db.query(`UPDATE tasks SET goal_id=$2,execution='{"baseWorkspace":"metadata/new","kind":"harness"}' WHERE id=$1`, [id, second]);
    expect((await db.query('SELECT goal_id,base_workspace FROM task_execution_metadata WHERE task_id=$1', [id])).rows[0])
      .toEqual({ goal_id: second, base_workspace: 'metadata/new' });
    expect((await read(id)).goal_id).toBe(first);
    await db.query('ROLLBACK');
    expect((await read(id)).base_workspace).toBe('metadata/alpha');
    await pool.query(`UPDATE tasks SET goal_id=$2,execution='{"baseWorkspace":"metadata/new"}' WHERE id=$1`, [id, second]);
    expect(await read(id)).toMatchObject({ goal_id: second, base_workspace: 'metadata/new' });
    await pool.query('DELETE FROM tasks WHERE id=$1', [id]);
    expect(await read(id)).toBeUndefined();
  } finally { await db.query('ROLLBACK'); db.release(); }
});

it('observes changes made by a BEFORE trigger during a state-only update', async () => {
  const goalId = await goal(), id = await task(goalId, undefined, 'before-final-metadata');
  await pool.query(`CREATE OR REPLACE FUNCTION metadata_test_before() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF NEW.key='before-final-metadata' THEN NEW.execution:=NEW.execution||'{"baseWorkspace":"metadata/from-before","workKind":"repair"}'::jsonb; END IF;
    RETURN NEW; END $$;
    CREATE TRIGGER zz_metadata_test_before BEFORE UPDATE OF state ON tasks FOR EACH ROW EXECUTE FUNCTION metadata_test_before()`);
  try {
    await pool.query("UPDATE tasks SET state='waiting_resource' WHERE id=$1", [id]);
    expect(await read(id)).toMatchObject({ base_workspace: 'metadata/from-before', work_kind: 'repair' });
    expect((await readObjectiveFrontierTasks(pool, goalId))[0].execution.workKind).toBe('repair');
  } finally { await pool.query('DROP TRIGGER zz_metadata_test_before ON tasks; DROP FUNCTION metadata_test_before()'); }
});

it('fails closed on missing rows in frontier, scoped lookup and progress instead of hiding work', async () => {
  const goalId = await goal(), id = await task(goalId);
  await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1', [id]);
  await expect(requireTaskExecutionMetadataReady()).rejects.toThrow('TASK_EXECUTION_METADATA_INCOMPLETE');
  await expect(readObjectiveFrontierTasks(pool, goalId)).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
  await expect(scopeSourceRevision(pool, goalId, 'metadata/unrelated')).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
  await expect(collectOperationProgress(pool, goalId, async () => { throw Error('UNEXPECTED'); })).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
  expect(await backfillTaskExecutionMetadataBatch(pool)).toContain(id);
  await expect(requireTaskExecutionMetadataReady()).resolves.toMatchObject({ state: 'complete' });
});

it('does not convert malformed execution or inputs into a valid empty contract', async () => {
  const goalId = await goal(), id = await task(goalId, { inputs: null });
  expect((await read(id)).inputs_valid).toBe(false);
  await expect(collectOperationProgress(pool, goalId, async () => { throw Error('UNEXPECTED'); })).rejects.toThrow('TASK_EXECUTION_INPUTS_INVALID');
  await pool.query("UPDATE tasks SET execution='[]' WHERE id=$1", [id]);
  await expect(readObjectiveFrontierTasks(pool, goalId)).rejects.toThrow('TASK_EXECUTION_METADATA_INVALID');
});

it('skips a writer and never backfills its old execution over the committed new value', async () => {
  const goalId = await goal(), id = await task(goalId), writer = await pool.connect();
  await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1', [id]);
  try {
    await writer.query('BEGIN');
    await writer.query("UPDATE tasks SET execution=execution||'{\"baseWorkspace\":\"metadata/concurrent\"}' WHERE id=$1", [id]);
    expect(await backfillTaskExecutionMetadataBatch(pool)).not.toContain(id);
    await expect(requireTaskExecutionMetadataReady()).rejects.toThrow('TASK_EXECUTION_METADATA_INCOMPLETE');
    await writer.query('COMMIT');
    expect(await backfillTaskExecutionMetadataBatch(pool)).toEqual([]);
    expect((await read(id)).base_workspace).toBe('metadata/concurrent');
  } finally { await writer.query('ROLLBACK'); writer.release(); }
});

it('holds the backfilled task row until commit, then a waiting update refreshes the projection', async () => {
  const goalId = await goal(), id = await task(goalId), backfill = await pool.connect(), writer = await pool.connect();
  await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1', [id]);
  let pending: Promise<any> | undefined;
  try {
    await backfill.query('BEGIN');
    expect(await backfillTaskExecutionMetadataBatch(backfill)).toContain(id);
    let finished = false;
    pending = writer.query("UPDATE tasks SET execution=execution||'{\"workspace\":\"metadata/after\"}' WHERE id=$1", [id]).then(result => { finished = true; return result; });
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(finished).toBe(false);
    await backfill.query('COMMIT'); await pending;
    expect((await read(id)).workspace).toBe('metadata/after');
  } finally { await backfill.query('ROLLBACK'); await pending; backfill.release(); writer.release(); }
});

it('keeps completed installation and boot checks read-only without replacing triggers or task rows', async () => {
  const goalId = await goal(), id = await task(goalId);
  const before = (await pool.query(`SELECT t.xmin::text task_version,m.xmin::text metadata_version,
    (SELECT oid FROM pg_trigger WHERE tgrelid='tasks'::regclass AND tgname='agentos_execution_metadata_sync') trigger_oid
    FROM tasks t JOIN task_execution_metadata m ON m.task_id=t.id WHERE t.id=$1`, [id])).rows[0];
  const receipt = await installTaskExecutionMetadata();
  const db = await pool.connect();
  try { await db.query('BEGIN READ ONLY'); await expect(requireTaskExecutionMetadataReady(db)).resolves.toEqual(receipt); await db.query('COMMIT'); }
  finally { await db.query('ROLLBACK'); db.release(); }
  const after = (await pool.query(`SELECT t.xmin::text task_version,m.xmin::text metadata_version,
    (SELECT oid FROM pg_trigger WHERE tgrelid='tasks'::regclass AND tgname='agentos_execution_metadata_sync') trigger_oid
    FROM tasks t JOIN task_execution_metadata m ON m.task_id=t.id WHERE t.id=$1`, [id])).rows[0];
  expect(after).toEqual(before);
});

it('rejects disabled triggers and a forged/incomplete readiness receipt', async () => {
  await pool.query('ALTER TABLE tasks DISABLE TRIGGER agentos_execution_metadata_sync');
  try { await expect(requireTaskExecutionMetadataReady()).rejects.toThrow('TASK_EXECUTION_METADATA_REQUIRED'); }
  finally { await pool.query('ALTER TABLE tasks ENABLE TRIGGER agentos_execution_metadata_sync'); }
  const original = (await pool.query('SELECT value FROM settings WHERE id=$1', [taskExecutionMetadataReceipt])).rows[0].value;
  await pool.query(`UPDATE settings SET value=value||'{"state":"preparing"}' WHERE id=$1`, [taskExecutionMetadataReceipt]);
  try { await expect(requireTaskExecutionMetadataReady()).rejects.toThrow('TASK_EXECUTION_METADATA_REQUIRED'); }
  finally { await pool.query('UPDATE settings SET value=$2 WHERE id=$1', [taskExecutionMetadataReceipt, original]); }
});

it('selects observer candidates with the same runtime-kind semantics without reading execution', async () => {
  const goalId = await goal(), harness = await task(goalId, { kind: 'harness' }), native = await task(goalId, { kind: 'opencode' });
  const run = randomUUID();
  for (const [id, taskId] of [[run, harness], [randomUUID(), native]]) await pool.query(`INSERT INTO runs
    (id,task_id,attempt,state,fence,runtime,agent_version,finished_at) VALUES($1,$2,1,'failed',$3,'harness',1,now())`, [id, taskId, randomUUID()]);
  expect((await readObservedRunCandidates(pool)).map(item => item.id)).toEqual([run]);
  await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1', [harness]);
  await expect(readObservedRunCandidates(pool)).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
});
