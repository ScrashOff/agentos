import { beforeAll, beforeEach, afterAll, it, expect } from 'vitest';
import { randomUUID, randomBytes, createHash } from 'node:crypto';
import type { PoolClient } from 'pg';
import { pool, migrate, type DB } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { installRunCriteriaMetadata, requireRunCriteriaMetadataReady,
  backfillRunCriteriaMetadataBatch, checkedRunCriteriaMetadata, runCriteriaMetadataColumns,
  runCriteriaMetadataVersion, runCriteriaMetadataReceipt } from '../server/run-criteria-metadata.js';

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installRunCriteriaMetadata();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
  await requireRunCriteriaMetadataReady();
});
afterAll(() => pool.end());

const criteria = [{ id: 'proof', type: 'json_field', name: 'proof.json', field: 'passed',
  equals: true, label: 'The original criterion remains exact', nested: { order: [null, 1, 'two'] } }];
async function goal(db: DB = pool) {
  const id = randomUUID();
  await db.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Frozen criteria','Exact run contract','run-criteria-metadata',$1::text)`, [id]);
  return id;
}
async function task(goalId: string, db: DB = pool) {
  const id = randomUUID();
  await db.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Proof','Original task instruction','research.requirements','failed','work',$4,1)`,
  [id, goalId, id, JSON.stringify(criteria)]);
  return id;
}
async function run(taskId: string, db: DB = pool, snapshot?: unknown) {
  const id = randomUUID();
  const supplied = arguments.length >= 3 ? snapshot : { task: { criteria: ['forged caller data'] } };
  await db.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,finished_at,instruction_snapshot)
    VALUES($1,$2,1,'failed',$3,'harness',1,now(),$4::jsonb)`,
  [id, taskId, randomUUID(), supplied === undefined ? null : JSON.stringify(supplied)]);
  return id;
}
async function fixture() {
  const goalId = await goal(), taskId = await task(goalId), runId = await run(taskId);
  return { goalId, taskId, runId };
}
const read = async (runId: string, db: DB = pool) => (await db.query(`SELECT m.*,
  m.criteria IS NULL sql_null,jsonb_typeof(m.criteria) criteria_type,
  r.instruction_snapshot->'task'->'criteria' frozen_criteria
  FROM runs r LEFT JOIN run_criteria_metadata m ON m.run_id=r.id WHERE r.id=$1`, [runId])).rows[0];
const readChecked = async (runId: string, db: DB = pool) => checkedRunCriteriaMetadata((await db.query(`SELECT
  r.id,r.task_id,r.attempt,criteria_metadata.criteria accepted_criteria,${runCriteriaMetadataColumns}
  FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN run_criteria_metadata criteria_metadata ON criteria_metadata.run_id=r.id
  WHERE r.id=$1`, [runId])).rows);
async function rollbackOnly(work: (db: PoolClient) => Promise<void>) {
  const db = await pool.connect();
  try { await db.query('BEGIN'); await work(db); }
  finally { await db.query('ROLLBACK'); db.release(); }
}

it('projects the canonical BEFORE snapshot, ignores caller snapshot data and never follows later task criteria', async () => {
  const f = await fixture();
  expect(await read(f.runId)).toMatchObject({ run_id: f.runId, task_id: f.taskId, goal_id: f.goalId,
    attempt: 1, version: runCriteriaMetadataVersion, criteria, frozen_criteria: criteria, sql_null: false });
  const before = (await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [f.runId])).rows[0];
  await pool.query(`UPDATE tasks SET description='Changed current instruction',criteria=$2 WHERE id=$1`,
    [f.taskId, JSON.stringify([{ id: 'different', type: 'artifact', name: 'new.json', label: 'A new requirement' }])]);
  expect((await read(f.runId)).criteria).toEqual(criteria);
  expect((await readChecked(f.runId))[0].accepted_criteria).toEqual(criteria);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [f.runId])).rows[0]).toEqual(before);
  await expect(pool.query(`UPDATE runs SET instruction_snapshot='{}' WHERE id=$1`, [f.runId]))
    .rejects.toThrow('INSTRUCTION_SNAPSHOT_IMMUTABLE');
  expect((await read(f.runId)).frozen_criteria).toEqual(criteria);
});

it('preserves SQL NULL, JSON null, missing criteria and unusual frozen JSONB without treating missing metadata as NULL', async () => {
  const snapshots = [undefined, null, {}, { task: {} }, { task: { criteria: null } },
    { task: { criteria: [] } }, { task: { criteria: false } }, { task: { criteria: 'literal' } },
    { task: { criteria: { nested: [null, { exact: true }] } } }];
  await rollbackOnly(async db => {
    // Simulate old run rows without changing the trigger visible after this transaction.
    // The AFTER projector remains enabled. Canonical readiness is checked only after re-enabling the BEFORE trigger.
    await db.query('ALTER TABLE runs DISABLE TRIGGER agentos_instruction_snapshot');
    const g = await goal(db), ids: string[] = [];
    for (const snapshot of snapshots) ids.push(await run(await task(g, db), db, snapshot));
    await db.query('ALTER TABLE runs ENABLE TRIGGER agentos_instruction_snapshot');
    const values = (await db.query(`SELECT r.id,m.criteria IS NULL sql_null,jsonb_typeof(m.criteria) criteria_type,
      m.criteria IS NOT DISTINCT FROM r.instruction_snapshot->'task'->'criteria' exact_value,
      m.run_id IS NOT NULL projection_present FROM runs r JOIN run_criteria_metadata m ON m.run_id=r.id WHERE r.id=ANY($1::uuid[])`, [ids])).rows;
    expect(values).toHaveLength(snapshots.length);
    expect(values.every(value => value.exact_value && value.projection_present)).toBe(true);
    const byId = new Map(values.map(value => [value.id, value]));
    for (const id of ids.slice(0, 4)) expect(byId.get(id)).toMatchObject({ sql_null: true, criteria_type: null });
    expect(byId.get(ids[4])).toMatchObject({ sql_null: false, criteria_type: 'null' });
    expect(byId.get(ids[5])).toMatchObject({ sql_null: false, criteria_type: 'array' });
    expect(byId.get(ids[6])).toMatchObject({ sql_null: false, criteria_type: 'boolean' });
    expect(byId.get(ids[7])).toMatchObject({ sql_null: false, criteria_type: 'string' });
    expect(byId.get(ids[8])).toMatchObject({ sql_null: false, criteria_type: 'object' });
    await expect(requireRunCriteriaMetadataReady(db)).resolves.toMatchObject({ state: 'complete' });
    await db.query('DELETE FROM run_criteria_metadata WHERE run_id=$1', [ids[0]]);
    await expect(readChecked(ids[0], db)).rejects.toThrow('RUN_CRITERIA_METADATA_MISSING');
    await expect(requireRunCriteriaMetadataReady(db)).rejects.toThrow('RUN_CRITERIA_METADATA_INCOMPLETE');
  });
  await expect(requireRunCriteriaMetadataReady()).resolves.toMatchObject({ state: 'complete' });
});

it('rebuilds missing rows from the immutable run snapshot rather than changed task criteria', async () => {
  const f = await fixture();
  await pool.query(`UPDATE tasks SET criteria='[]' WHERE id=$1`, [f.taskId]);
  await rollbackOnly(async db => {
    await db.query('DELETE FROM run_criteria_metadata WHERE run_id=$1', [f.runId]);
    await expect(requireRunCriteriaMetadataReady(db)).rejects.toThrow('RUN_CRITERIA_METADATA_INCOMPLETE');
    expect(await backfillRunCriteriaMetadataBatch(db, 1)).toEqual([f.runId]);
    expect((await read(f.runId, db)).criteria).toEqual(criteria);
    await expect(requireRunCriteriaMetadataReady(db)).resolves.toMatchObject({ state: 'complete' });
  });
});

it('does not duplicate input bytes, rewrite criteria on heartbeat or change the frozen run and its result during backfill', async () => {
  const goalId = await goal(), taskId = await task(goalId), bytes = randomBytes(128 * 1024);
  const digest = createHash('sha256').update(bytes).digest('hex');
  await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1', [taskId, JSON.stringify({
    kind: 'harness', workspace: 'run-criteria-metadata/large',
    inputs: [{ id: randomUUID(), name: 'source.bin', digest, base64: bytes.toString('base64') }],
  })]);
  const runId = await run(taskId), acceptanceId = randomUUID(), artifactId = randomUUID();
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,false,$4,'test-fixture')`, [acceptanceId, taskId, runId, JSON.stringify([{ id: 'proof', passed: false }])]);
  await pool.query(`INSERT INTO artifacts(id,run_id,name,mime,digest,size)
    VALUES($1,$2,'result.json','application/json',$3,2)`, [artifactId, runId, createHash('sha256').update('{}').digest('hex')]);
  const evidence = async () => ({
    run: (await pool.query(`SELECT state,error_code,summary,
      encode(sha256(convert_to(instruction_snapshot::text,'UTF8')),'hex') snapshot_hash,
      octet_length(instruction_snapshot::text) snapshot_bytes FROM runs WHERE id=$1`, [runId])).rows[0],
    acceptance: (await pool.query('SELECT * FROM acceptances WHERE id=$1', [acceptanceId])).rows[0],
    artifact: (await pool.query('SELECT * FROM artifacts WHERE id=$1', [artifactId])).rows[0],
  });
  const before = await evidence();
  expect(before.run.snapshot_bytes).toBeGreaterThan(128 * 1024);
  expect(JSON.stringify(await read(runId)).length).toBeLessThan(2000);
  expect(JSON.stringify(await read(runId))).not.toContain(bytes.toString('base64').slice(0, 64));
  const version = (await pool.query('SELECT xmin::text version FROM run_criteria_metadata WHERE run_id=$1', [runId])).rows[0].version;
  await pool.query(`UPDATE runs SET heartbeat_at=now(),usage='{"inputTokens":1,"outputTokens":2}' WHERE id=$1`, [runId]);
  expect((await pool.query('SELECT xmin::text version FROM run_criteria_metadata WHERE run_id=$1', [runId])).rows[0].version).toBe(version);
  await pool.query('DELETE FROM run_criteria_metadata WHERE run_id=$1', [runId]);
  expect(await backfillRunCriteriaMetadataBatch(pool, 1)).toEqual([runId]);
  expect((await read(runId)).criteria).toEqual(criteria);
  expect(await evidence()).toEqual(before);
});

it('resumes after a bounded preparation stops without issuing a complete receipt early', async () => {
  const fixtures = await Promise.all([fixture(), fixture(), fixture()]);
  await pool.query('DELETE FROM run_criteria_metadata WHERE run_id=ANY($1::uuid[])', [fixtures.map(f => f.runId)]);
  await pool.query(`UPDATE settings SET value=value||'{"state":"preparing"}' WHERE id=$1`, [runCriteriaMetadataReceipt]);
  try {
    await expect(installRunCriteriaMetadata(pool, { batchSize: 1, maxBatches: 1 }))
      .rejects.toThrow('RUN_CRITERIA_METADATA_INCOMPLETE');
    const partial = (await pool.query('SELECT run_id FROM run_criteria_metadata')).rows;
    expect(partial).toHaveLength(1);
    const receipt = (await pool.query('SELECT value FROM settings WHERE id=$1', [runCriteriaMetadataReceipt])).rows[0].value;
    expect(receipt.state).toBe('preparing');
    const saved = (await pool.query('SELECT run_id,xmin::text row_version FROM run_criteria_metadata')).rows[0];
    await expect(installRunCriteriaMetadata(pool, { batchSize: 1, maxBatches: 3 })).resolves.toMatchObject({ state: 'complete' });
    expect((await pool.query('SELECT run_id,xmin::text row_version FROM run_criteria_metadata WHERE run_id=$1', [saved.run_id])).rows[0]).toEqual(saved);
    expect((await pool.query('SELECT count(*)::int count FROM run_criteria_metadata')).rows[0].count).toBe(3);
  } finally { await installRunCriteriaMetadata(pool, { batchSize: 16, maxBatches: 8 }); }
});

it('skips a locked missing run, includes new inserts atomically and cannot certify skipped work', async () => {
  const f = await fixture(), writer = await pool.connect();
  await pool.query('DELETE FROM run_criteria_metadata WHERE run_id=$1', [f.runId]);
  try {
    await writer.query('BEGIN');
    await writer.query('SELECT id FROM runs WHERE id=$1 FOR UPDATE', [f.runId]);
    expect(await backfillRunCriteriaMetadataBatch(pool, 2)).toEqual([]);
    await expect(requireRunCriteriaMetadataReady()).rejects.toThrow('RUN_CRITERIA_METADATA_INCOMPLETE');
    const newTask = await task(f.goalId), newRun = await run(newTask);
    expect((await read(newRun)).criteria).toEqual(criteria);
    await expect(installRunCriteriaMetadata(pool, { batchSize: 1, maxBatches: 1 })).rejects.toThrow('RUN_CRITERIA_METADATA_INCOMPLETE');
    await writer.query('COMMIT');
    expect(await backfillRunCriteriaMetadataBatch(pool, 2)).toEqual([f.runId]);
    await expect(requireRunCriteriaMetadataReady()).resolves.toMatchObject({ state: 'complete' });
  } finally {
    await writer.query('ROLLBACK'); writer.release();
    await installRunCriteriaMetadata(pool, { batchSize: 16, maxBatches: 8 });
  }
});

it('two concurrent backfill transactions never process the same locked run and rolled-back work remains resumable', async () => {
  const a = await fixture(), b = await fixture(), first = await pool.connect(), second = await pool.connect();
  await pool.query('DELETE FROM run_criteria_metadata WHERE run_id=ANY($1::uuid[])', [[a.runId, b.runId]]);
  try {
    await first.query('BEGIN'); await second.query('BEGIN');
    const one = await backfillRunCriteriaMetadataBatch(first, 1), two = await backfillRunCriteriaMetadataBatch(second, 2);
    expect(one).toHaveLength(1); expect(two).toHaveLength(1); expect(two[0]).not.toBe(one[0]);
    await first.query('ROLLBACK'); await second.query('COMMIT');
    expect(await backfillRunCriteriaMetadataBatch(pool, 2)).toEqual(one);
    expect((await pool.query('SELECT count(*)::int count FROM run_criteria_metadata')).rows[0].count).toBe(2);
    await expect(requireRunCriteriaMetadataReady()).resolves.toMatchObject({ state: 'complete' });
  } finally {
    await first.query('ROLLBACK'); await second.query('ROLLBACK'); first.release(); second.release();
    await installRunCriteriaMetadata(pool, { batchSize: 16, maxBatches: 8 });
  }
});

it.each(['version', 'attempt', 'task', 'goal'] as const)('fails closed on invalid metadata %s without returning an accepted criterion', async kind => {
  const f = await fixture(), otherGoal = await goal(), otherTask = await task(otherGoal);
  await rollbackOnly(async db => {
    const mutations = { version: ['version', 0], attempt: ['attempt', 2], task: ['task_id', otherTask], goal: ['goal_id', otherGoal] } as const;
    const [column, value] = mutations[kind];
    await db.query(`UPDATE run_criteria_metadata SET ${column}=$2 WHERE run_id=$1`, [f.runId, value]);
    await expect(readChecked(f.runId, db)).rejects.toThrow('RUN_CRITERIA_METADATA_MISSING');
    await expect(requireRunCriteriaMetadataReady(db)).rejects.toThrow('RUN_CRITERIA_METADATA_INCOMPLETE');
    expect(await backfillRunCriteriaMetadataBatch(db, 1)).toEqual([f.runId]);
    expect((await readChecked(f.runId, db))[0].accepted_criteria).toEqual(criteria);
  });
});

it.each(['state', 'sourceHash', 'snapshotSourceHash', 'snapshotBodyHash', 'definitionHash', 'version'] as const)
  ('rejects a forged receipt %s without repairing it in the read-only entry point', async field => {
    await rollbackOnly(async db => {
      const before = (await db.query('SELECT value FROM settings WHERE id=$1', [runCriteriaMetadataReceipt])).rows[0].value;
      const forged = { ...before, [field]: field === 'version' ? 0 : 'invalid' };
      await db.query('UPDATE settings SET value=$2 WHERE id=$1', [runCriteriaMetadataReceipt, JSON.stringify(forged)]);
      await expect(requireRunCriteriaMetadataReady(db)).rejects.toThrow('RUN_CRITERIA_METADATA_REQUIRED');
      expect((await db.query('SELECT value FROM settings WHERE id=$1', [runCriteriaMetadataReceipt])).rows[0].value).toEqual(forged);
    });
  });

it('rejects a missing receipt, disabled AFTER trigger and changed projector definition, restoring each transaction', async () => {
  for (const sql of [
    `DELETE FROM settings WHERE id='schema:run-criteria-metadata'`,
    'ALTER TABLE runs DISABLE TRIGGER agentos_run_criteria_metadata_sync',
    `CREATE OR REPLACE FUNCTION agentos_run_criteria_metadata_project(value jsonb) RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT '[]'::jsonb $$`,
  ]) await rollbackOnly(async db => {
    await db.query(sql);
    await expect(requireRunCriteriaMetadataReady(db)).rejects.toThrow('RUN_CRITERIA_METADATA_REQUIRED');
  });
  await expect(requireRunCriteriaMetadataReady()).resolves.toMatchObject({ state: 'complete' });
});

it('refuses a noncanonical BEFORE snapshot body even if the metadata receipt otherwise matches', async () => {
  const original = (await pool.query(`SELECT prosrc FROM pg_proc WHERE oid='agentos_instruction_snapshot()'::regprocedure`)).rows[0].prosrc;
  await rollbackOnly(async db => {
    await db.query(`CREATE OR REPLACE FUNCTION agentos_instruction_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`);
    await expect(requireRunCriteriaMetadataReady(db)).rejects.toThrow('RUN_CRITERIA_METADATA_SNAPSHOT_REQUIRED');
    // The invalid canonical function is not blessed or installed by the readiness reader.
    expect((await db.query(`SELECT prosrc FROM pg_proc WHERE oid='agentos_instruction_snapshot()'::regprocedure`)).rows[0].prosrc).not.toBe(original);
  });
  expect((await pool.query(`SELECT prosrc FROM pg_proc WHERE oid='agentos_instruction_snapshot()'::regprocedure`)).rows[0].prosrc).toBe(original);
  await expect(requireRunCriteriaMetadataReady()).resolves.toMatchObject({ state: 'complete' });
});

it('keeps completed installation and readiness read-only with stable trigger and row identities', async () => {
  const f = await fixture();
  const state = async () => (await pool.query(`SELECT r.xmin::text run_version,m.xmin::text projection_version,
    (SELECT oid FROM pg_trigger WHERE tgrelid='runs'::regclass AND tgname='agentos_run_criteria_metadata_sync') trigger_oid
    FROM runs r JOIN run_criteria_metadata m ON m.run_id=r.id WHERE r.id=$1`, [f.runId])).rows[0];
  const before = await state(), receipt = await installRunCriteriaMetadata();
  const db = await pool.connect();
  try {
    await db.query('BEGIN READ ONLY');
    await expect(requireRunCriteriaMetadataReady(db)).resolves.toEqual(receipt);
    await db.query('COMMIT');
  } finally { await db.query('ROLLBACK'); db.release(); }
  expect(await state()).toEqual(before);
  await pool.query('DELETE FROM runs WHERE id=$1', [f.runId]);
  expect((await pool.query('SELECT run_id FROM run_criteria_metadata WHERE run_id=$1', [f.runId])).rowCount).toBe(0);
});

it.each([0, -1, 1.5, 129])('refuses an unbounded or invalid backfill batch %s before any write', async size => {
  await expect(backfillRunCriteriaMetadataBatch(pool, size)).rejects.toThrow('RUN_CRITERIA_METADATA_BATCH');
});
