import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import type { Pool } from 'pg';
import { pool, type DB } from './db.js';

export const taskExecutionMetadataVersion = 1;
export const taskExecutionMetadataReceipt = 'schema:task-execution-metadata';
const ownerLock = 4280106;
const failure = (code: string) => Object.assign(new Error(code), { code });
const sourceHash = async () => createHash('sha256').update(await readFile(new URL('./task-execution-metadata.ts', import.meta.url))).digest('hex');

/** Every LEFT JOIN reader selects these sentinels, including missing rows. Never
 * turn an absent projection into an empty scope or a completed objective. */
export const executionMetadataColumns = `metadata.task_id execution_metadata_id,
  metadata.version execution_metadata_version,metadata.execution_type execution_metadata_type,
  metadata.inputs_valid execution_metadata_inputs_valid,(metadata.goal_id=t.goal_id) execution_metadata_goal_valid`;
export function checkedExecutionMetadata<T extends Record<string, any>>(rows: T[], options: {
  objectRequired?: boolean; inputsRequired?: (row: T) => boolean;
} = {}): T[] {
  return rows.map(row => {
    const { execution_metadata_id: id, execution_metadata_version: version,
      execution_metadata_type: type, execution_metadata_inputs_valid: inputsValid,
      execution_metadata_goal_valid: goalValid, ...value } = row;
    if (id !== row.id || version !== taskExecutionMetadataVersion || goalValid !== true) throw failure('TASK_EXECUTION_METADATA_MISSING');
    if (options.objectRequired !== false && type !== null && type !== 'object') throw failure('TASK_EXECUTION_METADATA_INVALID');
    if (options.inputsRequired?.(row) && inputsValid !== true) throw failure('TASK_EXECUTION_INPUTS_INVALID');
    return value as T;
  });
}

async function metadataShape(db: DB) {
  const present = (await db.query(`SELECT to_regclass('public.task_execution_metadata') IS NOT NULL present,
    to_regprocedure('public.agentos_execution_metadata_project(jsonb)') IS NOT NULL projector,
    to_regprocedure('public.agentos_execution_metadata_sync()') IS NOT NULL synchronizer,
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.tasks'::regclass
      AND tgname='agentos_execution_metadata_sync' AND tgenabled IN('O','A') AND NOT tgisinternal
      AND tgtype=21 AND tgattr=''::int2vector AND tgqual IS NULL
      AND tgfoid=to_regprocedure('public.agentos_execution_metadata_sync()')) trigger_ready`)).rows[0];
  return present.present && present.projector && present.synchronizer && present.trigger_ready;
}

async function metadataDefinitionHash(db: DB) {
  const definitions = (await db.query(`SELECT
    pg_get_functiondef('public.agentos_execution_metadata_project(jsonb)'::regprocedure) projector,
    pg_get_functiondef('public.agentos_execution_metadata_sync()'::regprocedure) synchronizer,
    pg_get_triggerdef(oid) trigger_definition FROM pg_trigger
    WHERE tgrelid='public.tasks'::regclass AND tgname='agentos_execution_metadata_sync'`)).rows;
  return createHash('sha256').update(JSON.stringify(definitions)).digest('hex');
}

/** Boot check is read-only and reads only task identities and small metadata.
 * Missing rows/version never silently fall back to giant execution payloads. */
export async function requireTaskExecutionMetadataReady(db: DB = pool) {
  const receipt = (await db.query('SELECT value FROM settings WHERE id=$1', [taskExecutionMetadataReceipt])).rows[0]?.value;
  if (receipt?.version !== taskExecutionMetadataVersion || receipt?.state !== 'complete' ||
    receipt?.sourceHash !== await sourceHash() || !(await metadataShape(db)) ||
    receipt?.definitionHash !== await metadataDefinitionHash(db))
    throw failure('TASK_EXECUTION_METADATA_REQUIRED');
  if ((await db.query(`SELECT 1 FROM tasks t LEFT JOIN task_execution_metadata m ON m.task_id=t.id
    WHERE m.task_id IS NULL OR m.version<>$1 OR m.goal_id<>t.goal_id LIMIT 1`, [taskExecutionMetadataVersion])).rowCount)
    throw failure('TASK_EXECUTION_METADATA_INCOMPLETE');
  return receipt;
}

/** A single SQL statement owns each selected task row until its projection is
 * committed. SKIP LOCKED cannot overwrite a concurrent newer execution. Callers
 * must check completeness: zero selected rows can also mean rows are locked. */
export async function backfillTaskExecutionMetadataBatch(db: DB, limit = 16) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 128) throw failure('TASK_EXECUTION_METADATA_BATCH');
  const result = await db.query(`WITH batch AS MATERIALIZED (
    SELECT t.id,t.goal_id,t.execution FROM tasks t LEFT JOIN task_execution_metadata m ON m.task_id=t.id
    WHERE m.task_id IS NULL OR m.version<>$1 OR m.goal_id<>t.goal_id
    ORDER BY t.id LIMIT $2 FOR UPDATE OF t SKIP LOCKED
  ), projected AS MATERIALIZED (
    SELECT id,goal_id,agentos_execution_metadata_project(execution) p FROM batch
  ) INSERT INTO task_execution_metadata(task_id,goal_id,version,execution_type,execution_present,
      base_workspace,workspace,work_kind,control,input_digests,inputs_valid)
    SELECT id,goal_id,$1,p->>'type',(p->>'present')::boolean,p->'control'->>'baseWorkspace',
      p->'control'->>'workspace',p->'control'->>'workKind',p->'control',p->'inputDigests',(p->>'inputsValid')::boolean
    FROM projected ON CONFLICT(task_id) DO UPDATE SET goal_id=excluded.goal_id,version=excluded.version,
      execution_type=excluded.execution_type,execution_present=excluded.execution_present,
      base_workspace=excluded.base_workspace,workspace=excluded.workspace,work_kind=excluded.work_kind,
      control=excluded.control,input_digests=excluded.input_digests,inputs_valid=excluded.inputs_valid
    RETURNING task_id`, [taskExecutionMetadataVersion, limit]);
  return result.rows.map(row => row.task_id as string);
}

/** Explicit preparation only. No trigger replacement, table rewrite or backfill
 * runs on application boot. Completed installation is a read-only fast path. */
export async function installTaskExecutionMetadata(database: Pool = pool) {
  try { return await requireTaskExecutionMetadataReady(database); }
  catch (error: any) { if (!['TASK_EXECUTION_METADATA_REQUIRED','TASK_EXECUTION_METADATA_INCOMPLETE'].includes(error.code)) throw error; }
  const db = await database.connect();
  let owned = false;
  try {
    await db.query('SELECT pg_advisory_lock($1)', [ownerLock]); owned = true;
    try { return await requireTaskExecutionMetadataReady(db); }
    catch (error: any) { if (!['TASK_EXECUTION_METADATA_REQUIRED','TASK_EXECUTION_METADATA_INCOMPLETE'].includes(error.code)) throw error; }
    const previousDefinitions = await metadataShape(db) ? await metadataDefinitionHash(db) : null;
    await db.query('BEGIN');
    try {
      await db.query(`INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value`,
        [taskExecutionMetadataReceipt, JSON.stringify({ version: taskExecutionMetadataVersion, state: 'preparing', sourceHash: await sourceHash() })]);
      await db.query(`CREATE TABLE IF NOT EXISTS task_execution_metadata(
        task_id uuid PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
        goal_id uuid NOT NULL REFERENCES goals(id) ON DELETE CASCADE,version integer NOT NULL,
        execution_type text,execution_present boolean NOT NULL,base_workspace text,workspace text,work_kind text,
        control jsonb NOT NULL,input_digests jsonb NOT NULL,inputs_valid boolean NOT NULL);
        CREATE INDEX IF NOT EXISTS task_execution_metadata_scope ON task_execution_metadata(goal_id,base_workspace,task_id);
        CREATE OR REPLACE FUNCTION agentos_execution_metadata_project(value jsonb) RETURNS jsonb
        LANGUAGE plpgsql IMMUTABLE AS $$
        DECLARE v jsonb; control jsonb; input_digests jsonb='[]'; valid boolean;
        BEGIN
          v:=CASE WHEN jsonb_typeof(value)='object' THEN value ELSE '{}'::jsonb END;
          valid:=NOT(v ? 'inputs') OR jsonb_typeof(v->'inputs')='array';
          IF valid THEN
            SELECT COALESCE(jsonb_agg(jsonb_build_object('digest',i.value->'digest') ORDER BY i.ordinality),'[]')
              INTO input_digests FROM jsonb_array_elements(COALESCE(v->'inputs','[]')) WITH ORDINALITY i(value,ordinality);
          END IF;
          control:=jsonb_build_object('baseWorkspace',v->'baseWorkspace','workspace',v->'workspace',
            'workKind',v->'workKind','kind',v->'kind','readOnlySource',v->'readOnlySource',
            'workforceLane',v->'workforceLane','testFile',v->'testFile','protocolDelivery',v->'protocolDelivery',
            'backlogContext',CASE WHEN jsonb_typeof(v->'backlogContext')='object'
              THEN jsonb_build_object('sourceTaskId',v->'backlogContext'->'sourceTaskId') ELSE NULL END,
            'continuationRoutes',v->'continuationRoutes','verificationRecheck',v->'verificationRecheck');
          RETURN jsonb_build_object('present',value IS NOT NULL,'type',jsonb_typeof(value),
            'control',control,'inputDigests',input_digests,'inputsValid',valid);
        END $$;
        CREATE OR REPLACE FUNCTION agentos_execution_metadata_sync() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE p jsonb;
        BEGIN
          p:=agentos_execution_metadata_project(NEW.execution);
          INSERT INTO task_execution_metadata(task_id,goal_id,version,execution_type,execution_present,
            base_workspace,workspace,work_kind,control,input_digests,inputs_valid)
            VALUES(NEW.id,NEW.goal_id,${taskExecutionMetadataVersion},p->>'type',(p->>'present')::boolean,
              p->'control'->>'baseWorkspace',p->'control'->>'workspace',p->'control'->>'workKind',
              p->'control',p->'inputDigests',(p->>'inputsValid')::boolean)
            ON CONFLICT(task_id) DO UPDATE SET goal_id=excluded.goal_id,version=excluded.version,
              execution_type=excluded.execution_type,execution_present=excluded.execution_present,
              base_workspace=excluded.base_workspace,workspace=excluded.workspace,work_kind=excluded.work_kind,
              control=excluded.control,input_digests=excluded.input_digests,inputs_valid=excluded.inputs_valid;
          RETURN NEW;
        END $$;
        DO $$ BEGIN
          IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.tasks'::regclass AND tgname='agentos_execution_metadata_sync') THEN
            -- BEFORE triggers may mutate execution on a state-only UPDATE.
            -- Observe every final NEW row; the read path never pays this cost.
            CREATE TRIGGER agentos_execution_metadata_sync AFTER INSERT OR UPDATE ON tasks
              FOR EACH ROW EXECUTE FUNCTION agentos_execution_metadata_sync();
          END IF;
        END $$;`);
      // A changed projector cannot leave old rows carrying the current version.
      // Invalidation and function replacement commit atomically; readers fail
      // closed until the explicit, resumable backfill restores each row.
      if (previousDefinitions !== await metadataDefinitionHash(db))
        await db.query('UPDATE task_execution_metadata SET version=0');
      await db.query('COMMIT');
    } catch (error) { await db.query('ROLLBACK'); throw error; }
    while ((await backfillTaskExecutionMetadataBatch(db)).length) { /* each batch commits independently */ }
    if (!(await metadataShape(db)) || (await db.query(`SELECT 1 FROM tasks t LEFT JOIN task_execution_metadata m ON m.task_id=t.id
      WHERE m.task_id IS NULL OR m.version<>$1 OR m.goal_id<>t.goal_id LIMIT 1`, [taskExecutionMetadataVersion])).rowCount)
      throw failure('55P03'); // a skipped writer still owns a missing row; explicit preparer may retry
    const receipt = { version: taskExecutionMetadataVersion, state: 'complete', sourceHash: await sourceHash(),
      definitionHash: await metadataDefinitionHash(db), completedAt: new Date().toISOString() };
    await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
      [taskExecutionMetadataReceipt, JSON.stringify(receipt)]);
    return receipt;
  } finally {
    if (owned) await db.query('SELECT pg_advisory_unlock($1)', [ownerLock]).catch(() => {});
    db.release();
  }
}
