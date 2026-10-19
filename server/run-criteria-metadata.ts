import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import type { Pool } from 'pg';
import { pool, type DB } from './db.js';

export const runCriteriaMetadataVersion = 1;
export const runCriteriaMetadataReceipt = 'schema:run-criteria-metadata';
const ownerLock = 4280108;
const failure = (code: string) => Object.assign(new Error(code), { code });
const hash = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const sourceHash = async () => hash(await readFile(new URL('./run-criteria-metadata.ts', import.meta.url)));

/** Presence is independent of criteria: a legacy SQL NULL is a valid value,
 * whereas a missing projection must never become a completed objective. */
export const runCriteriaMetadataColumns = `criteria_metadata.run_id run_criteria_metadata_id,
  criteria_metadata.version run_criteria_metadata_version,
  (criteria_metadata.task_id=r.task_id AND criteria_metadata.goal_id=t.goal_id
    AND criteria_metadata.attempt=r.attempt) run_criteria_metadata_identity_valid`;
export function checkedRunCriteriaMetadata<T extends Record<string, any>>(rows: T[]): T[] {
  return rows.map(row => {
    const { run_criteria_metadata_id: id, run_criteria_metadata_version: version,
      run_criteria_metadata_identity_valid: identityValid, ...value } = row;
    if (id !== row.id || version !== runCriteriaMetadataVersion || identityValid !== true)
      throw failure('RUN_CRITERIA_METADATA_MISSING');
    return value as T;
  });
}

/** Do not bless an arbitrary BEFORE trigger by hashing whatever is installed.
 * Compare its body with the unchanged canonical installer in this release. */
async function canonicalSnapshot(db: DB) {
  const source = await readFile(new URL('./task-instructions.ts', import.meta.url), 'utf8');
  const start = 'CREATE OR REPLACE FUNCTION agentos_instruction_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$';
  const end = 'END $$;';
  const at = source.indexOf(start), until = source.indexOf(end, at + start.length);
  if (at < 0 || until < 0 || source.indexOf(start, at + start.length) !== -1)
    throw failure('RUN_CRITERIA_METADATA_SNAPSHOT_REQUIRED');
  const expected = source.slice(at + start.length, until + 'END '.length);
  // The current canonical SQL body is a literal. Refuse an interpolated or
  // escaped future installer instead of pretending to evaluate TypeScript.
  if (expected.includes('${') || expected.includes('\\') || expected.includes('`'))
    throw failure('RUN_CRITERIA_METADATA_SNAPSHOT_REQUIRED');
  const row = (await db.query(`SELECT p.prosrc body,
      l.lanname='plpgsql' AND p.prorettype='trigger'::regtype AND p.prokind='f'
      AND NOT p.prosecdef AND p.proconfig IS NULL function_valid,
      EXISTS(SELECT 1 FROM pg_trigger g JOIN pg_attribute a ON a.attrelid=g.tgrelid
        AND a.attname='instruction_snapshot' AND NOT a.attisdropped
        WHERE g.tgrelid='public.runs'::regclass AND g.tgname='agentos_instruction_snapshot'
        AND g.tgenabled IN('O','A') AND NOT g.tgisinternal AND g.tgtype=23
        AND g.tgattr::text=a.attnum::text AND g.tgqual IS NULL AND g.tgnargs=0
        AND g.tgfoid=p.oid) trigger_valid
    FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
    WHERE p.oid=to_regprocedure('public.agentos_instruction_snapshot()')`)).rows[0];
  if (!row?.function_valid || !row.trigger_valid ||
    row.body.replace(/\r\n/g, '\n') !== expected.replace(/\r\n/g, '\n'))
    throw failure('RUN_CRITERIA_METADATA_SNAPSHOT_REQUIRED');
  return { snapshotSourceHash: hash(source), snapshotBodyHash: hash(expected.replace(/\r\n/g, '\n')) };
}

async function metadataShape(db: DB) {
  const row = (await db.query(`SELECT to_regclass('public.run_criteria_metadata') IS NOT NULL present,
    to_regprocedure('public.agentos_run_criteria_metadata_project(jsonb)') IS NOT NULL projector,
    to_regprocedure('public.agentos_run_criteria_metadata_sync()') IS NOT NULL synchronizer,
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.runs'::regclass
      AND tgname='agentos_run_criteria_metadata_sync' AND tgenabled IN('O','A') AND NOT tgisinternal
      AND tgtype=5 AND tgattr=''::int2vector AND tgqual IS NULL AND tgnargs=0
      AND tgfoid=to_regprocedure('public.agentos_run_criteria_metadata_sync()')) trigger_ready`)).rows[0];
  return row.present && row.projector && row.synchronizer && row.trigger_ready;
}
async function metadataDefinitionHash(db: DB) {
  const rows = (await db.query(`SELECT
    pg_get_functiondef('public.agentos_run_criteria_metadata_project(jsonb)'::regprocedure) projector,
    pg_get_functiondef('public.agentos_run_criteria_metadata_sync()'::regprocedure) synchronizer,
    pg_get_triggerdef(oid) trigger_definition FROM pg_trigger
    WHERE tgrelid='public.runs'::regclass AND tgname='agentos_run_criteria_metadata_sync'`)).rows;
  return hash(JSON.stringify(rows));
}
const missing = `FROM runs r JOIN tasks t ON t.id=r.task_id
  LEFT JOIN run_criteria_metadata m ON m.run_id=r.id
  WHERE m.run_id IS NULL OR m.version<>$1 OR m.task_id<>r.task_id
    OR m.goal_id<>t.goal_id OR m.attempt<>r.attempt`;
const incomplete = async (db: DB) => Boolean((await db.query(`SELECT 1 ${missing} LIMIT 1`, [runCriteriaMetadataVersion])).rowCount);

/** Read-only on every entry point, including the dedicated backlog process.
 * No instruction_snapshot or current task criteria are read by readiness. */
export async function requireRunCriteriaMetadataReady(db: DB = pool) {
  const canonical = await canonicalSnapshot(db);
  const receipt = (await db.query('SELECT value FROM settings WHERE id=$1', [runCriteriaMetadataReceipt])).rows[0]?.value;
  if (receipt?.version !== runCriteriaMetadataVersion || receipt?.state !== 'complete' ||
    receipt?.sourceHash !== await sourceHash() || receipt?.snapshotSourceHash !== canonical.snapshotSourceHash ||
    receipt?.snapshotBodyHash !== canonical.snapshotBodyHash || !(await metadataShape(db)) ||
    receipt?.definitionHash !== await metadataDefinitionHash(db))
    throw failure('RUN_CRITERIA_METADATA_REQUIRED');
  if (await incomplete(db)) throw failure('RUN_CRITERIA_METADATA_INCOMPLETE');
  return receipt;
}

/** Bounded, independently committable batches. The immutable snapshot is the
 * only criteria source. Lock the selected run to coordinate delete/backfill;
 * skipped rows remain incomplete and cannot produce a complete receipt. */
export async function backfillRunCriteriaMetadataBatch(db: DB, limit = 16) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 128) throw failure('RUN_CRITERIA_METADATA_BATCH');
  const result = await db.query(`WITH batch AS MATERIALIZED (
    SELECT r.id,r.task_id,t.goal_id,r.attempt,r.instruction_snapshot ${missing}
    ORDER BY r.id LIMIT $2 FOR UPDATE OF r SKIP LOCKED
  ) INSERT INTO run_criteria_metadata(run_id,task_id,goal_id,attempt,version,criteria)
    SELECT id,task_id,goal_id,attempt,$1,agentos_run_criteria_metadata_project(instruction_snapshot) FROM batch
    ON CONFLICT(run_id) DO UPDATE SET task_id=excluded.task_id,goal_id=excluded.goal_id,
      attempt=excluded.attempt,version=excluded.version,criteria=excluded.criteria RETURNING run_id`,
  [runCriteriaMetadataVersion, limit]);
  return result.rows.map(row => row.run_id as string);
}

/** Explicit additive preparation, never boot-time DDL. Each invocation has a
 * finite batch budget; a later invocation resumes a preparing receipt. It must
 * run with the canonical preparer's PostgreSQL statement/lock timeouts. */
export async function installRunCriteriaMetadata(database: Pool = pool,
  options: { batchSize?: number; maxBatches?: number } = {}) {
  const batchSize = options.batchSize ?? 16, maxBatches = options.maxBatches ?? 512;
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 128 ||
    !Number.isInteger(maxBatches) || maxBatches < 1 || maxBatches > 4096)
    throw failure('RUN_CRITERIA_METADATA_BATCH');
  try { return await requireRunCriteriaMetadataReady(database); }
  catch (error: any) { if (!['RUN_CRITERIA_METADATA_REQUIRED','RUN_CRITERIA_METADATA_INCOMPLETE'].includes(error.code)) throw error; }
  const db = await database.connect();
  let owned = false;
  try {
    await db.query('SELECT pg_advisory_lock($1)', [ownerLock]); owned = true;
    try { return await requireRunCriteriaMetadataReady(db); }
    catch (error: any) { if (!['RUN_CRITERIA_METADATA_REQUIRED','RUN_CRITERIA_METADATA_INCOMPLETE'].includes(error.code)) throw error; }
    const canonical = await canonicalSnapshot(db), source = await sourceHash();
    const previous = (await db.query('SELECT value FROM settings WHERE id=$1', [runCriteriaMetadataReceipt])).rows[0]?.value;
    const priorDefinitions = await metadataShape(db) ? await metadataDefinitionHash(db) : null;
    const resume = ['preparing','complete'].includes(previous?.state) && previous?.version === runCriteriaMetadataVersion &&
      previous?.sourceHash === source && previous?.snapshotSourceHash === canonical.snapshotSourceHash &&
      previous?.snapshotBodyHash === canonical.snapshotBodyHash && priorDefinitions !== null && previous?.definitionHash === priorDefinitions;
    let definitions = priorDefinitions;
    if (!resume) {
      await db.query('BEGIN');
      try {
        await db.query(`CREATE TABLE IF NOT EXISTS run_criteria_metadata(
          run_id uuid PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
          task_id uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
          goal_id uuid NOT NULL REFERENCES goals(id) ON DELETE CASCADE,
          attempt integer NOT NULL,version integer NOT NULL,criteria jsonb);
          CREATE INDEX IF NOT EXISTS run_criteria_metadata_goal ON run_criteria_metadata(goal_id,run_id);
          CREATE OR REPLACE FUNCTION agentos_run_criteria_metadata_project(value jsonb) RETURNS jsonb
            LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT value->'task'->'criteria' $$;
          CREATE OR REPLACE FUNCTION agentos_run_criteria_metadata_sync() RETURNS trigger LANGUAGE plpgsql AS $$
          DECLARE goal uuid;
          BEGIN
            SELECT goal_id INTO STRICT goal FROM tasks WHERE id=NEW.task_id;
            INSERT INTO run_criteria_metadata(run_id,task_id,goal_id,attempt,version,criteria)
              VALUES(NEW.id,NEW.task_id,goal,NEW.attempt,${runCriteriaMetadataVersion},
                agentos_run_criteria_metadata_project(NEW.instruction_snapshot));
            RETURN NEW;
          END $$;
          DO $$ BEGIN
            IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.runs'::regclass AND tgname='agentos_run_criteria_metadata_sync') THEN
              CREATE TRIGGER agentos_run_criteria_metadata_sync AFTER INSERT ON runs
                FOR EACH ROW EXECUTE FUNCTION agentos_run_criteria_metadata_sync();
            END IF;
          END $$;`);
        if (!(await metadataShape(db))) throw failure('RUN_CRITERIA_METADATA_REQUIRED');
        definitions = await metadataDefinitionHash(db);
        if (priorDefinitions !== definitions) await db.query('UPDATE run_criteria_metadata SET version=0');
        await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
          [runCriteriaMetadataReceipt, JSON.stringify({ version: runCriteriaMetadataVersion, state: 'preparing',
            sourceHash: source, ...canonical, definitionHash: definitions })]);
        await db.query('COMMIT');
      } catch (error) { await db.query('ROLLBACK'); throw error; }
    }
    for (let batch = 0; batch < maxBatches; batch++) {
      if (!(await backfillRunCriteriaMetadataBatch(db, batchSize)).length) break;
    }
    if (await incomplete(db)) throw failure('RUN_CRITERIA_METADATA_INCOMPLETE');
    if (!(await metadataShape(db)) || definitions !== await metadataDefinitionHash(db) || source !== await sourceHash() ||
      JSON.stringify(canonical) !== JSON.stringify(await canonicalSnapshot(db))) throw failure('RUN_CRITERIA_METADATA_REQUIRED');
    const receipt = { version: runCriteriaMetadataVersion, state: 'complete', sourceHash: source,
      ...canonical, definitionHash: definitions, completedAt: new Date().toISOString() };
    await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
      [runCriteriaMetadataReceipt, JSON.stringify(receipt)]);
    return receipt;
  } finally {
    if (owned) await db.query('SELECT pg_advisory_unlock($1)', [ownerLock]).catch(() => {});
    db.release();
  }
}
