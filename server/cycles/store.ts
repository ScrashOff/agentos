import { randomUUID } from 'node:crypto';
import { pool, tx, lock, event, type DB } from '../db.js';
import { DomainError } from '../contracts.js';
import { hash } from '../memory.js';
import { parseCycle, conflict, type CycleDefinition } from './contracts.js';
import { occurrenceFrontier } from '../work-frontier.js';

export async function migrateCycles(db: DB = pool) {
  await db.query(`
CREATE TABLE IF NOT EXISTS cycles (
 id uuid PRIMARY KEY, name text NOT NULL, project text NOT NULL, definition jsonb NOT NULL,
 version int NOT NULL DEFAULT 1, state text NOT NULL DEFAULT 'draft', request_key text UNIQUE NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 next_check_at timestamptz, checked_at timestamptz, last_success_at timestamptz, last_error text, cursor jsonb NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS cycle_versions (
 cycle_id uuid REFERENCES cycles(id), version int, definition jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(cycle_id,version));
CREATE TABLE IF NOT EXISTS cycle_occurrences (
 id uuid PRIMARY KEY, cycle_id uuid NOT NULL REFERENCES cycles(id), version int NOT NULL,
 title text NOT NULL, state text NOT NULL DEFAULT 'queued', source_key text NOT NULL,
 input jsonb NOT NULL DEFAULT '{}', definition jsonb NOT NULL, reason text,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 UNIQUE(cycle_id,source_key));
CREATE TABLE IF NOT EXISTS cycle_steps (
 id uuid PRIMARY KEY, occurrence_id uuid NOT NULL REFERENCES cycle_occurrences(id), key text NOT NULL,
 definition jsonb NOT NULL, position int NOT NULL, state text NOT NULL DEFAULT 'queued',
 attempt int NOT NULL DEFAULT 0, next_at timestamptz, reason text, output jsonb, UNIQUE(occurrence_id,key));
CREATE TABLE IF NOT EXISTS cycle_attempts (
 id uuid PRIMARY KEY, step_id uuid NOT NULL REFERENCES cycle_steps(id), number int NOT NULL,
 state text NOT NULL DEFAULT 'running', fence uuid NOT NULL, owner text NOT NULL, account_id text, model text,
 started_at timestamptz NOT NULL DEFAULT now(), heartbeat_at timestamptz NOT NULL DEFAULT now(),
 finished_at timestamptz, error_code text, summary text, usage jsonb, context jsonb, receipt jsonb,
 UNIQUE(step_id,number));
CREATE UNIQUE INDEX IF NOT EXISTS one_live_cycle_attempt ON cycle_attempts(step_id) WHERE state IN('running','cancelling','reconciling');
CREATE TABLE IF NOT EXISTS cycle_artifacts (
 id uuid PRIMARY KEY, attempt_id uuid NOT NULL REFERENCES cycle_attempts(id), name text NOT NULL, mime text NOT NULL,
 digest text NOT NULL, size int NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(attempt_id,name));
CREATE TABLE IF NOT EXISTS cycle_events (
 id bigserial PRIMARY KEY, cycle_id uuid NOT NULL REFERENCES cycles(id), occurrence_id uuid REFERENCES cycle_occurrences(id),
 step_id uuid REFERENCES cycle_steps(id), attempt_id uuid REFERENCES cycle_attempts(id),
 kind text NOT NULL, payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS cycle_events_cursor ON cycle_events(occurrence_id,id);
CREATE TABLE IF NOT EXISTS cycle_commands (request_key text PRIMARY KEY, fingerprint text NOT NULL, result jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS cycle_action_receipts (
 action_key text PRIMARY KEY, occurrence_id uuid REFERENCES cycle_occurrences(id), action text NOT NULL,
 state text NOT NULL, input_hash text NOT NULL, receipt jsonb, updated_at timestamptz NOT NULL DEFAULT now());
`);
}
export async function cycleEvent(
  db: DB,
  cycleId: string,
  occurrenceId: string | null,
  kind: string,
  payload: any = {},
  stepId?: string,
  attemptId?: string,
) {
  await db.query(
    'INSERT INTO cycle_events(cycle_id,occurrence_id,step_id,attempt_id,kind,payload) VALUES($1,$2,$3,$4,$5,$6)',
    [cycleId, occurrenceId, stepId ?? null, attemptId ?? null, kind, JSON.stringify(payload)],
  );
  await event(db, 'CYCLE_UPDATED', cycleId, { occurrenceId, kind });
}
export async function createCycle(input: unknown, requestKey: string) {
  const definition = parseCycle(input);
  return tx(async (db) => {
    await lock(db);
    const old = (await db.query('SELECT * FROM cycles WHERE request_key=$1', [requestKey])).rows[0];
    if (old) {
      if (
        JSON.stringify(old.definition) !== JSON.stringify(JSON.parse(JSON.stringify(definition)))
      ) {
        // JSONB key order is not semantic; compare on the database side.
        if (
          !(
            await db.query('SELECT definition=$2::jsonb same FROM cycles WHERE id=$1', [
              old.id,
              JSON.stringify(definition),
            ])
          ).rows[0].same
        )
          conflict();
      }
      return old;
    }
    const id = randomUUID();
    const row = (
      await db.query(
        'INSERT INTO cycles(id,name,project,definition,request_key) VALUES($1,$2,$3,$4,$5) RETURNING *',
        [id, definition.name, definition.project, JSON.stringify(definition), requestKey],
      )
    ).rows[0];
    await db.query('INSERT INTO cycle_versions(cycle_id,version,definition) VALUES($1,1,$2)', [
      id,
      JSON.stringify(definition),
    ]);
    await cycleEvent(db, id, null, 'CREATED');
    return row;
  });
}
export async function updateCycle(id: string, version: number, input: unknown) {
  const d = parseCycle(input);
  return tx(async (db) => {
    await lock(db);
    const row = (
      await db.query(
        'UPDATE cycles SET name=$3,project=$4,definition=$5,version=version+1,updated_at=now() WHERE id=$1 AND version=$2 RETURNING *',
        [id, version, d.name, d.project, JSON.stringify(d)],
      )
    ).rows[0];
    if (!row) conflict();
    await db.query('INSERT INTO cycle_versions(cycle_id,version,definition) VALUES($1,$2,$3)', [
      id,
      row.version,
      JSON.stringify(d),
    ]);
    await cycleEvent(db, id, null, 'DEFINITION_UPDATED', { version: row.version });
    return row;
  });
}
export async function setCycleState(id: string, version: number, state: 'active' | 'paused') {
  return tx(async (db) => {
    await lock(db);
    const row = (
      await db.query(
        "UPDATE cycles SET state=$3,next_check_at=CASE WHEN $3='active' THEN now() ELSE next_check_at END,updated_at=now() WHERE id=$1 AND version=$2 RETURNING *",
        [id, version, state],
      )
    ).rows[0];
    if (!row) conflict();
    await cycleEvent(db, id, null, state === 'active' ? 'ACTIVATED' : 'PAUSED');
    return row;
  });
}
export async function createOccurrence(
  db: DB,
  cycle: any,
  sourceKey: string,
  input: any = {},
  title?: string,
) {
  const old = (
    await db.query('SELECT * FROM cycle_occurrences WHERE cycle_id=$1 AND source_key=$2', [
      cycle.id,
      sourceKey,
    ])
  ).rows[0];
  if (old) return old;
  const id = randomUUID(),
    definition = cycle.definition as CycleDefinition;
  const o = (
    await db.query(
      'INSERT INTO cycle_occurrences(id,cycle_id,version,title,source_key,input,definition) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING *',
      [
        id,
        cycle.id,
        cycle.version,
        title ?? cycle.name,
        sourceKey,
        JSON.stringify(input),
        JSON.stringify(definition),
      ],
    )
  ).rows[0];
  for (const [position, s] of definition.steps.entries())
    await db.query(
      'INSERT INTO cycle_steps(id,occurrence_id,key,definition,position) VALUES($1,$2,$3,$4,$5)',
      [randomUUID(), id, s.key, JSON.stringify(s), position],
    );
  await cycleEvent(db, cycle.id, id, 'QUEUED', { sourceKey, version: cycle.version });
  return o;
}
export async function runCycle(id: string, requestKey: string, input: unknown = {}) {
  return tx(async (db) => {
    await lock(db);
    const cycle = (await db.query('SELECT * FROM cycles WHERE id=$1', [id])).rows[0];
    if (!cycle) throw new DomainError('CYCLE_MISSING', 'Ciclo não encontrado.', 404);
    const fingerprint = hash(JSON.stringify({ id, input }));
    const previous = (
      await db.query('SELECT * FROM cycle_commands WHERE request_key=$1', [requestKey])
    ).rows[0];
    if (previous) {
      if (previous.fingerprint !== fingerprint) conflict();
      return previous.result;
    }
    const result = await createOccurrence(db, cycle, 'manual:' + requestKey, input);
    await db.query('INSERT INTO cycle_commands VALUES($1,$2,$3)', [
      requestKey,
      fingerprint,
      JSON.stringify(result),
    ]);
    return result;
  });
}
export async function listCycles() {
  return (
    await pool.query(`SELECT c.*, (SELECT count(*)::int FROM cycle_occurrences o WHERE o.cycle_id=c.id AND o.state NOT IN('completed','cancelled')) active_count,
   (SELECT count(*)::int FROM cycle_occurrences o WHERE o.cycle_id=c.id AND o.state IN('attention','reconciling')) attention_count,
   (SELECT max(finished_at) FROM cycle_occurrences o WHERE o.cycle_id=c.id AND o.state='completed') last_completed_at
   FROM cycles c ORDER BY c.created_at DESC`)
  ).rows;
}
export async function cycleDetail(id: string) {
  const cycle = (await pool.query('SELECT * FROM cycles WHERE id=$1', [id])).rows[0];
  if (!cycle) throw new DomainError('CYCLE_MISSING', 'Ciclo não encontrado.', 404);
  return {
    cycle,
    occurrences: (
      await pool.query(
        'SELECT id,title,state,reason,version,created_at,finished_at FROM cycle_occurrences WHERE cycle_id=$1 ORDER BY created_at DESC LIMIT 100',
        [id],
      )
    ).rows,
  };
}
export async function occurrenceDetail(id: string) {
  const occurrence = (await pool.query('SELECT * FROM cycle_occurrences WHERE id=$1', [id]))
    .rows[0];
  if (!occurrence) throw new DomainError('OCCURRENCE_MISSING', 'Execução não encontrada.', 404);
  const steps = (
    await pool.query('SELECT * FROM cycle_steps WHERE occurrence_id=$1 ORDER BY position', [id])
  ).rows;
  const attempts = (
    await pool.query(
      'SELECT a.* FROM cycle_attempts a JOIN cycle_steps s ON s.id=a.step_id WHERE s.occurrence_id=$1 ORDER BY a.started_at',
      [id],
    )
  ).rows;
  const artifacts = (
    await pool.query(
      'SELECT a.* FROM cycle_artifacts a JOIN cycle_attempts r ON r.id=a.attempt_id JOIN cycle_steps s ON s.id=r.step_id WHERE s.occurrence_id=$1',
      [id],
    )
  ).rows;
  const events = (
    await pool.query(
      'SELECT * FROM cycle_events WHERE occurrence_id=$1 ORDER BY id DESC LIMIT 200',
      [id],
    )
  ).rows.reverse();
  return { occurrence, steps, attempts, artifacts, events,
    frontier:occurrenceFrontier(occurrence,steps,attempts,artifacts) };
}
export async function commandOccurrence(id: string, command: 'pause' | 'resume' | 'cancel') {
  return tx(async (db) => {
    await lock(db);
    const o = (await db.query('SELECT * FROM cycle_occurrences WHERE id=$1', [id])).rows[0];
    if (!o || ['completed', 'cancelled'].includes(o.state))
      throw new DomainError('OCCURRENCE_TERMINAL', 'Execução encerrada ou inexistente.');
    const state = command === 'pause' ? 'paused' : command === 'cancel' ? 'cancelling' : 'queued';
    if (command === 'resume' && ['attention', 'reconciling', 'cancelling'].includes(o.state))
      throw new DomainError('REVIEW_REQUIRED', 'Resolva a etapa indicada antes de retomar.');
    await db.query('UPDATE cycle_occurrences SET state=$2,updated_at=now() WHERE id=$1', [
      id,
      state,
    ]);
    await cycleEvent(db, o.cycle_id, id, command.toUpperCase());
    return { id, state };
  });
}
