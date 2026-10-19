import { randomUUID } from 'node:crypto';
import type { DB } from './db.js';
import { managerTextHash, projectPersistedManagerDescription } from './manager-context.js';

/** Caller owns BEGIN/COMMIT. Never changes a live attempt, evidence, memory or an old manifest. */
export async function repairStoppedManagerContext(
  db: DB,
  taskId: string,
  expectedRunId: string,
  expectedDescriptionHash: string,
) {
  await db.query('SELECT pg_advisory_xact_lock(4280001)');
  const task = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [taskId])).rows[0];
  const run = (await db.query(
    'SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1 FOR UPDATE', [taskId],
  )).rows[0];
  if (!task || task.stage !== 'coordinate' || task.state !== 'failed' ||
      !run || run.id !== expectedRunId || run.state !== 'failed' ||
      run.error_code !== 'CONTEXT_TOO_LARGE') throw new Error('MANAGER_NOT_STOPPED_CONTEXT_FAILURE');
  if ((await db.query(
    "SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')", [taskId],
  )).rowCount) throw new Error('MANAGER_HAS_LIVE_RUN');
  const operation = (await db.query(`SELECT g.state,m.value management,a.value autonomy
    FROM goals g LEFT JOIN settings m ON m.id='management:'||g.id
    LEFT JOIN settings a ON a.id='autonomy:'||g.project WHERE g.id=$1`, [task.goal_id])).rows[0];
  if (operation?.state !== 'active' || operation.management?.enabled !== true ||
      operation.autonomy?.enabled === false) throw new Error('MANAGER_OPERATION_DISABLED');
  if (task.recovery?.mode === 'manual' && operation.autonomy?.enabled !== true)
    throw new Error('MANAGER_MANUAL_RECOVERY');
  if ((await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
    [run.id])).rowCount) throw new Error('MANAGER_CANCEL_REQUESTED');
  if (managerTextHash(task.description) !== expectedDescriptionHash)
    throw new Error('MANAGER_DESCRIPTION_CHANGED');
  const context = (await db.query('SELECT * FROM contexts WHERE id=$1 AND run_id=$2',
    [run.context_id, run.id])).rows[0];
  if (!context) throw new Error('MANAGER_ORIGINAL_CONTEXT_MISSING');
  if ((context.manifest.dependencyArtifacts?.length ?? 0) ||
      (context.manifest.recoveryArtifacts?.length ?? 0)) throw new Error('MANAGER_INPUTS_REQUIRE_REVIEW');
  const originalId = randomUUID();
  const reference = { contextId: originalId, hash: expectedDescriptionHash,
    characters: task.description.length, kind: 'manager_description_before_projection' };
  const description = projectPersistedManagerDescription(task.description, reference);
  const payloadCharacters = JSON.stringify({
    role: task.agent_id, task: task.title, description, criteria: task.criteria,
    checkpoint: run.checkpoint, context: context.content, dependencies: [],
  }).length;
  if (payloadCharacters > 180000) throw new Error('MANAGER_PROJECTION_STILL_TOO_LARGE');
  await db.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)', [
    originalId, run.id, JSON.stringify({ ...reference, sourceContextId: run.context_id }), task.description,
  ]);
  await db.query("SELECT set_config('agentos.management_wake','1',true)");
  await db.query(`UPDATE tasks SET description=$2,state='ready',priority=100,
    max_attempts=GREATEST(max_attempts,attempt_count+3),next_attempt_at=NULL,
    reason='Contexto do gestor projetado; originais preservados.',updated_at=now() WHERE id=$1`,
    [taskId, description]);
  const receipt = { taskId, runId: run.id, original: reference,
    projectedHash: managerTextHash(description), projectedCharacters: description.length, payloadCharacters };
  await db.query('INSERT INTO events(kind,entity_id,payload) VALUES($1,$2,$3)',
    ['MANAGER_CONTEXT_PROJECTED', taskId, JSON.stringify(receipt)]);
  return receipt;
}
