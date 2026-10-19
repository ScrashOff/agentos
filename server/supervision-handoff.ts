import type { DB } from './db.js';
import { hash } from './memory.js';
import { assertRetryProgress } from './retry-progress.js';
import {
  currentSemanticDecision,
  semanticRepairRejection,
  semanticRetryError,
} from './semantic-retry.js';

// Only persisted, applied supervisor decisions are semantic progress. A model
// string or a heartbeat cannot reset the unchanged-candidate circuit breaker.
export async function trustedSupervision(db: DB, task: any) {
  const ids = [
    task.execution?.supervisionHandoff?.caseId,
    task.execution?.supervision?.caseId,
  ].filter(
    (id) =>
      typeof id === 'string' &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id),
  );
  if (!ids.length) return null;
  return (
    (
      await db.query(
        "SELECT * FROM supervision_cases WHERE id=ANY($1::uuid[]) AND task_id=$2 AND goal_id=$3 AND state='applied' ORDER BY updated_at DESC LIMIT 1",
        [ids, task.id, task.goal_id],
      )
    ).rows[0] ?? null
  );
}

export async function continuationSignature(
  db: DB,
  task: any,
  code: string,
  digest: string | null,
) {
  const revision = await trustedSupervision(db, task);
  return hash(JSON.stringify([code, digest, ...(revision ? [revision.id] : [])]));
}

// Recheck under the same lock as queue admission: inspection runs outside the
// transaction and an explicit pause/cancel can arrive while SSH is in flight.
export async function assertSupervisedResume(db: DB, task: any, run: any, proof: any) {
  const rejection = await semanticRepairRejection(db, task, run);
  if (rejection) {
    if (rejection.kind === 'semantic_repair_evidence_invalid') throw semanticRetryError(rejection);
    const current = await trustedSupervision(db, task);
    if (
      typeof proof.supervisionResumeCaseId !== 'string' ||
      proof.supervisionResumeCaseId !== current?.id ||
      !(await currentSemanticDecision(db, task, run, current))
    )
      throw semanticRetryError(rejection);
  }
  await assertRetryProgress(db, task, run, proof);
  if (!proof.supervisionResumeCaseId) return;
  const c = await trustedSupervision(db, task);
  const directive =
    task.execution?.supervisionHandoff?.caseId === c?.id
      ? task.execution.supervisionHandoff
      : task.execution?.supervision;
  if (
    !c ||
    c.id !== proof.supervisionResumeCaseId ||
    c.applied_attempt !== run.attempt ||
    (directive?.sourceRunId ?? c.source_run_id) !== run.id ||
    task.attempt_count !== run.attempt
  )
    throw Error('SUPERVISION_HANDOFF_CHANGED');
  if (
    !(
      await db.query(
        `SELECT 1 FROM goals g JOIN settings s ON s.id='supervision:'||g.id
    LEFT JOIN settings a ON a.id='autonomy:'||g.project WHERE g.id=$1 AND g.state='active'
    AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
    AND ($2::text IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')`,
        [task.goal_id, task.recovery?.mode ?? null],
      )
    ).rowCount
  )
    throw Error('SUPERVISION_DISABLED');
  if (
    (
      await db.query(
        `SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1
    UNION ALL SELECT 1 FROM runs WHERE task_id=$2 AND state IN('queued','running','reconciling','cancelling')
    UNION ALL SELECT 1 FROM supervision_cases WHERE task_id=$2 AND state IN('open','decided','delegated','waiting_evidence')
    UNION ALL SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=$2 AND p.state<>'accepted' LIMIT 1`,
        [run.id, task.id],
      )
    ).rowCount
  )
    throw Error('SUPERVISION_HANDOFF_CHANGED');
}

// A native retry bypasses the generic scheduler, so every admission path must
// check the complete current DAG, including work added after a supervisor plan.
export async function assertDependenciesAccepted(db: DB, taskId: string) {
  if (
    (
      await db.query(
        "SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=$1 AND p.state<>'accepted' LIMIT 1",
        [taskId],
      )
    ).rowCount
  )
    throw Error('DEPENDENCIES_PENDING');
}
