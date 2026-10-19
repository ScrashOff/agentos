import type { DB } from './db.js';
import { event } from './db.js';

/** Routing only: storage recovery independently rechecks unit stop, latest
 * attempt, cancellation, workspace, dependencies and supervisory ownership. */
export async function hasStoragePreflightReceipt(db:DB,runId:string,taskId:string) {
 return !!(await db.query(`SELECT 1 FROM events e JOIN runs r ON r.id=$1::uuid AND r.task_id=$2::uuid
   WHERE e.entity_id=$2::text AND e.payload->>'runId'=$1::text AND e.payload->>'confirmed'='true'
   AND ((r.runtime='opencode' AND e.kind='OPENCODE_NOT_STARTED' AND e.payload->>'errorCode'='DISK_CAPACITY')
     OR (r.runtime='harness' AND e.kind='HARNESS_NOT_STARTED' AND e.payload->>'code'='DISK_CAPACITY'))
   AND NOT EXISTS(SELECT 1 FROM events x WHERE x.payload->>'runId'=$1::text AND x.kind IN
     ('CANCEL_REQUESTED','RUNTIME_STARTED','HARNESS_STARTED','MODEL_RESPONSE','TOOL_CALL','TOOL_RESULT','HARNESS_TOOL_EVENT','OPENCODE_TOOL_EVENT'))
   LIMIT 1`,[runId,taskId])).rowCount;
}

type Preflight = {
  runId: string;
  fence: string;
  workerId: string;
  remoteStartAttempted: boolean;
  cancelRequested: boolean;
  errorCode?: string;
};

/** Call under the controller's normal transaction/advisory lock, before failRun. */
export async function recordOpenCodeNotStarted(db: DB, input: Preflight) {
  if (input.remoteStartAttempted || input.cancelRequested) return false;
  const row = (
    await db.query(
      `SELECT r.task_id FROM runs r JOIN tasks t ON t.id=r.task_id
    JOIN opencode_jobs j ON j.run_id=r.id WHERE r.id=$1 AND r.fence=$2 AND r.worker_id=$3
    AND r.state='running' AND r.runtime='opencode' AND t.execution->>'kind'='opencode'
    AND t.state='running' AND j.state='running' AND r.attempt=t.attempt_count
    AND NOT EXISTS(SELECT 1 FROM runs n WHERE n.task_id=t.id AND n.attempt>r.attempt)
    AND NOT EXISTS(SELECT 1 FROM events e WHERE e.payload->>'runId'=r.id::text
      AND e.kind IN('CANCEL_REQUESTED','OPENCODE_LAUNCH_ATTEMPTED'))
    FOR UPDATE OF r,t`,
      [input.runId, input.fence, input.workerId],
    )
  ).rows[0];
  if (!row) return false;
  if (
    (
      await db.query(
        "SELECT 1 FROM events WHERE kind='OPENCODE_NOT_STARTED' AND payload->>'runId'=$1",
        [input.runId],
      )
    ).rowCount
  )
    return true;
  await event(db, 'OPENCODE_NOT_STARTED', row.task_id, {
    version: 1,
    runId: input.runId,
    workerId: input.workerId,
    runtime: 'opencode',
    confirmed: true,
    remoteStartAttempted: false,
    cancelRequested: false,
    phase: 'before_remote_spawn',
    errorCode: /^[A-Z][A-Z0-9_]{2,80}$/.test(input.errorCode ?? '')
      ? input.errorCode
      : 'OPENCODE_PREFLIGHT_ERROR',
  });
  return true;
}

// This legacy source was inspected byte-for-byte: context is durably committed
// before remote spawn. New digests require another code audit, never guessing
// from the error name or from a missing runtime record.
export const auditedLegacyOpenCodeControllerDigest =
  '52fa0b8deec37a39b57343b71276677c7420dbf678d3088b257c094f551ea7e8';
const preContextErrors = [
  'SUPERVISION_CONTEXT_CHANGED',
  'REQUIRED_CONTEXT_MISSING',
  'CONTEXT_TOO_LARGE',
];
export type LegacyNotStartedCandidate = {
  runId: string;
  taskId: string;
  fence: string;
  workerId: string;
  workspace: string;
  errorCode: string;
  controllerDigest: string;
  jobFinishedAt: string;
};

/** Read-only eligibility check before the real two-phase remote unit inspection. */
export async function legacyOpenCodeNotStartedCandidate(
  db: DB,
  runId: string,
  controllerDigest: string,
): Promise<LegacyNotStartedCandidate | null> {
  if (controllerDigest !== auditedLegacyOpenCodeControllerDigest) return null;
  const r = (
    await db.query(
      `SELECT r.id,r.task_id,r.fence,r.worker_id,r.error_code,t.execution->>'workspace' workspace,j.finished_at
    FROM runs r JOIN tasks t ON t.id=r.task_id JOIN opencode_jobs j ON j.run_id=r.id
    WHERE r.id=$1 AND r.state='failed' AND r.runtime='opencode' AND r.context_id IS NULL
    AND t.execution->>'kind'='opencode' AND t.state IN('failed','recovery_blocked')
    AND r.attempt=t.attempt_count AND r.worker_id LIKE 'opencode-native-%'
    AND r.error_code=ANY($2::text[]) AND r.finished_at IS NOT NULL
    AND j.state='finished' AND j.finished_at>=r.finished_at
    AND EXISTS(SELECT 1 FROM events e WHERE e.kind='RUN_QUEUED' AND e.payload->>'runId'=r.id::text AND e.created_at<=j.finished_at)
    AND EXISTS(SELECT 1 FROM events e WHERE e.kind='RUN_STARTED' AND e.payload->>'runId'=r.id::text AND e.created_at<=j.finished_at)
    AND EXISTS(SELECT 1 FROM events e WHERE e.kind='RUN_FAILED' AND e.payload->>'runId'=r.id::text AND e.payload->>'code'=r.error_code AND e.created_at<=j.finished_at)
    AND NOT EXISTS(SELECT 1 FROM contexts c WHERE c.run_id=r.id)
    AND NOT EXISTS(SELECT 1 FROM runs n WHERE n.task_id=t.id AND n.attempt>r.attempt)
    AND NOT EXISTS(SELECT 1 FROM events e WHERE e.payload->>'runId'=r.id::text
      AND NOT(e.kind IN('RUN_QUEUED','RUN_STARTED','RUN_FAILED') OR
        (e.kind='CONTINUATION_STATUS' AND e.created_at>=j.finished_at)))`,
      [runId, preContextErrors],
    )
  ).rows[0];
  if (!r?.workspace || !r?.worker_id || !/^opencode-native-\d+$/.test(r.worker_id)) return null;
  return {
    runId: r.id,
    taskId: r.task_id,
    fence: r.fence,
    workerId: r.worker_id,
    workspace: r.workspace,
    errorCode: r.error_code,
    controllerDigest,
    jobFinishedAt: new Date(r.finished_at).toISOString(),
  };
}

export function legacyNotStartedProofMatches(
  candidate: LegacyNotStartedCandidate,
  proof: any,
  now = Date.now(),
) {
  const age = now - Date.parse(proof?.checkedAt ?? '');
  return (
    proof?.runId === candidate.runId &&
    proof?.workspace === candidate.workspace &&
    proof?.state === 'not_started' &&
    proof?.process?.unit === 'agentos-run-' + candidate.runId &&
    proof?.process?.stopped === true &&
    proof?.process?.mainPid === 0 &&
    proof?.process?.populated === false &&
    ['inactive', 'failed'].includes(proof?.process?.activeState) &&
    Number.isFinite(age) &&
    age >= -5000 &&
    age <= 60000 &&
    Date.parse(proof.checkedAt) >= Date.parse(candidate.jobFinishedAt)
  );
}

/** Receives only the existing inspector's --not-started proof; never launches/stops anything. */
export async function recordLegacyOpenCodeNotStarted(
  db: DB,
  candidate: LegacyNotStartedCandidate,
  proof: any,
  now = Date.now(),
) {
  if (!legacyNotStartedProofMatches(candidate, proof, now)) return false;
  const current = await legacyOpenCodeNotStartedCandidate(
    db,
    candidate.runId,
    candidate.controllerDigest,
  );
  if (!current || JSON.stringify(current) !== JSON.stringify(candidate)) return false;
  // Caller holds the same scheduler lock used for cancellation/recovery. A row
  // lock also prevents a changed run from acquiring a retrospective receipt.
  const row = (
    await db.query("SELECT id FROM runs WHERE id=$1 AND fence=$2 AND state='failed' FOR UPDATE", [
      candidate.runId,
      candidate.fence,
    ])
  ).rows[0];
  if (!row) return false;
  await event(db, 'OPENCODE_NOT_STARTED', candidate.taskId, {
    version: 1,
    runId: candidate.runId,
    workerId: candidate.workerId,
    runtime: 'opencode',
    confirmed: true,
    remoteStartAttempted: false,
    cancelRequested: false,
    phase: 'before_context_commit',
    retrospective: true,
    errorCode: candidate.errorCode,
    controllerDigest: candidate.controllerDigest,
    jobFinishedAt: candidate.jobFinishedAt,
    proof: {
      workspace: proof.workspace,
      state: proof.state,
      process: proof.process,
      checkedAt: proof.checkedAt,
    },
  });
  return true;
}
