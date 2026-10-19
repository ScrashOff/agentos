import type { DB } from './db.js';
import { readPreparedNativeAdmission } from './supervised-opencode-admission.js';

const live = ['queued', 'running', 'cancelling', 'reconciling'];
const admissionStates = ['retry_scheduled', 'ready', 'waiting_quota', 'waiting_resource', 'waiting_configuration'];

/** The supervised resume already proved physical stop and queued the next
 * attempt. Until admission creates that attempt, the latest failed run still
 * describes the previous instructions. Comparing it with the authorized new
 * instructions must not manufacture another failure. This receipt only waits;
 * it neither launches work nor certifies the revised contract or its output. */
export async function scheduledSupervisionAdmission(db: DB, taskId: string, runId: string) {
  // A prepared native request shares the same applied supervisor owner. It is
  // not a new failure while capacity/inspection waits, and is not permission
  // to launch: the scheduler still requires a fresh physical-stop proof.
  try {
    const recorded=(await db.query(`SELECT 1 FROM events WHERE kind='SUPERVISION_RESUME_PREPARED'
      AND entity_id=$1 AND payload->>'sourceRunId'=$2 LIMIT 1`,[taskId,runId])).rowCount;
    const native=recorded?await readPreparedNativeAdmission(db,taskId):null;
    if(native && native.run.id===runId && (await db.query(`SELECT 1 FROM goals g
      JOIN settings s ON s.id='supervision:'||g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project
      WHERE g.id=$1 AND g.state='active' AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
      AND ($4::text IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
      AND NOT EXISTS(SELECT 1 FROM runs WHERE task_id=$2 AND state IN('queued','running','reconciling','cancelling'))
      AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$3)
      AND NOT EXISTS(SELECT 1 FROM supervision_cases WHERE task_id=$2 AND state IN('open','decided','delegated','waiting_evidence'))
      AND NOT EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=$2 AND p.state<>'accepted')`,
      [native.task.goal_id,taskId,runId,native.task.recovery?.mode??null])).rowCount){
      return {case_id:native.receipt.caseId,source_run_id:runId,attempt:native.run.attempt};
    }
  } catch(e:any) {
    // An invalid binding cannot suppress a new review. Unexpected database
    // failures propagate rather than pretending the ownership was absent.
    if(!/^SUPERVISED_NATIVE_/.test(e?.code??'') &&
      !['SUPERVISION_HANDOFF_CHANGED','SUPERVISION_DISABLED','REPEATED_NO_PROGRESS'].includes(e?.message))throw e;
  }
  return (await db.query(`SELECT c.id case_id,r.id source_run_id,r.attempt FROM tasks t JOIN goals g ON g.id=t.goal_id
    JOIN settings s ON s.id='supervision:'||g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project
    JOIN LATERAL(SELECT id,attempt,state,runtime,error_code,checkpoint,instruction_snapshot
      FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    JOIN supervision_cases c ON c.task_id=t.id AND c.goal_id=t.goal_id AND c.source_run_id=r.id
    WHERE t.id=$1 AND r.id=$2 AND r.attempt=t.attempt_count AND t.state=ANY($3::text[])
      AND r.runtime='harness' AND r.state IN('failed','submitted')
      AND COALESCE(r.error_code,'') !~ 'DIGEST|CORRUPT|INVALID_WRITE_SCOPE|INVALID_WORKSPACE'
      AND g.state='active' AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
      AND t.execution->>'kind'='harness'
      AND NOT(COALESCE(t.execution->>'readOnlySource','false')='true' AND t.stage IN('verify','review'))
      AND c.state='applied' AND c.applied_attempt=r.attempt
      AND c.id::text IN(t.execution#>>'{supervision,caseId}',t.execution#>>'{supervisionHandoff,caseId}')
      AND CASE WHEN t.execution#>>'{supervisionHandoff,caseId}'=c.id::text
        THEN t.execution#>>'{supervisionHandoff,sourceRunId}' ELSE t.execution#>>'{supervision,sourceRunId}' END=r.id::text
      AND t.execution#>>'{continuation,state}'='queued' AND t.execution#>>'{continuation,runId}'=r.id::text
      AND r.checkpoint->>'kind'='automatic_recovery' AND r.checkpoint->>'sourceRunId'=r.id::text
      AND r.checkpoint->>'preservedWorkspace'=t.execution->>'workspace'
      AND r.instruction_snapshot#>>'{task,execution,workspace}'=t.execution->>'workspace'
      AND NOT EXISTS(SELECT 1 FROM runs active WHERE active.task_id=t.id AND active.state=ANY($4::text[]))
      AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs leased ON leased.id=l.run_id WHERE leased.task_id=t.id)
      AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text)
      AND NOT EXISTS(SELECT 1 FROM supervision_cases pending WHERE pending.task_id=t.id
        AND pending.state IN('open','decided','delegated','waiting_evidence'))
    ORDER BY c.created_at DESC,c.id DESC LIMIT 1`, [taskId, runId, admissionStates, live])).rows[0] ?? null;
}
