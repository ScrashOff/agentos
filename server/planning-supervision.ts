import {randomUUID,createHash} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
import {event,type DB} from './db.js';
import {verifiedBatch} from './incremental-planning.js';
import {DomainError} from './contracts.js';

export const planningContractVersion='incremental-parts-v3-attempt-local-outputs';
const digest=(v:unknown)=>createHash('sha256').update(JSON.stringify(v)).digest('hex');

/** The existing planner remains the only owner. A committed invalid-plan
 * incident may resume once per contract version, never a transport/prepare
 * uncertainty, pause, cancellation, newer revision or workforce handover. */
export async function reconcilePlanningFailures(db:DB){
  const rows=(await db.query(`SELECT p.*,o.request FROM planning_runs p
    JOIN objectives o ON o.id=p.objective_id JOIN goals g ON g.id=o.id
    JOIN settings policy ON policy.id='objective-review:'||o.id AND policy.value->>'mode'='supervised'
    JOIN plan_revisions revision ON revision.objective_id=o.id AND revision.revision=o.revision
    JOIN LATERAL(SELECT payload FROM events WHERE entity_id=o.id::text AND kind='PLANNING_BLOCKED'
      AND payload->>'planningRunId'=p.id::text ORDER BY id DESC LIMIT 1)blocked ON true
    WHERE p.state='failed' AND p.base_revision=o.revision AND o.mode='execute'
      AND g.state IN('active','planning','replanning','failed') AND revision.plan ? 'incremental'
      AND p.output IS NULL AND blocked.payload#>>'{recovery,category}'='invalid_plan'
      AND p.feedback#>>'{planningRecovery,contractVersion}' IS DISTINCT FROM $1
      AND NOT EXISTS(SELECT 1 FROM objective_feedback f WHERE f.objective_id=o.id
        AND NOT(COALESCE(p.feedback->'feedbackIds','[]') ? f.id::text)
        AND NOT EXISTS(SELECT 1 FROM planning_runs consumed WHERE consumed.objective_id=o.id AND consumed.state='completed'
          AND (consumed.feedback->'feedbackIds' ? f.id::text OR consumed.created_at>f.created_at)))
      AND NOT EXISTS(SELECT 1 FROM settings w WHERE w.id='workforce:'||o.id AND w.value->>'enabled'='true')
      AND NOT EXISTS(SELECT 1 FROM planning_runs newer WHERE newer.objective_id=o.id AND
        (newer.created_at>p.created_at OR newer.id<>p.id AND newer.state IN('queued','running','waiting_quota')))
      AND NOT EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=o.id
        AND (r.state IN('queued','running','cancelling','reconciling') OR EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id)))
    ORDER BY p.created_at LIMIT 10`,[planningContractVersion])).rows;
  for(const row of rows){
    if(row.feedback?.incrementalAdvance){
      const proof=await verifiedBatch(db,row.objective_id,row.base_revision);
      if(!proof||!isDeepStrictEqual(proof,row.feedback.incrementalAdvance)||
        row.feedback.sourceOverrides?.[row.request.workspaces[0]]!==proof.sourceWorkspace)continue;
    }
    const recovery={contractVersion:planningContractVersion,incidentId:randomUUID(),mode:'supervised',
      originError:row.error,failureFingerprint:digest([row.id,row.base_revision,row.error,planningContractVersion])};
    await db.query(`UPDATE planning_runs SET state='queued',fence=gen_random_uuid(),finished_at=NULL,
      next_attempt_at=now(),feedback=$2 WHERE id=$1 AND state='failed'`,[row.id,JSON.stringify({...row.feedback,planningRecovery:recovery})]);
    await db.query("UPDATE objectives SET status='replanning',reason=NULL,updated_at=now() WHERE id=$1",[row.objective_id]);
    await db.query("UPDATE goals SET state='replanning' WHERE id=$1",[row.objective_id]);
    await event(db,'PLANNING_REVIEW_QUEUED',row.objective_id,{planningRunId:row.id,incidentId:recovery.incidentId,
      contractVersion:planningContractVersion,baseRevision:row.base_revision,failureFingerprint:recovery.failureFingerprint,
      preservedAdvanceProof:!!row.feedback?.incrementalAdvance,oldAttemptsPreserved:true});
  }
  return rows.length;
}

export async function assertPlanningFeedbackCurrent(db:DB,run:any){
  if((await db.query(`SELECT 1 FROM objective_feedback f WHERE f.objective_id=$1
    AND NOT($2::jsonb ? f.id::text) AND NOT EXISTS(SELECT 1 FROM planning_runs consumed
      WHERE consumed.objective_id=f.objective_id AND consumed.state='completed'
        AND (consumed.feedback->'feedbackIds' ? f.id::text OR consumed.created_at>f.created_at)) LIMIT 1`,
    [run.objective_id,JSON.stringify(run.feedback?.feedbackIds??[])])).rowCount)
    throw new DomainError('PLANNING_CONTEXT_CHANGED','Nova evidência precisa ser considerada antes de aplicar o plano.');
}

export function planningReviewFallback(feedback:any,rejection:{code:string;detail?:unknown;message:string}){
  const current=feedback?.planningRecovery;
  if(current?.contractVersion!==planningContractVersion||current.mode!=='supervised')return null;
  return {...feedback,planningRecovery:{...current,mode:'serial',rejection}};
}

/** A valid plan is only applied intent. Verify the effect after the new batch
 * actually passes independent acceptance, retaining its exact candidate proof. */
export async function verifyPlanningReviewEffects(db:DB){
  const rows=(await db.query(`SELECT p.id,p.objective_id,p.feedback,r.revision FROM planning_runs p
    JOIN plan_revisions r ON r.planning_run_id=p.id JOIN goals g ON g.id=p.objective_id
    WHERE p.state='completed' AND p.feedback#>>'{planningRecovery,contractVersion}'=$1
      AND p.feedback#>'{planningRecovery,effect}' IS NULL AND g.state NOT IN('paused','cancelled')
    ORDER BY p.created_at LIMIT 10`,[planningContractVersion])).rows;
  for(const row of rows){
    const proof=await verifiedBatch(db,row.objective_id,row.revision);if(!proof)continue;
    await db.query(`UPDATE planning_runs SET feedback=jsonb_set(feedback,'{planningRecovery,effect}',$2::jsonb) WHERE id=$1`,[row.id,JSON.stringify(proof)]);
    await event(db,'PLANNING_REVIEW_EFFECT_VERIFIED',row.objective_id,{planningRunId:row.id,
      incidentId:row.feedback.planningRecovery.incidentId,revision:row.revision,proof});
  }
}
