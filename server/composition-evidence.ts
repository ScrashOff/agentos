import type {DB} from './db.js';

/** Production callbacks persist task entity + payload.runId. Older events have
 * no fence: the immutable run UUID still binds its task and frozen workspace.
 * An explicit fence must match. Never synthesize it in historical evidence.
 * Remove only the validated envelope before comparing the runtime artifact. */
export async function readRuntimeComposition(db:DB,taskId:string,runId:string,workspace?:string) {
 const rows=(await db.query(`SELECT r.id run_id,r.task_id,r.fence,
   r.instruction_snapshot#>>'{task,execution,workspace}' frozen_workspace,
   e.id,e.payload FROM runs r LEFT JOIN LATERAL(
    SELECT id,payload FROM events WHERE kind='PATCH_COMPOSITION_RECORDED'
     AND entity_id=r.task_id::text AND payload->>'runId'=r.id::text
    ORDER BY id DESC LIMIT 2)e ON true WHERE r.id=$1 AND r.task_id=$2`,[runId,taskId])).rows;
 // Multiple receipts for one attempt are ambiguous. Do not fall back past a
 // malformed, mismatched or duplicate event, even if an older one was valid.
 if(rows.length!==1||!rows[0].id)return null;
 const row=rows[0],p=row.payload;
 if(!p||Array.isArray(p)||typeof p!=='object'||p.runId!==runId||
   (Object.hasOwn(p,'runFence')&&p.runFence!==row.fence)||
   typeof row.frozen_workspace!=='string'||p.workspace!==row.frozen_workspace||
   (workspace!==undefined&&workspace!==row.frozen_workspace)||p.version!==1||
   typeof p.complete!=='boolean'||typeof p.dryRun!=='boolean'||!Array.isArray(p.receipts)||
   p.acceptance!==false||p.independentValidationRequired!==true)return null;
 const {runId:_runId,runFence:_runFence,...payload}=p;
 return {id:row.id,payload,identity:{taskId:row.task_id,runId:row.run_id,
   fenceRecorded:Object.hasOwn(p,'runFence')}};
}

/** Small current-attempt directory, using the same identity/normalization as
 * delivery. No full task execution or historical prompt is materialized. */
export async function readManagementCompositions(db:DB,goalId:string,scope:string|null,scopeTaskIds:string[]|null) {
 const candidates=(await db.query(`SELECT r.id run_id,r.task_id,r.attempt,max(e.id) event_id
   FROM events e JOIN runs r ON r.id::text=e.payload->>'runId'
   JOIN tasks t ON t.id=r.task_id LEFT JOIN task_execution_metadata m ON m.task_id=t.id
   WHERE e.kind='PATCH_COMPOSITION_RECORDED' AND e.entity_id=r.task_id::text
    AND t.goal_id=$1 AND r.attempt=t.attempt_count
    AND ($2::text IS NULL OR (t.id=ANY($3::uuid[]) AND m.base_workspace=$2))
   GROUP BY r.id,r.task_id,r.attempt ORDER BY max(e.id) DESC LIMIT 5`,[goalId,scope,scopeTaskIds])).rows;
 const out=[];
 for(const row of candidates){
  const proof=await readRuntimeComposition(db,row.task_id,row.run_id);
  out.push({eventId:String(row.event_id),taskId:row.task_id,expectedAttempt:row.attempt,
   bindingStatus:proof?'BOUND':'INVALID_OR_AMBIGUOUS',payload:proof?.payload??null});
 }
 return out;
}
