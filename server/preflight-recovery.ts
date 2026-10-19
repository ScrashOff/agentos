import {pool,tx,lock,event} from './db.js';
import {instructionTask} from './task-instructions.js';
import {runtimeInputs} from './execution-inputs.js';

/** Reconcile the older missing receipt only when the immutable preflight still
 * proves the same failure before worker.execute. Process stop remains a
 * separate, fresh bridge inspection; missing observation alone is insufficient. */
export async function reconcileUnstartedPreflight(runId:string,readArtifact:(id:string)=>Promise<any>) {
 const record=(await pool.query(`SELECT r.*,t.goal_id,t.execution,c.manifest context_manifest,
  EXISTS(SELECT 1 FROM events e WHERE e.payload->>'runId'=r.id::text
   AND e.kind IN('HARNESS_STARTED','RUNTIME_STARTED','MODEL_RESPONSE','TOOL_CALL','TOOL_RESULT')) runtime_activity
  FROM runs r JOIN tasks t ON t.id=r.task_id JOIN contexts c ON c.id=r.context_id AND c.run_id=r.id
  WHERE r.id=$1 AND r.state='failed' AND r.runtime='harness'`,[runId])).rows[0];
 if(!record || record.runtime_activity || !/^SUPERVISION_INPUTS_(INVALID|DIGEST|TOO_LARGE|COLLISION)$/.test(record.error_code ?? '')) return false;
 const task=instructionTask(record,{id:record.task_id,goal_id:record.goal_id,execution:record.execution},record.context_manifest);
 if(task.execution?.kind!=='harness') return false;
 let proved=false;
 try {await runtimeInputs(task,record.context_manifest,readArtifact);}
 catch(error:any) {proved=error?.code===record.error_code;}
 if(!proved) return false;
 return tx(async db=>{
  await lock(db);
  const current=(await db.query(`SELECT r.id FROM runs r WHERE r.id=$1 AND r.state='failed'
   AND r.fence=$2 AND r.context_id=$3 AND r.error_code=$4
   AND NOT EXISTS(SELECT 1 FROM runs l WHERE l.task_id=r.task_id AND l.state IN('queued','running','cancelling','reconciling'))
   AND NOT EXISTS(SELECT 1 FROM events e WHERE e.payload->>'runId'=r.id::text
    AND e.kind IN('HARNESS_STARTED','RUNTIME_STARTED','MODEL_RESPONSE','TOOL_CALL','TOOL_RESULT'))`,
   [runId,record.fence,record.context_id,record.error_code])).rows[0];
  if(!current) return false;
  if(!(await db.query(`SELECT 1 FROM events WHERE kind='HARNESS_NOT_STARTED'
   AND entity_id=$1 AND payload->>'runId'=$2 AND payload->>'confirmed'='true'`,[record.task_id,runId])).rowCount)
   await event(db,'HARNESS_NOT_STARTED',record.task_id,{runId,confirmed:true,phase:'reconciled_preflight',
    code:record.error_code,contextId:record.context_id,workspace:task.execution.workspace,
    instructionSnapshotPreserved:true,processInspectionStillRequired:true});
  return true;
 });
}
