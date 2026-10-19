import {randomUUID} from 'node:crypto';
import {pool,tx,lock,event} from './db.js';
import {requestWorkspacePreparation} from './workspace-preparation.js';
import {acceptedSnapshotProof,sameSnapshotProof} from './deferred-handoffs.js';
import {inspectStopped,assertStopped} from './continuation.js';
import {supervisionInputs} from './supervision-context.js';
import {mergeSupervisorInputs} from './execution-inputs.js';

/** Version only auxiliary readiness. Integration/final QA gates are never removed. */
export async function applyPlanningDependencyChange(request:any,change:any):Promise<{applied:boolean;existing?:boolean;preparing?:boolean}>{
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[change.taskId])).rows[0];
  const source=(await pool.query('SELECT * FROM tasks WHERE id=$1',[change.sourceTaskId])).rows[0];
  if(!task || !source || task.goal_id!==request.goal_id || source.goal_id!==request.goal_id)throw Error('DEPENDENCY_SCOPE_INVALID');
  if(!['diagnostic','repair'].includes(task.execution?.workKind) || task.stage==='verify' ||
    task.execution?.baseWorkspace!==request.scope || source.execution?.baseWorkspace!==request.scope)throw Error('AUXILIARY_SOURCE_CHANGED');
  const operationKey='snapshot-readiness:'+request.id+':'+task.id;
  if((await pool.query("SELECT 1 FROM events WHERE kind='AUXILIARY_READINESS_VERSIONED' AND payload->>'operationKey'=$1",[operationKey])).rowCount)return {applied:true,existing:true};
  const sourceProof=await acceptedSnapshotProof(pool,source);
  const workspace=request.scope.split('/')[0]+'/o-rs-'+request.id.replaceAll('-','').slice(0,12)+'-'+task.id.replaceAll('-','').slice(0,12);
  if(!await requestWorkspacePreparation(request.goal_id,workspace,source.execution.workspace))return {applied:false,preparing:true};
  const latest=(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[task.id])).rows[0];
  const stop=latest?await inspectStopped(latest.id,task.execution.kind==='opencode'):null;
  const context=await supervisionInputs(pool,source,task);
  return tx(async db=>{
    await lock(db);
    if((await db.query("SELECT 1 FROM events WHERE kind='AUXILIARY_READINESS_VERSIONED' AND payload->>'operationKey'=$1",[operationKey])).rowCount)return {applied:true,existing:true};
    const current=(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[task.id])).rows[0];
    const currentSource=(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[source.id])).rows[0];
    const parent=(await db.query('SELECT * FROM tasks WHERE id=$1',[change.removeDependsOnId])).rows[0];
    if(!(await db.query("SELECT 1 FROM goals WHERE id=$1 AND state='active'",[request.goal_id])).rowCount)return {applied:false};
    if(current.attempt_count!==change.expectedAttempt || !['diagnostic','repair'].includes(current.execution?.workKind) ||
      current.stage==='verify' || parent?.stage!=='integrate' || current.execution?.baseWorkspace!==request.scope ||
      parent.execution?.baseWorkspace!==request.scope || currentSource.execution?.baseWorkspace!==request.scope ||
      ['queued','running','cancelling','reconciling','accepted','superseded','cancelled'].includes(current.state) ||
      (await db.query("SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')",[task.id])).rowCount)throw Error('AUXILIARY_SOURCE_CHANGED');
    const actual=(await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[task.id])).rows[0];
    if(actual?.id!==latest?.id)throw Error('AUXILIARY_SOURCE_CHANGED');
    if(actual)assertStopped(stop,actual,current);
    if(!sameSnapshotProof(await acceptedSnapshotProof(db,currentSource),sourceProof))throw Error('SOURCE_MANIFEST_CHANGED');
    const dep=(await db.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2',[task.id,parent.id])).rowCount;
    if(!dep)throw Error('DEPENDENCY_CHANGED');
    if((await db.query(`WITH RECURSIVE parents(id) AS(SELECT depends_on FROM dependencies WHERE task_id=$1 UNION
      SELECT d.depends_on FROM dependencies d JOIN parents p ON d.task_id=p.id) SELECT 1 FROM parents WHERE id=$2 UNION SELECT 1 WHERE $1::uuid=$2::uuid`,[source.id,task.id])).rowCount)throw Error('DEPENDENCY_CYCLE');
    const execution={...current.execution,workspace,sourceWorkspace:source.execution.workspace,inputs:mergeSupervisorInputs(current.execution.inputs??[],context.inputs),
      snapshotReadiness:{version:1,operationKey,requestId:request.id,sourceProof,removedDependency:parent.id,
        reason:change.reason,artifactIds:change.artifactIds,priorWorkspace:current.execution.workspace}};
    // Archive the exact before-image as an immutable context for its existing
    // attempt (or a persistent event if it has never run).
    if(actual)await db.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)',[
      randomUUID(),actual.id,JSON.stringify({kind:'auxiliary_readiness_before',operationKey}),JSON.stringify({execution:current.execution,criteria:current.criteria,dependency:parent.id})]);
    await db.query('DELETE FROM dependencies WHERE task_id=$1 AND depends_on=$2',[task.id,parent.id]);
    await db.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2) ON CONFLICT DO NOTHING',[task.id,source.id]);
    await db.query("UPDATE tasks SET execution=$2,state='ready',reason='Diagnóstico independente preparado em snapshot aceito; aceite final preservado.',next_attempt_at=NULL,max_attempts=GREATEST(max_attempts,attempt_count+3),updated_at=now() WHERE id=$1",[task.id,JSON.stringify(execution)]);
    await event(db,'AUXILIARY_READINESS_VERSIONED',task.id,{operationKey,requestId:request.id,sourceProof,removedDependency:parent.id,
      priorWorkspace:current.execution.workspace,workspace,criteriaPreserved:current.criteria,reason:change.reason,artifactIds:change.artifactIds});return {applied:true};
  });
}
