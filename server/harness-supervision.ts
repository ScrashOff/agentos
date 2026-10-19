import {pool,tx,lock,event} from './db.js';
import {heartbeat,writeArtifact,completeRun,validateTask,failRun,confirmStopped,refreshGoal} from './service.js';
import {assembleContext} from './memory.js';
import {acceptedDependencyArtifacts} from './recovery.js';
import {observeInputFailureBounded} from './input-failure-observation.js';
import {executeRuntime,type RuntimeInput,type RuntimeResult} from './runtime.js';
import {startDetachedNativeRun,reconcileNativeSupervisor,type NativeReservation,type NativeUnitObservation} from './native-supervision.js';
import {RunHeartbeat} from './run-heartbeat.js';
import {inspectStopped,assertStopped} from './continuation.js';

/** Record termination independently from the semantic decision to retry.
 * Both the version-pinned supervisor and its remote sandbox must be stopped.
 * This releases a dead reservation; it never queues or accepts another run. */
export async function reconcileStoppedDetachedHarness(reservation:NativeReservation,
  observation:NativeUnitObservation,inspect=inspectStopped){
  if(reservation.runtime!=='harness')return false;
  const proof=await inspect(reservation.runId,false);
  return tx(async db=>{
    if(await reconcileNativeSupervisor(db,reservation,observation)!=='reconciling')return false;
    const row=(await db.query(`SELECT r.id,r.fence,r.state,r.attempt,r.task_id,r.error_code,r.checkpoint,
      r.instruction_snapshot->'task'->'execution'->>'workspace' frozen_workspace,
      t.attempt_count,t.state task_state,t.goal_id,g.state goal_state
      FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
      WHERE r.id=$1 FOR UPDATE OF r,t`,[reservation.runId])).rows[0];
    if(!row || row.fence!==reservation.runFence || row.state!=='reconciling' ||
      !['reconciling','cancelling'].includes(row.task_state) || row.attempt_count!==row.attempt ||
      row.goal_state!=='active' || !row.frozen_workspace)return false;
    assertStopped(proof,row,{execution:{workspace:row.frozen_workspace}});
    const cancelled=!!(await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
      [row.id])).rowCount;
    const receipt={checkedAt:proof.checkedAt,workspace:proof.workspace,process:proof.process,
      supervisor:observation,runFence:row.fence,candidateDigest:proof.candidate?.candidateDigest,
      baseDigest:proof.candidate?.baseDigest};
    await db.query(`UPDATE runs SET state=$2,finished_at=COALESCE(finished_at,clock_timestamp()),
      checkpoint=COALESCE(checkpoint,'{}'::jsonb)||jsonb_build_object('nativeStopReceipt',$3::jsonb) WHERE id=$1`,
      [row.id,cancelled?'cancelled':'failed',JSON.stringify(receipt)]);
    await db.query("UPDATE tasks SET state=$2,reason=$3,updated_at=clock_timestamp() WHERE id=$1",
      [row.task_id,cancelled?'cancelled':'failed',cancelled?null:'Executor e sandbox encerrados; aguarda recuperação supervisionada.']);
    await db.query("UPDATE opencode_jobs SET state='finished',finished_at=COALESCE(finished_at,clock_timestamp()) WHERE run_id=$1",[row.id]);
    await db.query('DELETE FROM leases WHERE run_id=$1',[row.id]);
    await event(db,'DETACHED_RUN_STOP_RECONCILED',row.task_id,{runId:row.id,attempt:row.attempt,
      receipt,cancelled,semanticResumeRequired:!cancelled,newAttemptQueued:false});
    await event(db,cancelled?'RUN_CANCELLED':'RUN_FAILED',row.task_id,{runId:row.id,
      code:row.error_code,stopped:true,source:'detached_stop_reconciliation'});
    await refreshGoal(db,row.goal_id);
    return true;
  });
}

/** One version-pinned owner runs the existing Harness contract. The dispatcher
 * owns no abort controller for this actor and may upgrade without stopping it. */
export async function executeDetachedHarness(runId:string,reservation:NativeReservation,
  execute:(input:RuntimeInput)=>Promise<RuntimeResult>=executeRuntime) {
  if(reservation.runtime!=='harness')throw Error('HARNESS_RESERVATION_REQUIRED');
  const workerId='harness-native-'+process.pid;
  const run=await tx(db=>startDetachedNativeRun(db,reservation,workerId));
  if(!run)return;
  const controller=new AbortController();
  let runtimeStarted=false;
  const stopping=()=>controller.abort();
  process.once('SIGTERM',stopping);process.once('SIGINT',stopping);
  const pulse=new RunHeartbeat(()=>heartbeat(runId,run.fence),()=>controller.abort());
  const timer=setInterval(()=>void pulse.tick(),3000);
  try {
    const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[run.task_id])).rows[0];
    const account=(await pool.query('SELECT * FROM accounts WHERE id=$1',[run.account_id])).rows[0];
    task.model=run.model??task.model;
    const context=await assembleContext(run,task,await acceptedDependencyArtifacts(task.id));
    if((await heartbeat(runId,run.fence))?.state!=='running')controller.abort();
    if(controller.signal.aborted){await confirmStopped(runId,run.fence);return;}
    await event(pool,'HARNESS_LAUNCH_ATTEMPTED',run.task_id,{runId,workerId,
      candidate:reservation.candidate,digest:reservation.digest});
    runtimeStarted=true;
    const result=await execute({run,task,account,context,signal:controller.signal,
      onEvent:(kind,payload)=>event(pool,kind,task.id,{runId,...payload})});
    if(controller.signal.aborted){await confirmStopped(runId,run.fence);return;}
    for(const artifact of result.artifacts)
      await writeArtifact(runId,run.fence,artifact.name,artifact.content,artifact.mime);
    await completeRun(runId,run.fence,result.summary,result.usage);
    await validateTask(task.id).catch((e:any)=>{
      if(!['STATE_CHANGED','NOT_SUBMITTED'].includes(e?.code))throw e;
    });
  }catch(e:any){
    try{
      if(!runtimeStarted)await event(pool,'HARNESS_NOT_STARTED',run.task_id,
        {runId,confirmed:true,code:e?.code});
      if(e?.code==='STOP_UNCONFIRMED')await tx(async db=>{
        await lock(db);
        const changed=await db.query("UPDATE runs SET state='reconciling',error_code='STOP_UNCONFIRMED' WHERE id=$1 AND fence=$2 AND state IN('running','cancelling') RETURNING id",[runId,run.fence]);
        if(!changed.rowCount)return;
        await db.query("UPDATE tasks SET state='reconciling',reason='Encerramento não confirmado; recursos preservados.' WHERE id=$1",[run.task_id]);
        await event(db,'RUN_RECONCILING',run.task_id,{runId});
      });
      else if(controller.signal.aborted){
        await confirmStopped(runId,run.fence);
        if((await pool.query('SELECT state FROM runs WHERE id=$1',[runId])).rows[0]?.state==='running')
          await failRun(runId,run.fence,'WORKER_STOPPED','Executor interrompido com término confirmado.',true);
      }else await failRun(runId,run.fence,e?.code??'RUNTIME_ERROR',
        'O executor não produziu uma entrega válida. Evidências preservadas.');
    }finally{
      await observeInputFailureBounded(run,e);
    }
  }finally{
    clearInterval(timer);process.removeListener('SIGTERM',stopping);process.removeListener('SIGINT',stopping);
    await pulse.close();
  }
}
