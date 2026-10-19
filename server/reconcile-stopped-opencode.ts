/** Isolated proposal: canonical closure for a stopped detached OpenCode actor.
 * This never adopts output or schedules another attempt. */
import { tx, lock, event, type DB } from './db.js';
import { assertStopped } from './continuation.js';
import { nativeSupervisorLockKey, nativeSupervisorLockNamespace, type NativeReservation, type NativeUnitObservation } from './native-supervision.js';
import { releaseRuntimeMemory, validRuntimeMemory, sameRuntimeMemory } from './runtime-memory.js';
import { refreshGoal } from './service.js';
import {hasOperatorOpenCodeRecoveryIntent} from './operator-opencode-recovery-contract.js';

const sameReservation=(actual:any,expected:NativeReservation)=>actual?.version===1&&actual?.mode==='detached'&&
  (actual.runtime??'opencode')==='opencode'&&expected.runId===actual.runId&&expected.runFence===actual.runFence&&
  ['launchToken','unit','candidate','digest'].every(k=>actual[k]===(expected as any)[k])&&
  ((!actual.resources&&!expected.resources)||(validRuntimeMemory(actual.resources,expected)&&
    sameRuntimeMemory(actual.resources,expected.resources)));

/** The actor receipt is evidence of terminal outcome; the fresh inspected
 * process proof is independent evidence that no process remains. */
export function validateStoppedOpenCodeReceipt(reservation:NativeReservation,inspection:any){
  const s=inspection?.state;
  if(reservation.runtime==='harness'||inspection?.runId!==reservation.runId||inspection?.runtime!=='opencode'||
    s?.runId!==reservation.runId||s?.runtime!=='opencode'||s?.workspace!==inspection.workspace||
    !['failed','cancelled'].includes(s?.state)||typeof s?.code!=='string'||
    inspection?.process?.unit!=='agentos-run-'+reservation.runId)throw Error('OPENCODE_TERMINAL_RECEIPT_INVALID');
}

export async function reconcileStoppedDetachedOpenCode(reservation:NativeReservation,
  observation:NativeUnitObservation,inspection:any){
  if(reservation.runtime==='harness')return false;
  validateStoppedOpenCodeReceipt(reservation,inspection);
  return tx(async(db:DB)=>{
    await lock(db);
    const row=(await db.query(`SELECT r.*,j.state job_state,j.supervision,t.state task_state,t.attempt_count,
      t.goal_id,t.execution,g.state goal_state,g.project
      FROM runs r JOIN opencode_jobs j ON j.run_id=r.id JOIN tasks t ON t.id=r.task_id
      JOIN goals g ON g.id=t.goal_id WHERE r.id=$1 FOR UPDATE OF r,j,t,g`,[reservation.runId])).rows[0];
    if(!row||row.runtime!=='opencode'||row.fence!==reservation.runFence||row.state!=='reconciling'||
      row.task_state!=='reconciling'||row.attempt!==row.attempt_count||row.job_state!=='orphaned'||
      row.goal_state!=='active'||row.execution?.kind!=='opencode'||!sameReservation(row.supervision,reservation))return false;
    const observationAge=Date.now()-Date.parse(observation?.checkedAt);
    if(observation?.unit!==reservation.unit||!Number.isFinite(observationAge)||observationAge < -5000||observationAge>30000||
      !['inactive','failed'].includes(observation.activeState)||!['loaded','not-found'].includes(observation.loadState)||
      observation.mainPid!==0||observation.pendingJob!==false||observation.cgroupEmpty!==true)return false;
    if(!(await db.query('SELECT pg_try_advisory_xact_lock($1::int,$2::int) owned',
      [nativeSupervisorLockNamespace,nativeSupervisorLockKey(reservation.runId)])).rows[0].owned)return false;
    assertStopped(inspection,row,{id:row.task_id,goal_id:row.goal_id,execution:row.execution});
    if(inspection.workspace!==row.execution.workspace)throw Error('STOP_UNCONFIRMED');
    const administrative=await hasOperatorOpenCodeRecoveryIntent(db,row);
    if(administrative){
      const bridge=inspection.bridgeReceipt,physical=bridge?.proof;
      const stoppedAt=Date.parse(physical?.checkedAt),requestedAt=Date.parse(row.checkpoint.operatorRecoveryStop.requestedAt);
      if(inspection.state.stopped!==true||!/^([a-f0-9]{64})$/.test(bridge?.digest??'')||
        physical?.unit!=='agentos-run-'+row.id||physical.stopped!==true||physical.mainPid!==0||physical.populated!==false||
        !['inactive','failed'].includes(physical.activeState)||!Number.isFinite(stoppedAt)||stoppedAt<requestedAt||
        stoppedAt>Date.now()+5000)throw Error('OPENCODE_RECOVERY_BRIDGE_RECEIPT_REQUIRED');
    }
    const cancelRequested=!!(await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1 LIMIT 1",[row.id])).rowCount;
    // Cancellation is authoritative only when AgentOS recorded the request.
    const cancelled=cancelRequested;
    const receipt={checkedAt:inspection.checkedAt,workspace:inspection.workspace,process:inspection.process,
      supervisor:observation,runFence:row.fence,actorState:inspection.state.state,actorCode:inspection.state.code,
      ...(administrative?{bridgeReceiptDigest:inspection.bridgeReceipt.digest}: {})};
    await db.query(`UPDATE runs SET state=$2,finished_at=COALESCE(finished_at,clock_timestamp()),
      checkpoint=COALESCE(checkpoint,'{}'::jsonb)||jsonb_build_object('nativeStopReceipt',$3::jsonb) WHERE id=$1`,
      [row.id,cancelled?'cancelled':'failed',JSON.stringify(receipt)]);
    if(row.supervision.resources && !await releaseRuntimeMemory(db,row.supervision,inspection))
      throw Error('RUNTIME_MEMORY_RELEASE_UNCONFIRMED');
    await db.query('UPDATE tasks SET state=$2,reason=$3,updated_at=clock_timestamp() WHERE id=$1',
      [row.task_id,cancelled?'cancelled':'failed',cancelled?null:'OpenCode encerrado com recibo terminal; aguarda decisão supervisionada.']);
    await db.query("UPDATE opencode_jobs SET state='finished',finished_at=COALESCE(finished_at,clock_timestamp()) WHERE run_id=$1",[row.id]);
    await db.query('DELETE FROM leases WHERE run_id=$1',[row.id]);
    await event(db,'DETACHED_RUN_STOP_RECONCILED',row.task_id,{runId:row.id,attempt:row.attempt,receipt,cancelled,
      semanticResumeRequired:!cancelled,newAttemptQueued:false});
    await event(db,cancelled?'RUN_CANCELLED':'RUN_FAILED',row.task_id,{runId:row.id,code:row.error_code,
      actorCode:inspection.state.code,stopped:true,source:'detached_stop_reconciliation'});
    await refreshGoal(db,row.goal_id);
    return true;
  });
}
