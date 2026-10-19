import {pool,event} from './db.js';
import {prepareCompletedNativeArtifactRecovery,collectStoppedArtifacts,adoptRecoveredArtifacts} from './artifact-recovery.js';
import {inspectStopped} from './continuation.js';
import {observeNativeSupervisor} from '../scripts/native-supervisor-unit.js';

/** Recovery consumes existing output only. Slow SSH reads run in the delivery
 * lane, never the planner lane, and every mutation rechecks both stop proofs. */
export async function reconcileCompletedNativeArtifacts(deps={
  collect:collectStoppedArtifacts,inspect:inspectStopped,observe:observeNativeSupervisor,
}){
 const rows=(await pool.query(`SELECT r.id,r.fence,t.execution->'readOnlySource' readonly,t.execution->>'testFile' test_file
   FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
   JOIN opencode_jobs j ON j.run_id=r.id LEFT JOIN settings a ON a.id='autonomy:'||g.project
   LEFT JOIN LATERAL(SELECT e.created_at FROM events e WHERE e.kind='NATIVE_COLLECTION_CHECKED'
     AND e.entity_id=r.id::text ORDER BY e.id DESC LIMIT 1) checked ON true
   WHERE r.runtime='opencode' AND r.state='reconciling' AND r.error_code='SUPERVISOR_LOST'
     AND j.state='orphaned' AND t.state='reconciling' AND t.attempt_count=r.attempt AND g.state='active'
     AND COALESCE(a.value->>'enabled','true')='true'
     AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
     AND t.execution->>'workKind' IS DISTINCT FROM 'planning' AND t.execution->>'testFile' IS NOT NULL
     AND (checked.created_at IS NULL OR checked.created_at<now()-interval '60 seconds')
   ORDER BY checked.created_at NULLS FIRST,r.created_at,r.id LIMIT 3`)).rows;
 const recovered=[];
 for(const row of rows)try{
   // Persist the observation schedule before remote I/O, including incomplete
   // and unavailable results. An old orphan cannot starve later recoveries.
   await event(pool,'NATIVE_COLLECTION_CHECKED',row.id,{runId:row.id,modelsCalled:false});
   const prepared=await prepareCompletedNativeArtifactRecovery(row.id,row.fence);
   const initial=await deps.inspect(row.id,true);
   if(initial.state?.state!=='completed' || initial.process?.stopped!==true)continue;
   const delivery=await deps.collect(row.id,row.readonly===true,row.test_file,true);
   const [supervisorObservation,inspection]=await Promise.all([deps.observe(prepared.reservation),deps.inspect(row.id,true)]);
   recovered.push(await adoptRecoveredArtifacts(row.id,row.fence,delivery,
     {nativeCompleted:{...prepared,supervisorObservation,inspection}}));
 }catch(error:any){
   const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(error?.message??'')?error.message:'NATIVE_COLLECTION_UNAVAILABLE';
   const prior=(await pool.query("SELECT payload->>'code' code FROM events WHERE kind='NATIVE_COLLECTION_DEFERRED' AND payload->>'runId'=$1 ORDER BY id DESC LIMIT 1",[row.id])).rows[0];
   if(prior?.code!==code)await event(pool,'NATIVE_COLLECTION_DEFERRED',row.id,{runId:row.id,code,modelsCalled:false});
 }
 return recovered;
}
