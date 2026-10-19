import type {DB} from './db.js';
import {inputFailureMetadata} from './input-failure.js';
import pg from 'pg';
import {config} from './config.js';

type InputFailureOwner={id:string;task_id:string;fence:string;attempt:number};

/** The owner never lends its pool/transaction to optional telemetry. The server
 * cancels a blocked statement; client timeout additionally closes this one
 * private connection rather than abandoning SQL on the shared pool. */
export async function observeInputFailureBounded(run:InputFailureOwner,error:unknown):Promise<boolean>{
  let db:pg.Client|undefined;
  try{
    if(!inputFailureMetadata(error))return false;
    db=new pg.Client({connectionString:config.databaseUrl,application_name:'agentos-input-failure-observation',
      connectionTimeoutMillis:1000,statement_timeout:1000,lock_timeout:250,query_timeout:1500});
    db.on('error',()=>{});
    await db.connect();
    return await observeInputFailure(db,run,error);
  }catch{return false;}
  finally{try{await db?.end();}catch{/* Closing optional telemetry must not mask the original failure. */}}
}

/** Best-effort diagnosis only, after the owner's canonical stop/failure path.
 * Never certify a stop, release a lease, queue a retry or preserve raw errors.
 * A current attempt/fence is required even when its failure is already terminal.
 * The caller must not depend on this observation to finish its failure handling. */
export async function observeInputFailure(db:DB,run:InputFailureOwner,error:unknown):Promise<boolean>{
  try{
    const metadata=inputFailureMetadata(error);
    if(!metadata)return false;
    const result=await db.query(`INSERT INTO events(kind,entity_id,payload)
      SELECT 'RUN_INPUT_FAILURE',r.task_id::text,$5::jsonb||jsonb_build_object(
        'runId',r.id,'runFence',r.fence,'attempt',r.attempt,'runtime',r.runtime,'contextVersion',r.context_id)
      FROM runs r JOIN tasks t ON t.id=r.task_id
      WHERE r.id=$1::uuid AND r.task_id=$2::uuid AND r.fence=$3::uuid
        AND r.attempt=$4::int AND t.attempt_count=r.attempt
        AND NOT EXISTS(SELECT 1 FROM runs newer WHERE newer.task_id=r.task_id AND newer.attempt>r.attempt)
      RETURNING id`,[run.id,run.task_id,run.fence,run.attempt,JSON.stringify(metadata)]);
    return result.rowCount===1;
  }catch{
    // Diagnostic storage must never replace the original error or stop receipt.
    return false;
  }
}

/** A requested outcome page may expose one exact current diagnostic. It is
 * neither a stop proof nor a retry decision; missing/stale evidence is unknown.
 * Only persisted identities contribute to the reader's page digest. */
export async function readInputFailureObservation(db:DB,goalId:string,task:{id:string;attempt_count:number;
  run_id:string|null;run_attempt:number|null;run_fence:string|null;run_context_id:string|null;error_code:string|null}){
  if(!inputFailureMetadata({code:task.error_code})||!task.run_id||!task.run_fence||task.run_attempt!==task.attempt_count)return null;
  const row=(await db.query(`SELECT e.id::text event_id,e.payload
    FROM runs r JOIN tasks t ON t.id=r.task_id
    JOIN events e ON e.entity_id=t.id::text AND e.kind='RUN_INPUT_FAILURE'
    WHERE r.id=$1::uuid AND r.task_id=$2::uuid AND r.fence=$3::uuid
      AND r.attempt=$4::int AND r.context_id IS NOT DISTINCT FROM $5::uuid
      AND t.goal_id=$6::uuid AND t.attempt_count=r.attempt
      AND r.state NOT IN('queued','running','cancelling','reconciling')
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id
        AND (live.attempt>r.attempt OR live.state IN('queued','running','cancelling','reconciling')))
      AND e.payload->>'runId'=r.id::text AND e.payload->>'runFence'=r.fence::text
      AND e.payload->'attempt'=to_jsonb(r.attempt) AND e.payload->>'runtime'=r.runtime
      AND e.payload->'contextVersion'=COALESCE(to_jsonb(r.context_id),'null'::jsonb)
    ORDER BY e.id DESC LIMIT 1`,[task.run_id,task.id,task.run_fence,task.attempt_count,task.run_context_id,goalId])).rows[0];
  if(!row)return null;
  const metadata=inputFailureMetadata({code:row.payload?.code,inputFailure:row.payload});
  if(!metadata)return null;
  return {version:1,authority:'diagnostic_only',eventId:row.event_id,runId:task.run_id,
    runFence:task.run_fence,attempt:task.attempt_count,contextVersion:task.run_context_id,...metadata};
}
