import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { pool, tx, lock, event, type DB } from './db.js';
import { config } from './config.js';
import { bridgeScript } from './bridge-path.js';
import { independentNativeSupervisorSql } from './native-supervision.js';
import { checkedRuntimeProgressProof, runtimeProgressDigest, runtimeProgressCode,
  sameRuntimeProgressEpisode } from './runtime-progress-contract.js';

const exec = promisify(execFile);
let scanCursor: string | null = null;
export async function inspectRuntimeProgress(runId: string, workspace: string) {
  const {stdout} = await exec('ssh', ['-T','-o','BatchMode=yes','-o','ConnectTimeout=8',config.bridgeHost,
    '/opt/deepseek-harness/node/bin/node', bridgeScript('inspect-runtime-progress.mjs'), runId, workspace],
    { timeout: 20000, maxBuffer: 128 * 1024, windowsHide: true });
  return JSON.parse(stdout);
}

async function candidates(db: DB, runId: string | null = null, cursor: string | null = null) {
  return (await db.query(`SELECT r.id,r.fence,r.task_id,r.attempt,
    t.goal_id,metadata.workspace,
    j.supervision->>'launchToken' launch_token
    FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
    JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    JOIN opencode_jobs j ON j.run_id=r.id JOIN settings s ON s.id='supervision:'||g.id
    LEFT JOIN settings a ON a.id='autonomy:'||g.project
    WHERE ($1::uuid IS NULL OR r.id=$1) AND r.state='running' AND t.state='running'
      AND r.runtime='harness' AND r.attempt=t.attempt_count AND g.state='active'
      AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
      AND j.state='running' AND ${independentNativeSupervisorSql()}
      AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text)
    ORDER BY CASE WHEN $2::uuid IS NULL OR r.id>$2 THEN 0 ELSE 1 END,r.id LIMIT 24`, [runId,cursor])).rows;
}

/** The existing, version-pinned actor sees its heartbeat authority withdrawn
 * and executes its own mediated abort. No PID signal, fence replacement, lease
 * release or new attempt is performed by this controller. */
export async function requestRuntimeProgressStop(observed: any, rawProof: unknown) {
  const proof = checkedRuntimeProgressProof(rawProof);
  // Read the immutable snapshot only for a proven episode, outside the global
  // admission lock. Ordinary scans never detoast the worker's input bundle.
  const frozen = (await pool.query(`SELECT instruction_snapshot->'task'->'execution'->>'workspace' workspace,
    instruction_snapshot->'task'->>'id' task_id,instruction_snapshot->'task'->>'goal_id' goal_id
    FROM runs WHERE id=$1 AND fence=$2`,[observed.id,observed.fence])).rows[0];
  return tx(async db => {
    await lock(db);
    const current = (await candidates(db, observed.id))[0];
    if (!current || runtimeProgressDigest(current) !== runtimeProgressDigest(observed) ||
      proof.runId !== current.id || proof.workspace !== current.workspace ||
      current.workspace !== frozen?.workspace || current.task_id !== frozen?.task_id ||
      current.goal_id !== frozen?.goal_id || proof.state.runFence && proof.state.runFence !== current.fence)
      return false;
    checkedRuntimeProgressProof(proof); // The admission lock may have taken time.
    const receipt = { version: 1, runId: current.id, runFence: current.fence,
      launchToken: current.launch_token, requestedAt: new Date().toISOString(),
      proofDigest: runtimeProgressDigest(proof), proof };
    const changed = await db.query(`UPDATE runs SET state='reconciling',error_code=$3,
      checkpoint=COALESCE(checkpoint,'{}'::jsonb)||jsonb_build_object('runtimeProgressStop',$4::jsonb)
      WHERE id=$1 AND fence=$2 AND state='running' RETURNING id`,
      [current.id,current.fence,runtimeProgressCode,JSON.stringify(receipt)]);
    if (!changed.rowCount) return false;
    await db.query(`UPDATE tasks SET state='reconciling',reason=$2,updated_at=clock_timestamp() WHERE id=$1`,
      [current.task_id,'Espera repetida sem alteração de uma falha confirmada; encerramento supervisionado em andamento.']);
    await event(db,'RUNTIME_PROGRESS_STOP_REQUESTED',current.task_id,{runId:current.id,
      runFence:current.fence,proofDigest:receipt.proofDigest,proof,
      leasesRetained:true,newAttemptQueued:false});
    return true;
  });
}

export async function reconcileRuntimeProgress(inspect = inspectRuntimeProgress) {
  if (!config.bridgeHost || !config.remoteExecution) return {inspected:0,stopRequests:0,unavailable:0,changed:0};
  // Rotate bounded batches so older, legitimately long work cannot hide every
  // newer actor. The cursor changes inspection order only, never authority.
  const rows = await candidates(pool,null,scanCursor);
  if(rows.length)scanCursor=rows.at(-1).id;
  let inspected=0, stopRequests=0, unavailable=0, changed=0, next=0;
  await Promise.all(Array.from({length:Math.min(3,rows.length)},async()=>{
   while(next<rows.length) {
    const current=rows[next++];
    try {
      const first = await inspect(current.id,current.workspace); inspected++;
      if (!first?.stalled) {
        if(!first || ['PROGRESS_FILE_UNAVAILABLE','PROGRESS_OBSERVATION_UNAVAILABLE','UNSAFE_PROGRESS_PATH','PROGRESS_FILE_CHANGED'].includes(first.reason))unavailable++;
        else if(['SAMPLE_CHANGED','PROCESS_CHANGED','RUN_IDENTITY_OR_STATE_CHANGED'].includes(first.reason))changed++;
        continue;
      }
      const a = checkedRuntimeProgressProof(first);
      const b = checkedRuntimeProgressProof(await inspect(current.id,current.workspace));
      if (!sameRuntimeProgressEpisode(a,b)) {changed++;continue;}
      if (await requestRuntimeProgressStop(current,b)) stopRequests++;
    } catch {
      // Missing, partial, stale or changed evidence never stops an execution.
      unavailable++;
    }
   }
  }));
  return {inspected,stopRequests,unavailable,changed};
}
