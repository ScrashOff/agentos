import { createHash, randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { promisify } from 'node:util';
import { execFile } from 'node:child_process';
import { event, lock, pool, tx, type DB } from './db.js';
import { config } from './config.js';
import { bridgeScript } from './bridge-path.js';

// Resident sandbox memory, independent of model-request concurrency and task leases.
// The existing opencode_jobs row is the durable allocation; no expiry frees it.
export const runtimeMemoryLimits = { openCode: 12, elevated: 2, normalMiB: 1800, elevatedMiB: 3072, openCodeMiB: 24144, defaultHostBudgetMiB: 24144 } as const;
export type RuntimeMemoryReservation = {
  version: 1; id: string; runId: string; runFence: string; launchToken: string;
  runtime?: 'opencode' | 'harness';
  workspace: string; memoryMiB: 1800 | 3072; state: 'reserved' | 'released';
  reservedAt: string; releasedAt?: string; releaseProofHash?: string;
  oomSource?: { runId: string; attempt: number; terminationEventId: string; stopEventId: string;
    provenance?: 'legacy_label_with_confirmed_oom'; originalDatabaseCode?: 'PROVIDER_ERROR'; journalConfirmedOom?: true };
};
type Identity = { runId: string; runFence: string; launchToken: string; candidate: string; digest: string; runtime?: 'opencode' | 'harness' };
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const workspaceKey = /^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const sha = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const terminal = ['submitted','failed','cancelled'];

export function validRuntimeMemory(value: any, identity: Identity) {
  return value?.version === 1 && uuid.test(value.id) && value.runId === identity.runId &&
    value.runFence === identity.runFence && value.launchToken === identity.launchToken &&
    (value.runtime ?? 'opencode') === (identity.runtime ?? 'opencode') &&
    workspaceKey.test(value.workspace) && [1800,3072].includes(value.memoryMiB) &&
    ['reserved','released'].includes(value.state) && Number.isFinite(Date.parse(value.reservedAt));
}
export function sameRuntimeMemory(a: any, b: any) {
  // State is intentionally excluded: release is an audited state transition,
  // not a new launch identity. Start separately requires state=reserved.
  return !!a && !!b && ['version','id','runId','runFence','launchToken','workspace','memoryMiB','reservedAt']
    .every(key => a[key] === b[key]) && (a.runtime ?? 'opencode') === (b.runtime ?? 'opencode') &&
    isDeepStrictEqual(a.oomSource ?? null,b.oomSource ?? null);
}

/** Legacy/uncertain actors remain conservatively charged. Queued jobs which
 * never claimed/spawned do not consume memory, even if they have task leases.
 * No execution JSON or historical input payload is materialized here. */
export async function runtimeMemoryOccupancy(db: DB) {
  const rows = (await db.query(`WITH residents AS MATERIALIZED (
    SELECT r.id,r.runtime,r.state,r.started_at,r.worker_id,j.state job_state,j.supervision,
      EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id AND l.resource='remote-slot') remote
    FROM runs r LEFT JOIN opencode_jobs j ON j.run_id=r.id
    WHERE r.runtime IN ('opencode','harness') AND (
      (j.supervision ? 'resources' AND j.supervision->'resources'->>'state' IS DISTINCT FROM 'released') OR
      r.state IN ('running','cancelling','reconciling') OR
      (r.state='queued' AND (r.started_at IS NOT NULL OR r.worker_id IS NOT NULL)) OR
      j.state IN ('launching','running','orphaned')))
    SELECT id,runtime,supervision->'resources' allocation FROM residents
    WHERE (supervision ? 'resources' AND supervision->'resources'->>'state' IS DISTINCT FROM 'released') OR
      ((supervision->'resources' IS NULL OR supervision->'resources'->>'state' IS DISTINCT FROM 'released')
       AND (remote OR supervision->>'mode'='detached' OR job_state IN ('launching','running','orphaned')))`)).rows;
  let elevated = 0, memoryMiB = 0, openCode = 0, openCodeMiB = 0;
  for (const row of rows) {
    // A malformed high value must fail closed, never be discounted to normal.
    const value = row.allocation;
    if (value && (value.version !== 1 || value.state !== 'reserved' || ![1800,3072].includes(value.memoryMiB)))
      throw Error('RUNTIME_MEMORY_LEDGER_INVALID');
    const memory = value?.memoryMiB ?? 1800;
    if (memory === 3072) elevated++;
    memoryMiB += memory;
    if (row.runtime === 'opencode') { openCode++; openCodeMiB += memory; }
  }
  return { total: rows.length, openCode, elevated, memoryMiB, openCodeMiB };
}

/** Only the verified native recovery lineage can request the higher profile.
 * The old events are historical proof of the OOM, not fresh proof of stopping
 * the new attempt. No arbitrary task field, error text or supplied model output
 * can select a larger cgroup. */
async function oomSource(db: DB, run: any, workspace: string) {
  if (run.runtime === 'harness') return harnessOomSource(db,run,workspace);
  if (run.runtime !== 'opencode' || run.checkpoint?.kind !== 'opencode_recovery' ||
      !uuid.test(run.checkpoint.previousRunId) || run.checkpoint.preservedWorkspace !== workspace) return undefined;
  const previous = (await db.query(`SELECT id,task_id,attempt,runtime,state,error_code,checkpoint,
    instruction_snapshot->'task'->'execution'->>'workspace' workspace FROM runs WHERE id=$1`,
    [run.checkpoint.previousRunId])).rows[0];
  if (!previous || previous.task_id !== run.task_id || previous.runtime !== 'opencode' ||
      previous.attempt >= run.attempt || !terminal.includes(previous.state)) return undefined;
  let source = previous;
  if (previous.checkpoint?.kind === 'artifact_recovery') {
    if (!uuid.test(previous.checkpoint.sourceRunId)) return undefined;
    source = (await db.query(`SELECT id,task_id,attempt,runtime,state,error_code,
      instruction_snapshot->'task'->'execution'->>'workspace' workspace FROM runs WHERE id=$1`,
      [previous.checkpoint.sourceRunId])).rows[0];
    if (!source || source.task_id !== run.task_id || source.runtime !== 'opencode' ||
        source.attempt >= previous.attempt || !terminal.includes(source.state)) return undefined;
  }
  if (source.workspace !== workspace || source.error_code !== 'OPENCODE_SANDBOX_OOM') return undefined;
  const termination = (await db.query(`SELECT id FROM events WHERE entity_id=$1 AND kind='RUNTIME_TERMINATION'
    AND payload->>'runId'=$2 AND payload->>'source'='systemd-journal'
    AND payload->>'unit'=$3 AND payload->>'result'='oom-kill' ORDER BY created_at DESC LIMIT 1`,
    [run.task_id,source.id,'agentos-run-'+source.id+'.service'])).rows[0];
  const stop = (await db.query(`SELECT id FROM events WHERE entity_id=$1 AND kind='OPENCODE_STOPPED'
    AND payload->>'runId'=$2 AND payload->>'confirmed'='true'
    AND payload->'proof'->>'unit'=$3 AND payload->'proof'->>'stopped'='true'
    AND payload->'proof'->>'mainPid'='0' AND payload->'proof'->>'populated'='false'
    AND payload->'proof'->>'activeState' IN ('inactive','failed')
    ORDER BY created_at DESC LIMIT 1`,[run.task_id,source.id,'agentos-run-'+source.id])).rows[0];
  if (!termination || !stop) return undefined;
  return { runId: source.id, attempt: source.attempt, terminationEventId: String(termination.id), stopEventId: String(stop.id) };
}

/** Harness recovery is scheduled as a new run. Its frozen continuation and the
 * scheduler-copied checkpoint must name the same stopped source; live task
 * fields or an exit code alone cannot authorize more memory. */
async function harnessOomSource(db: DB, run: any, workspace: string) {
  const checkpoint=run.checkpoint;
  if(checkpoint?.kind!=='automatic_recovery' || checkpoint.strategy!=='targeted_repair' ||
    !uuid.test(checkpoint.sourceRunId) || checkpoint.preservedWorkspace!==workspace)return undefined;
  const current=(await db.query(`SELECT r.instruction_snapshot->>'schema' schema,
    r.instruction_snapshot#>>'{task,id}' task_id,
    r.instruction_snapshot#>>'{task,execution,kind}' kind,
    r.instruction_snapshot#>>'{task,execution,workspace}' workspace,
    r.instruction_snapshot#>>'{task,execution,continuation,runId}' source_run_id,
    a.provider,a.bridge_id FROM runs r LEFT JOIN accounts a ON a.id=r.account_id WHERE r.id=$1`,[run.id])).rows[0];
  if(current?.schema!=='1' || current.task_id!==run.task_id || current.kind!=='harness' ||
    current.workspace!==workspace || current.source_run_id!==checkpoint.sourceRunId ||
    !current.provider || current.provider==='muse' || current.bridge_id?.startsWith('agentos:muse:'))return undefined;
  const source=(await db.query(`SELECT id,task_id,fence,attempt,runtime,state,error_code,
    instruction_snapshot#>>'{task,execution,workspace}' workspace FROM runs WHERE id=$1`,[checkpoint.sourceRunId])).rows[0];
  if(!source || source.task_id!==run.task_id || source.runtime!=='harness' || source.state!=='failed' ||
    source.attempt>=run.attempt || source.workspace!==workspace ||
    !['HARNESS_SANDBOX_OOM','RUNTIME_EXITED','PROVIDER_ERROR'].includes(source.error_code))return undefined;
  if((await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1 LIMIT 1",[source.id])).rowCount)return undefined;
  const termination=(await db.query(`SELECT id FROM events WHERE entity_id=$1 AND kind='RUNTIME_TERMINATION'
    AND payload->>'runId'=$2 AND payload->>'runFence'=$3 AND payload->>'workspace'=$4
    AND payload->>'source'='systemd-journal' AND payload->>'unit'=$5 AND payload->>'result'='oom-kill'
    ORDER BY id DESC LIMIT 1`,[run.task_id,source.id,source.fence,workspace,'agentos-run-'+source.id+'.service'])).rows[0];
  const stop=(await db.query(`SELECT id FROM events WHERE entity_id=$1 AND kind='HARNESS_STOPPED'
    AND payload->>'runId'=$2 AND payload->>'runFence'=$3 AND payload->>'workspace'=$4
    AND payload->>'confirmed'='true' AND payload->'proof'->>'unit'=$5
    AND payload->'proof'->>'stopped'='true' AND payload->'proof'->>'mainPid'='0'
    AND payload->'proof'->>'populated'='false' AND payload->'proof'->>'activeState' IN('inactive','failed')
    ORDER BY id DESC LIMIT 1`,[run.task_id,source.id,source.fence,workspace,'agentos-run-'+source.id])).rows[0];
  if(!termination || !stop)return undefined;
  // Older bridges can collapse a typed runtime error to PROVIDER_ERROR. The
  // two exact owner events above prove an OOM and a physical stop, even when a
  // provider failure also occurred. This authorizes only bounded retry memory;
  // it does not reclassify the historical error or infer why the label differed.
  return {runId:source.id,attempt:source.attempt,terminationEventId:String(termination.id),stopEventId:String(stop.id),
    ...(source.error_code==='PROVIDER_ERROR'?{provenance:'legacy_label_with_confirmed_oom' as const,
      originalDatabaseCode:'PROVIDER_ERROR' as const,journalConfirmedOom:true as const}:{})};
}

/** Trusted administrative/controller path only, never a model tool. The SSH
 * inspector supplies the fresh physical proof. This appends provenance for a
 * legacy OOM without rewriting the run, its error, snapshot or task state. */
export async function recordHarnessTerminationEvidence(runId:string,runFence:string,proof:any) {
  if(!uuid.test(runId)||!uuid.test(runFence))return null;
  return tx(async db=>{
    await lock(db);
    const row=(await db.query(`SELECT r.id,r.fence,r.task_id,r.attempt,r.state,r.runtime,r.error_code,
      r.instruction_snapshot->>'schema' schema,r.instruction_snapshot#>>'{task,id}' snapshot_task,
      r.instruction_snapshot#>>'{task,goal_id}' snapshot_goal,
      r.instruction_snapshot#>>'{task,execution,kind}' snapshot_kind,
      r.instruction_snapshot#>>'{task,execution,workspace}' workspace,
      t.goal_id,t.attempt_count,t.state task_state,t.recovery,g.state goal_state,
      s.value supervision,a.value autonomy
      FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
      LEFT JOIN settings s ON s.id='supervision:'||g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project
      WHERE r.id=$1 FOR UPDATE OF r,t,g`,[runId])).rows[0];
    if(!row || row.fence!==runFence || row.runtime!=='harness' || row.state!=='failed' ||
      !['RUNTIME_EXITED','HARNESS_SANDBOX_OOM'].includes(row.error_code) || row.attempt!==row.attempt_count ||
      ['accepted','cancelled','superseded'].includes(row.task_state) || !['active','failed'].includes(row.goal_state) ||
      row.supervision?.enabled!==true || row.autonomy?.enabled===false || row.recovery?.mode==='manual' ||
      row.schema!=='1' || row.snapshot_task!==row.task_id || row.snapshot_goal!==row.goal_id ||
      row.snapshot_kind!=='harness' || !workspaceKey.test(row.workspace))return null;
    const p=proof?.process,termination=proof?.termination,age=Date.now()-Date.parse(proof?.checkedAt);
    if(proof?.version!==1 || proof.runId!==runId || proof.runFence!==runFence || proof.runtime!=='harness' ||
      proof.workspace!==row.workspace || proof.state!=='failed' || proof.code!==row.error_code ||
      !/^[0-9a-f]{64}$/.test(proof.stateDigest??'') || !Number.isFinite(age) || age< -5000 || age>60000 ||
      p?.unit!=='agentos-run-'+runId || p.stopped!==true || p.mainPid!==0 || p.populated!==false ||
      !['inactive','failed'].includes(p.activeState) || termination?.source!=='systemd-journal' ||
      termination.unit!=='agentos-run-'+runId+'.service' || termination.result!=='oom-kill')return null;
    if((await db.query(`SELECT 1 FROM runs WHERE task_id=$1 AND (attempt>$3 OR state IN('queued','running','reconciling','cancelling'))
      UNION ALL SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$2 LIMIT 1`,[row.task_id,runId,row.attempt])).rowCount)return null;
    const existing=(await db.query(`SELECT id FROM events WHERE entity_id=$1 AND kind='HARNESS_TERMINATION_EVIDENCE_RECORDED'
      AND payload->>'runId'=$2 AND payload->>'runFence'=$3 ORDER BY id DESC LIMIT 1`,[row.task_id,runId,runFence])).rows[0];
    if(existing)return {recorded:false,receiptEventId:String(existing.id),runId,runFence};
    const identity={runId,runFence,workspace:row.workspace,checkedAt:proof.checkedAt,stateDigest:proof.stateDigest,
      source:'trusted_harness_termination_inspector',proofHash:sha(proof)};
    await event(db,'RUNTIME_TERMINATION',row.task_id,{...identity,source:'systemd-journal',
      unit:termination.unit,result:'oom-kill'});
    await event(db,'HARNESS_STOPPED',row.task_id,{...identity,confirmed:true,proof:{unit:p.unit,
      stopped:true,mainPid:0,populated:false,activeState:p.activeState}});
    const receipt=(await db.query(`INSERT INTO events(kind,entity_id,payload)
      VALUES('HARNESS_TERMINATION_EVIDENCE_RECORDED',$1,$2) RETURNING id`,[row.task_id,JSON.stringify(identity)])).rows[0];
    return {recorded:true,receiptEventId:String(receipt.id),runId,runFence};
  });
}

function runtimeHostBudget(policy: any) {
  const amount = policy?.hostRuntimeBudgetMiB ?? runtimeMemoryLimits.defaultHostBudgetMiB;
  // The authorized pool budget remains bounded independently of host upgrades.
  if (!Number.isSafeInteger(amount) || amount < 3072 || amount > 28672)
    throw Error('RUNTIME_MEMORY_BUDGET_INVALID');
  return amount;
}

/** Native Muse keeps its existing runner. The launch row itself is
 * charged conservatively at1800. Caller persists j.state=launching under the
 * same4280001 transaction, so concurrent detached OpenCode/Harness claims see
 * each other. Legacy non-detached dispatch remains outside this guarantee. */
export async function canReserveHarnessMemory(db: DB) {
  const policy = (await db.query("SELECT value FROM settings WHERE id='runtime-memory-policy'")).rows[0]?.value;
  return (await runtimeMemoryOccupancy(db)).memoryMiB + 1800 <= runtimeHostBudget(policy);
}

/** Caller holds the scheduler lock and the selected queued run/job row locks.
 * The caller persists this value in the SAME transaction before launching. */
export async function allocateRuntimeMemory(db: DB, identity: Identity, run: any, workspace: string) {
  if (!workspaceKey.test(workspace)) throw Error('RUNTIME_MEMORY_WORKSPACE_INVALID');
  const source = await oomSource(db,run,workspace);
  const memoryMiB = source ? 3072 : 1800;
  const policy = (await db.query("SELECT value FROM settings WHERE id='runtime-memory-policy'")).rows[0]?.value;
  const hostBudget = runtimeHostBudget(policy);
  if (source) {
    // One-time policy activation follows inventory. Each publication becomes
    // eligible automatically when its four controllers have adopted; no manual
    // renewal or per-release allowlist is needed.
    if (policy?.version !== 1 || policy.enabled !== true || policy.maxOpenCode !== 12 ||
        policy.maxElevated !== 2 || policy.inventoryConfirmed !== true) return null;
    const ready = (await db.query(`SELECT count(*)::int n FROM settings WHERE id=ANY($1::text[])
      AND value->>'candidate'=$2 AND value->>'digest'=$3`,[
      ['agentos-workers','agentos-backlog','agentos-cycles','agentos-opencode'].map(x=>'worker-readiness:'+x),
      identity.candidate,identity.digest])).rows[0].n;
    if (ready !== 4) return null;
  }
  const occupied = await runtimeMemoryOccupancy(db);
  if ((run.runtime==='opencode' && (occupied.openCode >= 12 || occupied.openCodeMiB + memoryMiB > 24144)) ||
      occupied.memoryMiB + memoryMiB > hostBudget ||
      (memoryMiB === 3072 && occupied.elevated >= 2)) return null;
  return { version: 1, id: randomUUID(), runId: identity.runId, runFence: identity.runFence,
    ...(run.runtime==='harness'?{runtime:'harness' as const}:{}),
    launchToken: identity.launchToken, workspace, memoryMiB, state: 'reserved',
    reservedAt: new Date().toISOString(), ...(source ? {oomSource: source} : {}) } as RuntimeMemoryReservation;
}

/** Never expose this as a model tool. The caller obtains proof over the trusted
 * SSH inspector before this transaction. A terminal DB state alone is not proof. */
export async function releaseRuntimeMemory(db: DB, expected: Identity & {resources?: RuntimeMemoryReservation}, proof: any) {
  await lock(db);
  const row = (await db.query(`SELECT r.id,r.task_id,r.state,r.fence,
    r.instruction_snapshot->'task'->'execution'->>'workspace' frozen_workspace,j.supervision
    FROM runs r JOIN opencode_jobs j ON j.run_id=r.id WHERE r.id=$1 FOR UPDATE OF r,j`,[expected.runId])).rows[0];
  const allocation = row?.supervision?.resources;
  if (!row || row.fence !== expected.runFence || row.supervision.launchToken !== expected.launchToken ||
      row.supervision.candidate !== expected.candidate || row.supervision.digest !== expected.digest ||
      !validRuntimeMemory(allocation,expected) || !sameRuntimeMemory(allocation,expected.resources)) return false;
  if (allocation.state === 'released') return true;
  if (!terminal.includes(row.state)) return false;
  const process = proof?.process, age = Date.now()-Date.parse(proof?.checkedAt);
  if (proof?.runId !== row.id || (proof.executionRunId && proof.executionRunId !== row.id) ||
      proof.workspace !== allocation.workspace ||
      (row.frozen_workspace && row.frozen_workspace !== allocation.workspace) ||
      process?.unit !== 'agentos-run-'+row.id || process.stopped !== true || process.mainPid !== 0 ||
      process.populated !== false || !['inactive','failed'].includes(process.activeState) ||
      !Number.isFinite(age) || age < -5000 || age > 60000) return false;
  const released = { ...allocation, state: 'released', releasedAt: new Date().toISOString(), releaseProofHash: sha(proof) };
  await db.query("UPDATE opencode_jobs SET supervision=jsonb_set(supervision,'{resources}',$2::jsonb) WHERE run_id=$1",[row.id,JSON.stringify(released)]);
  await event(db,'RUNTIME_MEMORY_RELEASED',row.task_id,{runId:row.id,reservationId:allocation.id,
    runFence:row.fence,memoryMiB:allocation.memoryMiB,workspace:allocation.workspace,
    stopProofHash:released.releaseProofHash,checkedAt:proof.checkedAt});
  return true;
}

/** The existing dispatcher calls this bounded reconciliation, including jobs
 * already finished. Inspect outside the scheduler lock; uncertain stops retain
 * their allocation. No cancellation, retry, expiry or new LLM call is performed. */
const execute = promisify(execFile);
export async function inspectReservedRuntimeMemory(reservation: Identity & {resources:RuntimeMemoryReservation}) {
  if(!validRuntimeMemory(reservation.resources,reservation))throw Error('RUNTIME_MEMORY_RESERVATION_INVALID');
  const notStarted=Boolean((await pool.query(`SELECT 1 FROM events e JOIN runs r ON r.id=$1
    WHERE r.fence=$2 AND e.entity_id=r.task_id::text AND e.kind=$3
    AND e.payload->>'runId'=r.id::text AND e.payload->>'confirmed'='true' LIMIT 1`,
    [reservation.runId,reservation.runFence,reservation.runtime==='harness'?'HARNESS_NOT_STARTED':'OPENCODE_NOT_STARTED'])).rowCount);
  const {stdout}=await execute('ssh',['-T','-o','BatchMode=yes','-o','ConnectTimeout=2',config.bridgeHost,
    '/opt/deepseek-harness/node/bin/node',bridgeScript('inspect-runtime-memory.mjs'),
    reservation.runId,reservation.resources.workspace,...(reservation.runtime==='harness'?['--runtime=harness']:[]),...(notStarted?['--not-started']:[])],
    {timeout:8000,maxBuffer:32768,windowsHide:true});
  return JSON.parse(stdout);
}
export async function reconcileRuntimeMemory(inspect: (id: string,native: boolean,reservation:any)=>Promise<any> =
  (_id,_native,reservation)=>inspectReservedRuntimeMemory(reservation)) {
  const rows = (await pool.query(`SELECT j.supervision,r.runtime FROM opencode_jobs j JOIN runs r ON r.id=j.run_id
    WHERE j.supervision->'resources'->>'state'='reserved' AND r.state IN ('submitted','failed','cancelled')
    ORDER BY j.finished_at NULLS LAST,j.created_at`)).rows;
  let released = 0, next = 0;
  await Promise.all(Array.from({length:Math.min(2,rows.length)},async()=>{
    while(next<rows.length) {
      const row=rows[next++];
      try {
        const proof = await inspect(row.supervision.runId,row.runtime==='opencode',row.supervision);
        if (await tx(db=>releaseRuntimeMemory(db,row.supervision,proof))) released++;
      } catch { /* A failed inspection cannot free capacity. */ }
    }
  }));
  return released;
}
