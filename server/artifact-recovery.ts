import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';
import { pool, tx, lock, event, type DB } from './db.js';
import { config } from './config.js';
import { bridgeScript } from './bridge-path.js';
import { hash } from './memory.js';
import { validateTask } from './service.js';
import { appliedSupervisionOwnsContinuation } from './recovery-supervision.js';
import { verifyOpenCodeDelivery, verifyGenericNativeDelivery } from './opencode-delivery.js';
import {verifyScopedPlanningDelivery,validateScopedPlan,scopedPlanningRejection} from './scoped-plan-contract.js';
import { assertNativeRelease,nativeSupervisorUnit,nativeSupervisorLockKey,nativeSupervisorLockNamespace,
  type NativeReservation,type NativeUnitObservation } from './native-supervision.js';

export type NativeCompletedRecovery = {
  reservation:NativeReservation;
  supervisorObservation:NativeUnitObservation;
  inspection:any;
  taskContractDigest:string;
  sourceManifestDigest:string;
};
type RecoveryOptions={nativeCompleted?:NativeCompletedRecovery};
const taskContractDigest=(task:any,run:any)=>hash(JSON.stringify([task.id,task.goal_id,task.description,task.instruction_revision_id,
  task.criteria,task.covers,task.stage,task.execution,task.recovery,task.allowed_accounts,task.account_id,task.model,
  run.context_id,run.agent_version,run.checkpoint]));
const deliveryDigest=(delivery:any)=>hash(JSON.stringify([delivery.runId,delivery.workspace,delivery.candidate,
  (delivery.artifacts??[]).map((artifact:any)=>[artifact.name,artifact.digest]).sort()]));
async function publishedManifestDigest(db:DB,runId:string){
  return hash(JSON.stringify((await db.query('SELECT id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[runId])).rows));
}

/** Read before external inspection/collection; changing a contract during those
 * reads invalidates adoption. This returns no prompt, credential or raw context. */
export async function prepareCompletedNativeArtifactRecovery(sourceId:string,fence:string){
  const row=(await pool.query(`SELECT r.state run_state,r.error_code,r.runtime,r.attempt,j.state job_state,j.supervision,t.*,
    r.context_id native_context_id,r.agent_version native_agent_version,r.checkpoint native_checkpoint
    FROM runs r JOIN tasks t ON t.id=r.task_id JOIN opencode_jobs j ON j.run_id=r.id
    WHERE r.id=$1 AND r.fence=$2`,[sourceId,fence])).rows[0];
  if(!row || row.runtime!=='opencode' || row.run_state!=='reconciling' || row.state!=='reconciling' || row.error_code!=='SUPERVISOR_LOST' ||
    row.job_state!=='orphaned' || row.attempt!==row.attempt_count || !row.supervision)throw Error('NATIVE_RECOVERY_NOT_ORPHANED');
  return {reservation:row.supervision as NativeReservation,sourceManifestDigest:await publishedManifestDigest(pool,sourceId),taskContractDigest:taskContractDigest(row,
    {context_id:row.native_context_id,agent_version:row.native_agent_version,checkpoint:row.native_checkpoint})};
}

async function assertCompletedNativeRecovery(db:DB,run:any,task:any,delivery:any,proof:NativeCompletedRecovery){
  const job=(await db.query('SELECT * FROM opencode_jobs WHERE run_id=$1 FOR UPDATE',[run.id])).rows[0];
  const reservation=proof.reservation,supervisor=proof.supervisorObservation,inspection=proof.inspection;
  assertNativeRelease(reservation);
  if(reservation.version!==1 || reservation.mode!=='detached' || (reservation.runtime??'opencode')!=='opencode' ||
    reservation.runId!==run.id || reservation.runFence!==run.fence || reservation.unit!==nativeSupervisorUnit(run.id) ||
    !job || !['orphaned','finished'].includes(job.state) ||
    ['version','mode','runId','runFence','launchToken','unit','candidate','digest','launchAttempt','reservedAt']
      .some(key=>(reservation as any)[key]!==job.supervision?.[key]) ||
    (job.supervision.runtime??'opencode')!=='opencode')throw Error('NATIVE_RECOVERY_RESERVATION_CHANGED');
  const age=Date.now()-Date.parse(supervisor?.checkedAt);
  if(supervisor?.unit!==reservation.unit || !Number.isFinite(age) || age< -5000 || age>30000 ||
    !['inactive','failed'].includes(supervisor.activeState) || !['loaded','not-found'].includes(supervisor.loadState) ||
    supervisor.mainPid!==0 || supervisor.pendingJob!==false || supervisor.cgroupEmpty!==true)
    throw Error('NATIVE_SUPERVISOR_STOP_UNCONFIRMED');
  // Prevent a delayed owner from registering between absence observation and
  // terminalization. A live owner already holding the session lock wins.
  if(!(await db.query('SELECT pg_try_advisory_xact_lock($1::int,$2::int) owned',
    [nativeSupervisorLockNamespace,nativeSupervisorLockKey(run.id)])).rows[0].owned)throw Error('NATIVE_SUPERVISOR_ACTIVE');
  const process=inspection?.process,inspectionAge=Date.now()-Date.parse(inspection?.checkedAt),completion=inspection?.state;
  if(inspection?.runId!==run.id || inspection.workspace!==task.execution?.workspace || !completion || typeof completion!=='object' ||
    completion.state!=='completed' || completion.runId!==run.id || completion.runtime!=='opencode' ||
    completion.stopped!==true || completion.exitCode!==0 ||
    process?.unit!=='agentos-run-'+run.id || process.stopped!==true || process.mainPid!==0 || process.populated!==false ||
    !['inactive','failed'].includes(process.activeState) || !Number.isFinite(inspectionAge) || inspectionAge< -5000 || inspectionAge>60000)
    throw Error('NATIVE_COMPLETION_UNCONFIRMED');
  if(!Array.isArray(completion.artifacts) || !completion.artifacts.length || completion.artifacts.length>32 ||
    new Set(completion.artifacts.map((artifact:any)=>artifact.name)).size!==completion.artifacts.length ||
    completion.artifacts.some((artifact:any)=>!artifact || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(artifact.name??'') ||
      !/^[a-f0-9]{64}$/.test(artifact.digest??'') ||
      !delivery.artifacts?.some((collected:any)=>collected.name===artifact.name && collected.digest===artifact.digest)))
    throw Error('NATIVE_COMPLETION_MANIFEST_CHANGED');
  if(inspection.candidate?.candidateDigest!==delivery.candidate?.candidateDigest ||
    inspection.candidate?.baseDigest!==delivery.candidate?.baseDigest)throw Error('SOURCE_CHANGED_DURING_COLLECTION');
  if(!/^[a-f0-9]{64}$/.test(proof.taskContractDigest))throw Error('NATIVE_RECOVERY_CONTRACT_CHANGED');
  return job;
}

const exec = promisify(execFile);
export const packagingFailureCodes: string[] = [
  'ARTIFACT_PATH', 'ARTIFACT_NAME', 'ARTIFACT_NAME_COLLISION',
  'ARTIFACT_MISSING', 'RESULT_FORMAT', 'RESERVED_ARTIFACT_NAME',
];
export const packagingFailure = (code: string) => packagingFailureCodes.includes(code);

export async function collectStoppedArtifacts(runId: string, readOnly = false, testFile?: string, exactCompletedNative=false) {
  try {
    const planning=(await pool.query("SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1 AND t.execution->>'workKind'='planning'",[runId])).rowCount;
    const { stdout } = await exec(
      'ssh',
      [
        '-T',
        '-o',
        'BatchMode=yes',
        '-o',
        'ConnectTimeout=8',
        config.bridgeHost,
        '/opt/deepseek-harness/node/bin/node',
        bridgeScript(planning ? 'recover-scope-plan.mjs' : 'recover-artifacts.mjs'),
        runId,
        ...(testFile
          ? [readOnly ? '--read-only' : '--writable', testFile]
          : readOnly
            ? ['--read-only']
            : []),
        ...(exactCompletedNative?['--completed-native']:[]),
      ],
      { timeout: 45000, maxBuffer: 36 * 1024 * 1024, windowsHide: true },
    );
    return JSON.parse(stdout);
  } catch (e: any) {
    try {
      const value = JSON.parse(e.stdout);
      if (value.error) throw Object.assign(Error(value.error), { detail: value });
    } catch (parsed: any) {
      if (parsed.detail) throw parsed;
    }
    throw Error('COLLECTION_UNAVAILABLE');
  }
}

export async function adoptRecoveredArtifacts(sourceId: string, fence: string, delivery: any,options:RecoveryOptions={}) {
  const result = await tx(async (db) => {
    await lock(db);
    const r = (
      await db.query('SELECT * FROM runs WHERE id=$1 AND fence=$2 FOR UPDATE', [sourceId, fence])
    ).rows[0];
    const nativeProof=options.nativeCompleted;
    const completedNative=!!nativeProof && r?.runtime==='opencode' && r.state==='reconciling' && r.error_code==='SUPERVISOR_LOST';
    if(nativeProof && r?.runtime==='opencode' && r.state==='failed' && r.error_code==='SUPERVISOR_LOST'){
      const task=(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[r.task_id])).rows[0];
      await assertCompletedNativeRecovery(db,r,task,delivery,nativeProof);
      const prior=(await db.query(`SELECT * FROM runs WHERE task_id=$1 AND attempt=$2
        AND checkpoint->>'kind'='artifact_recovery' AND checkpoint->>'sourceRunId'=$3`,[r.task_id,r.attempt+1,r.id])).rows[0];
      if(prior && task.attempt_count===prior.attempt && prior.checkpoint.nativeCompletedRecovery?.taskContractDigest===nativeProof.taskContractDigest &&
        prior.checkpoint.nativeCompletedRecovery?.sourceManifestDigest===nativeProof.sourceManifestDigest &&
        await publishedManifestDigest(db,sourceId)===nativeProof.sourceManifestDigest &&
        prior.checkpoint.nativeCompletedRecovery?.deliveryDigest===deliveryDigest(delivery) &&
        ['submitted','failed'].includes(prior.state))return {taskId:task.id,runId:prior.id};
      throw Error('TASK_CHANGED');
    }
    const submittedNative = r?.state === 'submitted' && r.runtime === 'opencode';
    if (!r || (!submittedNative && !completedNative && (r.state !== 'failed' || !packagingFailure(r.error_code))))
      throw Error('STALE_RUN');
    const t = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [r.task_id])).rows[0];
    // A collector may have started before a supervisor applied its decision.
    // Recheck under the same admission lock: old output must not replace that
    // decision with an unsupervised collection attempt.
    if (await appliedSupervisionOwnsContinuation(db, t.id, sourceId))
      throw Error('TASK_CHANGED');
    if(nativeProof && !completedNative)throw Error('NATIVE_RECOVERY_NOT_ORPHANED');
    if(completedNative){
      const job=await assertCompletedNativeRecovery(db,r,t,delivery,nativeProof!);
      if(job.state!=='orphaned' || t.state!=='reconciling' || taskContractDigest(t,r)!==nativeProof!.taskContractDigest ||
        await publishedManifestDigest(db,sourceId)!==nativeProof!.sourceManifestDigest)
        throw Error('NATIVE_RECOVERY_CONTRACT_CHANGED');
      if((await db.query('SELECT id FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[t.id])).rows[0]?.id!==sourceId)
        throw Error('TASK_CHANGED');
    }
    const g = (await db.query('SELECT state,project FROM goals WHERE id=$1', [t.goal_id])).rows[0];
    const autonomy = (
      await db.query('SELECT value FROM settings WHERE id=$1', ['autonomy:' + g.project])
    ).rows[0]?.value;
    if (
      ![
        ...(submittedNative ? ['submitted'] : []),
        ...(completedNative ? ['reconciling'] : []),
        'failed',
        'recovery_blocked',
        'recovering_artifacts',
      ].includes(t.state) ||
      t.attempt_count !== r.attempt ||
      !['active', 'failed'].includes(g.state) ||
      autonomy?.enabled === false ||
      (t.recovery?.mode === 'manual' && autonomy?.enabled !== true)
    )
      throw Error('TASK_CHANGED');
    const oldArtifacts = submittedNative || completedNative
      ? (await db.query('SELECT name,digest FROM artifacts WHERE run_id=$1', [sourceId])).rows
      : [];
    let missingNames: string[] = [];
    if (submittedNative) {
      const checks =
        (
          await db.query(
            'SELECT checks FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC LIMIT 1',
            [sourceId],
          )
        ).rows[0]?.checks ?? [];
      const failed = checks.filter((c: any) => !c.passed);
      if (
        !failed.length ||
        !failed.every((c: any) => {
          const criterion = t.criteria.find((v: any) => v.id === c.id);
          return (
            criterion?.name &&
            c.type !== 'review' &&
            !oldArtifacts.some((a) => a.name === criterion.name)
          );
        })
      )
        throw Error('NOT_MISSING_ARTIFACTS');
      missingNames = failed.map((c: any) => t.criteria.find((v: any) => v.id === c.id).name);
    }
    if (
      (
        await db.query(
          "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
          [sourceId],
        )
      ).rowCount
    )
      throw Error('TASK_CHANGED');
    const p = delivery?.process,
      age = Date.now() - Date.parse(delivery?.checkedAt);
    if (
      delivery?.runId !== sourceId ||
      delivery.workspace !== t.execution.workspace ||
      p?.unit !== `agentos-run-${sourceId}` ||
      p.stopped !== true ||
      p.mainPid !== 0 ||
      p.populated !== false ||
      !['inactive', 'failed'].includes(p.activeState) ||
      !Number.isFinite(age) ||
      age < -5000 ||
      age > 60000
    )
      throw Error('STOP_UNCONFIRMED');
    if (
      (
        await db.query(
          `SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
      WHERE r.state IN ('running','queued','reconciling','cancelling') AND t.execution->>'workspace'=$1
        AND ($2::uuid IS NULL OR r.id<>$2)`,
          [t.execution.workspace,completedNative?sourceId:null],
        )
      ).rowCount
    )
      throw Error('WORKSPACE_BUSY');
    if (
      t.execution.readOnlySource &&
      delivery.candidate?.candidateDigest !== delivery.candidate?.baseDigest
    )
      throw Error('SOURCE_CHANGED_DURING_VALIDATION');
    if (
      !delivery.candidate?.candidateDigest ||
      typeof delivery.summary !== 'string' ||
      !Array.isArray(delivery.artifacts)
    )
      throw Error('INVALID_COLLECTION');
    const names = new Set<string>();
    const files = delivery.artifacts.map((a: any) => {
      if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(a.name) || names.has(a.name))
        throw Error('INVALID_COLLECTION');
      names.add(a.name);
      const bytes = Buffer.from(a.base64, 'base64');
      if (hash(bytes) !== a.digest) throw Error('ARTIFACT_CORRUPT');
      return { ...a, id: randomUUID(), bytes };
    });
    if (
      !names.has('candidate.json') ||
      !names.has('changes.patch') ||
      files.length > 32 ||
      files.some((a: any) => a.bytes.length > 8 * 1024 * 1024) ||
      files.reduce((n: number, a: any) => n + a.bytes.length, 0) > 24 * 1024 * 1024
    )
      throw Error('INVALID_COLLECTION');
    const manifest = JSON.parse(
      files.find((a: any) => a.name === 'candidate.json').bytes.toString(),
    );
    if (
      manifest.candidateDigest !== delivery.candidate.candidateDigest ||
      manifest.baseDigest !== delivery.candidate.baseDigest
    )
      throw Error('ARTIFACT_CORRUPT');
    if (submittedNative || completedNative) {
      // Collect only missing output; never replace already-published evidence or its snapshot.
      if (
        oldArtifacts.some(
          (a) => !files.some((f: any) => f.name === a.name && f.digest === a.digest),
        )
      )
        throw Error('SOURCE_CHANGED_DURING_COLLECTION');
      if (missingNames.some((name) => !names.has(name))) throw Error('ARTIFACT_MISSING');
    }
    let planningRejection:any;
    if(r.runtime==='opencode' && t.execution.workKind==='planning'){
      verifyScopedPlanningDelivery(delivery);
      const request=(await db.query('SELECT * FROM planning_requests WHERE id=$1 AND task_id=$2',[t.execution.planningRequestId,t.id])).rows[0];
      if(!request)throw Error('PLANNING_REQUEST_INVALID');
      try{validateScopedPlan(JSON.parse(files.find((f:any)=>f.name==='scope-plan.json').bytes.toString()),request);}
      catch(error:any){planningRejection=scopedPlanningRejection(error);}
      if(!planningRejection){const bytes=Buffer.from(JSON.stringify({verified:true,requestId:request.id,sourceRevision:request.source_revision}));
        files.push({id:randomUUID(),name:'runtime-plan-check.json',mime:'application/json',digest:hash(bytes),bytes});}
    }else if (r.runtime === 'opencode' && Object.hasOwn(t.execution, 'testFile')) verifyOpenCodeDelivery(delivery, t.execution.testFile);
    else if (r.runtime === 'opencode') {
      if (delivery.collectorVersion !== 'artifact-manifest-v1') throw Error('GENERIC_DELIVERY_MANIFEST_REQUIRED');
      verifyGenericNativeDelivery(delivery, sourceId, t.execution);
    }
    const id = randomUUID(),
      attempt = r.attempt + 1;
    const checkpoint = {
      kind: 'artifact_recovery',
      sourceRunId: sourceId,
      originalError: r.error_code,
      collectorVersion: delivery.collectorVersion,
      process: p,
      candidateDigest: delivery.candidate.candidateDigest,
      modelInvoked: false,
      originalContextId: r.context_id,
      reservedClaims: delivery.reservedClaims ?? [],
      ...(completedNative?{nativeCompletedRecovery:{taskContractDigest:nativeProof!.taskContractDigest,
        sourceManifestDigest:nativeProof!.sourceManifestDigest,
        deliveryDigest:deliveryDigest(delivery),reservation:nativeProof!.reservation,
        supervisorObservation:nativeProof!.supervisorObservation,
        inspection:{runId:sourceId,workspace:delivery.workspace,state:{state:'completed',runId:sourceId,runtime:'opencode',stopped:true,exitCode:0,
          artifacts:nativeProof!.inspection.state.artifacts.map((artifact:any)=>({name:artifact.name,digest:artifact.digest}))},checkedAt:nativeProof!.inspection.checkedAt,
          process:nativeProof!.inspection.process,candidateDigest:delivery.candidate.candidateDigest,baseDigest:delivery.candidate.baseDigest}}}:{}),
    };
    if(completedNative){
      // The original model attempt still failed to report through its owner.
      // Preserve its error, context, usage, artifacts and instructions; only
      // release the reservation now that both processes are proven stopped.
      await db.query("UPDATE runs SET state='failed',finished_at=COALESCE(finished_at,now()) WHERE id=$1 AND fence=$2",[sourceId,fence]);
      await db.query("UPDATE opencode_jobs SET state='finished',finished_at=COALESCE(finished_at,now()) WHERE run_id=$1",[sourceId]);
      await db.query('DELETE FROM leases WHERE run_id=$1',[sourceId]);
      await event(db,'NATIVE_COMPLETED_OUTPUTS_RECONCILED',t.id,{runId:sourceId,collectionRunId:id,
        originalError:r.error_code,reservation:nativeProof!.reservation,
        supervisorObservation:nativeProof!.supervisorObservation,inspection:checkpoint.nativeCompletedRecovery?.inspection,
        modelInvoked:false});
    }
    // Keep the failed execution immutable; this is a separate collection attempt,
    // already submitted, with no worker job, resource reservation or model cost.
    await db.query(
      `INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,worker_id,
      started_at,finished_at,summary,usage,checkpoint) VALUES($1,$2,$3,$4,$9,$5,'submitted',
      'artifact-collector',now(),now(),$6,$7,$8)`,
      [
        id,
        t.id,
        attempt,
        randomUUID(),
        r.agent_version,
        'Entrega recuperada dos arquivos existentes, sem nova chamada ao modelo. ' +
          delivery.summary,
        JSON.stringify({ input: 0, output: 0, totalTokens: 0, cost: 0 }),
        JSON.stringify(checkpoint),
        r.runtime,
      ],
    );
    if(planningRejection)await db.query("UPDATE runs SET state='failed',error_code=$2 WHERE id=$1",[id,planningRejection.code]);
    await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
    for (const f of files) {
      await writeFile(path.join(config.dataDir, 'artifacts', f.id), f.bytes, { flag: 'wx' });
      await db.query(
        'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
        [f.id, id, f.name, f.mime, f.digest, f.bytes.length],
      );
    }
    const execution = { ...t.execution };
    delete execution.continuation;
    await db.query(
      "UPDATE tasks SET state='submitted',attempt_count=$2,max_attempts=GREATEST(max_attempts,$2),execution=$3,next_attempt_at=NULL,reason='Arquivos recuperados; conferindo os critérios originais.',updated_at=now() WHERE id=$1",
      [t.id, attempt, JSON.stringify(execution)],
    );
    if(planningRejection){
      const artifact=files.find((f:any)=>f.name==='scope-plan.json');
      await db.query("UPDATE tasks SET state='failed',reason='Plano recuperado exige correção dos campos identificados.' WHERE id=$1",[t.id]);
      await db.query("UPDATE planning_requests SET state='replanning_required',receipt=$2,updated_at=now() WHERE id=$1",[
        t.execution.planningRequestId,JSON.stringify({...planningRejection,artifactId:artifact.id})]);
    }
    await db.query("UPDATE goals SET state='active' WHERE id=$1 AND state='failed'", [t.goal_id]);
    await db.query("UPDATE recoveries SET status='resolved' WHERE source_run_id=$1", [sourceId]);
    await event(db, 'ARTIFACTS_RECOVERED', t.id, {
      runId: id,
      sourceRunId: sourceId,
      originalError: r.error_code,
      artifactCount: files.length,
      modelInvoked: false,
    });
    return { taskId: t.id, runId: id };
  });
  try {
    await validateTask(result.taskId);
  } catch (e: any) {
    if (!['STATE_CHANGED', 'NOT_SUBMITTED'].includes(e.code)) throw e;
  }
  return result;
}
