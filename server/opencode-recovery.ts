import { pendingProviderBackoff } from '../bridge/opencode-provider-error.mjs';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { pool, tx, lock, event } from './db.js';
import { config } from './config.js';
import { hash } from './memory.js';
import { quotaFor } from './accounts.js';
import { assertSupervisedResume, assertDependenciesAccepted } from './supervision-handoff.js';
import { appliedSupervisionOwnsContinuation } from './recovery-supervision.js';
import { protocolReportRepairInstruction } from '../bridge/protocol-report-contract.mjs';
import { prepareTaskInstructions } from './task-instructions.js';
import {assertOperatorOpenCodePlannerContext} from './operator-opencode-recovery-contract.js';
import { checkNativePreparation, createNativeAdmissionReceipt } from './supervised-opencode-admission.js';

// Used by the native recovery command, never by the generic Harness dispatcher.
// The caller obtains the inspection over the authenticated SSH bridge.
type RecoveryResult={previousRunId:string;runId:string;taskId:string;attempt:number;artifacts:{id:string;name:string;digest:string}[]};
type PreparedRecoveryResult=Omit<RecoveryResult,'runId'>&{runId:null;prepared:true};
export function recoverOpenCodeRun(id:string,fence:string,inspection:any,options:{prepareSupervised:true}):Promise<PreparedRecoveryResult>;
export function recoverOpenCodeRun(id:string,fence:string,inspection:any,options?:{prepareSupervised?:false}):Promise<RecoveryResult>;
export async function recoverOpenCodeRun(id: string, fence: string, inspection: any, options:{prepareSupervised?:boolean}={}):Promise<RecoveryResult|PreparedRecoveryResult> {
  const preparing=options.prepareSupervised===true;
  if(preparing&&!inspection?.supervisionResumeCaseId)throw Error('SUPERVISION_HANDOFF_CHANGED');
  const process = inspection?.process;
  const age = Date.now() - Date.parse(inspection?.checkedAt);
  if (
    inspection?.runId !== id ||
    inspection.runtime !== 'opencode' ||
    process?.stopped !== true ||
    !['inactive', 'failed'].includes(process.activeState) ||
    process.mainPid !== 0 ||
    process.populated !== false ||
    !Number.isFinite(age) ||
    age < -5000 ||
    age > 60000
  )
    throw Error('INVALID_STOP_PROOF');
  const diagnostic = { ...inspection, candidate: { ...inspection.candidate, patch: undefined } };
  const files = [
    {
      name: 'recovery-diagnostic.json',
      mime: 'application/json',
      bytes: Buffer.from(JSON.stringify(diagnostic, null, 2)),
    },
    {
      name: 'recovery-changes.patch',
      mime: 'text/plain',
      bytes: Buffer.from(inspection.candidate?.patch ?? ''),
    },
  ].map((a) => ({ ...a, id: randomUUID(), digest: hash(a.bytes) }));
  if (files.some((f) => f.bytes.length > 8 * 1024 * 1024))
    throw Error('RECOVERY_ARTIFACT_TOO_LARGE');
  return tx(async (db) => {
    await lock(db);
    const old = (
      await db.query('SELECT * FROM runs WHERE id=$1 AND fence=$2 FOR UPDATE', [id, fence])
    ).rows[0];
    if (
      !old ||
      old.runtime !== 'opencode' ||
      !['reconciling', 'failed', 'submitted'].includes(old.state)
    )
      throw Error('STALE_RUN');
    // Artifact collection creates an attempt, but no second agent process.
    // Inspection therefore belongs to the original execution. Bind that
    // identity to the persisted checkpoint and immutable source workspace;
    // never accept a process ID supplied by the caller alone.
    const executionId=old.checkpoint?.kind==='artifact_recovery'?old.checkpoint.sourceRunId:id;
    let execution=old;
    if(old.checkpoint?.kind==='artifact_recovery' && executionId===id)throw Error('INVALID_STOP_PROOF');
    if(process.unit!==`agentos-run-${executionId}`)throw Error('INVALID_STOP_PROOF');
    if(executionId!==id){
      const source=(await db.query(`SELECT id,task_id,runtime,attempt,state,account_id,model,agent_version,
        instruction_snapshot->'task'->'execution'->>'workspace' workspace FROM runs WHERE id=$1`,
        [executionId])).rows[0];
      if(inspection.executionRunId!==executionId || !source || source.task_id!==old.task_id ||
        source.runtime!=='opencode' || source.attempt>=old.attempt ||
        !['failed','submitted'].includes(source.state) || source.workspace!==inspection.workspace)
        throw Error('INVALID_STOP_PROOF');
      // Collection has no model invocation or account reservation. Reuse only
      // the route of the verified original execution, never caller metadata or
      // a subsequently changed task route. Its current quota is checked below.
      execution=source;
    }
    const task = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [old.task_id]))
      .rows[0];
    await assertOperatorOpenCodePlannerContext(db,task,old);
    if (
      inspection.notStarted &&
      (old.state !== 'failed' ||
        inspection.state?.state !== 'not_started' ||
        !(
          await db.query(
            `SELECT 1 FROM events WHERE kind='OPENCODE_NOT_STARTED'
        AND entity_id=$1 AND payload->>'runId'=$2 AND payload->>'confirmed'='true'`,
            [old.task_id, id],
          )
        ).rowCount)
    )
      throw Error('NOT_STARTED_RECEIPT_MISSING');
    const supervisedPackaging = task.state==='recovering_artifacts' && !!inspection.supervisionResumeCaseId &&
      await appliedSupervisionOwnsContinuation(db,task.id,id);
    if (
      task.execution?.kind !== 'opencode' ||
      task.execution.workspace !== inspection.workspace ||
      task.attempt_count !== old.attempt ||
      ![
        'reconciling',
        'failed',
        'submitted',
        ...(inspection.supervisionResumeCaseId ? ['recovery_blocked'] : []),
        ...(inspection.packagingFallback || supervisedPackaging ? ['recovering_artifacts'] : []),
      ].includes(task.state)
    )
      throw Error('TASK_CHANGED');
    // Collection can fail after a supervisor has applied its next approach.
    // Generic packaging fallback cannot take that stopped attempt away from
    // its exact owner; the supervised resume path keeps its existing checks.
    if(inspection.packagingFallback && !inspection.supervisionResumeCaseId &&
      await appliedSupervisionOwnsContinuation(db,task.id,id))throw Error('TASK_CHANGED');
    if(preparing)await checkNativePreparation(db,task,old,inspection);
    else await assertSupervisedResume(db, task, old, inspection);
    await assertDependenciesAccepted(db, task.id);
    if (inspection.packagingFallback) {
      const authorized = await db.query(
        `SELECT 1 FROM goals g LEFT JOIN settings a ON a.id='autonomy:'||g.project
        WHERE g.id=$1 AND g.state='active' AND COALESCE(a.value->>'enabled','true')='true'
        AND ($2::text IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')`,
        [task.goal_id, task.recovery?.mode ?? null],
      );
      if (
        !authorized.rowCount ||
        (
          await db.query(
            "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
            [id],
          )
        ).rowCount ||
        (
          await db.query(
            "SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','reconciling','cancelling')",
            [task.id],
          )
        ).rowCount
      )
        throw Error('TASK_CHANGED');
    }
    if (old.state === 'submitted') {
      const checks =
        (
          await db.query(
            'SELECT checks FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC LIMIT 1',
            [id],
          )
        ).rows[0]?.checks ?? [];
      // A later applied supervisor decision can invalidate an earlier auxiliary
      // delivery. assertSupervisedResume above binds that decision to this exact
      // stopped attempt and rechecks autonomy, cancellation and dependencies.
      if (
        !checks.some((c: any) => !c.passed && c.type !== 'review') &&
        !inspection.supervisionResumeCaseId
      )
        throw Error('NO_FAILED_CHECK');
    }
    const goal = (await db.query('SELECT state FROM goals WHERE id=$1', [task.goal_id])).rows[0];
    if (goal.state !== 'active') throw Error('GOAL_NOT_ACTIVE');
    const objective = (await db.query('SELECT request FROM objectives WHERE id=$1', [task.goal_id]))
      .rows[0];
    const objectiveCapacity=(await db.query('SELECT value FROM settings WHERE id=$1',['objective-capacity:'+task.goal_id])).rows[0]?.value?.maxRuns??objective?.request.requestedCapacity;
    if (
      !preparing && objective &&
      Number(
        (
          await db.query(
            "SELECT count(*) FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 AND r.id<>$2 AND r.state IN ('queued','running','cancelling','reconciling')",
            [task.goal_id, id],
          )
        ).rows[0].count,
      ) >= objectiveCapacity
    )
      throw Error('CAPACITY_UNAVAILABLE');
    const providerWait = pendingProviderBackoff(inspection.state, {
      runId: executionId, workspace: task.execution.workspace, model: execution.model,
    });
    if (providerWait) throw Object.assign(Error('PROVIDER_RETRY_PENDING'), { notBefore: providerWait.notBefore });
    const account = (await db.query('SELECT * FROM accounts WHERE id=$1', [execution.account_id]))
      .rows[0];
    const quota = account && quotaFor(account, execution.model);
    if (
      !account?.enabled ||
      !account.models.includes(execution.model) ||
      !quota ||
      ['exhausted', 'auth_required'].includes(quota.status) ||
      (quota.status === 'unknown' && !account.allow_unknown)
    )
      throw Error('ACCOUNT_UNAVAILABLE');
    const limits = Object.fromEntries(
      (await db.query("SELECT * FROM settings WHERE id IN('capacity','remote-capacity')")).rows.map(
        (s) => [s.id, s.value],
      ),
    );
    const active = Number(
      (
        await db.query(
          "SELECT (SELECT count(*) FROM runs WHERE id<>$1 AND state IN('queued','running','cancelling','reconciling'))+(SELECT count(*) FROM cycle_attempts WHERE account_id IS NOT NULL AND state IN('running','cancelling','reconciling')) count",
          [id],
        )
      ).rows[0].count,
    );
    const used = Number(
      (
        await db.query(
          "SELECT (SELECT count(*) FROM runs WHERE id<>$1 AND account_id=$2 AND state IN('queued','running','cancelling','reconciling'))+(SELECT count(*) FROM cycle_attempts WHERE account_id=$2 AND state IN('running','cancelling','reconciling'))+(SELECT count(*) FROM chat_messages WHERE account_id=$2 AND state IN('queued','running','cancelling'))+(SELECT count(*) FROM planning_runs WHERE account_id=$2 AND state='running' AND output IS NULL) count",
          [id, execution.account_id],
        )
      ).rows[0].count,
    );
    if (!preparing && (active >= (limits.capacity?.maxRuns ?? 1) || used >= account.max_concurrency))
      throw Error('CAPACITY_UNAVAILABLE');
    if (!preparing &&
      (
        await db.query("SELECT 1 FROM leases WHERE resource='host:agentos-remote' AND run_id<>$1", [
          id,
        ])
      ).rowCount
    )
      throw Error('EXCLUSIVE_HOST_BUSY');
    for (const resource of task.resources) {
      const count = Number(
        (
          await db.query('SELECT count(*) FROM leases WHERE resource=$1 AND run_id<>$2', [
            resource,
            id,
          ])
        ).rows[0].count,
      );
      const limit =
        resource === 'remote-slot'
          ? (limits['remote-capacity']?.maxRuns ?? 1)
          : resource === 'browser'
            ? (limits.capacity?.browserSlots ?? 1)
            : 1;
      if (!preparing && count >= limit) throw Error('RESOURCE_BUSY');
    }
    const lastError = (
      await db.query(
        "SELECT payload->>'code' code FROM events WHERE payload->>'runId'=$1 AND kind='MODEL_ERROR' ORDER BY id DESC LIMIT 1",
        [id],
      )
    ).rows[0]?.code;
    const collected=old.checkpoint?.kind==='artifact_recovery';
    const code =
      (collected?old.error_code:inspection.state.originalCode) ??
      inspection.state.code ??
      old.error_code ??
      lastError ??
      'OPENCODE_INTERRUPTED';
    const summary = `OpenCode interrompido por ${code}. Encerramento confirmado no servidor; diagnóstico e alterações parciais preservados. Retomada em uma nova tentativa, sem aceite desta entrega.`;
    await db.query(
      "UPDATE runs SET state=CASE WHEN state='submitted' THEN state ELSE 'failed' END,error_code=CASE WHEN state='submitted' OR $6 THEN error_code ELSE $3 END,summary=CASE WHEN state='submitted' OR $6 THEN summary ELSE $4 END,finished_at=COALESCE(finished_at,now()),usage=COALESCE(usage,$5::jsonb) WHERE id=$1 AND fence=$2",
      [id, fence, code, summary, JSON.stringify(collected?old.usage:inspection.state.usage ?? null),collected],
    );
    await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
    for (const file of files) {
      await writeFile(path.join(config.dataDir, 'artifacts', file.id), file.bytes, { flag: 'wx' });
      await db.query(
        'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
        [file.id, id, file.name, file.mime, file.digest, file.bytes.length],
      );
      await event(db, 'ARTIFACT_CREATED', task.id, {
        runId: id,
        artifactId: file.id,
        name: file.name,
        digest: file.digest,
        recovery: true,
      });
    }
    await event(db, 'OPENCODE_STOPPED', task.id, {
      runId: id,
      confirmed: true,
      proof: process,
      reconciled: true,
    });
    await event(db, 'RUN_RECONCILED', task.id, {
      runId: id,
      originalCode: old.error_code,
      code,
      state: old.state === 'submitted' ? 'submitted' : 'failed',
    });
    await db.query('DELETE FROM leases WHERE run_id=$1', [id]);
    const runId = randomUUID(),
      nextFence = randomUUID(),
      attempt = old.attempt + 1;
    const checkpoint = {
      kind: 'opencode_recovery',
      previousRunId: id,
      previousSummary: summary,
      instruction:task.execution.workKind==='planning'
        ? 'Retome somente o planejamento do escopo e contrato original. Preserve scope-plan.json existente; corrija sua entrega estruturada em evidence/scope-plan.json e retorne JSON final com summary e artifacts. Não produza protocol-report.json nem substitua o plano por uma suíte de regressão.'
        : /RESULT_FORMAT|ARTIFACT_|DELIVERY/.test(code)
        ? 'Os arquivos já foram produzidos. Corrija apenas a entrega: ' +
          (task.execution.testFile ? protocolReportRepairInstruction(code, task.execution.testFile) : 'Preserve o contrato substantivo da tarefa e todos os artefatos reais exigidos; não substitua uma integração ou relatório por uma suíte de protocolo.') +
          ' Confira os arquivos exigidos, paths relativos e JSON final com summary e artifacts. Use o validador delivery-manifest desta nova tentativa antes de terminar. Não refaça a suíte se os logs e o relatório já forem válidos. Preserve falhas reais no relatório.'
        : 'Retome os arquivos e corrija a causa da falha anterior. Execute somente o trabalho pendente, publique a suíte, relatório e logs verdadeiros. Retorne JSON com summary e artifacts e paths relativos a /workspace.',
      preservedWorkspace: task.execution.workspace,
      artifacts: files.map((f) => ({ id: f.id, name: f.name, digest: f.digest })),
      collectionFailure: inspection.collectionFailure,
    };
    if(preparing){
      // A durable request, not a run or capacity reservation. All actual slot,
      // quota, resource and fresh-stop checks belong to the common scheduler.
      await db.query(`UPDATE tasks SET account_id=$2,model=$3,execution=jsonb_set(execution,'{recoveryContext}',$4::jsonb)
        WHERE id=$1`,[task.id,execution.account_id,execution.model,JSON.stringify(checkpoint)]);
      await prepareTaskInstructions(db,task.id);
      const preparedTask=(await db.query('SELECT * FROM tasks WHERE id=$1',[task.id])).rows[0];
      const receipt=await createNativeAdmissionReceipt(db,preparedTask,old,execution,inspection,checkpoint);
      await db.query(`UPDATE tasks SET state='retry_scheduled',max_attempts=GREATEST(max_attempts,attempt_count+1),
        next_attempt_at=now(),reason='Retomada supervisionada preparada; aguardando a fila comum.',
        execution=jsonb_set(jsonb_set(execution,'{supervisedAdmission}',$2::jsonb),'{continuation}',$3::jsonb),updated_at=now()
        WHERE id=$1`,[task.id,JSON.stringify(receipt),JSON.stringify({state:'queued',owner:'scheduler',runId:id,
        caseId:receipt.caseId,admissionReceiptId:receipt.id,updatedAt:new Date().toISOString()})]);
      await event(db,'SUPERVISION_RESUME_PREPARED',task.id,{caseId:receipt.caseId,sourceRunId:id,
        admissionReceiptId:receipt.id,attempt:old.attempt,stopConfirmed:true});
      return {previousRunId:id,runId:null,taskId:task.id,attempt:old.attempt,artifacts:checkpoint.artifacts,prepared:true as const};
    }
    // The immutable run snapshot must include the recovery inputs consumed by
    // the executor. Keep admission/state transitions after the run INSERT.
    await db.query(
      "UPDATE tasks SET execution=jsonb_set(jsonb_set(execution,'{recoveryContext}',$2::jsonb),'{continuation}',$3::jsonb) WHERE id=$1",
      [task.id, JSON.stringify(checkpoint), JSON.stringify({
        state: 'queued',
        message: 'Retomada automática preparada.',
        runId,
        updatedAt: new Date().toISOString(),
      })],
    );
    await prepareTaskInstructions(db, task.id);
    await db.query(
      "INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,checkpoint) VALUES($1,$2,$3,$4,$5,$6,'opencode',$7,$8)",
      [
        runId,
        task.id,
        attempt,
        nextFence,
        execution.account_id,
        execution.model,
        execution.agent_version,
        JSON.stringify(checkpoint),
      ],
    );
    for (const resource of task.resources)
      await db.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)', [resource, runId]);
    await db.query(
      "UPDATE tasks SET state='queued',attempt_count=$2,max_attempts=GREATEST(max_attempts,$2),reason='OpenCode retomando do workspace preservado após confirmação do encerramento.',updated_at=now() WHERE id=$1",
      [task.id, attempt],
    );
    await db.query('INSERT INTO opencode_jobs(run_id) VALUES($1)', [runId]);
    await db.query(
      `WITH RECURSIVE b(id) AS (SELECT task_id FROM dependencies WHERE depends_on=$1 UNION SELECT d.task_id FROM dependencies d JOIN b ON d.depends_on=b.id) UPDATE tasks SET state='blocked_dependencies',reason='Aguarda correção automática da etapa anterior.' WHERE id IN (SELECT id FROM b) AND state='blocked_failed'`,
      [task.id],
    );
    await event(db, 'RUN_QUEUED', task.id, {
      runId,
      attempt,
      runtime: 'opencode',
      previousRunId: id,
      recovery: true,
    });
    if (inspection.supervisionResumeCaseId || inspection.packagingFallback)
      await event(db, 'CONTINUATION_QUEUED', task.id, {
        runId: id,
        nextRunId: runId,
        signature: inspection.continuationSignature,
        caseId: inspection.supervisionResumeCaseId,
      });
    return { previousRunId: id, runId, taskId: task.id, attempt, artifacts: checkpoint.artifacts };
  });
}
