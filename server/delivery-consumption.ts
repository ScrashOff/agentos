import { createHash, randomUUID } from 'node:crypto';
import { pool, tx, lock, event, type DB } from './db.js';
import { readArtifact } from './service.js';
import { assertStopped, inspectStopped } from './continuation.js';
import { runtimeCandidateDigest } from './operation-progress.js';
import {readRuntimeComposition} from './composition-evidence.js';
import { appliedSupervisionOwnsContinuation } from './recovery-supervision.js';
import { deliveryDigest, requestDeliverySupervision } from './delivery-supervision.js';
import { scheduledSupervisionAdmission } from './supervised-admission.js';
import { selectDeliveryConsumptionBatch } from './delivery-consumption-selection.js';
import {readDeliverySourceIdentity,deliverySourceConflict,readDeliverySourceResolution,effectiveDeliverySource,deliverySourceFollowthrough} from './delivery-source-resolution.js';
export {readDeliveryConsumptions} from './delivery-consumption-catalog.js';

const live = ['queued', 'running', 'cancelling', 'reconciling'];
const blockedDependencies = (dependencies: any[]) => dependencies.some(d => ['failed', 'recovery_blocked', 'blocked_failed', 'cancelled', 'superseded'].includes(d.state));
const sha = (b: Buffer) => createHash('sha256').update(b).digest('hex');
type Artifact = { id: string; run_id: string; name: string; digest: string; size: number };
type Reader = (id: string) => Promise<{ bytes: Buffer; digest?: string }>;

/** This identity fixes the requested result, not a supervisor's subsequent
 * authorized instructions. Each consuming run must also match its own frozen
 * instructions; no historic acceptance is inferred from today's task. */
function contract(task: any) {
  return deliveryDigest({ criteria: task.criteria, covers: task.covers, stage: task.stage,
    baseWorkspace: task.execution?.baseWorkspace ?? null, sourceWorkspace: task.execution?.sourceWorkspace ?? null,
    readOnlySource: task.execution?.readOnlySource ?? false, writePaths: task.execution?.writePaths ?? null });
}

export async function installDeliveryConsumptions(db: DB = pool) {
  await db.query(`CREATE TABLE IF NOT EXISTS delivery_consumptions(
    id uuid PRIMARY KEY,handoff_id uuid NOT NULL UNIQUE REFERENCES deferred_handoffs(id),
    goal_id uuid NOT NULL REFERENCES goals(id),producer_task_id uuid NOT NULL REFERENCES tasks(id),
    producer_run_id uuid NOT NULL REFERENCES runs(id),consumer_task_id uuid NOT NULL REFERENCES tasks(id),
    scope text,consumer_contract_digest text NOT NULL,producer_manifest_digest text NOT NULL,
    binding jsonb NOT NULL,state text NOT NULL DEFAULT 'attached',receipt jsonb NOT NULL DEFAULT '{}',
    candidate_run_id uuid REFERENCES runs(id),candidate_digest text,qa_task_id uuid REFERENCES tasks(id),qa_run_id uuid REFERENCES runs(id),
    created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),checked_at timestamptz);
    CREATE INDEX IF NOT EXISTS delivery_consumption_pending ON delivery_consumptions(checked_at NULLS FIRST,created_at,id)
      WHERE state NOT IN('completed','superseded');
    CREATE INDEX IF NOT EXISTS delivery_consumption_goal ON delivery_consumptions(goal_id,state,created_at);`);
}

/** Runs inside the handoff transaction. Metadata is pinned here; content bytes
 * are verified outside the admission lock before any consumption receipt. */
export async function registerDeliveryConsumption(db: DB, handoff: any, consumer: any, producerRun: any) {
  if (handoff.handoff?.workKind !== 'repair') return null;
  if (!producerRun || producerRun.task_id !== handoff.producer_task_id || producerRun.state !== 'submitted')
    throw Error('DELIVERY_PRODUCER_BINDING');
  const artifacts = (await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id', [producerRun.id])).rows as Artifact[];
  const refs = artifacts.filter(a => ['implementation.patch', 'repair-report.json'].includes(a.name));
  const id = randomUUID(), key = contract(consumer);
  const inserted = await db.query(`INSERT INTO delivery_consumptions(id,handoff_id,goal_id,producer_task_id,producer_run_id,
    consumer_task_id,scope,consumer_contract_digest,producer_manifest_digest,binding)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(handoff_id) DO NOTHING RETURNING id`,
    [id, handoff.id, handoff.goal_id, handoff.producer_task_id, producerRun.id, consumer.id,
      consumer.execution?.baseWorkspace ?? null, key, deliveryDigest(artifacts), JSON.stringify({ version: 1,
        producerAttempt: producerRun.attempt, producerFence: producerRun.fence, producerArtifacts: artifacts, repairArtifacts: refs,
        consumerAttempt: consumer.attempt_count, consumerWorkspace: consumer.execution?.workspace, consumerContract: key,
        appliedConsumerRunId: handoff.receipt?.consumerRunId ?? null })]);
  if (inserted.rowCount) await event(db, 'DELIVERY_CONSUMPTION_CREATED', handoff.goal_id,
    { obligationId: id, handoffId: handoff.id, producerTaskId: handoff.producer_task_id, producerRunId: producerRun.id, consumerTaskId: consumer.id });
  return inserted.rows[0]?.id ?? null;
}

// Project only control fields. Never select large runtime inputs, old prompts or
// historical execution bundles for every pending obligation.
async function taskView(db: DB, id: string) {
  return (await db.query(`SELECT t.id,t.goal_id,t.state,t.attempt_count,t.description,t.criteria,t.covers,t.stage,
    t.instruction_revision_id,t.recovery,t.agent_id,t.resources,t.required_memories,t.allowed_accounts,
    jsonb_build_object('kind',e.kind,'workKind',e."workKind",'workspace',e.workspace,'baseWorkspace',e."baseWorkspace",
      'sourceWorkspace',e."sourceWorkspace",'readOnlySource',e."readOnlySource",'writePaths',e."writePaths") execution,
    EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state=ANY($2::text[])) live,
    EXISTS(SELECT 1 FROM leases l JOIN runs x ON x.id=l.run_id WHERE x.task_id=t.id) leased
    FROM tasks t LEFT JOIN LATERAL jsonb_to_record(t.execution) AS e(kind text,"workKind" text,workspace text,
      "baseWorkspace" text,"sourceWorkspace" text,"readOnlySource" boolean,"writePaths" jsonb) ON true WHERE t.id=$1`, [id, live])).rows[0];
}

async function runView(db: DB, taskId: string) {
  // The continuation envelope is controller status, never an instruction.
  // Recovery context, inline inputs, tools and permissions remain compared.
  const row = (await db.query(`SELECT r.id,r.task_id,r.attempt,r.fence,r.state,r.context_id,r.runtime,
    r.instruction_snapshot->>'schema' snapshot_schema,r.instruction_snapshot#>>'{task,description}' frozen_description,
    r.instruction_snapshot#>'{task,criteria}' frozen_criteria,r.instruction_snapshot#>>'{task,instruction_revision_id}' frozen_revision,
    r.instruction_snapshot#>>'{task,execution,workspace}' frozen_workspace,
    r.instruction_snapshot#>>'{task,execution,sourceWorkspace}' frozen_source_workspace,
    (r.instruction_snapshot#>'{task,agent_id}'=to_jsonb(t.agent_id) AND
      r.instruction_snapshot#>'{task,resources}' IS NOT DISTINCT FROM t.resources AND
      r.instruction_snapshot#>'{task,required_memories}' IS NOT DISTINCT FROM t.required_memories AND
      r.instruction_snapshot#>'{task,allowed_accounts}' IS NOT DISTINCT FROM to_jsonb(t.allowed_accounts) AND
      (r.instruction_snapshot#>'{task,execution}')-'continuation' IS NOT DISTINCT FROM t.execution-'continuation') frozen_policy,
    a.id acceptance_id,a.passed,a.created_at accepted_at,
    EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text) cancelled
    FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN LATERAL(SELECT id,passed,created_at FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    WHERE r.task_id=$1 ORDER BY r.attempt DESC LIMIT 1`, [taskId])).rows[0];
  if (!row) return null;
  row.artifacts = (await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id', [row.id])).rows;
  return row;
}
const accepted = (task: any, run: any) => task?.state === 'accepted' && !task.live && !task.leased && run?.state === 'submitted' &&
  run.attempt === task.attempt_count && run.passed === true && !run.cancelled;
const frozen = (task: any, run: any) => run?.snapshot_schema === '1' && run.frozen_policy === true && run.frozen_description === task.description &&
  deliveryDigest(run.frozen_criteria) === deliveryDigest(task.criteria) && (run.frozen_revision ?? null) === (task.instruction_revision_id ?? null) &&
  (run.frozen_workspace ?? null) === (task.execution?.workspace ?? null) &&
  (run.frozen_source_workspace ?? null) === (task.execution?.sourceWorkspace ?? null);


async function inputsFor(db: DB, run: any, source: string) {
  if (!run?.context_id) return [];
  return (await db.query(`SELECT entry FROM contexts c CROSS JOIN LATERAL jsonb_array_elements(COALESCE(c.manifest->'dependencyArtifacts','[]')) entry
    WHERE c.id=$1 AND c.run_id=$2 AND entry->>'task_id'=$3 ORDER BY entry->>'id'`, [run.context_id, run.id, source])).rows.map(r => r.entry);
}

async function observation(db: DB, row: any) {
  const sourceResolution=await readDeliverySourceResolution(db,row),source=effectiveDeliverySource(row,sourceResolution);
  const consumer = await taskView(db, row.consumer_task_id), producer = await taskView(db, source.producer_task_id);
  const run = await runView(db, row.consumer_task_id), producerRun = await runView(db, source.producer_task_id);
  const sourceIdentity=await readDeliverySourceIdentity(db,source.producer_task_id);
  const conflict=deliverySourceConflict(source,sourceIdentity,producerRun?.artifacts??[]);
  const sourceFollowthrough=conflict?await deliverySourceFollowthrough(db,row,conflict.generation):{recertificationSources:[],fulfilledOtherObligation:null};
  const goal = (await db.query(`SELECT g.id,g.state,a.value->>'enabled' autonomy,s.value->>'enabled' supervision
    FROM goals g LEFT JOIN settings a ON a.id='autonomy:'||g.project LEFT JOIN settings s ON s.id='supervision:'||g.id WHERE g.id=$1`, [row.goal_id])).rows[0];
  const dependencies = (await db.query(`SELECT t.id,t.state FROM dependencies d JOIN tasks t ON t.id=d.depends_on
    WHERE d.task_id=$1 ORDER BY t.id`, [row.consumer_task_id])).rows;
  const inputs = await inputsFor(db, run, source.producer_task_id);
  const composition = run ? await readRuntimeComposition(db,consumer.id,run.id,consumer.execution.workspace) : null;
  // Select the latest real QA of this consumer, not an old successful QA from
  // another branch or a diagnostic that happens to publish a candidate file.
  const qaId = consumer?.stage === 'integrate' ? (await db.query(`SELECT t.id FROM dependencies d JOIN tasks t ON t.id=d.task_id
    WHERE d.depends_on=$1 AND t.goal_id=$2 AND t.stage='verify' AND t.state NOT IN('cancelled','superseded')
      AND t.execution->>'readOnlySource'='true' AND t.execution->>'sourceWorkspace'=$3
      AND t.execution->>'baseWorkspace' IS NOT DISTINCT FROM $4
      AND COALESCE(t.execution->>'workKind','') NOT IN('diagnostic','repair','planning')
    ORDER BY t.created_at DESC,t.id DESC LIMIT 1`, [consumer.id, row.goal_id, consumer.execution.workspace, row.scope])).rows[0]?.id : null;
  const qa = qaId ? await taskView(db, qaId) : null, qaRun = qaId ? await runView(db, qaId) : null;
  const qaInputs = qaId ? await inputsFor(db, qaRun, consumer.id) : [];
  const pendingCases = (await db.query(`SELECT id,task_id,state FROM supervision_cases WHERE task_id=ANY($1::uuid[])
    AND state IN('open','decided','delegated','waiting_evidence') ORDER BY id`, [[consumer?.id, qa?.id].filter(Boolean)])).rows;
  const consumerAdmission = consumer && run ? await scheduledSupervisionAdmission(db, consumer.id, run.id) : null;
  return { goal, consumer, producer, run, producerRun, dependencies, inputs, composition, qa, qaRun, qaInputs, pendingCases, consumerAdmission, sourceResolution, sourceIdentity, sourceFollowthrough };
}

async function content(ref: Artifact, read: Reader) {
  const value = await read(ref.id);
  if (value.bytes.length !== ref.size || sha(value.bytes) !== ref.digest || (value.digest && value.digest !== ref.digest))
    throw Error('DELIVERY_ARTIFACT_CHANGED');
  return value.bytes;
}
const one = (refs: Artifact[], name: string) => { const matches = refs.filter(r => r.name === name); return matches.length === 1 ? matches[0] : null; };
const bound = (inputs: any[], ref: Artifact, taskId: string) => inputs.filter(i => i.id === ref.id && i.name === ref.name &&
  i.digest === ref.digest && i.task_id === taskId).length === 1;

/** Verifies delivery linkage, not semantic acceptance. Only the original
 * validateTask receipt can certify criteria; the independent QA is separate. */
async function verifiedDelivery(row: any, o: any, read: Reader) {
  const refs = row.binding.repairArtifacts as Artifact[];
  const patch = one(refs, 'implementation.patch'), report = one(refs, 'repair-report.json');
  if (!patch || !report) throw Error('REPAIR_ARTIFACTS_REQUIRED');
  await content(patch, read); await content(report, read);
  if (!o.run || ![patch, report].every(ref => bound(o.inputs, ref, row.producer_task_id)))
    return { code: 'CONSUMER_INPUTS_NOT_BOUND' };
  if (!frozen(o.consumer, o.run)) return { code: 'CONSUMER_CONTRACT_CHANGED' };
  if (o.consumer.stage !== 'integrate') return accepted(o.consumer, o.run) ?
    { code: 'CONSUMER_ACCEPTED', completed: true, consumerRunId: o.run.id, acceptanceId: o.run.acceptance_id, resultKind: 'consumer_result' } :
    { code: 'CONSUMER_NOT_ACCEPTED' };
  const candidateRef = one(o.run.artifacts, 'candidate.json'), compositionRef = one(o.run.artifacts, 'runtime-composition.json');
  if (!candidateRef || !compositionRef || !o.composition) return { code: 'COMPOSITION_PROOF_REQUIRED' };
  const candidate = await content(candidateRef, read), digest = runtimeCandidateDigest(candidate, candidateRef.digest);
  if (!digest) throw Error('DELIVERY_CANDIDATE_INVALID');
  const composition = JSON.parse((await content(compositionRef, read)).toString());
  if (deliveryDigest(composition) !== deliveryDigest(o.composition.payload) || composition.workspace !== o.consumer.execution.workspace ||
    composition.dryRun !== false || composition.complete !== true || !Array.isArray(composition.receipts)) return { code: 'COMPOSITION_PROOF_REQUIRED' };
  const receipt = composition.receipts.filter((r: any) => r.producerTaskId === row.producer_task_id);
  if (receipt.length !== 1 || !['applied', 'already_applied'].includes(receipt[0].status) ||
    receipt[0].patchDigest !== patch.digest || receipt[0].reportDigest !== report.digest ||
    receipt[0].inputs?.patch !== patch.id || receipt[0].inputs?.report !== report.id) return { code: 'COMPOSITION_REPAIR_NOT_CONSUMED' };
  const evidence: any = { code: 'CANDIDATE_PRODUCED', candidateRunId: o.run.id, candidateDigest: digest,
    candidateArtifactId: candidateRef.id, candidateArtifactDigest: candidateRef.digest,
    compositionArtifactId: compositionRef.id, compositionArtifactDigest: compositionRef.digest,
    compositionEventId: String(o.composition.id), consumerAcceptanceId: o.run.acceptance_id };
  if (!accepted(o.consumer, o.run)) return evidence;
  evidence.code = 'QA_PENDING';
  if (!o.qa) return { ...evidence, code: 'QA_NOT_PLANNED' };
  evidence.qaTaskId = o.qa.id;
  if (!accepted(o.qa, o.qaRun)) return evidence;
  if (!frozen(o.qa, o.qaRun) || !bound(o.qaInputs, candidateRef, o.consumer.id)) return { ...evidence, code: 'QA_CANDIDATE_UNBOUND' };
  const testedRef = one(o.qaRun.artifacts, 'candidate.json');
  if (!testedRef) return { ...evidence, code: 'QA_CANDIDATE_UNBOUND' };
  const testedBytes = await content(testedRef, read), tested = JSON.parse(testedBytes.toString());
  if (runtimeCandidateDigest(testedBytes, testedRef.digest) !== digest || tested.baseDigest !== digest)
    return { ...evidence, code: 'QA_CANDIDATE_CHANGED' };
  return { ...evidence, code: 'INDEPENDENT_QA_ACCEPTED', completed: true, resultKind: 'candidate_and_independent_qa',
    qaRunId: o.qaRun.id, qaAcceptanceId: o.qaRun.acceptance_id, qaCandidateArtifactId: testedRef.id, qaCandidateArtifactDigest: testedRef.digest };
}

async function update(db: DB, row: any, state: string, receipt: any) {
  const changed = await db.query(`UPDATE delivery_consumptions SET state=$2,receipt=$3::jsonb,updated_at=now(),
    candidate_run_id=$4,candidate_digest=$5,qa_task_id=$6,qa_run_id=$7
    WHERE id=$1 AND (state IS DISTINCT FROM $2 OR receipt IS DISTINCT FROM $3::jsonb) RETURNING id`,
    [row.id, state, JSON.stringify(receipt), receipt.candidateRunId ?? null, receipt.candidateDigest ?? null, receipt.qaTaskId ?? null, receipt.qaRunId ?? null]);
  if (changed.rowCount) await event(db, 'DELIVERY_CONSUMPTION_CHANGED', row.goal_id,
    { obligationId: row.id, producerTaskId: row.producer_task_id, consumerTaskId: row.consumer_task_id, state, ...receipt });
}

/** A bounded fair sweep, independent transactions per obligation. No remote
 * inspection or artifact reads occur while holding the global admission lock. */
export async function reconcileDeliveryConsumptions(inspect = inspectStopped, read: Reader = readArtifact) {
  // Enroll old applied repairs only from their original frozen consumer and
  // producer receipt. A current task is never passed off as an old snapshot.
  const legacy = (await pool.query(`SELECT h.id FROM deferred_handoffs h JOIN goals g ON g.id=h.goal_id
    JOIN runs original ON original.id::text=h.consumer_proof->>'runId' AND original.task_id=h.consumer_task_id
    JOIN runs producer ON producer.id::text=h.receipt->>'producerRunId' AND producer.task_id=h.producer_task_id
    LEFT JOIN delivery_consumptions d ON d.handoff_id=h.id WHERE d.id IS NULL AND h.state='applied'
    AND h.handoff->>'workKind'='repair' AND original.instruction_snapshot->>'schema'='1'
    AND producer.state='submitted' AND g.state='active' ORDER BY h.applied_at,h.id LIMIT 8`)).rows;
  for (const item of legacy) await tx(async db => {
    await lock(db);
    const h = (await db.query('SELECT * FROM deferred_handoffs WHERE id=$1 FOR UPDATE', [item.id])).rows[0];
    const r = h?.receipt?.producerRunId ? (await db.query('SELECT id,task_id,attempt,fence,state FROM runs WHERE id=$1', [h.receipt.producerRunId])).rows[0] : null;
    const oldRunId = h?.consumer_proof?.runId;
    const old = oldRunId ? (await db.query('SELECT instruction_snapshot->\'task\' task FROM runs WHERE id=$1 AND task_id=$2', [oldRunId, h.consumer_task_id])).rows[0]?.task : null;
    if (h?.state === 'applied' && r && old) await registerDeliveryConsumption(db, h, old, r);
  });
  const rows = await selectDeliveryConsumptionBatch(pool);
  const inspections = new Map<string, Promise<any>>();
  const inspectOnce = (run: any, task: any) => {
    if (!inspections.has(run.id)) inspections.set(run.id, inspect(run.id, task.execution.kind === 'opencode'));
    return inspections.get(run.id)!;
  };
  for (const row of rows) {
    const owner = await pool.connect(); let owned = false;
    try {
      owned = (await owner.query('SELECT pg_try_advisory_lock(hashtextextended($1,4280098)) owned', [row.id])).rows[0].owned;
      if (!owned) continue;
      await owner.query('UPDATE delivery_consumptions SET checked_at=clock_timestamp() WHERE id=$1', [row.id]);
      const observed = await observation(pool, row), stamp = deliveryDigest(observed);
      const observedSource=effectiveDeliverySource(row,observed.sourceResolution);
      const observedConflict=deliverySourceConflict(observedSource,observed.sourceIdentity,observed.producerRun?.artifacts??[]);
      let verified: any = {}, proof: any, qaProof: any;
      if (observed.consumer && observed.producer && !observed.consumer.live && !observed.consumer.leased) {
        try { verified = await verifiedDelivery(observedSource, observed, read); }
        catch (e: any) { verified = { code: /^[A-Z_]+$/.test(e.message ?? '') ? e.message : 'DELIVERY_PROOF_INVALID' }; }
        if (observed.run && !observed.run.cancelled && !observed.pendingCases.some(c => c.task_id === observed.consumer.id) &&
          ['failed', 'submitted'].includes(observed.run.state) && !accepted(observed.consumer, observed.run) &&
          (observedConflict || blockedDependencies(observed.dependencies) || (!observed.consumerAdmission &&
            !await appliedSupervisionOwnsContinuation(pool, observed.consumer.id, observed.run.id)))) {
          try { proof = await inspectOnce(observed.run, observed.consumer); } catch { /* No successor without a current stop proof. */ }
        }
        if (observed.qaRun && !observed.qaRun.cancelled && !observed.qa.live && !observed.qa.leased && !accepted(observed.qa, observed.qaRun) &&
          !observed.pendingCases.some(c => c.task_id === observed.qa.id) && ['failed', 'submitted'].includes(observed.qaRun.state)) {
          try { qaProof = await inspectOnce(observed.qaRun, observed.qa); } catch { /* Preserve QA and its prior decisions. */ }
        }
      }
      await tx(async db => {
        await lock(db);
        const current = (await db.query('SELECT * FROM delivery_consumptions WHERE id=$1 FOR UPDATE', [row.id])).rows[0];
        if (!current || ['completed', 'superseded'].includes(current.state) || deliveryDigest(current.binding) !== deliveryDigest(row.binding)) return;
        const o = await observation(db, row);
        if (deliveryDigest(o) !== stamp) return;
        const { goal, consumer, producer, run, producerRun } = o;
        const source=effectiveDeliverySource(row,o.sourceResolution);
        const sourceConflict=deliverySourceConflict(source,o.sourceIdentity,producerRun?.artifacts??[]);
        if (!goal || goal.state !== 'active' || goal.autonomy === 'false' || !consumer || !producer ||
          (consumer.recovery?.mode === 'manual' && goal.autonomy !== 'true')) return;
        const generation = deliveryDigest({ producerRunId: source.producer_run_id, producerManifestDigest: source.producer_manifest_digest,
          consumerRunId: run?.id ?? null, contract: contract(consumer), candidateDigest: verified.candidateDigest ?? null });
        const receipt = (code: string, owner: string, nextAction: string, extra: any = {}) => ({
          ...(o.sourceResolution?{sourceResolution:{eventId:o.sourceResolution.eventId,caseId:o.sourceResolution.caseId,
            decisionRunId:o.sourceResolution.decisionRunId,original:o.sourceResolution.original,
            replacementTaskId:source.producer_task_id,replacementRunId:source.producer_run_id}}:{}),
          ...extra, code, owner, nextAction, generation });
        if (['cancelled', 'superseded'].includes(consumer.state) || run?.cancelled)
          return update(db, row, 'superseded', receipt('CONSUMER_CLOSED', 'supervisor', 'Preservar histórico; nenhuma retomada automática.'));
        if (contract(consumer) !== row.consumer_contract_digest || consumer.goal_id !== row.goal_id || producer.goal_id !== row.goal_id)
          return update(db, row, 'blocked', receipt('CONSUMER_CONTRACT_CHANGED', 'supervisor', 'Decidir o destino da entrega sem alterar a prova histórica.'));
        if (sourceConflict) {
          const extra={sourceConflict,consumerRunId:run?.id??null,...o.sourceFollowthrough};
          if(consumer.live||consumer.leased) return update(db,row,'blocked',receipt('SOURCE_REVIEW_WAIT_CONSUMER','consumer',
            'Aguardar o encerramento deste consumidor; a divergência de fonte não autoriza interromper uma execução.',extra));
          if(!run||!['failed','submitted'].includes(run.state)||accepted(consumer,run))
            return update(db,row,'blocked',receipt('SOURCE_REVIEW_TARGET_UNAVAILABLE','planner',
              'Selecionar um consumidor parado elegível; preserve qualquer entrega já aceita.',extra));
          if(!proof&&!o.pendingCases.some(c=>c.task_id===consumer.id))
            return update(db,row,'waiting_supervision',receipt('STOP_PROOF_REQUIRED','supervisor','Confirmar encerramento para revisar a fonte.',extra));
          if(proof)try{assertStopped(proof,run,consumer);}catch{
            return update(db,row,'waiting_supervision',receipt('STOP_PROOF_REQUIRED','supervisor','Revalidar encerramento para revisar a fonte.',extra));}
          const requested=await requestDeliverySupervision(db,goal,consumer,run,{
            kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationKey:'delivery-consumption:'+row.id,
            generation:o.sourceFollowthrough.recertificationSources.length||o.sourceFollowthrough.fulfilledOtherObligation?
              deliveryDigest({sourceConflict:sourceConflict.generation,...o.sourceFollowthrough}):sourceConflict.generation,
            code:sourceConflict.code,sourceConflict,...o.sourceFollowthrough,
            obligationId:row.id,handoffId:row.handoff_id,producerTaskId:source.producer_task_id,
            producerRunId:source.producer_run_id,consumerTaskId:consumer.id,consumerRunId:run.id,
            criteriaUnchanged:true,instruction:'Decida compatibilidade sem reescrever o aceite histórico. Use revise com sourceTaskId de reparo aceito/frozen e deliveryResolution {obligationId,generation,expectedSourceRunId}. Se não existe fonte comprovada, delegue recertificação/reparo focal a partir de evidência aceita; a resolução não é composição nem QA.',
          },{stopProof:proof});
          return update(db,row,'waiting_supervision',receipt(requested.code,'supervisor',
            'Resolver a fonte por decisão aplicada e vínculo explícito; depois integrar e validar a candidata.',{...extra,...requested}));
        }
        if (!o.dependencies.some(d => d.id === producer.id))
          return update(db, row, 'blocked', receipt('CONSUMPTION_DEPENDENCY_CHANGED', 'supervisor', 'Revisar vínculo do consumidor, preservando critérios e grafo.'));
        if (consumer.live || consumer.leased)
          return update(db, row, 'executing', receipt(consumer.live ? 'CONSUMER_ACTIVE' : 'CONSUMER_STOP_UNCONFIRMED', 'consumer', 'Aguardar resultado e encerramento comprovado.', { consumerRunId: run?.id }));
        if (verified.completed) return update(db, row, 'completed', receipt(verified.code, 'verification', 'Entrega consumida com recibos vinculados.', verified));
        const blocked = o.dependencies.filter(d => d.state !== 'accepted');
        const semanticBlock = blockedDependencies(blocked);
        if (blocked.length && (!semanticBlock || !run || !['failed', 'submitted'].includes(run.state))) return update(db, row, 'waiting_dependencies', receipt('CONSUMER_DEPENDENCIES_PENDING', 'scheduler',
          'Concluir as dependências existentes; nenhuma frente duplicada.', { blockedTaskIds: blocked.map(d => d.id) }));
        const target = accepted(consumer, run) && o.qa ? o.qa : consumer;
        const targetRun = target.id === consumer.id ? run : o.qaRun;
        const stop = target.id === consumer.id ? proof : qaProof;
        if (targetRun && ['failed', 'submitted'].includes(targetRun.state) && !accepted(target, targetRun) && !target.live && !target.leased) {
          if (!semanticBlock && target.id === consumer.id && o.consumerAdmission)
            return update(db, row, 'attached', receipt('SUPERVISION_ADMISSION_PENDING', 'scheduler',
              'Aguardar a admissão da retomada já autorizada; a falha anterior não é um novo resultado.',
              { caseId: o.consumerAdmission.case_id, sourceRunId: o.consumerAdmission.source_run_id, sourceAttempt: o.consumerAdmission.attempt }));
          if (!semanticBlock && await appliedSupervisionOwnsContinuation(db, target.id, targetRun.id))
            return update(db, row, 'waiting_supervision', receipt('SUPERVISION_RESUME_OWNED', 'supervisor', 'A decisão existente deve retomar com os insumos atuais.', verified));
          if (!stop && !o.pendingCases.some(c => c.task_id === target.id)) return update(db, row, 'waiting_supervision', receipt('STOP_PROOF_REQUIRED', 'supervisor', 'Confirmar encerramento antes de solicitar decisão sucessora.', verified));
          if (stop) try { assertStopped(stop, targetRun, target); } catch {
            return update(db, row, 'waiting_supervision', receipt('STOP_PROOF_REQUIRED', 'supervisor', 'Revalidar encerramento; a observação anterior não permite agir.', verified));
          }
          const requested = await requestDeliverySupervision(db, goal, target, targetRun, {
            kind: 'DELIVERY_CONSUMPTION_REQUIRED', obligationKey: 'delivery-consumption:' + row.id, generation,
            code: verified.code ?? 'ACCEPTED_REPAIR_PENDING', handoffId: row.handoff_id, producerTaskId: producer.id,
            producerRunId: source.producer_run_id, producerManifestDigest: source.producer_manifest_digest,
            repairArtifacts: source.binding.repairArtifacts, consumerTaskId: consumer.id, consumerRunId: run?.id,
            blockedTaskIds: blocked.map(d => d.id),
            criteriaUnchanged: true, instruction: 'Encaminhe o consumo desta correção aceita com a ação existente de revisão. Preserve as provas e os critérios; não delegue outro diagnóstico equivalente.',
          }, { stopProof: stop });
          return update(db, row, 'waiting_supervision', receipt(requested.code, 'supervisor', 'Aplicar decisão competente e retomar pelos guards existentes.',
            { ...verified, ...requested, ...(blocked.length ? { blockedTaskIds: blocked.map(d => d.id) } : {}) }));
        }
        if (verified.candidateDigest) return update(db, row, accepted(consumer, run) ? 'qa_pending' : 'composed',
          receipt(verified.code, verified.code === 'QA_NOT_PLANNED' ? 'planner' : 'verification',
            'Executar a validação independente da candidata exata; preserve todos os critérios.', verified));
        return update(db, row, accepted(consumer, run) ? 'blocked' : 'attached', receipt(verified.code ?? 'CONSUMER_PENDING',
          accepted(consumer, run) ? 'supervisor' : 'scheduler', 'Aguardar o consumidor elegível; recibo de anexo não comprova consumo.'));
      });
    } finally {
      if (owned) await owner.query('SELECT pg_advisory_unlock(hashtextextended($1,4280098))', [row.id]).catch(() => {});
      owner.release();
    }
  }
}
