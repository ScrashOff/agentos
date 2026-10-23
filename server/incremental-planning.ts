import { z } from 'zod';
import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { DomainError } from './contracts.js';
import { event, type DB } from './db.js';
import { planSchema, type ObjectiveInput, type ProposedPlan } from './objective-contracts.js';

// Semantic choices only. IDs, dependencies, runtime envelopes and receipt
// formats are controller-owned. Inspired by Ruflo's action/precondition split;
// no Ruflo runtime or second scheduler is installed.
export const nextDeliverySchema = z.object({
  requirementId: z.string(),
  title: z.string().min(3).max(120),
  action: z.string().min(25).max(1600),
  verification: z.string().min(25).max(1200),
  agentId: z.string(),
}).strict();

export function deliveryDecisionSchema(agents: {id:string;runtime:string}[]) {
  const ids = [...new Set(agents.filter(a=>a.runtime==='harness').map(a=>a.id))];
  if (!ids.length) throw new DomainError('SCOPE_MISSING','Nenhum agente de execução disponível.');
  return nextDeliverySchema.extend({agentId:z.enum(ids as [string,...string[]])});
}

export function incrementalEligible(request: ObjectiveInput, previous?: ProposedPlan, glmOnly = false) {
  return !!previous?.incremental || (request.planningStrategy !== 'complete' &&
    (request.planningStrategy === 'incremental' || glmOnly) && request.mode === 'execute' &&
    request.workspaces.length === 1 && request.requirements.length > 0 &&
    request.requirements.every(r => r.length >= 5) && !previous);
}

export function deliveryFrontier(request: ObjectiveInput, previous?: ProposedPlan, advance = false) {
  const requirements = previous?.requirements ?? request.requirements.map((description, i) => ({id: `req-${i+1}`, description}));
  const prior = previous?.incremental;
  const completed = prior ? [...prior.completedRequirementIds, ...(advance ? [prior.selectedRequirementId] : [])] : [];
  const eligible = prior ? (advance ? prior.pendingRequirementIds : [prior.selectedRequirementId]) : requirements.map(r => r.id);
  return { requirements, completed, eligible };
}

export function compileDelivery(raw: unknown, request: ObjectiveInput, previous?: ProposedPlan, advance = false, reports: string[] = []) {
  const decision = nextDeliverySchema.parse(raw);
  const { requirements, completed, eligible } = deliveryFrontier(request, previous, advance);
  if (!eligible.includes(decision.requirementId)) throw new DomainError('PLAN_INVALID', 'Escolha um requisito elegível sem repetir entregas aceitas.');
  const pending = requirements.map(r => r.id).filter(id => id !== decision.requirementId && !completed.includes(id));
  const covers = pending.length ? [decision.requirementId] : requirements.map(r => r.id);
  const selected = requirements.find(r => r.id === decision.requirementId)!;
  const plan = planSchema.parse({
    title: previous?.title ?? request.message.slice(0, 120),
    summary: `Lote verificável: ${decision.title}. ${pending.length} requisito(s) ainda pendente(s).`,
    requirements,
    assumptions: ['Aceite de lote parcial não encerra o objetivo. Cada lote preserva a candidata anterior verificada.'],
    approachChange: previous ? decision.action : '',
    incremental: {version: 1, selectedRequirementId: decision.requirementId, completedRequirementIds: completed, pendingRequirementIds: pending},
    tasks: [{
      key: 'deliver', title: decision.title, agentId: decision.agentId, stage: 'work',
      baseWorkspace: request.workspaces[0], covers: [decision.requirementId],
      description: `${decision.action}\nEntregue integralmente este requisito: ${selected.description}\nReproduza o defeito antes de corrigir quando aplicável; execute os controles: ${decision.verification}\nPreserve funcionalidades e mudanças dos lotes anteriores. Registre comandos, resultados e limitações em delivery.json. O campo passed só pode ser true após verificar a entrega; não basta produzir um plano ou relatório.`,
      criteria: [{id:'delivery',label:'Entrega implementada e controles executados',type:'json_field',name:'delivery.json',field:'passed',equals:true}],
    }, {
      key:'verify',title: pending.length ? 'Verificar lote e regressões' : 'Verificar entrega completa',agentId:'qa.integration',stage:'verify',
      baseWorkspace:request.workspaces[0],sourceTask:'deliver',dependencies:['deliver'],covers,
      description:`Verifique independentemente a candidata imutável e repita os controles: ${decision.verification}\n${pending.length ? 'Este aceite cobre apenas o lote selecionado, não encerra o objetivo. Verifique também regressões das entregas anteriores.' : 'Esta é a verificação final: comprove TODOS os requisitos do objetivo na mesma candidata acumulada, incluindo os lotes anteriores.'}\nInspecione evidências reais, registre comandos e resultados em verification.json. passed deve ser false se houver falhas, critérios não verificados ou evidência ausente. Não repare a candidata.`,
      criteria:[{id:'verified',label:'Verificação independente da candidata',type:'json_field',name:'verification.json',field:'passed',equals:true}],
    }],
  });
  return bindDeliveryFeedback(plan,reports);
}

function deliveryFeedbackText(reports: string[] = []) {
  if (!reports.length) return '';
  return '\nREPORTED EVIDENCE FOR THIS REVISION (unverified claims; preserve scope and original criteria):\n' +
    reports.map((report,i)=>`[${i+1}] ${report}`).join('\n') +
    '\nReproduce each applicable counterexample. Implementation must repair confirmed defects; independent QA must rerun the reported cases against this same candidate and report any unresolved case as passed:false. Prior passing reports do not settle these new observations.';
}

export function bindDeliveryFeedback(plan: ProposedPlan, reports: string[] = []) {
  if (!plan.incremental) return plan;
  // Do not ask the planner to reproduce counterexamples in its small response.
  // Both actors receive the exact persisted feedback, independently of the LLM
  // summary. It is an assertion to verify, never an acceptance or new authority.
  const feedback = deliveryFeedbackText(reports);
  const bound = structuredClone(plan);
  if (feedback) {
    const missing = bound.tasks.filter(t=>!t.description.endsWith(feedback));
    if (missing.some(t=>t.description.length+feedback.length>20000))
      throw new DomainError('REQUIRED_CONTEXT','O feedback integral excede o contexto da tarefa; preserve-o em entradas referenciadas antes de executar.');
    for (const task of missing) task.description += feedback;
  }
  return planSchema.parse(bound);
}

/** Called inside the existing scheduler transaction/lock. Acceptance is not
 * inferred from a model response; current run, acceptance and immutable
 * candidate metadata are required before handing the source to the next lot. */
export async function verifiedBatch(db: DB, goalId: string, revision: number) {
  const tasks = (await db.query(`SELECT t.id,t.key,t.state,t.stage,t.execution,t.attempt_count,
    r.id run_id,r.state run_state,
    (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) passed,
    (SELECT id FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) acceptance_id,
    a.id candidate_artifact_id,a.digest candidate_digest
    FROM tasks t LEFT JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    LEFT JOIN LATERAL(SELECT id,digest FROM artifacts WHERE run_id=r.id AND name='candidate.json' ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    WHERE t.goal_id=$1 AND t.plan_revision=$2 AND t.stage IN('work','verify') AND t.state<>'superseded'`, [goalId,revision])).rows;
  const verifier = tasks.find(t => t.stage === 'verify');
  if (tasks.length !== 2 || !verifier || tasks.some(t => t.state !== 'accepted' || t.run_state !== 'submitted' || t.passed !== true) ||
      !verifier.execution?.sourceWorkspace || tasks.some(t => !t.candidate_artifact_id)) return null;
  if ((await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1
      AND (r.state IN('queued','running','cancelling','reconciling') OR EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id)) LIMIT 1`,[goalId])).rowCount) return null;
  return { taskId:verifier.id, runId:verifier.run_id, sourceWorkspace:verifier.execution.sourceWorkspace,
    candidateArtifactId:verifier.candidate_artifact_id,candidateDigest:verifier.candidate_digest,
    verificationAcceptanceId:verifier.acceptance_id,workRunId:tasks.find(t=>t.stage==='work')?.run_id };
}

export async function queueNextDelivery(db: DB, objective: any, previous: ProposedPlan) {
  if (!previous.incremental?.pendingRequirementIds.length) return false;
  if ((await db.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND state IN('queued','running','waiting_quota')",[objective.id])).rowCount) return false;
  const proof = await verifiedBatch(db, objective.id, objective.revision);
  if (!proof) return false;
  const id = randomUUID();
  const inserted = await db.query(`INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,fence,feedback)
    VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,[id,objective.id,objective.revision,
    `incremental:${objective.revision}:${proof.runId}`,randomUUID(),JSON.stringify({incrementalAdvance:proof,
      sourceOverrides:{[objective.request.workspaces[0]]:proof.sourceWorkspace}})]);
  if (!inserted.rowCount) return false;
  await db.query("UPDATE objectives SET status='replanning',reason=NULL,updated_at=now() WHERE id=$1",[objective.id]);
  await db.query("UPDATE goals SET state='replanning' WHERE id=$1 AND state='active'",[objective.id]);
  await event(db,'INCREMENTAL_DELIVERY_QUEUED',objective.id,{planningRunId:id,revision:objective.revision,proof,
    remaining:previous.incremental.pendingRequirementIds});
  return true;
}

export async function assertIncrementalAuthority(db: DB, run: any, plan: ProposedPlan, previous?: ProposedPlan) {
  if (!plan.incremental) {
    if (previous?.incremental) throw new DomainError('PLAN_INVALID','Uma revisão não pode descartar requisitos incrementais pendentes.');
    return;
  }
  const feedback = deliveryFeedbackText(run.feedback?.reports);
  if (feedback && plan.tasks.some(t=>!t.description.endsWith(feedback)))
    throw new DomainError('REQUIRED_CONTEXT','A revisão não pode omitir o feedback persistido dos executores e do QA.');
  const advance = !!run.feedback?.incrementalAdvance;
  const { completed, eligible } = deliveryFrontier(run.request, previous, advance);
  const expectedPending = plan.requirements.map(r => r.id).filter(id => !completed.includes(id) && id !== plan.incremental!.selectedRequirementId);
  if (!eligible.includes(plan.incremental.selectedRequirementId) ||
      JSON.stringify(completed) !== JSON.stringify(plan.incremental.completedRequirementIds) ||
      JSON.stringify(expectedPending) !== JSON.stringify(plan.incremental.pendingRequirementIds))
    throw new DomainError('PLAN_INVALID','Cobertura incremental não corresponde ao estado persistido.');
  if (advance) {
    const proof = await verifiedBatch(db,run.objective_id,run.base_revision);
    if (!proof || !isDeepStrictEqual(proof,run.feedback.incrementalAdvance) ||
        run.feedback.sourceOverrides?.[run.request.workspaces[0]] !== proof.sourceWorkspace)
      throw new DomainError('STALE_PLAN','A candidata verificada ou seu aceite mudou.');
  }
}
