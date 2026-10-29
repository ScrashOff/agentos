import { z } from 'zod';
import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { DomainError } from './contracts.js';
import { event, type DB } from './db.js';
import { planSchema, type ObjectiveInput, type ProposedPlan } from './objective-contracts.js';
import {isAttemptLocalWriteScope} from './attempt-local-write-scope.js';

// Semantic choices only. IDs, dependencies, runtime envelopes and receipt
// formats are controller-owned. Inspired by Ruflo's action/precondition split;
// no Ruflo runtime or second scheduler is installed.
const writePathItemSchema = z
  .string()
  .min(1)
  .max(200)
  .regex(/^(?![/\\])(?!.*\.\.)(?!.*\\)(?!\.git(?:\/|$))(?!node_modules(?:\/|$))(?!assets(?:\/|$))[a-zA-Z0-9._/-]+$/)
  .refine(p=>!p.includes('//')&&!p.split('/').includes('.'),'Use caminhos relativos sem aliases.');

const singleDeliveryFields = {
  key: z.string().regex(/^[a-z][a-z0-9_-]{0,39}$/).optional(),
  dependsOn: z.array(z.string().regex(/^[a-z][a-z0-9_-]{0,39}$/)).max(6).optional(),
  requirementId: z.string(),
  title: z.string().min(3).max(120),
  action: z.string().min(25).max(1600),
  verification: z.string().min(25).max(1200),
  agentId: z.string(),
  writePaths: z.array(writePathItemSchema).max(20).optional(),
};

export const nextDeliveryItemSchema = z.object(singleDeliveryFields).strict();

export const nextDeliverySchema = z.object({
  key: singleDeliveryFields.key,
  dependsOn: singleDeliveryFields.dependsOn,
  requirementId: z.string().optional(),
  title: z.string().min(3).max(120).optional(),
  action: z.string().min(25).max(1600).optional(),
  verification: z.string().min(25).max(1200).optional(),
  agentId: z.string().optional(),
  writePaths: z.array(writePathItemSchema).max(20).optional(),
  deliveries: z.array(nextDeliveryItemSchema).min(1).max(6).optional(),
}).strict();

export function deliveryDecisionSchema(agents: {id:string;runtime:string}[],eligible?:string[],serial=false) {
  const ids = [...new Set(agents.filter(a=>a.runtime==='harness'&&a.id!=='qa.integration').map(a=>a.id))];
  if (!ids.length) throw new DomainError('SCOPE_MISSING','Nenhum agente de execução disponível.');
  const agentEnum = z.enum(ids as [string,...string[]]);
  const requirementId=eligible?.length?z.enum(eligible as [string,...string[]]):z.string();
  const item=nextDeliveryItemSchema.extend({agentId:agentEnum,requirementId});
  if(serial)return item;
  return nextDeliverySchema.extend({
    requirementId:requirementId.optional(),
    agentId: agentEnum.optional(),
    deliveries: z.array(item).min(1).max(6).optional(),
  });
}

function invalid(violation:string,message:string,detail:Record<string,unknown>={}):never{
  throw Object.assign(new DomainError('PLAN_INVALID',message),{detail:{violation,...detail}});
}

export function selectedRequirementIds(incremental?: ProposedPlan['incremental']): string[] {
  if (!incremental) return [];
  return incremental.selectedRequirementIds?.length ? incremental.selectedRequirementIds : [incremental.selectedRequirementId];
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
  const priorSelected = selectedRequirementIds(prior);
  const completed = prior ? [...prior.completedRequirementIds, ...(advance ? priorSelected : [])] : [];
  const eligible = prior ? (advance ? prior.pendingRequirementIds : priorSelected) : requirements.map(r => r.id);
  return { requirements, completed, eligible };
}

export function compileDelivery(raw: unknown, request: ObjectiveInput, previous?: ProposedPlan, advance = false, reports: string[] = []) {
  const parsed = nextDeliverySchema.parse(raw);
  if(parsed.deliveries&&Object.keys(singleDeliveryFields).some(key=>(parsed as any)[key]!==undefined))
    invalid('AMBIGUOUS_DECISION','Use uma entrega única ou deliveries; não misture os dois formatos.');
  const items = parsed.deliveries?.length ? parsed.deliveries : [nextDeliveryItemSchema.parse(raw)];
  const { requirements, completed, eligible } = deliveryFrontier(request, previous, advance);
  const selectedIds = [...new Set(items.map(i => i.requirementId))];
  if (selectedIds.some(id => !eligible.includes(id)))
    invalid('INELIGIBLE_REQUIREMENT','Selecione somente requisitos elegíveis.',{eligible,received:selectedIds});
  if(items.some(i=>i.agentId==='qa.integration'))
    invalid('VALIDATOR_AS_WORKER','O QA independente é criado pelo controlador; selecione um implementador.');
  const keyed=items.map((item,index)=>({...item,partKey:item.key??`part-${index+1}`,index}));
  const byKey=new Map(keyed.map(item=>[item.partKey,item]));
  if(byKey.size!==keyed.length)invalid('DUPLICATE_TASK_KEY','Cada subtarefa precisa de uma chave única.');
  const visiting=new Set<string>(),ancestors=new Map<string,Set<string>>(),ordered:typeof keyed=[];
  function visit(key:string):Set<string>{
    if(ancestors.has(key))return ancestors.get(key)!;
    if(visiting.has(key))invalid('DEPENDENCY_CYCLE','Dependências entre subtarefas não podem formar ciclos.',{key});
    const item=byKey.get(key);
    if(!item)invalid('UNKNOWN_DEPENDENCY','A dependência precisa nomear uma subtarefa deste lote.',{key});
    visiting.add(key);const all=new Set<string>();
    if(new Set(item.dependsOn??[]).size!==(item.dependsOn??[]).length)
      invalid('DUPLICATE_DEPENDENCY','Não repita uma dependência.',{key});
    for(const dependency of item.dependsOn??[]){all.add(dependency);for(const parent of visit(dependency))all.add(parent);}
    visiting.delete(key);ancestors.set(key,all);ordered.push(item);return all;
  }
  for(const item of keyed)visit(item.partKey);
  const pending = requirements.map(r => r.id).filter(id => !selectedIds.includes(id) && !completed.includes(id));
  const covers = pending.length ? selectedIds : requirements.map(r => r.id);
  if (items.length === 1) {
    const decision = items[0];
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
        ...(decision.writePaths?.length ? { writePaths: decision.writePaths } : {}),
        description: `${decision.action}\nEntregue integralmente este requisito: ${selected.description}\nReproduza o defeito antes de corrigir quando aplicável; execute os controles: ${decision.verification}\nPreserve funcionalidades e mudanças dos lotes anteriores. Trabalhe de forma incremental e verificavel: escreva primeiro um esqueleto minimo dos arquivos de entrega e sele evidencias a cada etapa concluida, em vez de auditar a arvore inteira antes da primeira escrita. Limite a exploracao aos arquivos do contrato e das dependencias diretas. Nao crie bancos, containers ou dados de runtime locais (nada de pgdata, docker ou DATABASE_URL inventada): validacao com PostgreSQL e somente pelo request_validation protegido; sem ele, registre a limitacao em evidence/. Registre comandos, resultados e limitações em evidence/delivery.json. O campo passed só pode ser true após verificar a entrega; não basta produzir um plano ou relatório.`,
        criteria: [{id:'delivery',label:'Entrega implementada e controles executados',type:'json_field',name:'delivery.json',field:'passed',equals:true}],
      }, {
        key:'verify',title: pending.length ? 'Verificar lote e regressões' : 'Verificar entrega completa',agentId:'qa.integration',stage:'verify',
        baseWorkspace:request.workspaces[0],sourceTask:'deliver',dependencies:['deliver'],covers,
        description:`Verifique independentemente a candidata imutável e repita os controles: ${decision.verification}\n${pending.length ? 'Este aceite cobre apenas o lote selecionado, não encerra o objetivo. Verifique também regressões das entregas anteriores.' : 'Esta é a verificação final: comprove TODOS os requisitos do objetivo na mesma candidata acumulada, incluindo os lotes anteriores.'}\nInspecione evidências reais, registre comandos e resultados em evidence/verification.json. passed deve ser false se houver falhas, critérios não verificados ou evidência ausente. Não repare a candidata.`,
        criteria:[{id:'verified',label:'Verificação independente da candidata',type:'json_field',name:'verification.json',field:'passed',equals:true}],
      }],
    });
    return bindDeliveryFeedback(plan,reports);
  }
  const seenPaths: { idx: number; path: string }[] = [];
  for (const [idx, item] of items.entries()) {
    if (!item.writePaths?.length)
      invalid('WRITE_SCOPE_REQUIRED','Subtarefas no mesmo lote exigem writePaths explícitos.',{key:item.key??`part-${idx+1}`});
    for (const rawPath of item.writePaths) {
      if(isAttemptLocalWriteScope(rawPath))continue;
      const norm = rawPath.replace(/\/+$/, '');
      if (seenPaths.some(other => other.idx !== idx && (other.path === norm || other.path.startsWith(norm + '/') || norm.startsWith(other.path + '/'))))
        invalid('WRITE_SCOPE_CONFLICT','Subtarefas precisam de escopos de escrita disjuntos; agrupe escrita compartilhada na mesma tarefa.',{path:rawPath,key:item.key??`part-${idx+1}`});
      seenPaths.push({ idx, path: norm });
    }
  }
  const taskKey=(key:string)=>`deliver-${byKey.get(key)!.index+1}`;
  const joins:z.input<typeof planSchema>['tasks']=[];
  const workTasks = ordered.map((item) => {
    const idx=item.index;
    const selected = requirements.find(r => r.id === item.requirementId)!;
    const dependencies=(item.dependsOn??[]).map(taskKey);
    let sourceTask=dependencies[0];
    if(dependencies.length>1){
      sourceTask=`join-${idx+1}`;
      const parents=ordered.filter(p=>ancestors.get(item.partKey)!.has(p.partKey));
      joins.push({key:sourceTask,title:`Preparar insumos de ${item.title}`.slice(0,200),
        agentId:'engineering.integration',stage:'integrate',baseWorkspace:request.workspaces[0],
        dependencies:parents.map(p=>taskKey(p.partKey)),covers:[...new Set(parents.map(p=>p.requirementId))],
        writePaths:[...new Set(parents.flatMap(p=>p.writePaths??[]))],
        description:`Integre os patches canônicos das dependências ${parents.map(p=>taskKey(p.partKey)).join(', ')} na ordem indicada sobre a base comum. Preserve todas as pré-imagens e os lotes anteriores. Esta preparação fornece insumos à subtarefa ${item.partKey}; não conclui nenhum requisito. Execute os controles das dependências: ${parents.map(p=>p.verification).join(' | ')}. Registre evidence/delivery.json, passed:true somente se todos passarem.`,
        criteria:[{id:'joined',label:'Insumos integrados e verificados',type:'json_field',name:'delivery.json',field:'passed',equals:true}]});
    }
    const shared=items.filter(i=>i.requirementId===item.requirementId).length>1;
    return {
      key: `deliver-${idx + 1}`,
      title: item.title,
      agentId: item.agentId,
      stage: 'work' as const,
      ...(sourceTask?{dependencies:[sourceTask],sourceTask}:{dependencies:[]}),
      baseWorkspace: request.workspaces[0],
      covers: [item.requirementId],
      writePaths: item.writePaths!,
      description: `${item.action}\n${shared?'Entregue a contribuição delimitada desta subtarefa; a integração e o QA deverão comprovar o requisito inteiro':'Entregue integralmente este requisito'}: ${selected.description}\nIdentidade da subtarefa: ${item.partKey}. Dependências: ${(item.dependsOn??[]).join(', ')||'nenhuma'}.\nEscopo exclusivo de escrita desta subtarefa: ${item.writePaths!.join(', ')}\nReproduza o defeito antes de corrigir quando aplicável; execute os controles: ${item.verification}\nPreserve funcionalidades e mudanças dos lotes anteriores. Trabalhe de forma incremental e verificavel: escreva primeiro um esqueleto minimo dos arquivos de entrega e sele evidencias a cada etapa concluida, em vez de auditar a arvore inteira antes da primeira escrita. Limite a exploracao aos arquivos do contrato e das dependencias diretas. Nao crie bancos, containers ou dados de runtime locais (nada de pgdata, docker ou DATABASE_URL inventada): validacao com PostgreSQL e somente pelo request_validation protegido; sem ele, registre a limitacao em evidence/. Registre comandos, resultados e limitações em evidence/delivery.json. O campo passed só pode ser true após verificar a entrega; não basta produzir um plano ou relatório.`,
      criteria: [{id:'delivery',label:'Entrega implementada e controles executados',type:'json_field' as const,name:'delivery.json',field:'passed',equals:true}],
    };
  });
  const combinedVerification = items.map(i => `[${i.requirementId}] ${i.verification}`).join(' | ');
  const combinedWritePaths = [...new Set(items.flatMap(i => i.writePaths ?? []))];
  const plan = planSchema.parse({
    title: previous?.title ?? request.message.slice(0, 120),
    summary: `Janela paralela verificável (${items.length} entregas): ${items.map(i => i.title).join('; ')}. ${pending.length} requisito(s) ainda pendente(s).`,
    requirements,
    assumptions: ['Aceite de lote parcial não encerra o objetivo. Entregas paralelas têm escopos de fonte disjuntos, integração explícita e QA independente. evidence/ e data/ pertencem a cada workspace isolado; consumidores precisam de artefatos publicados, não de diretórios locais implícitos.'],
    approachChange: previous ? items.map(i => i.action).join(' | ').slice(0, 4000) : '',
    incremental: {
      version: 1,
      selectedRequirementId: selectedIds[0],
      selectedRequirementIds: selectedIds,
      completedRequirementIds: completed,
      pendingRequirementIds: pending,
    },
    tasks: [
      ...joins,...workTasks,
      {
        key: 'integrate',
        title: 'Integrar entregas paralelas do lote',
        agentId: 'engineering.integration',
        stage: 'integrate',
        baseWorkspace: request.workspaces[0],
        dependencies: workTasks.map(w => w.key),
        covers: selectedIds,
        writePaths: combinedWritePaths,
        description: `Aplique os patches canônicos (changes.patch em /home/agent/inputs) das tarefas aceitas deste lote (${workTasks.map(w => w.key).join(', ')}) sobre a base comum na ordem indicada sem omitir nenhuma alteração. Cada patch de uma subtarefa dependente é o delta da sua própria fonte já integrada; não substitua a base por uma branch parcial.\nRequisitos completos que o conjunto deve satisfazer: ${JSON.stringify(requirements.filter(r=>selectedIds.includes(r.id)))}\nExecute os controles combinados: ${combinedVerification}\nPreserve todas as entregas dos lotes anteriores e registre o resultado real da integração em evidence/delivery.json com passed:true somente após comprovar os requisitos inteiros e todos os controles combinados.`,
        criteria: [{id:'integrated',label:'Patches integrados e controles combinados executados',type:'json_field',name:'delivery.json',field:'passed',equals:true}],
      },
      {
        key: 'verify',
        title: pending.length ? 'Verificar lote integrado e regressões' : 'Verificar entrega completa',
        agentId: 'qa.integration',
        stage: 'verify',
        baseWorkspace: request.workspaces[0],
        sourceTask: 'integrate',
        dependencies: ['integrate'],
        covers,
        description: `Verifique independentemente a candidata integrada imutável e repita os controles combinados: ${combinedVerification}\nComprove os requisitos COMPLETOS: ${JSON.stringify(requirements.filter(r=>selectedIds.includes(r.id)))}. Aceites parciais das subtarefas não comprovam a entrega conjunta.\n${pending.length ? 'Este aceite cobre apenas a janela selecionada, não encerra o objetivo. Verifique também regressões das entregas anteriores.' : 'Esta é a verificação final: comprove TODOS os requisitos do objetivo na mesma candidata acumulada, incluindo os lotes anteriores.'}\nInspecione evidências reais, registre comandos e resultados em evidence/verification.json. passed deve ser false se houver falhas, critérios não verificados ou evidência ausente. Não repare a candidata.`,
        criteria: [{id:'verified',label:'Verificação independente da candidata',type:'json_field',name:'verification.json',field:'passed',equals:true}],
      },
    ],
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
    WHERE t.goal_id=$1 AND t.plan_revision=$2 AND t.stage IN('work','integrate','verify') AND t.state<>'superseded'`, [goalId,revision])).rows;
  const verifiers = tasks.filter(t => t.stage === 'verify');
  const verifier = verifiers[0];
  const integrations = tasks.filter(t => t.stage === 'integrate');
  const integrator = integrations.find(t=>t.execution?.workspace===verifier?.execution?.sourceWorkspace);
  const workers = tasks.filter(t => t.stage === 'work');
  const expectedCount = integrations.length ? workers.length + integrations.length + 1 : 2;
  if (!workers.length || verifiers.length!==1 || tasks.length !== expectedCount || !verifier ||
      (integrations.length>0&&!integrator) ||
      tasks.some(t => t.state !== 'accepted' || t.run_state !== 'submitted' || t.passed !== true) ||
      !verifier.execution?.sourceWorkspace || tasks.some(t => !t.candidate_artifact_id)) return null;
  if ((await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1
      AND (r.state IN('queued','running','cancelling','reconciling') OR EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id)) LIMIT 1`,[goalId])).rowCount) return null;
  return { taskId:verifier.id, runId:verifier.run_id, sourceWorkspace:verifier.execution.sourceWorkspace,
    candidateArtifactId:verifier.candidate_artifact_id,candidateDigest:verifier.candidate_digest,
    verificationAcceptanceId:verifier.acceptance_id,workRunId:(integrator ?? workers[0])?.run_id };
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
  const selectedIds = selectedRequirementIds(plan.incremental);
  const expectedPending = plan.requirements.map(r => r.id).filter(id => !completed.includes(id) && !selectedIds.includes(id));
  if (!selectedIds.length || new Set(selectedIds).size !== selectedIds.length ||
      selectedIds.some(id => !eligible.includes(id)) ||
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
