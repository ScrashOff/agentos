import {readManagerCaseEvidence} from './manager-case-evidence.js';
import {readInputFailureObservation} from './input-failure-observation.js';
import {managerContextSectionNames} from './manager-context-sections.js';
import {readManagerContextSection} from './manager-context-section-store.js';
import {readManagerValidationHistory} from './manager-validation-history.js';
import {readPlanningDemandPage,readPlanningRemainderItem} from './planning-remainders.js';
import {readPlanningActionReviewDetail,samePlanningActionReviewSource} from './planning-action-review.js';
import { z } from 'zod';
import type { DB } from './db.js';
import { hash } from './memory.js';
import {managerTaskInstructionReference} from './manager-task-intent.js';
import { readManagementScopeTaskIds } from './management-projections.js';
import { checkedExecutionMetadata, executionMetadataColumns } from './task-execution-metadata.js';
import {readDeliveryConsumptionPage,deliveryConsumptionRecord,deliveryConsumptionDirectoryRow,deliveryResolutionGenerationGuide} from './delivery-consumption-catalog.js';

const taskRequest = {
  taskId: z.uuid(),
  expectedAttempt: z.number().int().min(0),
  offset: z.number().int().min(0).max(10000000).default(0),
};
export const managementContextRequests = z.array(z.discriminatedUnion('kind', [
  z.object({kind:z.literal('candidate_validation'),actionId:z.uuid(),
    snapshotDigest:z.string().regex(/^[a-f0-9]{64}$/),offset:taskRequest.offset}),
  z.object({kind:z.literal('context_section'),section:z.enum(managerContextSectionNames),
    digest:z.string().regex(/^[a-f0-9]{64}$/),offset:taskRequest.offset}),
  z.object({kind:z.literal('case_evidence'),caseId:z.uuid(),evidenceIndex:z.number().int().min(0).max(10000000),
    digest:z.string().regex(/^[a-f0-9]{64}$/),offset:taskRequest.offset}),
  z.object({kind:z.literal('planning_demands'),offset:taskRequest.offset,
    snapshotDigest:z.string().regex(/^[a-f0-9]{64}$/).optional()}),
  z.object({kind:z.literal('planning_request'),requestId:z.uuid(),itemIndex:z.number().int().min(0).max(11).optional(),offset:taskRequest.offset,
    snapshotDigest:z.string().regex(/^[a-f0-9]{64}$/).optional()}),
  z.object({ kind: z.literal('coverage'), baseWorkspace: z.string().min(1).max(300),
    query: z.string().max(160).default(''), offset: taskRequest.offset }),
  z.object({ kind: z.literal('artifacts'), ...taskRequest }),
  z.object({ kind: z.literal('artifact'), ...taskRequest, name: z.string().min(1).max(240) }),
  z.object({ kind: z.literal('task'), ...taskRequest,
    snapshotDigest:z.string().regex(/^[a-f0-9]{64}$/).optional() }),
  z.object({kind:z.literal('live_task_instruction'),goalId:z.uuid(),taskId:z.uuid(),
    expectedAttempt:z.number().int().positive(),runId:z.uuid(),runFence:z.uuid(),contextVersion:z.uuid(),
    offset:taskRequest.offset,snapshotDigest:z.string().regex(/^[a-f0-9]{64}$/).optional()}),
  z.object({ kind: z.literal('task_metadata'), ...taskRequest,
    part:z.enum(['decision','criteria','dependencies','outcome']).optional(),
    snapshotDigest:z.string().regex(/^[a-f0-9]{64}$/).optional() }),
  z.object({kind:z.literal('delivery_consumptions'),scope:z.string().min(1).max(300).optional(),
    consumerTaskId:z.uuid().optional(),offset:taskRequest.offset,snapshotDigest:z.string().regex(/^[a-f0-9]{64}$/).optional()}),
  z.object({kind:z.literal('delivery_consumption'),obligationId:z.uuid(),offset:taskRequest.offset,
    snapshotDigest:z.string().regex(/^[a-f0-9]{64}$/).optional()}),
])).max(4);
type ArtifactReader = (id: string) => Promise<{ bytes: Buffer }>;
export const managementRetrievalCharacterLimit = 24000;
export const managementRetrievalContract = `
CANDIDATE_VALIDATION_HISTORY_READ_V1: {kind:"candidate_validation",actionId:UUID,snapshotDigest:SHA256,offset?:number} reads the exact same-goal validation observation whose historical diagnostic/sourceDrift has a MANAGER_VALIDATION_HISTORY_DETAIL_V1 reference. Use its exact actionId/digest and nextOffset. A changed source returns STALE_CONTEXT without content; never combine versions. Current results, ownership and outcomes stay inline. This read grants no action, acceptance, or materialContextGap authority. Prefer the current validation for the current candidate; read history only for a concrete necessary difference.
LIVE_TASK_INSTRUCTION_V1: {kind:"live_task_instruction",goalId:UUID,taskId:UUID,expectedAttempt:number,runId:UUID,runFence:UUID,contextVersion:UUID,offset?:number,snapshotDigest?:SHA256} recupera somente run.instruction_snapshot.task.description da tentativa Harness ainda ativa indicada pela operation.tasks[].intent.liveInstructionReference.request. Use o digest da primeira página nas seguintes; identidade, fence, contexto, tentativa e snapshot são revalidados. Snapshot ausente/incompatível ou vínculo mudado recusa a leitura; nunca substitua por task.description atual. Não libera artifacts nem arquivos do workspace do ator vivo.
${deliveryResolutionGenerationGuide}
CONTEXT_SECTION_READ_V1: {kind:"context_section",section:"workOpportunities"|"deferredWorkOpportunities"|"progress"|"frontier"|"sourceCatalog"|"deferredDeliveries"|"deliveryConsumptions",digest:SHA256,offset?:number} reads the exact goal-scoped immutable JSON section referenced in this context. Use the reference's section and digest and the returned nextOffset (UTF-16 characters); never combine different digests. NOT_FOUND, INTEGRITY_FAILED and PAGE_OUT_OF_RANGE refuse missing, changed or invalid pages. This operational read neither authorizes an action nor qualifies as a related materialContextGap request; use the existing exact case/task/obligation readers for that decision.
PLANNING_REMAINDER_READ_V1: {kind:"planning_demands",offset?:number,snapshotDigest?:SHA256} discovers all pending requests in ten-row pages. Use nextOffset and the returned snapshotDigest; a changed catalog requires starting at zero.
PLANNING_ACTION_REVIEW_V1: deliveryActionReview in the directory is REPORTED_REQUIRES_DETAIL, a bounded report from the persisted request, not verified action evidence. Request its planning_request detail to read goal/run/artifact/event-bound deliveryActionReceipts and producer/reservedConsumer relationships, including already-applied handoffs. VERIFIED_ACCEPTED_ARTIFACT confirms the historical plan bytes and action receipts only; receiptApplied can mean supervision requested, never actual consumption or task acceptance. Unknown/stale evidence remains a pending issue. currentScopeRevision in the detail is a fresh read by scopeSourceRevision; its observedAt is separate from the stable paginated document. Only status OBSERVED supplies the revision for the existing management.nextTransition mode plan, with a concrete materially distinct hypothesis respecting reservations. Do not use the historical source_revision as current, rebind a producer, retry the accepted planner, or stop an actor merely to fix a plan. All existing admission/source/fence/supervision guards still apply; this read grants no action authority.
 {kind:"planning_request",requestId:UUID,itemIndex?:number,offset?:number,snapshotDigest?:SHA256} reads the exact current planning receipt, including every remaining item and bound replacement. It is goal-scoped and paginated text; offset>0 requires the first page snapshotDigest. Prefer itemIndex from remainingItemRequest to retrieve exactly that proposal from the hash-checked scope-plan artifact in one bounded response; omit itemIndex to read the receipt directory. The full original plan stays separately recoverable. A changed receipt returns STALE_CATALOG; no plan is applied or accepted by reading it.
MANAGEMENT_RETRIEVAL_V1: Pedir recuperação apenas em texto não executa uma leitura. Para recuperar conteúdo omitido, inclua management.contextRequests (até quatro pedidos) no coordination.json:
{kind:"coverage",baseWorkspace:string,query?:string,offset?:number} busca literal no título/chave/covers da cobertura persistida, incluindo entregas aceitas e falhas; query vazio pagina o catálogo completo desse escopo.
{kind:"artifacts",taskId:UUID,expectedAttempt:number,offset?:number} lista os artefatos da tentativa atual encerrada.
{kind:"artifact",taskId:UUID,expectedAttempt:number,name:string,offset?:number} recupera os bytes de texto de um artefato identificado, conferindo hash/tamanho.
{kind:"task",taskId:UUID,expectedAttempt:number,offset?:number,snapshotDigest?:SHA256} recupera a instrução integral persistida, inclusive requisitos não estruturados. Use o digest da referência e o nextOffset recebido para ler somente os intervalos necessários; STALE_CONTEXT exige nova observação em zero, sem juntar versões.
{kind:"task_metadata",taskId:UUID,expectedAttempt:number,part?:"decision"|"criteria"|"dependencies"|"outcome",offset?:number,snapshotDigest?:SHA256} recupera JSON estruturado exato, sem copiar a instrução integral. O padrão decision contém identidade, critérios, covers, dependências, resumo/erro e instructionReference com hash/tamanho/revisão e pedido task. As partes criteria, dependencies e outcome isolam esses campos sem trocar a identidade. Critérios estruturados e excertos NÃO esgotam requisitos em texto: consulte instructionReference.request antes de decidir sobre conteúdo não representado. Pagine a mesma parte usando snapshotDigest e nextOffset; PAGE_OUT_OF_RANGE exige reiniciar em zero. Uma referência não comprova leitura, conclusão nem permissão. A presença das outras frentes permanece no diretório; consulte somente os detalhes necessários à decisão.
{kind:"delivery_consumptions",scope?:string,consumerTaskId?:UUID,offset?:number,snapshotDigest?:SHA256} pagina TODAS as obrigações do objetivo (dez por página), incluindo recibos e donos. Sem scope consulta todos os escopos. Nas páginas seguintes use o snapshotDigest recebido; mudança material devolve STALE_CATALOG e exige reiniciar em zero.
{kind:"delivery_consumption",obligationId:UUID,offset?:number,snapshotDigest?:SHA256} recupera o registro completo em páginas JSON de texto, inclusive recibo grande. Offset é de caracteres; não confundir com o offset de linhas do catálogo. Leitura nunca muda contrato ou aceite.
Na próxima revisão do mesmo caso, operation.recoveredContext traz resultado, identidade, hash, totalCharacters/totalRows, offset e nextOffset. Uma rotação de foco não transfere nem apaga os pedidos do caso anterior; FOCUS_RESUMED identifica a revisão aceita de origem. Um pacote vazio na revisão seguinte desse caso encerra seus pedidos anteriores. Use nextOffset para continuar páginas; truncamento não comprova ausência. São leituras do próprio objetivo, sem criar tarefa/sessão, sem alterar instruções, fonte ou aceite. STALE_ATTEMPT, LIVE_SOURCE, NOT_FOUND e INTEGRITY_FAILED são impedimentos explícitos; não reutilize identidade antiga nem trate leitura como evidência de conclusão. Para discovery_needed por conteúdo omitido, solicite a página concreta que falta, depois use o resultado para propor trabalho útil distinto.`;

type RequestFocus = {kind?:string;caseId?:string;scope?:string|null;goalId?:string;taskId?:string;
  runId?:string;runFence?:string;contextVersion?:string;expectedAttempt?:number};
const liveActorFields=['goalId','taskId','runId','runFence','contextVersion'] as const;
/** A frozen instruction belongs to an actor, not to one of its failure events.
 * Different causes may reuse it; different runs/fences/contexts never may. */
export function sameLiveInstructionActor(a:any,b:any){
  return !!a&&!!b&&liveActorFields.every(key=>typeof a[key]==='string'&&
    /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(a[key])&&a[key]===b[key])&&
    Number.isSafeInteger(a.expectedAttempt)&&a.expectedAttempt>0&&a.expectedAttempt===b.expectedAttempt;
}
async function readLiveFocusedRequests(db:DB,goalId:string,focus:RequestFocus){
  if(focus.kind!=='live_failure'||focus.goalId!==goalId||!sameLiveInstructionActor(focus,focus))return null;
  // Materialize the actor's events before touching runs: joining every run's
  // text UUID first can scan the entire review history under the manager lock.
  const row=(await db.query(`WITH actor_events AS MATERIALIZED (
      SELECT id,payload FROM events
      WHERE kind='MANAGEMENT_REVIEWED' AND entity_id=$1::text
        AND created_at >= (SELECT created_at FROM runs WHERE id=$3::text::uuid)
        AND payload#>>'{contextRequestFocus,kind}'='live_failure'
        AND payload#>>'{contextRequestFocus,goalId}'=$1::text
        AND payload#>>'{contextRequestFocus,taskId}'=$2
        AND payload#>>'{contextRequestFocus,runId}'=$3
        AND payload#>>'{contextRequestFocus,runFence}'=$4
        AND payload#>>'{contextRequestFocus,contextVersion}'=$5
        AND payload#>>'{contextRequestFocus,expectedAttempt}'=$6
    ) SELECT e.payload->'contextRequests' requests,
      e.payload->'contextRequestFocus' requested_focus,r.id run_id,
      r.instruction_snapshot#>>'{task,description}' description,a.passed,m.receipts
    FROM actor_events e JOIN runs r ON r.id=CASE
      WHEN e.payload->>'runId' ~* '^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$'
      THEN (e.payload->>'runId')::uuid END
    JOIN tasks t ON t.id=r.task_id AND t.goal_id=$1::uuid AND t.stage='coordinate'
    JOIN management_reviews m ON m.run_id=r.id AND m.goal_id=t.goal_id
    LEFT JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=t.id
      ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    WHERE r.state='submitted'
    ORDER BY r.created_at DESC,r.id DESC,e.id DESC LIMIT 1`,
    [goalId,focus.taskId,focus.runId,focus.runFence,focus.contextVersion,String(focus.expectedAttempt)])).rows[0];
  // A newer empty/invalid/unaccepted packet must never uncover an older one.
  if(!row||row.passed!==true||!Array.isArray(row.receipts)||
    row.receipts.some((receipt:any)=>receipt?.code==='MANAGEMENT_FORMAT'))return null;
  const start=typeof row.description==='string'?row.description.lastIndexOf('\n{"cases":'):-1;
  if(start<0)return null;
  let frozen:RequestFocus;try{frozen=JSON.parse(row.description.slice(start+1)).operation?.focus;}catch{return null;}
  const recorded=row.requested_focus;
  if(frozen?.kind!=='live_failure'||!sameLiveInstructionActor(frozen,focus)||
    !sameLiveInstructionActor(recorded,frozen)||typeof frozen.scope!=='string'||
    recorded.scope!==frozen.scope||(focus.scope!==undefined&&focus.scope!==frozen.scope))return null;
  return {runId:row.run_id,contextRequests:row.requests??[],contextRequestFocus:recorded};
}
/** The global manager state is only the latest turn. Recover the latest durable
 * review for this decision, without replaying another case's pages or cached
 * mutable catalog results. Reading requests never authorizes their contents.
 * A latest empty/invalid packet must not uncover an older packet underneath it. */
async function readFocusedRequests(db:DB,goalId:string,focus:RequestFocus) {
  const row=(await db.query(`SELECT e.payload->'contextRequests' requests,
    e.payload->'contextRequestFocus' requested_focus,r.id run_id,
    r.instruction_snapshot#>>'{task,description}' description,a.passed,m.receipts
    FROM events e JOIN runs r ON r.id::text=e.payload->>'runId'
    JOIN tasks t ON t.id=r.task_id AND t.goal_id=$1 AND t.stage='coordinate'
    JOIN management_reviews m ON m.run_id=r.id AND m.goal_id=t.goal_id
    LEFT JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id AND task_id=t.id
      ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    WHERE e.kind='MANAGEMENT_REVIEWED' AND e.entity_id=$1::text AND r.state='submitted'
      AND e.payload#>>'{contextRequestFocus,caseId}'=$2
    ORDER BY r.created_at DESC,r.id DESC,e.id DESC LIMIT 1`,[goalId,focus.caseId])).rows[0];
  if(!row || row.passed!==true || !Array.isArray(row.receipts) ||
    row.receipts.some((receipt:any)=>receipt?.code==='MANAGEMENT_FORMAT'))return null;
  const start=typeof row.description==='string'?row.description.lastIndexOf('\n{"cases":'):-1;
  if(start<0)return null;
  let frozen:RequestFocus;
  try { frozen=JSON.parse(row.description.slice(start+1)).operation?.focus; }
  catch { return null; }
  // operation.focus remains literal even in shared-context snapshots. Confirm
  // the event's binding against that same immutable run, never mutable state.
  const recorded=row.requested_focus;
  if(!frozen || !recorded || frozen.caseId!==focus.caseId || recorded.caseId!==frozen.caseId ||
    typeof frozen.scope!=='string' || recorded.scope!==frozen.scope ||
    (focus.scope!==undefined && frozen.scope!==focus.scope) ||
    recorded.taskId!==frozen.taskId || (focus.taskId!==undefined && frozen.taskId!==focus.taskId))return null;
  return {runId:row.run_id,contextRequests:row.requests??[],contextRequestFocus:recorded};
}

/** Read-only, bounded, goal-scoped recovery. Every page keeps its original identity. */
export async function readManagementRetrieval(db: DB, goalId: string, readArtifact: ArtifactReader, focus?: RequestFocus) {
  const state = (await db.query('SELECT value FROM settings WHERE id=$1', ['management-state:' + goalId])).rows[0]?.value;
  const results: any[] = [];
  let requests=state?.contextRequests ?? [];
  let selection:{status:'FOCUS_RESUMED';reviewRunId:string}|undefined;
  // A request belongs to the review that asked for it. Do not replay its pages
  // into a different decision just because that decision shares the same goal.
  // Legacy records recover that binding from the immutable requesting run.
  let requestedFocus=state?.contextRequestFocus;
  if(focus?.kind==='live_failure'){
    if(requestedFocus?.kind!=='live_failure'||!sameLiveInstructionActor(focus,requestedFocus)||
      focus.scope!==requestedFocus.scope){
      const previous=await readLiveFocusedRequests(db,goalId,focus);
      if(previous){requests=previous.contextRequests;requestedFocus=previous.contextRequestFocus;
        selection={status:'FOCUS_RESUMED',reviewRunId:previous.runId};}
      else {
        const pending=managementContextRequests.safeParse(requests);
        return {characterLimit:managementRetrievalCharacterLimit,results,selection:{status:'FOCUS_CHANGED',
          omittedRequests:pending.success?pending.data.length:0,requestedFocus,currentFocus:focus,fullSourcesPreserved:true}};
      }
    }
    return readManagementRequestPages(db,goalId,readArtifact,requests,
      {characterLimit:managementRetrievalCharacterLimit,...(selection?{selection}:{})},focus);
  }
  if(focus?.caseId && requestedFocus===undefined && state?.runId){
    const original=(await db.query(`SELECT r.instruction_snapshot->'task'->>'description' description
      FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate'`,[state.runId,goalId])).rows[0]?.description;
    const start=original?.lastIndexOf('\n{"cases":') ?? -1;
    if(start>=0)try{requestedFocus=JSON.parse(original.slice(start+1)).operation?.focus;}catch{/* Legacy global reviews have no focus binding. */}
  }
  if(focus?.caseId && (requestedFocus?.kind==='live_failure' || requestedFocus?.caseId &&
    (requestedFocus.caseId!==focus.caseId || (focus.scope!==undefined && requestedFocus.scope!==focus.scope) ||
      (focus.taskId!==undefined && requestedFocus.taskId!==focus.taskId)))) {
    const previous=await readFocusedRequests(db,goalId,focus);
    if(previous){requests=previous.contextRequests;requestedFocus=previous.contextRequestFocus;
      selection={status:'FOCUS_RESUMED',reviewRunId:previous.runId};}
    else {
      const pending=managementContextRequests.safeParse(requests);
      return {characterLimit:managementRetrievalCharacterLimit,results,selection:{status:'FOCUS_CHANGED',
        omittedRequests:pending.success?pending.data.length:0,requestedFocus,currentFocus:focus,fullSourcesPreserved:true,
        recovery:{tool:'management.contextRequests',requests:pending.success?pending.data:[]}}};
    }
  }
  if(requestedFocus?.kind==='live_failure'){
    const pending=managementContextRequests.safeParse(requests);
    return {characterLimit:managementRetrievalCharacterLimit,results,selection:{status:'FOCUS_CHANGED',
      omittedRequests:pending.success?pending.data.length:0,requestedFocus,currentFocus:focus??null,fullSourcesPreserved:true}};
  }
  return readManagementRequestPages(db,goalId,readArtifact,requests,
    {characterLimit:managementRetrievalCharacterLimit,...(selection?{selection}:{})});
}

/** Same bounded canonical reader for explicit requests during a manager turn.
 * Goal identity comes from the controller, never from tool arguments. */
export async function readManagementRequestPages(db:DB,goalId:string,readArtifact:ArtifactReader,requests:unknown,
  envelope:{characterLimit:number;selection?:{status:'FOCUS_RESUMED';reviewRunId:string}}={characterLimit:managementRetrievalCharacterLimit},
  liveFocus?:RequestFocus) {
  const results:any[]=[];
  const parsed=managementContextRequests.safeParse(requests);
  if (!parsed.success) return {...envelope, results, invalidRequests: true };
  for (const [requestIndex, request] of parsed.data.entries()) {
    let result = liveFocus?.kind==='live_failure'&&request.kind==='live_task_instruction'&&!sameLiveInstructionActor(request,liveFocus)
      ?{request,status:'FOCUS_BINDING_MISMATCH'}
      :await readManagementRetrievalRequest(db,goalId,readArtifact,request);
    // Budget the actual JSON envelope (including escaped text), never silently drop a request.
    const used = JSON.stringify({ ...envelope, results }).length;
    const share = Math.floor((managementRetrievalCharacterLimit - 80 - used) / (parsed.data.length - requestIndex));
    const fits = (reserved = 0) => JSON.stringify({ ...envelope, results: [...results, result] }).length <= used + share - reserved;
    while (!fits(80) && result.rows?.length) result.rows.pop();
    while (!fits(80) && result.text?.length) result.text = result.text.slice(0, Math.max(0, result.text.length - 100));
    if (result.status === 'OK') {
      const end = request.offset + (result.rows?.length ?? result.text?.length ?? 0);
      const total = result.totalRows ?? result.totalCharacters;
      result.nextOffset = end < total ? end : null;
      result.truncated = end < total;
      if (result.truncated && end === request.offset) result.status = 'BUDGET_DEFERRED';
    }
    if (!fits()) result = { request, status: 'BUDGET_DEFERRED' };
    results.push(result);
  }
  return { ...envelope, results };
}


/** Resolve one explicit request without consulting manager state. This is the
 * same goal/attempt/live/hash guarded read used by the review and its evidence
 * ledger. The caller may budget the returned exact page further. */
export async function readManagementRetrievalRequest(db:DB,goalId:string,readArtifact:ArtifactReader,
  input:z.input<typeof managementContextRequests.element>) {
  const request=managementContextRequests.element.parse(input);
    let result: any = { request, status: 'NOT_FOUND' };
    if(request.kind==='live_task_instruction') {
      if(request.goalId!==goalId) result.status='GOAL_MISMATCH';
      else if(request.offset>0&&!request.snapshotDigest) result.status='SNAPSHOT_DIGEST_REQUIRED';
      else {
        const bound=(await db.query(`SELECT t.id task_id,t.goal_id,t.attempt_count,t.state task_state,t.stage,t.execution->>'kind' execution_kind,
            r.id run_id,r.attempt,r.state run_state,r.runtime,r.fence,r.context_id,r.instruction_snapshot,
            c.id persisted_context_id,
            EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND
              (e.payload->>'runId'=r.id::text OR e.entity_id IN(r.id::text,t.id::text))) cancelled,
            (SELECT max(x.attempt) FROM runs x WHERE x.task_id=t.id) latest_attempt
          FROM tasks t JOIN runs r ON r.task_id=t.id AND r.id=$3
          LEFT JOIN contexts c ON c.id=r.context_id AND c.run_id=r.id
          WHERE t.id=$1 AND t.goal_id=$2`,[request.taskId,goalId,request.runId])).rows[0];
        if(!bound) result.status='NOT_FOUND';
        else if(bound.attempt_count!==request.expectedAttempt||bound.attempt!==request.expectedAttempt||bound.latest_attempt!==request.expectedAttempt)
          result.status='STALE_ATTEMPT';
        else if(bound.fence!==request.runFence||bound.context_id!==request.contextVersion||bound.persisted_context_id!==request.contextVersion)
          result.status='STALE_BINDING';
        else if(bound.task_state!=='running'||bound.run_state!=='running'||bound.cancelled||bound.stage==='coordinate'||
          bound.execution_kind!=='harness'||bound.runtime!=='harness')
          result.status='LIVE_BINDING_UNAVAILABLE';
        else {
          const snapshot=bound.instruction_snapshot;
          const instruction=snapshot?.task?.description;
          const snapshotTask=snapshot?.task;
          if(typeof instruction!=='string'||snapshotTask?.id!==request.taskId||snapshotTask?.goal_id!==goalId)
            result.status='SNAPSHOT_UNAVAILABLE';
          else {
            const digest=hash(instruction);
            if(request.snapshotDigest&&request.snapshotDigest!==digest)
              result={request,status:'STALE_CONTEXT',snapshotDigest:digest,nextOffset:0};
            else if(request.offset>instruction.length)
              result={request,status:'PAGE_OUT_OF_RANGE',snapshotDigest:digest,nextOffset:0};
            else result={request,status:'OK',runId:bound.run_id,snapshotDigest:digest,digest,totalCharacters:instruction.length,
              text:instruction.slice(request.offset,request.offset+12000)};
          }
        }
      }
    } else if(request.kind==='candidate_validation') {
      result={request,...await readManagerValidationHistory(db,goalId,request)};
    } else if(request.kind==='context_section') {
      result={request,...await readManagerContextSection(db,goalId,request)};
    } else if(request.kind==='case_evidence') {
      result={request,...await readManagerCaseEvidence(db,goalId,request)};
    } else if(request.kind==='planning_demands') {
      if(request.offset>0&&!request.snapshotDigest)result.status='CATALOG_DIGEST_REQUIRED';
      else {const page=await readPlanningDemandPage(db,goalId,{offset:request.offset,limit:10,expectedDigest:request.snapshotDigest});
        result={request,status:page.status,snapshotDigest:page.snapshotDigest,totalRows:page.totalRows,rows:page.rows};
        if(page.status==='STALE_CATALOG')result.nextOffset=0;
      }
    } else if(request.kind==='planning_request') {
      if(request.offset>0&&!request.snapshotDigest)result.status='CATALOG_DIGEST_REQUIRED';
      else if(request.itemIndex!==undefined){
        const item=await readPlanningRemainderItem(db,goalId,request.requestId,request.itemIndex,readArtifact);
        result={request,status:item.status};
        if(item.status==='OK'){
          const text=JSON.stringify(item.value),digest=hash(text);
          result={request,status:request.snapshotDigest&&request.snapshotDigest!==digest?'STALE_CATALOG':'OK',snapshotDigest:digest};
          if(result.status==='OK')result={...result,digest,totalCharacters:text.length,text:text.slice(request.offset,request.offset+12000)};
          else result.nextOffset=0;
        }
      } else {
        const row=(await db.query(`SELECT id,scope,state,task_id,source_revision,receipt->'remaining' remaining,
          receipt->'remainingProjectionFailure' projection_failure,receipt->>'code' code,
          receipt->'backlog'->'task_ids' created_task_ids,updated_at::text request_updated_at,xmin::text request_version
          FROM planning_requests WHERE id=$1 AND goal_id=$2`,
          [request.requestId,goalId])).rows[0];
        if(row){
          const review=await readPlanningActionReviewDetail(db,goalId,request.requestId,readArtifact);
          let actionDetail=review?.value,currentScopeRevision:any=undefined;
          if(review?.value.status==='VERIFIED_ACCEPTED_ARTIFACT'){
            try{
              // Runtime-only import: planning-demand already depends on the
              // remainder reader; discovery/projector modules must not import it.
              const {scopeSourceRevision}=await import('./planning-demand.js');
              currentScopeRevision={status:'OBSERVED',value:await scopeSourceRevision(db,goalId,row.scope),
                source:'server/planning-demand.ts::scopeSourceRevision',timeBasis:'response.observedAt',authority:'read_only'};
            }catch{currentScopeRevision={status:'UNKNOWN',source:'server/planning-demand.ts::scopeSourceRevision',authority:'read_only'};}
          }
          if(review&&(review.basis.request_version!==row.request_version||
            !await samePlanningActionReviewSource(db,goalId,review))){
            actionDetail={version:1,authority:'review_only',status:'STALE_SOURCE',requestId:row.id};
            currentScopeRevision={status:'UNKNOWN',source:'server/planning-demand.ts::scopeSourceRevision',authority:'read_only'};
          }
          const {request_version,...document}=row;
          const text=JSON.stringify({...document,...(actionDetail?{deliveryActionReview:actionDetail,
            currentScopeRevision:currentScopeRevision??{status:'UNKNOWN',source:'server/planning-demand.ts::scopeSourceRevision',authority:'read_only'}}:{})}),digest=hash(text);
          result={request,status:request.snapshotDigest&&request.snapshotDigest!==digest?'STALE_CATALOG':'OK',snapshotDigest:digest};
          if(result.status==='OK')result={...result,digest,totalCharacters:text.length,text:text.slice(request.offset,request.offset+12000),
            ...(actionDetail?{observedAt:new Date().toISOString()}: {})};
          else result.nextOffset=0;
        }
      }
    } else if(request.kind==='delivery_consumptions' || request.kind==='delivery_consumption') {
      if(request.offset>0 && !request.snapshotDigest)result.status='CATALOG_DIGEST_REQUIRED';
      else {
        const page=await readDeliveryConsumptionPage(db,goalId,{scope:request.kind==='delivery_consumptions'?request.scope:undefined,
          consumerTaskId:request.kind==='delivery_consumptions'?request.consumerTaskId:undefined,
          obligationId:request.kind==='delivery_consumption'?request.obligationId:undefined,
          offset:request.kind==='delivery_consumptions'?request.offset:0,limit:10,expectedDigest:request.snapshotDigest});
        result={request,status:page.status,snapshotDigest:page.snapshotDigest};
        if(page.status==='STALE_CATALOG')result.nextOffset=0;
        else if(request.kind==='delivery_consumptions')result={...result,totalRows:page.totalRows,rows:page.rows.map(deliveryConsumptionDirectoryRow)};
        else if(!page.rows.length)result.status='NOT_FOUND';
        else {const text=JSON.stringify(deliveryConsumptionRecord(page.rows[0]));result={...result,digest:hash(text),totalCharacters:text.length,
          text:text.slice(request.offset,request.offset+12000)};}
      }
    } else if (request.kind === 'coverage') {
      const scopeIds = await readManagementScopeTaskIds(db, goalId, request.baseWorkspace);
      const rows = (await db.query(`SELECT t.id task_id,t.title,t.state,t.stage,t.attempt_count expected_attempt,
        t.covers,metadata.work_kind,metadata.base_workspace,b.coverage_key,
        (SELECT r.id FROM runs r WHERE r.task_id=t.id ORDER BY r.attempt DESC LIMIT 1) run_id,
        EXISTS(SELECT 1 FROM runs r WHERE r.task_id=t.id AND r.state IN('queued','running','cancelling','reconciling')) live
        FROM tasks t JOIN task_execution_metadata metadata ON metadata.task_id=t.id
        LEFT JOIN workforce_backlog_items b ON b.task_id=t.id AND b.goal_id=$1
        WHERE t.goal_id=$1 AND t.id=ANY($5::uuid[]) AND metadata.base_workspace=$2
        AND (b.task_id IS NOT NULL OR metadata.work_kind IN('diagnostic','repair'))
        AND ($3='' OR strpos(lower(t.title||' '||COALESCE(b.coverage_key,'')||' '||t.covers::text),lower($3))>0)
        ORDER BY t.id LIMIT 11 OFFSET $4`, [goalId, request.baseWorkspace, request.query, request.offset, scopeIds])).rows;
      const total = (await db.query(`SELECT count(*)::int n FROM tasks t
        JOIN task_execution_metadata metadata ON metadata.task_id=t.id
        LEFT JOIN workforce_backlog_items b ON b.task_id=t.id AND b.goal_id=$1
        WHERE t.goal_id=$1 AND t.id=ANY($4::uuid[]) AND metadata.base_workspace=$2
        AND (b.task_id IS NOT NULL OR metadata.work_kind IN('diagnostic','repair'))
        AND ($3='' OR strpos(lower(t.title||' '||COALESCE(b.coverage_key,'')||' '||t.covers::text),lower($3))>0)`,
        [goalId, request.baseWorkspace, request.query, scopeIds])).rows[0].n;
      result = { request, status: 'OK', totalRows: total, rows: rows.slice(0, 10) };
    } else {
      const task = checkedExecutionMetadata((await db.query(`SELECT t.id,t.description,t.instruction_revision_id,t.attempt_count,t.title,t.stage,t.state,t.criteria,t.covers,
        ${executionMetadataColumns},metadata.base_workspace,metadata.work_kind,
        (SELECT jsonb_agg(d.depends_on ORDER BY d.depends_on) FROM dependencies d WHERE d.task_id=t.id) dependencies,
        r.id run_id,r.attempt run_attempt,r.state run_state,r.fence run_fence,r.context_id run_context_id,r.summary,r.error_code,
        EXISTS(SELECT 1 FROM runs l WHERE l.task_id=t.id AND l.state IN('queued','running','cancelling','reconciling')) live
        FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
        LEFT JOIN LATERAL(SELECT id,attempt,state,fence,context_id,summary,error_code FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
        WHERE t.id=$1 AND t.goal_id=$2`, [request.taskId, goalId])).rows, { objectRequired: false })[0];
      if (task?.attempt_count !== undefined && task.attempt_count !== request.expectedAttempt) {
        result.status = 'STALE_ATTEMPT';
        // Identity is observed only after the same-goal query and metadata
        // guards. Do not retarget the request or return a stale content page.
        if(request.kind==='task_metadata')result={...result,currentAttempt:task.attempt_count,
          currentState:task.state,currentRunId:task.run_id??null,currentRunAttempt:task.run_attempt??null,currentRunState:task.run_state??null,
          currentHasLiveRun:task.live===true,
          nextAction:'Submit a new task_metadata request for this observed currentAttempt before deciding. This observation is not a source binding, permission to retry, or accepted evidence.'};
      }
      else if (task?.live) result.status = 'LIVE_SOURCE';
      else if (task) {
        result.runId = task.run_id;
        if (request.kind === 'task' || request.kind==='task_metadata') {
          const part=request.kind==='task_metadata'?(request.part??'decision'):undefined;
          const inputFailureDiagnosis=part==='decision'||part==='outcome'
            ?await readInputFailureObservation(db,goalId,task):null;
          const text=request.kind==='task' ? task.description : JSON.stringify({
            representation:'observed_task_metadata_v2',part,taskId:task.id,title:task.title,
            stage:task.stage,state:task.state,expectedAttempt:task.attempt_count,runId:task.run_id,
            runAttempt:task.run_attempt,runState:task.run_state,
            runFence:task.run_attempt===task.attempt_count?(task.run_fence??null):null,
            contextVersion:task.run_attempt===task.attempt_count?(task.run_context_id??null):null,
            baseWorkspace:task.base_workspace,workKind:task.work_kind,
            instructionReference:managerTaskInstructionReference(task),
            ...(part==='decision'||part==='criteria'?{criteria:task.criteria,covers:task.covers}:{}),
            ...(part==='decision'||part==='dependencies'?{dependencies:task.dependencies??[]}:{}),
            ...(part==='decision'||part==='outcome'?{summary:task.summary,errorCode:task.error_code,inputFailureDiagnosis}:{}),
          });
          const digest=hash(text);
          result={...result,digest,snapshotDigest:digest,totalCharacters:text.length};
          if(request.snapshotDigest && request.snapshotDigest!==digest)
            result={...result,status:'STALE_CONTEXT',nextOffset:0,
              nextAction:'Observe this task and part again at offset zero without the old digest; never combine different versions.'};
          else if(request.offset>text.length)
            result={...result,status:'PAGE_OUT_OF_RANGE',nextOffset:0,
              nextAction:'This document has no page at that offset. Restart the same task and part at zero with the returned snapshotDigest.'};
          else result={...result,status:'OK',text:text.slice(request.offset,request.offset+12000)};
        } else if (request.kind === 'artifacts') {
          const rows = (await db.query('SELECT id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY name,id LIMIT 11 OFFSET $2',
            [task.run_id, request.offset])).rows;
          const total = (await db.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1', [task.run_id])).rows[0].n;
          result = { ...result, status: 'OK', totalRows: total, rows: rows.slice(0, 10) };
        } else {
          const ref = (await db.query('SELECT id,digest,size,mime FROM artifacts WHERE run_id=$1 AND name=$2',
            [task.run_id, request.name])).rows[0];
          if (ref) {
            result.artifactId = ref.id;
            result.digest = ref.digest;
            if (ref.size > 8 * 1024 * 1024) result.status = 'TOO_LARGE';
            else if (!/^(text\/|application\/(json|.+\+json))/.test(ref.mime)) result.status = 'NOT_TEXT';
            else try {
              const { bytes } = await readArtifact(ref.id);
              if (bytes.length !== ref.size || hash(bytes) !== ref.digest) throw Error('INTEGRITY_FAILED');
              const text = bytes.toString('utf8');
              result = { ...result, status: 'OK', size: bytes.length,
                totalCharacters: text.length, text: text.slice(request.offset, request.offset + 12000) };
            } catch { result.status = 'INTEGRITY_FAILED'; }
          }
        }
      }
    }
    if(result.status==='OK') {
      const end=request.offset+(result.rows?.length??result.text?.length??0);
      const total=result.totalRows??result.totalCharacters;
      result.nextOffset=end<total?end:null;
      result.truncated=end<total;
    }
    return result;
}
