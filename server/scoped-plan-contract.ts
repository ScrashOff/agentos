import { z } from 'zod';
import { createHash } from 'node:crypto';

const evidence=z.object({taskId:z.uuid(),criterionId:z.string().min(1),
  reason:z.string().min(15).max(1600),nextAction:z.string().min(15).max(1600),
  blockedByTaskIds:z.array(z.uuid()).max(20),artifactIds:z.array(z.uuid()).max(20)}).strict();
export const scopedPlanActionSchema=z.object({
  kind:z.enum(['reuse_existing','prepare_integration','request_verification']),
  taskId:z.uuid(),expectedAttempt:z.number().int().nonnegative(),sourceTaskId:z.uuid(),
  expectedSourceRunId:z.uuid(),artifactIds:z.array(z.uuid()).min(1).max(20),
  reason:z.string().min(40).max(2000),
}).strict();
export type ScopedPlanAction=z.infer<typeof scopedPlanActionSchema>;
export const scopedPlanSchema=z.object({version:z.literal(1),requestId:z.uuid(),
  sourceRevision:z.string().regex(/^[a-f0-9]{64}$/),reason:z.string().min(15).max(3000),
  tasks:z.array(z.object({coverageKey:z.string().regex(/^[a-z0-9_-]{3,80}$/),
    title:z.string().min(8).max(180),description:z.string().min(40).max(12000),
    baseWorkspace:z.string(),deliverToTaskId:z.uuid(),sourceTaskId:z.uuid(),
    workKind:z.enum(['diagnostic','repair']),lane:z.enum(['glm','muse']),
    delivery:z.enum(['immediate','deferred']).default('deferred')})).max(12),
  blockers:z.array(evidence).max(100),
  actions:z.array(scopedPlanActionSchema).max(12).default([]),
  dependencyChanges:z.array(z.object({taskId:z.uuid(),expectedAttempt:z.number().int().nonnegative(),
    removeDependsOnId:z.uuid(),sourceTaskId:z.uuid(),reason:z.string().min(40).max(2000),
    artifactIds:z.array(z.uuid()).min(1).max(20)}).strict()).max(6).default([])}).strict();
export type ScopedPlan=z.infer<typeof scopedPlanSchema>;
export const scopedPlanContractVersion='SCOPED_PLAN_CONTRACT_V1';
/** A producer's existing reservation is not transferable by a plan action.
 * This describes the frozen contract only; the live applier owns all effects. */
export function scopedPlanHandoffGuide(){
  return {kinds:['reuse_existing','prepare_integration'],authority:'frozen_catalog_only',
    query:{operation:'find',jsonPath:['deferredDeliveries'],where:{producer_task_id:'<sourceTaskId>'},offset:0,limit:12},
    instruction:'Before either handoff action, query deferredDeliveries by producer_task_id=sourceTaskId and read the exact consumer_task_id. One producer reserves one consumer, including an applied handoff. Reuse preserves that consumer; it cannot redirect the reservation. Two handoff actions in one plan cannot reserve that producer for different consumers. An unavailable or mismatching binding needs an evidenced blocker, not a fabricated consumer. request_verification uses its existing accepted dependency and creates no handoff. A matching frozen reservation is not current stop/source authority or proof of consumption.'};
}
/** The worker and controller describe the same input contract, including
 * defaults. This is syntax documentation, never permission to apply a plan. */
export function scopedPlanJsonSchema(){
  return z.toJSONSchema(scopedPlanSchema,{target:'draft-2020-12',io:'input'});
}
export function scopedPlanContractGuide(){
  const schema=scopedPlanJsonSchema() as any,fields=schema.properties;
  const limits=(field:any)=>Object.fromEntries(['minLength','maxLength','minItems','maxItems','pattern']
    .filter(key=>field[key]!==undefined).map(key=>[key,field[key]]));
  return {version:scopedPlanContractVersion,identifiers:'Use complete existing UUIDs from the frozen catalog.',
    handoffReservations:scopedPlanHandoffGuide(),
    fields:{reason:limits(fields.reason),coverageKey:limits(fields.tasks.items.properties.coverageKey),
      title:limits(fields.tasks.items.properties.title),description:limits(fields.tasks.items.properties.description),
      actionReason:limits(fields.actions.items.properties.reason),actionArtifactIds:limits(fields.actions.items.properties.artifactIds),
      dependencyChangeReason:limits(fields.dependencyChanges.items.properties.reason),
      dependencyChangeArtifactIds:limits(fields.dependencyChanges.items.properties.artifactIds),
      blockerReason:limits(fields.blockers.items.properties.reason),blockerNextAction:limits(fields.blockers.items.properties.nextAction)},
    authority:'Preflight checks a frozen catalog only. The controller still revalidates current source, permissions, stop proof and acceptance.'};
}
/** Only schema metadata is feedback. Never echo rejected values or messages. */
export function scopedPlanningRejection(error:any){
  const code=/^PLANNING_[A-Z0-9_]{1,65}$/.test(error?.message??'')?error.message:
    error instanceof SyntaxError?'PLANNING_JSON_INVALID':'PLANNING_CONTRACT_INVALID';
  const fields=new Set(['version','requestId','sourceRevision','reason','tasks','coverageKey','title','description',
    'baseWorkspace','deliverToTaskId','sourceTaskId','workKind','lane','delivery','blockers','taskId','criterionId',
    'nextAction','blockedByTaskIds','artifactIds','dependencyChanges','expectedAttempt','removeDependsOnId',
    'actions','kind','expectedSourceRunId']);
  const allIssues=Array.isArray(error?.issues)?error.issues:[];
  const issues=allIssues.slice(0,40).map((issue:any)=>({
    path:(Array.isArray(issue.path)?issue.path:[]).slice(0,8).map((part:any)=>
      Number.isInteger(part)&&part>=0&&part<=1000?part:fields.has(part)?part:'unknown_field'),
    code:/^[a-z_]{1,40}$/.test(issue.code??'')?issue.code:'invalid_value',
    ...(issue.format==='uuid'?{expected:'uuid'}:{}),
    ...(Number.isInteger(issue.maximum)&&issue.maximum>=0&&issue.maximum<=100000?{maximum:issue.maximum}:{}),
    ...(Number.isInteger(issue.minimum)&&issue.minimum>=0&&issue.minimum<=100000?{minimum:issue.minimum}:{}),
  }));
  return {code,issues,omittedIssueCount:Math.max(0,allIssues.length-40),nextAction:code.startsWith('PLANNING_HANDOFF_')?
    'Consulte deferredDeliveries com find por producer_task_id igual ao sourceTaskId da ação indicada; leia o consumer_task_id exato. Preserve a reserva existente e as ações válidas do rascunho. Não redirecione o produtor nem invente um consumidor. Confira também as outras ações do mesmo produtor. Se não houver encaminhamento permitido, registre blocker com evidência; request_verification mantém seu contrato próprio. Execute o preflight novamente.':
    'Corrija os campos indicados de scope-plan.json preservado e confira todos os IDs contra o catálogo autorizado. Preserve as frentes válidas e os critérios.'};
}
function planningError(code:string,path:Array<string|number>,issueCode:string):never{
  throw Object.assign(Error(code),{issues:[{path,code:issueCode}]});
}
export function scopedSourceRevision(tasks:any[],artifacts:any[],deliveries:any[]=[]){
  // Planning activity cannot create fresh demand by changing its own state.
  const rows=tasks.filter(t=>t.execution?.workKind!=='planning').map(t=>[
    t.id,['accepted','cancelled','superseded'].includes(t.state)?t.state:'open',
    t.state==='accepted'?t.attempt_count:0,t.execution?.workspace,t.dependencies??[],t.criteria,t.covers,
    t.state==='accepted'?t.acceptance_passed??null:null,
    t.state==='accepted'?t.last_run_id??null:null]).sort((a,b)=>String(a[0]).localeCompare(String(b[0])));
  // Failure logs remain in the frozen catalog, but their changing timestamps
  // cannot invalidate a queued planner whose authorized sources are unchanged.
  // New accepted findings, changed criteria/dependencies/workspaces and handoff
  // receipts all remain material revisions and are rechecked at admission.
  const acceptedIds=new Set(tasks.filter(t=>t.state==='accepted' && t.execution?.workKind!=='planning').map(t=>t.id));
  return createHash('sha256').update(JSON.stringify([rows,artifacts.filter(a=>acceptedIds.has(a.task_id)).map(a=>[a.task_id,a.name,a.digest]).sort(),
    deliveries.map(d=>[d.id,d.state,d.producer_task_id,d.consumer_task_id,d.receipt?.code??null]).sort()])).digest('hex');
}
export function validateScopedPlan(raw:unknown,request:any){
  const plan=scopedPlanSchema.parse(raw);
  if(plan.requestId!==request.id || plan.sourceRevision!==request.source_revision)throw Error('PLANNING_SOURCE_CHANGED');
  const snapshot=request.snapshot,byId=new Map<string,any>(snapshot.tasks.map((t:any)=>[t.id,t]));
  const ids=new Set(snapshot.artifacts.map((a:any)=>a.id));
  for(const [index,p] of plan.tasks.entries()){
    const consumer=byId.get(p.deliverToTaskId),source=byId.get(p.sourceTaskId);
    if(p.baseWorkspace!==request.scope)planningError('PLANNING_SCOPE_INVALID',['tasks',index,'baseWorkspace'],'scope_mismatch');
    if(!consumer)planningError('PLANNING_SCOPE_INVALID',['tasks',index,'deliverToTaskId'],'task_not_in_snapshot');
    if(consumer.execution?.baseWorkspace!==request.scope)
      planningError('PLANNING_SCOPE_INVALID',['tasks',index,'deliverToTaskId'],'consumer_scope_mismatch');
    if(['accepted','cancelled','superseded'].includes(consumer.state))
      planningError('PLANNING_SCOPE_INVALID',['tasks',index,'deliverToTaskId'],'consumer_closed');
    if(!['work','integrate','verify'].includes(consumer.stage))
      planningError('PLANNING_SCOPE_INVALID',['tasks',index,'deliverToTaskId'],'consumer_stage_invalid');
    if(!source)planningError('PLANNING_SCOPE_INVALID',['tasks',index,'sourceTaskId'],'task_not_in_snapshot');
    if(source.execution?.baseWorkspace!==request.scope)
      planningError('PLANNING_SCOPE_INVALID',['tasks',index,'sourceTaskId'],'source_scope_mismatch');
    if(source.state!=='accepted' || source.acceptance_passed!==true)
      planningError('PLANNING_SCOPE_INVALID',['tasks',index,'sourceTaskId'],'source_not_accepted');
    if(source.last_run_state!=='submitted' || source.last_run_attempt!==source.attempt_count)
      planningError('PLANNING_SCOPE_INVALID',['tasks',index,'sourceTaskId'],'source_not_current');
    if(source.has_live_run)planningError('PLANNING_SCOPE_INVALID',['tasks',index,'sourceTaskId'],'source_live');
  }
  for(const [index,b] of plan.blockers.entries()){
    const task=byId.get(b.taskId);
    if(!task)planningError('PLANNING_BLOCKER_UNVERIFIED',['blockers',index,'taskId'],'task_not_in_snapshot');
    if(!(task.criteria??[]).some((c:any)=>c.id===b.criterionId))
      planningError('PLANNING_BLOCKER_UNVERIFIED',['blockers',index,'criterionId'],'criterion_not_in_snapshot');
    const missingTask=b.blockedByTaskIds.findIndex(id=>!byId.has(id));
    if(missingTask>=0)planningError('PLANNING_BLOCKER_UNVERIFIED',['blockers',index,'blockedByTaskIds',missingTask],'task_not_in_snapshot');
    const missingArtifact=b.artifactIds.findIndex(id=>!ids.has(id));
    if(missingArtifact>=0)planningError('PLANNING_BLOCKER_UNVERIFIED',['blockers',index,'artifactIds',missingArtifact],'artifact_not_in_snapshot');
    if(!b.blockedByTaskIds.length && !b.artifactIds.length)
      planningError('PLANNING_BLOCKER_UNVERIFIED',['blockers',index],'evidence_missing');
  }
  for(const change of plan.dependencyChanges){
    const task=byId.get(change.taskId),parent=byId.get(change.removeDependsOnId),source=byId.get(change.sourceTaskId);
    if(!task || !['diagnostic','repair'].includes(task.execution?.workKind) || task.stage==='verify' ||
      task.has_live_run || task.attempt_count!==change.expectedAttempt || !task.dependencies.includes(change.removeDependsOnId) ||
      !parent || parent.stage!=='integrate' || !source || source.state!=='accepted' || source.acceptance_passed!==true ||
      source.last_run_state!=='submitted' || source.last_run_attempt!==source.attempt_count || source.has_live_run ||
      change.artifactIds.some(id=>!ids.has(id)))throw Error('PLANNING_DEPENDENCY_CHANGE_INVALID');
  }
  const actionKeys=new Set<string>(),handoffConsumers=new Map<string,string>();
  for(const [index,action] of plan.actions.entries()){
    const consumer=byId.get(action.taskId),source=byId.get(action.sourceTaskId);
    const reject=(field:string,code:string):never=>planningError('PLANNING_ACTION_INVALID',['actions',index,field],code);
    if(!consumer || consumer.execution?.baseWorkspace!==request.scope ||
      !['work','integrate','verify'].includes(consumer.stage) ||
      ['accepted','cancelled','superseded','cancelling'].includes(consumer.state))reject('taskId','consumer_scope_or_state');
    if(consumer.attempt_count!==action.expectedAttempt)reject('expectedAttempt','consumer_changed');
    if(!source || source.id===consumer.id || source.execution?.baseWorkspace!==request.scope ||
      source.state!=='accepted' || source.acceptance_passed!==true || source.has_live_run ||
      source.last_run_state!=='submitted' || source.last_run_attempt!==source.attempt_count ||
      source.last_run_id!==action.expectedSourceRunId)reject('sourceTaskId','source_not_current_accepted');
    if(action.artifactIds.some(id=>!snapshot.artifacts.some((a:any)=>a.id===id && a.task_id===source.id)))
      reject('artifactIds','evidence_not_from_source');
    if(action.kind==='request_verification'){
      if(consumer.stage!=='verify' || consumer.execution?.readOnlySource!==true ||
        !(consumer.dependencies??[]).includes(source.id))reject('taskId','verification_dependency_required');
    }else{
      if(!['diagnostic','repair'].includes(source.execution?.workKind))reject('sourceTaskId','auxiliary_delivery_required');
      if(source.execution?.workKind==='repair' && (consumer.execution?.readOnlySource || consumer.stage==='verify'))
        reject('taskId','repair_requires_writable_consumer');
      if(action.kind==='prepare_integration' && (source.execution?.workKind!=='repair' || consumer.stage!=='integrate'))
        reject('kind','integration_requires_accepted_repair');
      // deferred_handoffs has a UNIQUE producer and a NOT NULL consumer. The
      // applier finds that producer without a state filter: even an applied
      // reservation cannot be redirected. Do not guess a new owner from state,
      // source_task_id (the earlier baseline), or the producer's acceptance.
      // Legacy catalogs without this directory cannot establish availability;
      // keep their prior validation and the unchanged live authority checks.
      const reserved=(Array.isArray(snapshot.deferredDeliveries)?snapshot.deferredDeliveries:[])
        .filter((h:any)=>h?.producer_task_id===action.sourceTaskId);
      if(reserved.some((h:any)=>typeof h.consumer_task_id!=='string'||!z.uuid().safeParse(h.consumer_task_id).success))
        planningError('PLANNING_HANDOFF_BINDING_UNVERIFIED',['actions',index,'taskId'],'handoff_consumer_unverified');
      if(reserved.some((h:any)=>h.consumer_task_id!==action.taskId))
        planningError('PLANNING_HANDOFF_CONSUMER_RESERVED',['actions',index,'taskId'],'handoff_consumer_reserved');
      const planned=handoffConsumers.get(action.sourceTaskId);
      if(planned!==undefined&&planned!==action.taskId)
        planningError('PLANNING_HANDOFF_CONSUMER_RESERVED',['actions',index,'taskId'],'handoff_consumer_conflict_in_plan');
      handoffConsumers.set(action.sourceTaskId,action.taskId);
    }
    const key=action.taskId+':'+action.sourceTaskId;
    if(actionKeys.has(key))reject('taskId','duplicate_delivery');
    actionKeys.add(key);
  }
  if(!plan.tasks.length && !plan.dependencyChanges.length && !plan.actions.length){
    const obligations=snapshot.tasks.filter((t:any)=>t.execution?.workKind!=='planning' &&
      !['diagnostic','repair'].includes(t.execution?.workKind) &&
      !['accepted','cancelled','superseded'].includes(t.state));
    if(!obligations.length || obligations.some((t:any)=>(t.criteria??[]).some((c:any)=>
      !plan.blockers.some(b=>b.taskId===t.id && b.criterionId===c.id))))throw Error('PLANNING_NO_WORK_UNCOVERED');
  }
  return plan;
}
/** A dated policy hint is not quota or execution authority. Legacy catalogs
 * lack this field and keep exactly their previous semantic validation. */
export function scopedPlanningLaneHints(policy:any,observedAt:string){
  const target=(value:any,fallback:number)=>Number.isInteger(value)&&value>=0?value:fallback;
  return {version:1,authority:'advisory',observedAt,
    lanes:{glm:{enabled:policy?.enabled===true&&!!policy.glmAccountId,targetSessions:target(policy?.glmTarget,12)},
      muse:{enabled:policy?.enabled===true&&target(policy?.museTarget,0)>0,targetSessions:target(policy?.museTarget,0)}},
    instruction:'Do not propose a disabled lane. Explicitly choose a currently useful enabled lane or report the blocked item. Targets are not free slots; quota, accounts and authority are rechecked at application.'};
}
/** Effect-free feedback shared by the in-session CLI and controller tests.
 * Preserve the authoritative validator's ordering, codes and sanitization. */
export function scopedPlanPreflight(raw:unknown,request:any){
  const identity={contractVersion:scopedPlanContractVersion,requestId:request.id,
    sourceRevision:request.source_revision,scope:request.scope,liveAuthorization:false,effects:'none'};
  try{
    const plan=validateScopedPlan(raw,request);
    const warnings=plan.tasks.flatMap((task,index)=>request.snapshot?.laneAvailability?.version===1 &&
      request.snapshot.laneAvailability.authority==='advisory' && request.snapshot.laneAvailability.lanes?.[task.lane]?.enabled===false
      ?[{path:['tasks',index,'lane'],code:'PLANNING_LANE_DISABLED_AT_SNAPSHOT',lane:task.lane,
        nextAction:'Correct this draft item explicitly before publishing. The lane was disabled at the frozen snapshot; this warning grants no route or quota and never changes the lane automatically.'}]:[]);
    return {...identity,validAgainstFrozenCatalog:true,code:'PLANNING_PREFLIGHT_VALID',...(warnings.length?{warnings,advisoryReadyToPublish:false}:{}),
      counts:{tasks:plan.tasks.length,actions:plan.actions.length,dependencyChanges:plan.dependencyChanges.length,blockers:plan.blockers.length},
      nextAction:warnings.length?'Correct the disabled-lane items explicitly before publishing; consult laneAvailability. The contract is valid but this draft has an actionable capacity warning.': 'Publish the checked scope-plan.json through the normal delivery contract. This receipt is not acceptance or current authorization.'};
  }catch(error){
    return {...identity,validAgainstFrozenCatalog:false,...scopedPlanningRejection(error)};
  }
}
/** Native planning publishes a structured plan, not a regression suite. */
export function verifyScopedPlanningDelivery(result:any):Array<{name:string;content:Buffer;mime:string}>{
  if(!Array.isArray(result?.artifacts) || result.artifacts.length>30)throw Error('PLANNING_DELIVERY_INVALID');
  const seen=new Set<string>();let bytes=0;
  const artifacts:Array<{name:string;content:Buffer;mime:string}>=result.artifacts.map((a:any)=>{
    if(a.name==='runtime-plan-check.json' || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(a.name??'') || seen.has(a.name) ||
      typeof a.base64!=='string')throw Error('PLANNING_DELIVERY_INVALID');
    seen.add(a.name);const content=Buffer.from(a.base64,'base64');bytes+=content.length;
    if(content.length>8*1024*1024 || bytes>24*1024*1024 || content.toString('base64')!==a.base64 ||
      createHash('sha256').update(content).digest('hex')!==a.digest)throw Error('ARTIFACT_CORRUPT');
    return {name:a.name,content,mime:a.mime??'application/octet-stream'};
  });
  const plan=artifacts.find((a:any)=>a.name==='scope-plan.json');
  if(!plan)throw Error('PLANNING_DELIVERY_INVALID');
  // Semantic validation is bound to the frozen request by the native consumer.
  // Preserve the original bytes before rejecting an invalid proposal.
  return artifacts;
}
