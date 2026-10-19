import {it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {validateScopedPlan,scopedPlanningRejection,scopedPlanPreflight} from '../server/scoped-plan-contract.js';

function fixture(){
  const source=randomUUID(),target=randomUUID(),sourceRun=randomUUID(),artifact=randomUUID(),scope='project/component';
  const tasks=[{id:source,state:'accepted',stage:'work',attempt_count:1,last_run_id:sourceRun,
    last_run_state:'submitted',last_run_attempt:1,acceptance_passed:true,has_live_run:false,
    execution:{baseWorkspace:scope,workKind:'repair'},dependencies:[],criteria:[]},
  {id:target,state:'recovery_blocked',stage:'integrate',attempt_count:2,has_live_run:false,
    execution:{baseWorkspace:scope,workKind:undefined},dependencies:[],criteria:[{id:'verified'}]}];
  const request={id:randomUUID(),scope,source_revision:'a'.repeat(64),snapshot:{tasks,artifacts:[{id:artifact,task_id:source}],deferredDeliveries:[] as any[]}};
  const action={kind:'prepare_integration',taskId:target,expectedAttempt:2,sourceTaskId:source,
    expectedSourceRunId:sourceRun,artifactIds:[artifact],reason:'Consume the accepted repair and preserve final verification.'};
  const plan={version:1,requestId:request.id,sourceRevision:request.source_revision,
    reason:'Existing accepted repair can be delivered without duplicate implementation.',tasks:[],blockers:[],actions:[action]};
  return {request,plan,action,tasks};
}
it('accepts a version-bound existing repair delivery without inventing another task',()=>{
  const f=fixture();expect(validateScopedPlan(f.plan,f.request).actions).toEqual([f.action]);
});
it.each(['source_run','target_attempt','artifact','scope','unaccepted','live_source','readonly','duplicate'])('rejects unsafe delivery %s',kind=>{
  const f=fixture();
  if(kind==='source_run')f.action.expectedSourceRunId=randomUUID();
  if(kind==='target_attempt')f.action.expectedAttempt++;
  if(kind==='artifact')f.action.artifactIds=[randomUUID()];
  if(kind==='scope')f.tasks[0].execution.baseWorkspace='project/other';
  if(kind==='unaccepted')f.tasks[0].acceptance_passed=false;
  if(kind==='live_source')f.tasks[0].has_live_run=true;
  if(kind==='readonly')Object.assign(f.tasks[1].execution,{readOnlySource:true});
  if(kind==='duplicate')f.plan.actions.push({...f.action});
  expect(()=>validateScopedPlan(f.plan,f.request)).toThrow('PLANNING_ACTION_INVALID');
});
it('requires the actual readonly QA and its accepted dependency when requesting verification',()=>{
  const f=fixture();f.action.kind='request_verification';
  expect(()=>validateScopedPlan(f.plan,f.request)).toThrow('PLANNING_ACTION_INVALID');
  f.tasks[1].stage='verify';Object.assign(f.tasks[1].execution,{readOnlySource:true});
  f.tasks[1].dependencies=[f.tasks[0].id] as never[];
  expect(validateScopedPlan(f.plan,f.request).actions[0].kind).toBe('request_verification');
  f.tasks[0].acceptance_passed=false;
  expect(()=>validateScopedPlan(f.plan,f.request)).toThrow('PLANNING_ACTION_INVALID');
});
it('preserves the empty-plan coverage requirement and sanitizes action errors',()=>{
  const f=fixture();f.plan.actions=[];
  expect(()=>validateScopedPlan(f.plan,f.request)).toThrow('PLANNING_NO_WORK_UNCOVERED');
  f.plan.actions=[f.action];(f.action as any).expectedSourceRunId='untrusted-private-value';
  try{validateScopedPlan(f.plan,f.request);throw Error('expected invalid');}catch(error){
    const feedback=scopedPlanningRejection(error);
    expect(JSON.stringify(feedback)).not.toContain('untrusted-private-value');
    expect(feedback.issues[0].path).toEqual(['actions',0,'expectedSourceRunId']);
  }
});

it.each(['reuse_existing','prepare_integration'])('refuses %s to another consumer already reserved in the frozen catalog',kind=>{
  const f=fixture(),reservedConsumer=randomUUID();f.action.kind=kind;
  f.request.snapshot.deferredDeliveries=[{id:randomUUID(),producer_task_id:f.action.sourceTaskId,
    consumer_task_id:reservedConsumer,source_task_id:randomUUID(),state:'applied'}];
  const before=JSON.stringify(f),response=scopedPlanPreflight(f.plan,f.request);
  expect(()=>validateScopedPlan(f.plan,f.request)).toThrow('PLANNING_HANDOFF_CONSUMER_RESERVED');
  expect(response).toMatchObject({validAgainstFrozenCatalog:false,liveAuthorization:false,effects:'none',
    code:'PLANNING_HANDOFF_CONSUMER_RESERVED',issues:[{path:['actions',0,'taskId'],code:'handoff_consumer_reserved'}]});
  expect(response.nextAction).toContain('producer_task_id');
  expect(JSON.stringify(response)).not.toContain(reservedConsumer);
  expect(JSON.stringify(f)).toBe(before);
});

it.each(['waiting_result','staged','applied','replanning_required','legacy_terminal'])
  ('preserves reservation ownership in %s without inferring current application eligibility',state=>{
    const f=fixture();f.request.snapshot.deferredDeliveries=[{id:randomUUID(),producer_task_id:f.action.sourceTaskId,
      consumer_task_id:f.action.taskId,source_task_id:randomUUID(),state}];
    // Same consumer is not a reservation conflict. The live applier retains
    // its existing state, source/manifest, stop, cancellation and retry gates.
    expect(validateScopedPlan(f.plan,f.request).actions).toEqual([f.action]);
    f.request.snapshot.deferredDeliveries[0].consumer_task_id=randomUUID();
    expect(()=>validateScopedPlan(f.plan,f.request)).toThrow('PLANNING_HANDOFF_CONSUMER_RESERVED');
  });

it.each([null,undefined,'untrusted-private-binding'])('does not fabricate a consumer from an incomplete reserved binding (%s)',consumer=>{
  const f=fixture();f.request.snapshot.deferredDeliveries=[{id:randomUUID(),producer_task_id:f.action.sourceTaskId,
    ...(consumer!==undefined?{consumer_task_id:consumer}:{}),source_task_id:f.action.taskId,state:'applied'}];
  const result=scopedPlanPreflight(f.plan,f.request);
  expect(result).toMatchObject({validAgainstFrozenCatalog:false,code:'PLANNING_HANDOFF_BINDING_UNVERIFIED',
    issues:[{path:['actions',0,'taskId'],code:'handoff_consumer_unverified'}]});
  expect(JSON.stringify(result)).not.toContain('untrusted-private-binding');
});

it('keeps legacy catalogs and unreserved producers subject to existing live authority checks',()=>{
  const f=fixture();expect(scopedPlanPreflight(f.plan,f.request)).toMatchObject({validAgainstFrozenCatalog:true,liveAuthorization:false});
  const {deferredDeliveries,...legacy}=f.request.snapshot;
  expect(validateScopedPlan(f.plan,{...f.request,snapshot:legacy}).actions).toEqual([f.action]);
  f.request.snapshot.deferredDeliveries=[{id:randomUUID(),producer_task_id:randomUUID(),consumer_task_id:randomUUID(),state:'applied'}];
  expect(validateScopedPlan(f.plan,f.request).actions).toEqual([f.action]);
});

it.each([
  ['reuse_existing','reuse_existing'],['reuse_existing','prepare_integration'],
  ['prepare_integration','reuse_existing'],['prepare_integration','prepare_integration'],
])('rejects conflicting reservations in one plan: %s then %s',(first,second)=>{
  const f=fixture(),other={...f.tasks[1],id:randomUUID()};f.tasks.push(other);
  f.action.kind=first;f.plan.actions.push({...f.action,kind:second,taskId:other.id});
  const original=JSON.stringify(f.plan),result=scopedPlanPreflight(f.plan,f.request);
  expect(result).toMatchObject({validAgainstFrozenCatalog:false,code:'PLANNING_HANDOFF_CONSUMER_RESERVED',
    issues:[{path:['actions',1,'taskId'],code:'handoff_consumer_conflict_in_plan'}]});
  expect(JSON.stringify(f.plan)).toBe(original);
  // Correct only the second invalid proposal; the valid first action survives.
  f.plan.actions.pop();expect(validateScopedPlan(f.plan,f.request).actions).toEqual([f.action]);
});

it.each([false,true])('does not reserve request_verification as a handoff (verification first: %s)',verificationFirst=>{
  const f=fixture(),qa={...f.tasks[1],id:randomUUID(),stage:'verify',dependencies:[f.action.sourceTaskId],
    execution:{...f.tasks[1].execution,readOnlySource:true}};
  f.tasks.push(qa as any);
  const verify={...f.action,kind:'request_verification',taskId:qa.id};
  f.request.snapshot.deferredDeliveries=[{id:randomUUID(),producer_task_id:f.action.sourceTaskId,
    consumer_task_id:f.action.taskId,state:'applied'}];
  f.plan.actions=verificationFirst?[verify,f.action]:[f.action,verify];
  expect(validateScopedPlan(f.plan,f.request).actions).toEqual(f.plan.actions);
});

it('reserves different producers independently without blocking useful parallel actions',()=>{
  const f=fixture(),source={id:randomUUID(),state:'accepted',stage:'work',attempt_count:1,last_run_id:randomUUID(),
    last_run_state:'submitted',last_run_attempt:1,acceptance_passed:true,has_live_run:false,
    execution:{baseWorkspace:f.request.scope,workKind:'repair'},dependencies:[],criteria:[]},
    consumer={...f.tasks[1],id:randomUUID()},artifact=randomUUID();
  f.tasks.push(source,consumer);f.request.snapshot.artifacts.push({id:artifact,task_id:source.id});
  f.plan.actions.push({...f.action,taskId:consumer.id,sourceTaskId:source.id,expectedSourceRunId:source.last_run_id!,artifactIds:[artifact]});
  expect(validateScopedPlan(f.plan,f.request).actions).toHaveLength(2);
});
