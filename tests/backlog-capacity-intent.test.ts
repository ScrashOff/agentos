import {beforeAll,beforeEach,afterAll,afterEach,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner,applyBacklog,reconcileBacklogCapacityIntents} from '../server/backlog.js';
import {installManagement,managementSnapshot} from '../server/management.js';
import {installSupervision} from '../server/supervision.js';
import {installDeliveryConsumptions} from '../server/delivery-consumption.js';
import {writeArtifact,readArtifact,completeRun,validateTask} from '../server/service.js';
import {accountBridge} from '../server/bridge.js';
import {deliveryDigest} from '../server/delivery-supervision.js';
import {readDeliverySourceIdentity,sourceIdentityAccepted,sourceIdentityFrozen} from '../server/delivery-source-resolution.js';
import {backlogProposalSchema} from '../server/planning-remainder-contract.js';
import {packManagerRecords} from '../server/manager-context.js';
import {captureBacklogCapacityIntent,checkBacklogCapacityIntent,hasCapacityIntentPriority,projectBacklogCapacityReceipts} from '../server/backlog-capacity-intent.js';
import {readWorkforceReserve} from '../server/workforce-reserve.js';
import * as backlogScopes from '../server/backlog-scopes.js';
import {installWorkspacePreparation,requestWorkspacePreparation} from '../server/workspace-preparation.js';
import {installPlanningDemand,assignPlanningRequest,readScopedPlannerCapacity,scopeSnapshot,scopeSourceRevision} from '../server/planning-demand.js';
import {hash} from '../server/memory.js';
import {readManagementFollowThrough} from '../server/management-follow-through.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installBacklogPlanner();await installSupervision();await installManagement();await installDeliveryConsumptions();await installWorkspacePreparation();await installPlanningDemand();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='autonomy:capacity-intent-test'");
  vi.spyOn(accountBridge,'request').mockRejectedValue(Error('NO_EXTERNAL_PROVIDER_ALLOWED'));
});
afterEach(()=>vi.restoreAllMocks());
afterAll(async()=>{await pool.end();});

async function fixture(withCase=true,proposalMode:'normal'|'source-alias'|'base-only'='normal',largeProposals=false,proposalLane:'glm'|'muse'='glm'){
  const goal=randomUUID(),consumer=randomUUID(),consumerRun=randomUUID(),source=randomUUID(),sourceRun=randomUUID(),
    coordinator=randomUUID(),reviewRun=randomUUID(),sourceFence=randomUUID(),reviewFence=randomUUID(),caseId=randomUUID(),
    obligationId=randomUUID(),handoffId=randomUUID();
  const scope='capacity-intent/game';
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Deliver exact repair','Preserve final QA','capacity-intent-test',$2)",[goal,goal]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('capacity-glm','GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'capacity-glm')`);
  await pool.query('INSERT INTO settings VALUES($1,$2)',[
    'workforce:'+goal,JSON.stringify({enabled:true,glmAccountId:'capacity-glm',glmTarget:12,museTarget:0,workerAccounts:[]})]);
  for(const key of ['management:'+goal,'supervision:'+goal,'autonomy:capacity-intent-test'])
    await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":true}')",[key]);
  const execution=(id:string,workKind?:string)=>({kind:'opencode',baseWorkspace:scope,workspace:'capacity-intent/'+id,...(workKind?{workKind}:{})});
  const criteria=[{id:'final-proof',type:'json_field',name:'verification.json',field:'passed',equals:true,label:'Actual final evidence'}];
  for(const [id,stage,state,taskCriteria,taskExecution] of [
    [consumer,'integrate','recovery_blocked',criteria,execution(consumer)],
    [source,'work','running',[],execution(source,'diagnostic')],
  ] as const)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,$3,'Original task','Preserved substantive contract','engineering.integration',$4,$5,$6,$7,1)`,
    [id,goal,id,state,stage,JSON.stringify(taskCriteria),JSON.stringify(taskExecution)]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'failed')",[consumerRun,consumer,randomUUID()]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[sourceRun,source,sourceFence]);
  await writeArtifact(sourceRun,sourceFence,'diagnostic.json',JSON.stringify({result:'Exact diagnostic evidence',passed:false}));
  await completeRun(sourceRun,sourceFence,'Accepted diagnostic, no final QA claim',{});
  expect((await validateTask(source)).passed).toBe(true);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  const identity=await readDeliverySourceIdentity(pool,source);
  expect(sourceIdentityAccepted(identity)).toBe(true);expect(sourceIdentityFrozen(identity)).toBe(true);
  const manifest=(await pool.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[sourceRun])).rows;
  const observed={taskId:source,runId:sourceRun,contractDigest:identity.contract_digest,manifestDigest:deliveryDigest(manifest),
    snapshotSchema:'1',currentAccepted:true,currentFrozen:true};
  const conflictBody={version:1,code:'SOURCE_RECOVERY_CONTEXT_CHANGED',original:{taskId:source,runId:sourceRun},observed};
  const sourceConflict={...conflictBody,generation:deliveryDigest(conflictBody)};
  const evidence=[{kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId,sourceConflict}];
  await pool.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,source_proof,consumer_proof,handoff,state)
    VALUES($1,$2,$3,$4,$3,'{}','{}','{}','applied')`,[handoffId,goal,source,consumer]);
  await pool.query(`INSERT INTO delivery_consumptions(id,handoff_id,goal_id,producer_task_id,producer_run_id,consumer_task_id,
    scope,consumer_contract_digest,producer_manifest_digest,binding,state,receipt)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'{}','waiting_supervision',$10)`,
    [obligationId,handoffId,goal,source,sourceRun,consumer,scope,deliveryDigest(criteria),deliveryDigest(manifest),
      JSON.stringify({generation:deliveryDigest({obligationId}),sourceConflict})]);
  if(withCase)await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
    VALUES($1,$2,$3,$4,'open','Repair the exact rejected evidence',$5)`,[caseId,goal,consumer,consumerRun,JSON.stringify(evidence)]);
  const consumerRow=(await pool.query('SELECT * FROM tasks WHERE id=$1',[consumer])).rows[0];
  const focal={id:consumer,title:consumerRow.title,state:consumerRow.state,stage:consumerRow.stage,
    baseWorkspace:scope,workspace:consumerRow.execution.workspace,expectedAttempt:1,live:false,
    criteria:consumerRow.criteria,execution:consumerRow.execution,description:consumerRow.description,
    instructionRevisionId:consumerRow.instruction_revision_id,currentRun:{id:consumerRun,attempt:1,state:'failed'}};
  const snapshot={cases:withCase?[{id:caseId,taskId:consumer,scope,currentRun:{id:consumerRun,attempt:1,state:'failed'},evidence}]:[],
    operation:{focus:{...(withCase?{caseId}:{}),taskId:consumer,scope},tasks:[{id:consumer,$rowOf:'operation.focusedTaskDirectory.rows',index:0}],
      focusedTaskDirectory:packManagerRecords([focal]),workOpportunities:{acceptedSources:[],selection:{truncated:true}},
      decisionFeedback:{entries:[{caseId,sourceTaskId:source,expectedSourceRunId:sourceRun}]}}};
  const proposal=backlogProposalSchema.parse({coverageKey:'repair-frozen-boundary',title:'Repair exact failed boundary',
    description:'Implement the bounded repair from the frozen accepted diagnostic and preserve exact final integration and QA criteria.',
    baseWorkspace:proposalMode==='source-alias'?'capacity-intent/'+source:scope,deliverToTaskId:consumer,
    workKind:proposalMode==='base-only'?'diagnostic':'repair',
    ...(proposalMode==='base-only'?{}:{sourceTaskId:source}),lane:proposalLane,delivery:proposalMode==='base-only'?'immediate':'deferred'});
  const proposals=largeProposals?Array.from({length:12},(_,index)=>({...proposal,coverageKey:'capacity-large-'+index,
    title:('Proposal '+index+' ').repeat(20).slice(0,180),description:('Unique instruction '+index+' ').repeat(900).slice(0,12000)})):[proposal];
  const output={summary:'Execute the exact repair after available capacity',capacityReason:'The shared lane is full',actions:[],
    contextRequests:[{kind:'task_metadata',taskId:source,expectedAttempt:1,offset:0}],
    backlog:{reason:'Missing real repair against the accepted source',tasks:proposals}};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Accepted manager decision',$4,'research.requirements','running','coordinate',$5,1)`,
    [coordinator,goal,coordinator,'MANAGEMENT_V1\n'+JSON.stringify(snapshot),JSON.stringify([{id:'decision',type:'artifact',name:'coordination.json'}])]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[reviewRun,coordinator,reviewFence]);
  await writeArtifact(reviewRun,reviewFence,'coordination.json',JSON.stringify(output));
  await completeRun(reviewRun,reviewFence,'Accepted exact coordination',{});
  expect((await validateTask(coordinator)).passed).toBe(true);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  await pool.query(`INSERT INTO management_reviews(run_id,goal_id,summary,capacity_reason,receipts)
    VALUES($1,$2,'Accepted exact coordination','Shared capacity unavailable','[]')`,[reviewRun,goal]);
  const capture=()=>captureBacklogCapacityIntent(pool,goal,reviewRun,0,proposal,readArtifact);
  return {goal,consumer,consumerRun,source,sourceRun,coordinator,reviewRun,caseId,obligationId,scope,snapshot,proposal,proposals,capture};
}

it('projects twelve large durable proposals at the actual manager boundary without changing their authority or bytes',async()=>{
  const f=await fixture(true,'normal',true);
  const ref=(await pool.query("SELECT id,digest FROM artifacts WHERE run_id=$1 AND name='coordination.json'",[f.reviewRun])).rows[0];
  const receipts=[];
  for(let index=0;index<12;index++){
    const captured=await captureBacklogCapacityIntent(pool,f.goal,f.reviewRun,index,f.proposals[index],readArtifact);
    expect(captured.ok).toBe(true);if(!captured.ok)throw Error(captured.code);
    receipts.push({index,code:'PENDING_LIMIT',status:'rejected',coverageKey:f.proposals[index].coverageKey,capacityIntent:captured.intent});
  }
  const original=structuredClone(receipts),serialized=JSON.stringify(receipts);
  expect(serialized.length).toBeGreaterThan(144000);
  const projection=projectBacklogCapacityReceipts(receipts);
  expect(receipts).toEqual(original);expect(projection).toHaveLength(12);
  expect(JSON.stringify(projection).length).toBeLessThan(32000);
  for(const [index,row]of projection.entries()){
    const before=original[index].capacityIntent;
    const {proposal,...metadata}=before;
    expect(row).toMatchObject({index,code:'PENDING_LIMIT',status:'rejected',capacityIntent:metadata});
    expect(row.capacityIntent.binding).toEqual(before.binding);
    expect(row.capacityIntent.proposal).toMatchObject({detailsProjected:true,characters:JSON.stringify(proposal).length,
      valueSha256:deliveryDigest(proposal),summary:{lane:'glm',workKind:'repair',titleProjected:true},
      auditSource:{artifactId:ref.id,artifactDigest:ref.digest,reviewRunId:f.reviewRun,path:'/api/artifacts/'+ref.id,jsonPointer:'/backlog/tasks/'+index}});
    expect(row.capacityIntent.proposal).not.toHaveProperty('description');
  }
  await pool.query(`INSERT INTO workforce_backlog_plans(run_id,goal_id,state,reason,receipts,task_ids)
    VALUES($1,$2,'applied','Capacity intent fixture',$3,'[]')`,[f.reviewRun,f.goal,serialized]);
  const policy=(await pool.query('SELECT value FROM settings WHERE id=$1',['workforce:'+f.goal])).rows[0].value;
  const actual=await managementSnapshot(pool,f.goal,policy,{scope:f.scope,caseId:f.caseId,taskId:f.consumer});
  expect(actual.backlog?.receipts).toEqual(projection);
  const persisted=(await pool.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1',[f.reviewRun])).rows[0].receipts;
  expect(persisted).toEqual(original);
  const artifact=await readArtifact(ref.id),advice=JSON.parse(artifact.bytes.toString());
  expect(hash(artifact.bytes)).toBe(ref.digest);
  for(const row of projection){
    const recovered=backlogProposalSchema.parse(advice.backlog.tasks[row.index]);
    expect(deliveryDigest(recovered)).toBe(row.capacityIntent.proposalDigest);
    expect(recovered).toEqual(original[row.index].capacityIntent.proposal);
  }
  expect(projectBacklogCapacityReceipts([{index:0,code:'CONSUMER_CHANGED',status:'rejected'}])).toEqual([{index:0,code:'CONSUMER_CHANGED',status:'rejected'}]);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('captures an accepted frozen decision even when its source is absent from the bounded catalog',async()=>{
  const f=await fixture(),captured=await f.capture();
  expect(captured.ok).toBe(true);if(!captured.ok)throw Error(captured.code);
  expect(await checkBacklogCapacityIntent(pool,f.goal,captured.intent)).toMatchObject({ok:true});
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it.each(['run','attempt','manifest','contract','acceptance','review-acceptance','consumer','consumer-description','goal','episode'])('refuses current %s drift after capturing without allocating work',async(change)=>{
  const f=await fixture(),captured=await f.capture();
  expect(captured.ok).toBe(true);if(!captured.ok)throw Error(captured.code);
  const before=(await pool.query('SELECT count(*)::int n FROM tasks WHERE goal_id=$1',[f.goal])).rows[0].n;
  if(change==='run'||change==='attempt'){
    await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.source]);
    if(change==='run')await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'opencode',1,'submitted')",[randomUUID(),f.source,randomUUID()]);
  }
  if(change==='manifest')await pool.query("UPDATE artifacts SET digest=$2 WHERE run_id=$1",[f.sourceRun,'e'.repeat(64)]);
  if(change==='contract')await pool.query("UPDATE tasks SET description='A changed substantive contract' WHERE id=$1",[f.source]);
  if(change==='acceptance')await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,false,'[]','fixture',clock_timestamp()+interval '1 second')`,[randomUUID(),f.source,f.sourceRun]);
  if(change==='review-acceptance')await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,false,'[]','fixture',clock_timestamp()+interval '1 second')`,[randomUUID(),f.coordinator,f.reviewRun]);
  if(change==='consumer')await pool.query("UPDATE tasks SET criteria=$2 WHERE id=$1",[f.consumer,JSON.stringify([{id:'different',type:'artifact',name:'new.json'}])]);
  if(change==='consumer-description')await pool.query("UPDATE tasks SET description='New instruction without a revision change' WHERE id=$1",[f.consumer]);
  if(change==='episode')await pool.query("UPDATE delivery_consumptions SET receipt=jsonb_set(receipt,'{sourceConflict,generation}',to_jsonb($2::text)) WHERE id=$1",[f.obligationId,'d'.repeat(64)]);
  const checked=await checkBacklogCapacityIntent(pool,change==='goal'?randomUUID():f.goal,captured.intent);
  expect(checked.ok).toBe(false);
  expect((await pool.query('SELECT count(*)::int n FROM tasks WHERE goal_id=$1',[f.goal])).rows[0].n).toBe(before);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('does not require a historic accepted review to remain the current coordinator attempt',async()=>{
  const f=await fixture();
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.coordinator]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'failed')",[randomUUID(),f.coordinator,randomUUID()]);
  const captured=await f.capture();expect(captured.ok).toBe(true);
});

it('rejects altered accepted artifact bytes or proposal identity and does not infer another source',async()=>{
  const f=await fixture();
  const wrong=await captureBacklogCapacityIntent(pool,f.goal,f.reviewRun,0,{...f.proposal,coverageKey:'renamed-decision'},readArtifact);
  expect(wrong.ok).toBe(false);
  await pool.query("UPDATE artifacts SET digest=$2 WHERE run_id=$1 AND name='coordination.json'",[f.reviewRun,'f'.repeat(64)]);
  expect((await f.capture()).ok).toBe(false);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

const reservePolicy={enabled:true,glmTarget:12,museTarget:0};
async function fillReserve(goal:string,scope:string,count=24){
  const ids:string[]=[];
  for(let i=0;i<count;i++){
    const id=randomUUID();ids.push(id);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
      VALUES($1,$2,$3,'Distinct waiting worker','Preserve the existing work','qa.backend','ready','work','[]',$4)`,
      [id,goal,id,JSON.stringify({kind:'opencode',workKind:i%2?'planning':'diagnostic',baseWorkspace:scope,workspace:'capacity-intent/'+id})]);
  }
  return ids;
}
const planRow=async(run:string)=>(await pool.query('SELECT * FROM workforce_backlog_plans WHERE run_id=$1',[run])).rows[0];

it('persists a full-reserve rejection without preparing a clone, then admits exactly once when capacity and preparation are ready',async()=>{
  const f=await fixture(false),existing=await fillReserve(f.goal,f.scope);
  expect(await readWorkforceReserve(pool,f.goal,'glm',reservePolicy)).toEqual({pending:24,limit:24,available:false});
  expect(await applyBacklog(f.goal,f.reviewRun,{reason:'Execute the accepted focal repair',tasks:[f.proposal]},
    {durablePreparation:true})).toEqual([]);
  const original=(await planRow(f.reviewRun)).receipts[0];expect(original.code).toBe('PENDING_LIMIT');
  await applyBacklog(f.goal,f.reviewRun,{reason:'Repeat delivery of the same accepted coordination',tasks:[f.proposal]},
    {durablePreparation:true});
  expect((await planRow(f.reviewRun)).receipts[0]).toEqual(original);
  const sibling={index:1,coverageKey:'preserved-sibling',lane:'glm',status:'ignored',code:'COVERAGE_ALREADY_PLANNED',taskId:existing[1]};
  await pool.query('UPDATE workforce_backlog_plans SET receipts=receipts||$2::jsonb WHERE run_id=$1',[f.reviewRun,JSON.stringify([sibling])]);
  await reconcileBacklogCapacityIntents();await reconcileBacklogCapacityIntents();
  let plan=await planRow(f.reviewRun);
  expect(plan.receipts[0]).toMatchObject({...original,capacityIntent:{version:1,state:'waiting_capacity',binding:{mode:'initial'}}});
  expect(plan.task_ids).toEqual([]);
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
  await pool.query("UPDATE tasks SET state='cancelled' WHERE id=$1",[existing[0]]);
  await reconcileBacklogCapacityIntents();
  plan=await planRow(f.reviewRun);expect(plan.receipts[0].capacityIntent.state).toBe('preparing');
  const prep=(await pool.query('SELECT * FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rows;
  expect(prep).toHaveLength(1);expect(prep[0].state).toBe('pending');
  expect(await readWorkforceReserve(pool,f.goal,'glm',reservePolicy)).toEqual({pending:23,limit:24,available:true});
  // A prepared copy is external to this test; only its durable handoff is supplied.
  await pool.query("UPDATE workspace_preparations SET state='prepared',receipt='{\"confirmed\":true}' WHERE id=$1",[prep[0].id]);
  await Promise.all([reconcileBacklogCapacityIntents(),reconcileBacklogCapacityIntents()]);
  await reconcileBacklogCapacityIntents();
  plan=await planRow(f.reviewRun);
  expect(plan.task_ids).toHaveLength(1);
  expect(plan.receipts[1]).toEqual(sibling);
  expect(plan.receipts[0]).toMatchObject({...original,capacityIntent:{state:'admitted',code:'TASK_CREATED',taskId:plan.task_ids[0]}});
  expect((await pool.query('SELECT 1 FROM workforce_backlog_items WHERE goal_id=$1 AND coverage_key=$2',[f.goal,f.proposal.coverageKey])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='BACKLOG_CAPACITY_ADMITTED'",[f.goal])).rowCount).toBe(1);
  expect(await readWorkforceReserve(pool,f.goal,'glm',reservePolicy)).toEqual({pending:24,limit:24,available:false});
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('keeps the first preparation binding when its free slot fills, then admits once without a historical case',async()=>{
  const f=await fixture(false),existing=await fillReserve(f.goal,f.scope,23);
  expect(await applyBacklog(f.goal,f.reviewRun,{reason:'Prepare the already authorized repair',tasks:[f.proposal]},
    {durablePreparation:true})).toEqual([]);
  const original=(await planRow(f.reviewRun)).receipts[0];
  expect(original).toMatchObject({code:'WORKSPACE_PREPARING',capacityIntent:{state:'preparing',code:'WORKSPACE_PREPARING',binding:{mode:'initial'}}});
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  const preparation=(await pool.query('SELECT * FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rows;
  expect(preparation).toHaveLength(1);expect(preparation[0].state).toBe('pending');
  await fillReserve(f.goal,f.scope,1);
  expect(await readWorkforceReserve(pool,f.goal,'glm',reservePolicy)).toEqual({pending:24,limit:24,available:false});
  await pool.query("UPDATE workspace_preparations SET state='prepared',receipt='{\"confirmed\":true}' WHERE id=$1",[preparation[0].id]);
  await applyBacklog(f.goal,f.reviewRun,{reason:'Redeliver while the original copy is ready but reserve is full',tasks:[f.proposal]},
    {durablePreparation:true});
  await reconcileBacklogCapacityIntents();
  let plan=await planRow(f.reviewRun);
  expect(plan.receipts[0]).toEqual(original);expect(plan.task_ids).toEqual([]);
  await pool.query("UPDATE tasks SET state='cancelled' WHERE id=$1",[existing[0]]);
  await Promise.all([reconcileBacklogCapacityIntents(),reconcileBacklogCapacityIntents()]);
  await reconcileBacklogCapacityIntents();plan=await planRow(f.reviewRun);
  expect(plan.task_ids).toHaveLength(1);
  expect(plan.receipts[0]).toMatchObject({...original,capacityIntent:{state:'admitted',code:'TASK_CREATED',taskId:plan.task_ids[0]}});
  expect(plan.receipts[0].capacityIntent.binding).toEqual(original.capacityIntent.binding);
  expect(plan.receipts[0].capacityIntent.proposalDigest).toBe(original.capacityIntent.proposalDigest);
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM workforce_backlog_items WHERE goal_id=$1 AND coverage_key=$2',[f.goal,f.proposal.coverageKey])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='BACKLOG_CAPACITY_ADMITTED'",[f.goal])).rowCount).toBe(1);
  expect(await readWorkforceReserve(pool,f.goal,'glm',reservePolicy)).toEqual({pending:24,limit:24,available:false});
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('recovers a historical refusal only from its frozen episode and does not substitute current authority for missing history',async()=>{
  const f=await fixture();await fillReserve(f.goal,f.scope);
  const original={index:0,coverageKey:f.proposal.coverageKey,lane:'glm',sourceTaskId:f.source,
    consumerTaskId:f.consumer,status:'rejected',code:'PENDING_LIMIT'};
  await pool.query(`INSERT INTO workforce_backlog_plans(run_id,goal_id,state,reason,task_ids,validation_version,receipts)
    VALUES($1,$2,'rejected','Original historical refusal','[]',2,$3)`,[f.reviewRun,f.goal,JSON.stringify([original])]);
  await reconcileBacklogCapacityIntents();
  expect((await planRow(f.reviewRun)).receipts[0]).toMatchObject({...original,
    capacityIntent:{version:1,state:'waiting_capacity',binding:{mode:'episode',caseId:f.caseId}}});
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('keeps an unprovable historical refusal visible for revalidation without constructing a new current binding',async()=>{
  const f=await fixture(false);
  const original={index:0,coverageKey:f.proposal.coverageKey,lane:'glm',sourceTaskId:f.source,
    consumerTaskId:f.consumer,status:'rejected',code:'PENDING_LIMIT'};
  await pool.query(`INSERT INTO workforce_backlog_plans(run_id,goal_id,state,reason,task_ids,validation_version,receipts)
    VALUES($1,$2,'rejected','Original historical refusal','[]',2,$3)`,[f.reviewRun,f.goal,JSON.stringify([original])]);
  await reconcileBacklogCapacityIntents();
  const plan=await planRow(f.reviewRun);
  expect(plan.receipts[0]).toMatchObject({...original,capacityIntent:{version:1,state:'needs_revalidation'}});
  expect(plan.task_ids).toEqual([]);
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it.each([{}, {version:1}, {version:1,binding:{}}, {version:1,proposal:{},binding:{consumer:{},source:{}}}])(
  'rejects malformed durable intent without throwing or reserving capacity: %j',async raw=>{
    await expect(checkBacklogCapacityIntent(pool,randomUUID(),raw as never)).resolves.toMatchObject({ok:false,code:'CAPACITY_INTENT_INVALID'});
  });

it.each(['source-alias','base-only'] as const)('captures a newly authorized %s proposal without inventing historical source proof',async mode=>{
  const f=await fixture(false,mode);await fillReserve(f.goal,f.scope);
  await applyBacklog(f.goal,f.reviewRun,{reason:'Persist the exact newly accepted proposal',tasks:[f.proposal]},{durablePreparation:true});
  const plan=await planRow(f.reviewRun),intent=plan.receipts[0].capacityIntent;
  expect(plan.receipts[0].code).toBe('PENDING_LIMIT');
  expect(intent).toMatchObject({state:'waiting_capacity',binding:{mode:'initial'},proposal:f.proposal});
  expect(await checkBacklogCapacityIntent(pool,f.goal,intent)).toMatchObject({ok:true});
  if(mode==='base-only')expect(intent.binding.source).toBeNull();
  else expect(intent.binding.source).toMatchObject({taskId:f.source,runId:f.sourceRun,workspace:'capacity-intent/'+f.source});
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('reserves only the required free slot and ignores invalid or administratively disabled intents',async()=>{
  const f=await fixture(),captured=await f.capture();expect(captured.ok).toBe(true);if(!captured.ok)throw Error(captured.code);
  const receipt={index:0,coverageKey:f.proposal.coverageKey,lane:'glm',code:'PENDING_LIMIT',status:'rejected',capacityIntent:captured.intent};
  await pool.query(`INSERT INTO workforce_backlog_plans(run_id,goal_id,state,reason,task_ids,validation_version,receipts)
    VALUES($1,$2,'rejected','Shared reserve was full','[]',2,$3)`,[f.reviewRun,f.goal,JSON.stringify([receipt])]);
  expect(await hasCapacityIntentPriority(pool,f.goal,'glm',undefined,24)).toBe(false);
  expect(await hasCapacityIntentPriority(pool,f.goal,'glm',undefined,1)).toBe(true);
  await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['management:'+f.goal]);
  expect(await hasCapacityIntentPriority(pool,f.goal,'glm',undefined,1)).toBe(false);
  await pool.query("UPDATE settings SET value='{\"enabled\":true}' WHERE id=$1",['management:'+f.goal]);
  await pool.query("DELETE FROM settings WHERE id='autonomy:capacity-intent-test'");
  expect(await hasCapacityIntentPriority(pool,f.goal,'glm',undefined,1)).toBe(true);
  await pool.query("UPDATE tasks SET description='Changed immutable source binding' WHERE id=$1",[f.source]);
  expect(await hasCapacityIntentPriority(pool,f.goal,'glm',undefined,1)).toBe(false);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('marks a stale stored intent for revalidation before any workspace preparation and preserves the original refusal',async()=>{
  const f=await fixture(),existing=await fillReserve(f.goal,f.scope);
  await applyBacklog(f.goal,f.reviewRun,{reason:'Preserve accepted repair intent',tasks:[f.proposal]},{durablePreparation:true});
  await reconcileBacklogCapacityIntents();const original=(await planRow(f.reviewRun)).receipts[0];
  await pool.query("UPDATE tasks SET state='cancelled' WHERE id=$1",[existing[0]]);
  await pool.query("UPDATE tasks SET description='A different source contract now requires revalidation' WHERE id=$1",[f.source]);
  await reconcileBacklogCapacityIntents();
  const plan=await planRow(f.reviewRun);
  expect(plan.receipts[0].code).toBe(original.code);
  expect(plan.receipts[0].capacityIntent).toMatchObject({state:'needs_revalidation',code:'CAPACITY_SOURCE_CHANGED'});
  expect(plan.task_ids).toEqual([]);
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('refuses the real scoped planner before workspace or catalog preflight when the shared reserve has 24 workers',async()=>{
  const f=await fixture();await fillReserve(f.goal,f.scope);
  const request=randomUUID();
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot)
    VALUES($1,$2,$3,'glm',$4,'pending','DELIVERY_EVIDENCE_CHANGED','{}')`,[request,f.goal,f.scope,'a'.repeat(64)]);
  const capacity=await readScopedPlannerCapacity(pool,f.goal);
  expect(capacity.maxScopePlanners).toBeGreaterThan(0);expect(capacity.occupiedScopePlanners).toBe(0);
  expect(await assignPlanningRequest(request)).toBeNull();
  expect((await pool.query('SELECT state,task_id FROM planning_requests WHERE id=$1',[request])).rows[0]).toEqual({state:'pending',task_id:null});
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

async function independentPlanner(goal:string){
  const scope='capacity-intent/another-game',source=randomUUID(),consumer=randomUUID(),run=randomUUID(),fence=randomUUID(),id=randomUUID();
  for(const [task,stage,state,criteria,workKind] of [
    [source,'work','running',[],'diagnostic'],
    [consumer,'integrate','recovery_blocked',[{id:'final',type:'artifact',name:'verification.json'}],null],
  ] as const)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
    VALUES($1,$2,$3,'Independent scope task','Deliver another real criterion','qa.backend',$4,$5,$6,$7,$8)`,
    [task,goal,task,state,stage,JSON.stringify(criteria),task===source?1:0,
      JSON.stringify({kind:'opencode',baseWorkspace:scope,workspace:'capacity-intent/'+task,...(workKind?{workKind}:{})})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,source,fence]);
  await writeArtifact(run,fence,'diagnostic.json',JSON.stringify({finding:'Independent planner source'}));
  await completeRun(run,fence,'Independent accepted source',{});expect((await validateTask(source)).passed).toBe(true);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  const snapshot=await scopeSnapshot(pool,goal,scope),revision=await scopeSourceRevision(pool,goal,scope);
  expect(snapshot.opportunities.acceptedSources.some(row=>row.taskId===source)).toBe(true);
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot)
    VALUES($1,$2,$3,'glm',$4,'pending','DELIVERY_EVIDENCE_CHANGED',$5)`,[id,goal,scope,revision,JSON.stringify(snapshot)]);
  expect(await assignPlanningRequest(id)).toBeNull();
  const preparation=(await pool.query('SELECT * FROM workspace_preparations WHERE goal_id=$1 AND source_workspace=$2',[goal,'capacity-intent/'+source])).rows;
  expect(preparation).toHaveLength(1);
  await pool.query("UPDATE workspace_preparations SET state='prepared',receipt='{\"confirmed\":true}' WHERE id=$1",[preparation[0].id]);
  return id;
}

it('serializes the real planner and backlog admission against the same final reserve slot across independent scopes',async()=>{
  const f=await fixture(false),planner=await independentPlanner(f.goal);
  const workspace='capacity-intent/obg-'+hash(f.goal+':'+f.proposal.coverageKey).slice(0,18);
  expect(await requestWorkspacePreparation(f.goal,workspace,'capacity-intent/'+f.source)).toBe(false);
  await pool.query("UPDATE workspace_preparations SET state='prepared',receipt='{\"confirmed\":true}' WHERE goal_id=$1",[f.goal]);
  const existing=await fillReserve(f.goal,'capacity-intent/unrelated',23);
  expect(await readWorkforceReserve(pool,f.goal,'glm',reservePolicy)).toEqual({pending:23,limit:24,available:true});
  const [plannerTask,backlogTasks]=await Promise.all([
    assignPlanningRequest(planner),
    applyBacklog(f.goal,f.reviewRun,{reason:'Admit an independent accepted repair',tasks:[f.proposal]},{durablePreparation:true}),
  ]);
  expect(Number(!!plannerTask)+backlogTasks.length).toBe(1);
  expect(await readWorkforceReserve(pool,f.goal,'glm',reservePolicy)).toEqual({pending:24,limit:24,available:false});
  await pool.query("UPDATE tasks SET state='cancelled' WHERE id=$1",[existing[0]]);
  if(!plannerTask)expect(await assignPlanningRequest(planner)).toBeTypeOf('string');
  else {await reconcileBacklogCapacityIntents();expect((await planRow(f.reviewRun)).task_ids).toHaveLength(1);}
  expect(await readWorkforceReserve(pool,f.goal,'glm',reservePolicy)).toEqual({pending:24,limit:24,available:false});
  expect(accountBridge.request).not.toHaveBeenCalled();
});


it('keeps a mixed rejected sibling actionable even when another item has a durable capacity owner',async()=>{
  const f=await fixture(),captured=await f.capture();expect(captured.ok).toBe(true);if(!captured.ok)throw Error(captured.code);
  const waiting={index:0,coverageKey:f.proposal.coverageKey,lane:'glm',consumerTaskId:f.consumer,
    code:'PENDING_LIMIT',status:'rejected',capacityIntent:captured.intent};
  const rejected={index:1,coverageKey:'still-invalid',lane:'glm',consumerTaskId:f.consumer,
    code:'SOURCE_CHANGED',status:'rejected'};
  await pool.query(`INSERT INTO workforce_backlog_plans(run_id,goal_id,state,reason,task_ids,validation_version,receipts)
    VALUES($1,$2,'rejected','One wait and one unresolved item','[]',2,$3)`,[f.reviewRun,f.goal,JSON.stringify([waiting,rejected])]);
  expect(await readManagementFollowThrough(pool,f.goal,f.reviewRun)).toMatchObject({
    code:'MANAGEMENT_BACKLOG_REVALIDATION_REQUIRED',satisfied:false,owner:'supervisor'});
  await pool.query('UPDATE workforce_backlog_plans SET receipts=$2 WHERE run_id=$1',[f.reviewRun,JSON.stringify([waiting])]);
  expect(await readManagementFollowThrough(pool,f.goal,f.reviewRun)).toMatchObject({
    code:'MANAGEMENT_BACKLOG_CAPACITY_WAIT',satisfied:true,owner:'scheduler'});
  expect(accountBridge.request).not.toHaveBeenCalled();
});


it('keeps a disabled Muse lane a policy refusal without capturing a capacity wait or preparing a workspace',async()=>{
  const f=await fixture(false,'normal',false,'muse');
  expect(await applyBacklog(f.goal,f.reviewRun,{reason:'Respect the disabled lane',tasks:[f.proposal]},
    {durablePreparation:true})).toEqual([]);
  const plan=await planRow(f.reviewRun);
  expect(plan.receipts[0]).toMatchObject({index:0,lane:'muse',status:'rejected',code:'MUSE_LANE_DISABLED'});
  expect(plan.receipts[0]).not.toHaveProperty('capacityIntent');expect(plan.task_ids).toEqual([]);
  await reconcileBacklogCapacityIntents();
  expect((await planRow(f.reviewRun)).receipts).toEqual(plan.receipts);
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM workforce_backlog_items WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it('preserves the initial WORKFORCE_DISABLED refusal without recording a capacity intent',async()=>{
  const f=await fixture(false);
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{enabled}','false') WHERE id=$1",['workforce:'+f.goal]);
  await expect(applyBacklog(f.goal,f.reviewRun,{reason:'Do not override disabled workforce',tasks:[f.proposal]},
    {durablePreparation:true})).rejects.toThrow('WORKFORCE_DISABLED');
  expect(await planRow(f.reviewRun)).toBeUndefined();
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it.each(['workforce','muse'] as const)('rechecks %s policy under the admission lock before classifying capacity or creating preparation',async disabled=>{
  const f=await fixture(false,'normal',false,disabled==='muse'?'muse':'glm');
  if(disabled==='muse')await pool.query("UPDATE settings SET value=jsonb_set(value,'{museTarget}','2') WHERE id=$1",['workforce:'+f.goal]);
  const original=backlogScopes.readBacklogScopes;
  vi.spyOn(backlogScopes,'readBacklogScopes').mockImplementationOnce(async(db,goal)=>{
    const scopes=await original(db,goal);
    await pool.query(disabled==='muse'?"UPDATE settings SET value=jsonb_set(value,'{museTarget}','0') WHERE id=$1":
      "UPDATE settings SET value=jsonb_set(value,'{enabled}','false') WHERE id=$1",['workforce:'+f.goal]);
    return scopes;
  });
  expect(await applyBacklog(f.goal,f.reviewRun,{reason:'Policy changed after the first read',tasks:[f.proposal]},
    {durablePreparation:true})).toEqual([]);
  const plan=await planRow(f.reviewRun);
  expect(plan.receipts[0].code).toBe(disabled==='muse'?'MUSE_LANE_DISABLED':'WORKFORCE_DISABLED');
  expect(plan.receipts[0]).not.toHaveProperty('capacityIntent');expect(plan.task_ids).toEqual([]);
  expect((await pool.query('SELECT 1 FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM workforce_backlog_items WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
});

it.each(['workforce','muse'] as const)('retains the final %s policy guard when preparation races a disable',async disabled=>{
  const f=await fixture(false,'normal',false,disabled==='muse'?'muse':'glm');
  if(disabled==='muse')await pool.query("UPDATE settings SET value=jsonb_set(value,'{museTarget}','2') WHERE id=$1",['workforce:'+f.goal]);
  vi.mocked(accountBridge.request).mockImplementationOnce(async(method)=>{
    expect(method).toBe('worker.prepare');
    await pool.query(disabled==='muse'?"UPDATE settings SET value=jsonb_set(value,'{museTarget}','0') WHERE id=$1":
      "UPDATE settings SET value=jsonb_set(value,'{enabled}','false') WHERE id=$1",['workforce:'+f.goal]);
    return {prepared:true} as never;
  });
  expect(await applyBacklog(f.goal,f.reviewRun,{reason:'Concurrent policy change during preparation',tasks:[f.proposal]})).toEqual([]);
  const plan=await planRow(f.reviewRun);
  expect(plan.receipts[0].code).toBe(disabled==='muse'?'MUSE_LANE_DISABLED':'WORKFORCE_DISABLED');
  expect(plan.receipts[0]).not.toHaveProperty('capacityIntent');expect(plan.task_ids).toEqual([]);
  expect((await pool.query('SELECT 1 FROM workforce_backlog_items WHERE goal_id=$1',[f.goal])).rowCount).toBe(0);
  expect(accountBridge.request).toHaveBeenCalledTimes(1);
});
