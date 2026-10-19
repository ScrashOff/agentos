import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {config} from '../server/config.js';
import {installSupervision,recordSupervisionDecisions} from '../server/supervision.js';
import {installManagement,recordManagementReviews} from '../server/management.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
import {deliveryDigest} from '../server/delivery-supervision.js';
import {readMaterialDecisionOutcome,readRequiredMaterialDecision} from '../server/management-material-decision.js';
import {readBacklogDecisionRejections} from '../server/management-decision-feedback.js';
import {readManagementFollowThrough} from '../server/management-follow-through.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installSupervision();await installBacklogPlanner();await installManagement();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('material-decision','Test','test','[]','test',1,'Fixture')");});
afterAll(async()=>{await pool.end();});

async function fixture() {
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID(),manager=randomUUID(),review=randomUUID(),fence=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Delivery','Strict criteria','material-decision',$2)",[goal,goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Integrator','Exact instruction','material-decision','failed','integrate',
    '[{"id":"candidate","type":"artifact","name":"candidate.json"}]',1)`,[task,goal,task]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'test',1,'failed')",[run,task,randomUUID()]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
    VALUES($1,$2,$3,$4,'open','Consume current repair','[]')`,[caseId,goal,task,run]);
  const binding={goalId:goal,caseId,consumerTaskId:task,consumerRunId:run,consumerAttempt:1,obligations:[{
    obligationId:randomUUID(),generation:'a'.repeat(64),handoffId:randomUUID(),producerTaskId:randomUUID(),
    producerRunId:randomUUID(),producerManifestDigest:'b'.repeat(64),repairArtifacts:[
      {id:randomUUID(),name:'implementation.patch',digest:'c'.repeat(64),size:12},
      {id:randomUUID(),name:'repair-report.json',digest:'d'.repeat(64),size:13}]}]};
  const hint={version:1,authority:'review_only',...binding,identityDigest:deliveryDigest(binding),actionValidationRequired:true};
  const focus={caseId,taskId:task};
  const snapshot:any={cases:[{id:caseId,taskId:task,materialConsumptionReview:hint}],operation:{focus}};
  const description=()=>`MANAGEMENT_V1\n${JSON.stringify(snapshot)}`;
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Review',$4,'material-decision','running','coordinate',
    '[{"id":"review","type":"artifact","name":"coordination.json"}]',1)`,[manager,goal,manager,description()]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,instruction_snapshot)
    VALUES($1,$2,1,$3,'test',1,'running',$4)`,[review,manager,fence,JSON.stringify({task:{description:description()}})]);
  const coordination:any={management:{summary:'Resolve material obligation',capacityReason:'Use exact accepted evidence',actions:[]},backlog:{tasks:[],reason:'No independent work'}};
  const inspect=()=>readMaterialDecisionOutcome(pool,goal,review,description(),coordination);
  const accept=async()=>{await writeArtifact(review,fence,'coordination.json',JSON.stringify(coordination),'application/json');
    await completeRun(review,fence,'Recorded review',{});expect((await validateTask(manager)).passed).toBe(true);};
  return {goal,task,run,caseId,manager,review,fence,binding,hint,focus,snapshot,description,coordination,inspect,accept};
}
const action=(caseId:string)=>({caseId,kind:'revise',diagnosis:'The current failed consumer requires a validated focal revision.',
  instruction:'Use the current immutable inputs and preserve all acceptance criteria and independent verification.'});

async function persistControlBinding(f:Awaited<ReturnType<typeof fixture>>) {
  const source=f.binding.obligations[0];
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Accepted source','Fixture control binding','material-decision','accepted','work','[]',1)`,
    [source.producerTaskId,f.goal,source.producerTaskId]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'test',1,'submitted')`,[source.producerRunId,source.producerTaskId,randomUUID()]);
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','fixture')`,
    [randomUUID(),source.producerTaskId,source.producerRunId]);
  await pool.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,
    source_proof,consumer_proof,handoff,state) VALUES($1,$2,$3,$4,$3,'{}','{}','{}','applied')`,
    [source.handoffId,f.goal,source.producerTaskId,f.task]);
  await pool.query(`INSERT INTO delivery_consumptions(id,handoff_id,goal_id,producer_task_id,producer_run_id,consumer_task_id,
    consumer_contract_digest,producer_manifest_digest,binding,state,receipt)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'waiting_supervision',$10)`,
    [source.obligationId,source.handoffId,f.goal,source.producerTaskId,source.producerRunId,f.task,'f'.repeat(64),
      source.producerManifestDigest,JSON.stringify({repairArtifacts:source.repairArtifacts}),
      JSON.stringify({code:'SUPERVISION_REQUESTED',caseId:f.caseId,generation:source.generation})]);
}

it('requires the exact material decision despite prioritize, tracking or no_work, without modifying actors',async()=>{
  const f=await fixture();f.coordination.management.actions=[{kind:'prioritize',taskId:f.task,expectedAttempt:1,priority:79,reason:'Material input ready'}];
  f.coordination.management.nextTransition={mode:'task',taskId:f.task,expectedAttempt:1,expectedArtifact:'candidate.json'};
  const before=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
  const result=await f.inspect();expect(result).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false,
    caseId:f.caseId,taskId:f.task,requiredDecision:{actionAddress:'supervision.actions',identityDigest:f.hint.identityDigest}});
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(0);
});

it('proposed JSON alone cannot satisfy the action, but the existing recorder can acquire ownership',async()=>{
  const f=await fixture();f.coordination.supervision={actions:[action(f.caseId)]};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_ACTION_UNAPPLIED',satisfied:false});
  await f.accept();await recordSupervisionDecisions();
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_ACTION_OWNED',satisfied:true,decisionRunId:f.review});
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('failed');
  // The event is an explicit fixture of the canonical applier receipt, not a
  // claim that this test executed tools, attached a repair or accepted a game.
  await event(pool,'SUPERVISION_APPLIED',f.task,{caseId:f.caseId,sourceRunId:f.run,decisionRunId:f.review,stopConfirmed:true});
  await pool.query("UPDATE supervision_cases SET state='applied' WHERE id=$1",[f.caseId]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_ACTION_APPLIED',effectKind:'SUPERVISION_APPLIED',satisfied:true});
});

it('requires accepted same-goal decision authority and exact case/run for ownership',async()=>{
  const f=await fixture();await pool.query("UPDATE supervision_cases SET state='decided',decision=$2,decision_run_id=$3 WHERE id=$1",
    [f.caseId,JSON.stringify({actions:[action(f.caseId)]}),f.review]);
  expect(await f.inspect()).toMatchObject({satisfied:false});
  await f.accept();await pool.query('UPDATE supervision_cases SET source_run_id=NULL WHERE id=$1',[f.caseId]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_EPISODE_CHANGED',satisfied:false});
});

it.each(['accepted','cancelled','superseded'])('does not reopen a legitimately %s consumer',async(state)=>{
  const f=await fixture();await pool.query('UPDATE tasks SET state=$2 WHERE id=$1',[f.task,state]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_EPISODE_CLOSED',satisfied:true});
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe(state);
});

it('keeps a same-run live actor as a verified wait and rejects a changed attempt',async()=>{
  const f=await fixture();await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_WAIT_ACTOR',satisfied:true});
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_EPISODE_CHANGED',satisfied:false});
});

it('allows a concrete unresolved read, but not repeating an already delivered read as a new gap',async()=>{
  const f=await fixture(),request={kind:'task_metadata',taskId:f.task,expectedAttempt:1,offset:0};
  f.coordination.management.contextRequests=[request];
  f.coordination.management.materialContextGap={caseId:f.caseId,consumerRunId:f.run,identityDigest:f.hint.identityDigest,
    reason:'The exact current contract fields have not been delivered.',requestIndices:[0]};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_GAP',satisfied:false});
  f.snapshot.operation.recoveredContext={results:[{request,status:'OK',text:'Actual complete fields',truncated:false,nextOffset:null}]};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_ALREADY_DELIVERED',satisfied:false});
  f.snapshot.operation.recoveredContext.results[0].truncated=true;
  f.snapshot.operation.recoveredContext.results[0].nextOffset=21;
  f.coordination.management.contextRequests[0]={...request,offset:21};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_GAP',satisfied:false});
  f.coordination.management.contextRequests[0]={...request,taskId:randomUUID()};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false});
});

it('fails closed for altered frozen bindings and preserves legacy nonmaterial reviews',async()=>{
  const f=await fixture();f.snapshot.cases[0].materialConsumptionReview.identityDigest='e'.repeat(64);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_BINDING_INVALID',satisfied:false});
  delete f.snapshot.cases[0].materialConsumptionReview;
  expect(await f.inspect()).toBeNull();
});

it('preserves nonmaterial waits when the frozen review has no focus or retained decision',async()=>{
  const f=await fixture();
  f.snapshot.cases=[];
  for(const operation of [{},{focus:null,requiredDecision:null},{focus:f.focus,requiredDecision:null}]){
    f.snapshot.operation=operation;
    expect(await f.inspect()).toBeNull();
  }
  // Absence must not manufacture a malformed material binding. Restoring a
  // real focal obligation still requires the existing supervised decision.
  f.snapshot.cases=[{id:f.caseId,taskId:f.task,materialConsumptionReview:f.hint}];
  f.snapshot.operation={focus:f.focus};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false});
  f.hint.identityDigest='e'.repeat(64);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_BINDING_INVALID',satisfied:false});
});

it('links planning reads to the focal case and identifies repeated intervals even in a mixed gap',async()=>{
  const f=await fixture(),requestId=randomUUID();
  f.snapshot.cases[0].evidence=[{kind:'SCOPED_DELIVERY_ACTION',requestId}];
  const taskRead={kind:'task_metadata',taskId:f.task,expectedAttempt:1};
  const planRead={kind:'planning_request',requestId,itemIndex:4,offset:0};
  f.coordination.management.contextRequests=[taskRead,planRead];
  f.coordination.management.materialContextGap={caseId:f.caseId,consumerRunId:f.run,identityDigest:f.hint.identityDigest,
    reason:'A cited proposal is still missing its exact source binding.',requestIndices:[0,1]};
  f.snapshot.operation.recoveredContext={results:[{request:{...taskRead,offset:0},status:'OK',text:'First delivered interval',
    digest:'f'.repeat(64),truncated:true,nextOffset:24}]};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_GAP',contextGap:{requestIndices:[1],
    alreadyDeliveredRequestIndices:[0]},readFeedback:[{requestIndex:0,nextOffset:24}]});
  f.coordination.management.contextRequests=[planRead];f.coordination.management.materialContextGap.requestIndices=[0];
  f.snapshot.operation.retainedEvidence={entries:[{request:planRead,digest:'a'.repeat(64),snapshotDigest:'a'.repeat(64),
    totalCharacters:5,complete:true,pages:[{offset:0,text:'Exact'}]}]};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_ALREADY_DELIVERED'});
  f.coordination.management.contextRequests[0]={...planRead,snapshotDigest:'b'.repeat(64)};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_GAP'});
  f.coordination.management.contextRequests[0]={...planRead,requestId:randomUUID()};
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED'});
});

it('records the obligation before the stagnation threshold and retains it by case across another focus',async()=>{
  const f=await fixture();await persistControlBinding(f);await f.accept();
  await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['management:'+f.goal,JSON.stringify({enabled:true,enabledAt:'2026-01-01T00:00:00Z'})]);
  await recordManagementReviews();
  const receipts=(await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1',[f.review])).rows[0].receipts;
  expect(receipts).toEqual(expect.arrayContaining([expect.objectContaining({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false}),
    expect.objectContaining({code:'OPERATION_REPLAN_REQUIRED'})]));
  expect(await readRequiredMaterialDecision(pool,f.goal,f.focus)).toMatchObject({caseId:f.caseId,consumerRunId:f.run,bindingState:'control_metadata_revalidated'});
  expect(await readRequiredMaterialDecision(pool,randomUUID(),f.focus)).toBeNull();
  expect(await readRequiredMaterialDecision(pool,f.goal,{...f.focus,caseId:randomUUID()})).toBeNull();
  await pool.query("UPDATE delivery_consumptions SET receipt=jsonb_set(receipt,'{generation}',to_jsonb($2::text)) WHERE id=$1",
    [f.binding.obligations[0].obligationId,'e'.repeat(64)]);
  const changed=await readRequiredMaterialDecision(pool,f.goal,f.focus);
  expect(changed).toMatchObject({bindingState:'needs_revalidation',actionAddress:'management.contextRequests'});
  expect(changed).not.toHaveProperty('materialReview');
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[f.task]);
  expect(await readRequiredMaterialDecision(pool,f.goal,f.focus)).toBeNull();
});

it('keeps a closed-consumer refusal visible across reviews and rejects satisfaction by an unrelated transition',async()=>{
  const f=await fixture();await f.accept();await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[f.task]);
  const receipt={index:0,sourceTaskId:f.binding.obligations[0].producerTaskId,consumerTaskId:f.task,status:'rejected',code:'CONSUMER_CLOSED'};
  await pool.query(`INSERT INTO workforce_backlog_plans(run_id,goal_id,state,reason,receipts)
    VALUES($1,$2,'rejected','Exact refusal',$3)`,[f.review,f.goal,JSON.stringify([receipt])]);
  expect(await readBacklogDecisionRejections(pool,f.goal)).toMatchObject({entries:[{
    reviewRunId:f.review,sourceTaskId:receipt.sourceTaskId,consumerTaskId:f.task,currentConsumerState:'accepted',occurrencesInHistoryWindow:1}]});
  expect(await readManagementFollowThrough(pool,f.goal,f.review)).toMatchObject({code:'MANAGEMENT_BACKLOG_CONSUMER_CLOSED',satisfied:false});
  expect((await readBacklogDecisionRejections(pool,randomUUID())).entries).toEqual([]);
  // The task may legitimately reopen through existing guarded recovery. The
  // old closed receipt must not pretend its former state is still current.
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[f.task]);
  expect((await readBacklogDecisionRejections(pool,f.goal)).entries).toEqual([]);
});
