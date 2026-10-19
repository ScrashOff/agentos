import {beforeAll,beforeEach,afterAll,it,expect,vi,afterEach} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installSupervision} from '../server/supervision.js';
import {installDeliveryConsumptions} from '../server/delivery-consumption.js';
import {installPlanningDemand,scopeSourceRevision,reconcilePendingStrategyIntents,createPlanningDemands} from '../server/planning-demand.js';
import {recordDeliveryTransition,reconcileDeliveryTransitions,readDeliveryTransitions} from '../server/delivery-transitions.js';
import {readManagementFollowThrough} from '../server/management-follow-through.js';
import {accountBridge} from '../server/bridge.js';
import {readManagementDecisionFeedback} from '../server/management-decision-feedback.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();
  await installBacklogPlanner();await installManagement();await installPlanningDemand();await installSupervision();await installDeliveryConsumptions();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='autonomy:intent-test'");vi.spyOn(accountBridge,'request').mockRejectedValue(Error('NO_EXTERNAL_PROVIDER_ALLOWED'));});
afterEach(()=>vi.restoreAllMocks());afterAll(async()=>{await pool.end();});

async function fixture(){
  const goal=randomUUID(),consumer=randomUUID(),consumerRun=randomUUID(),caseId=randomUUID(),planner=randomUUID(),plannerRun=randomUUID(),request=randomUUID();
  const scope='test/intent-unit';
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Deliver exact unit','Keep acceptance intact','intent-test',$2)",[goal,goal]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('intent-glm','GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'intent-glm') ON CONFLICT(id) DO NOTHING`);
  for(const [key,value]of Object.entries({workforce:{enabled:true,glmAccountId:'intent-glm',glmTarget:12,museTarget:0,workerAccounts:[]},management:{enabled:true},supervision:{enabled:true}}))
    await pool.query('INSERT INTO settings VALUES($1,$2)',[key+':'+goal,JSON.stringify(value)]);
  for(const [id,stage,state,kind] of [[consumer,'integrate','recovery_blocked',null],[planner,'work','running','planning']]){
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
      VALUES($1,$2,$7,'Preserved task','Frozen original task','engineering.integration',$3,$4,$6,1,$5)`,
      [id,goal,state,stage,JSON.stringify({kind:'opencode',baseWorkspace:scope,workspace:'test/'+id,workKind:kind}),
        JSON.stringify(stage==='integrate'?[{id:'final-proof',type:'json_field',name:'verification.json',field:'passed',equals:true,label:'Actual final evidence'}]:[]),id]);
  }
  for(const [id,task,state]of[[consumerRun,consumer,'submitted'],[plannerRun,planner,'running']])
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,started_at) VALUES($1,$2,1,$3,'opencode',1,$4,now())",[id,task,randomUUID(),state]);
  await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence) VALUES($1,$2,$3,$4,'open','Missing real repair proof','[]')",[caseId,goal,consumer,consumerRun]);
  const revision=await scopeSourceRevision(pool,goal,scope);
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,task_id)
    VALUES($1,$2,$3,'glm',$4,'running','DELIVERY_EVIDENCE_CHANGED','{}',$5)`,[request,goal,scope,revision,planner]);
  const snapshot={cases:[{id:caseId,taskId:consumer,scope,currentRun:{id:consumerRun,attempt:1,state:'submitted'},evidence:[]}],
    operation:{focus:{caseId,taskId:consumer,scope},decisionFeedback:{entries:[]}}};
  const proposal={mode:'plan',scope,sourceRevision:revision,strategyKey:'repair-exact-preimages',
    cause:'The old source proof does not cover each real changed file.',
    hypothesis:'Execute a focal repair from the exact accepted source and report actual before and after bytes.',expectedArtifact:'scope-plan.json'};
  const manager=async(context:any=snapshot)=>{
    const task=randomUUID(),run=randomUUID();
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
      VALUES($1,$2,$4,'Accepted strategy','MANAGEMENT_V1\n'||$3,'research.requirements','accepted','coordinate','[]',1)`,[task,goal,JSON.stringify(context),task]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')",[run,task,randomUUID()]);
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','fixture')",[randomUUID(),task,run]);
    return run;
  };
  const finish=async()=>{await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1",[plannerRun]);
    await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[planner]);
    await pool.query("UPDATE planning_requests SET state='applied',receipt=$2 WHERE id=$1",[request,JSON.stringify({evaluatedRunId:plannerRun,code:'APPLIED'})]);};
  const record=async(p:any=proposal)=>{const review=await manager();const receipt=await tx(db=>recordDeliveryTransition(db,goal,review,p));return{review,receipt};};
  return{goal,consumer,consumerRun,caseId,planner,plannerRun,request,scope,revision,snapshot,proposal,manager,finish,record};
}
const transitions=async(goal:string)=>(await pool.query('SELECT * FROM delivery_transitions WHERE goal_id=$1 ORDER BY created_at,id',[goal])).rows;
const requests=async(goal:string)=>(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 ORDER BY created_at,id',[goal])).rows;

it('preserves a running planner and immutable new intent, then dispatches one successor after applied effects',async()=>{
  const f=await fixture(),before=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.plannerRun])).rows[0],{review,receipt}=await f.record();
  expect(receipt.code).toBe('STRATEGY_DEFERRED');expect(await requests(f.goal)).toHaveLength(1);
  expect(await readManagementFollowThrough(pool,f.goal,review)).toMatchObject({code:'MANAGEMENT_STRATEGY_DEFERRED',satisfied:true,owner:'scheduler',requestId:f.request});
  await reconcilePendingStrategyIntents();
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.plannerRun])).rows[0]).toEqual(before);
  await f.finish();await reconcilePendingStrategyIntents();await reconcilePendingStrategyIntents();
  const plans=await requests(f.goal),d=(await transitions(f.goal))[0];expect(plans).toHaveLength(2);
  expect(d).toMatchObject({state:'planning',proposal:f.proposal,planning_request_id:plans[1].id});
  expect(plans[1].snapshot.strategy).toMatchObject({key:f.proposal.strategyKey,reviewRunId:review});
  expect(plans[1].source_revision).toBe(f.revision);expect(plans[1].demand_epoch).toBe(2);
  const intent=d.receipt.intent;await reconcileDeliveryTransitions();
  expect((await transitions(f.goal))[0].receipt.intent).toEqual(intent);
  expect(accountBridge.request).not.toHaveBeenCalled();
});
it('recovers the exact historical occupancy rejection without erasing its original receipt or event',async()=>{
  const f=await fixture(),review=await f.manager(),id=randomUUID(),old={code:'PLANNER_ALREADY_ACTIVE',transitionId:id,planningRequestId:null};
  await pool.query(`INSERT INTO delivery_transitions(id,goal_id,review_run_id,scope,source_revision,strategy_key,state,proposal,receipt)
    VALUES($1,$2,$3,$4,$5,$6,'rejected',$7,$8)`,[id,f.goal,review,f.scope,f.revision,f.proposal.strategyKey,JSON.stringify(f.proposal),JSON.stringify(old)]);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('DELIVERY_TRANSITION_BOUND',$1,$2)",[f.goal,JSON.stringify(old)]);
  await reconcilePendingStrategyIntents();let d=(await transitions(f.goal))[0];
  expect(d.state).toBe('awaiting_planner');expect(d.receipt.intent.originalReceipt).toEqual(old);expect(d.proposal).toEqual(f.proposal);
  await f.finish();await reconcilePendingStrategyIntents();d=(await transitions(f.goal))[0];expect(d.state).toBe('planning');
  expect((await pool.query("SELECT payload FROM events WHERE kind='DELIVERY_TRANSITION_BOUND'")).rows[0].payload).toEqual(old);
});
it('coalesces concurrent identical reviews, aliases an unchanged body, and does not overwrite a conflicting key',async()=>{
  const f=await fixture(),reviews=await Promise.all([f.manager(),f.manager()]);
  await Promise.all(reviews.map(review=>tx(db=>recordDeliveryTransition(db,f.goal,review,f.proposal))));
  const alias=await f.record({...f.proposal,strategyKey:'renamed-same-repair'});
  expect(alias.receipt.code).toBe('STRATEGY_INTENT_ALREADY_RECORDED');
  const conflict=await f.record({...f.proposal,hypothesis:'A genuinely different instruction must not overwrite an existing stable key.'});
  expect(conflict.receipt.code).toBe('STRATEGY_IDENTITY_CONFLICT');
  expect((await transitions(f.goal)).filter(d=>d.state==='awaiting_planner')).toHaveLength(1);
  await f.finish();await Promise.all([reconcilePendingStrategyIntents(),reconcilePendingStrategyIntents()]);
  expect(await requests(f.goal)).toHaveLength(2);
});
it('retains an explicitly different accepted approach on the same evidence',async()=>{
  const f=await fixture();await f.record();
  const other=await f.record({...f.proposal,strategyKey:'discriminate-runtime-path',hypothesis:'Investigate a different runtime path with an exact isolated discriminating experiment.'});
  expect(other.receipt.code).toBe('STRATEGY_DEFERRED');
  expect((await transitions(f.goal)).filter(d=>d.state==='awaiting_planner')).toHaveLength(2);
});
it.each([false,true])('preserves real COVERAGE feedback identity across repeated events; changed cause=%s is revalidated',async(changed)=>{
  const f=await fixture(),oldDecision=await f.manager();
  const rejected=async(review:string,code:string)=>pool.query(`INSERT INTO events(kind,entity_id,payload)
    VALUES('DELIVERY_SOURCE_DECISION_REJECTED',$1,$2)`,[f.consumer,JSON.stringify({caseId:f.caseId,decisionRunId:review,code})]);
  await rejected(oldDecision,'DELIVERY_RESOLUTION_PROOF_COVERAGE');
  const feedback=await readManagementDecisionFeedback(pool,f.goal,{caseId:f.caseId});expect(feedback.entries).toHaveLength(1);
  const snapshot={...f.snapshot,operation:{...f.snapshot.operation,decisionFeedback:feedback}};
  const review=await f.manager(snapshot),receipt=await tx(db=>recordDeliveryTransition(db,f.goal,review,f.proposal));
  expect(receipt.code).toBe('STRATEGY_DEFERRED');
  await rejected(await f.manager(snapshot),changed?'DELIVERY_RESOLUTION_ARTIFACT_CHANGED':'DELIVERY_RESOLUTION_PROOF_COVERAGE');
  await f.finish();await reconcilePendingStrategyIntents();
  expect((await transitions(f.goal))[0].state).toBe(changed?'strategy_revalidation':'planning');
  expect(await requests(f.goal)).toHaveLength(changed?1:2);
});
it.each(['source','attempt','case','scope','generation','acceptance','snapshot'])('requires supervisor revalidation after %s changes, without a successor',async(change)=>{
  const f=await fixture(),{review}=await f.record();await f.finish();
  if(change==='source')await pool.query("UPDATE tasks SET criteria='[{\"id\":\"new\",\"type\":\"artifact\",\"name\":\"new.json\",\"label\":\"New criterion\"}]' WHERE id=$1",[f.consumer]);
  if(change==='attempt'){await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.consumer]);await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'opencode',1,'submitted')",[randomUUID(),f.consumer,randomUUID()]);}
  if(change==='case')await pool.query("UPDATE supervision_cases SET state='applied' WHERE id=$1",[f.caseId]);
  if(change==='scope')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','\"test/changed\"') WHERE id=$1",[f.consumer]);
  if(change==='generation')await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[f.caseId,JSON.stringify([{kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:randomUUID(),sourceConflict:{generation:'f'.repeat(64)}}])]);
  if(change==='acceptance')await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) SELECT $1,task_id,id,false,'[]','new-check' FROM runs WHERE id=$2",[randomUUID(),review]);
  // Corruption of a frozen description is never simulated by mutating it. Use a
  // separate accepted review with an absent snapshot and preserve the old one.
  if(change==='snapshot'){
    const bad=await f.manager({cases:[],operation:{}});await pool.query('UPDATE delivery_transitions SET review_run_id=$2 WHERE review_run_id=$1',[review,bad]);
  }
  await reconcilePendingStrategyIntents();expect((await transitions(f.goal))[0].state).toBe('strategy_revalidation');
  expect(await requests(f.goal)).toHaveLength(1);
});
it.each(['paused','cancelled','autonomy','policy'])('keeps %s from creating an unapproved successor',async(change)=>{
  const f=await fixture();await f.record();await f.finish();
  if(change==='paused'||change==='cancelled')await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.goal,change]);
  if(change==='autonomy')await pool.query("INSERT INTO settings VALUES('autonomy:intent-test','{\"enabled\":false}')");
  if(change==='policy')await pool.query("UPDATE settings SET value=jsonb_set(value,'{enabled}','false') WHERE id=$1",['workforce:'+f.goal]);
  await reconcilePendingStrategyIntents();expect(await requests(f.goal)).toHaveLength(1);
  expect((await transitions(f.goal))[0].state).toBe('awaiting_planner');
});
it.each(['preparing','replanning_required'])('waits for %s ownership even after the process returned',async(state)=>{
  const f=await fixture();await f.record();await f.finish();await pool.query('UPDATE planning_requests SET state=$2 WHERE id=$1',[f.request,state]);
  await reconcilePendingStrategyIntents();expect(await requests(f.goal)).toHaveLength(1);
});
it.each(['queued','cancelling','reconciling'])('never treats an uncertain %s actor as a released scope',async(state)=>{
  const f=await fixture();await f.record();await pool.query('UPDATE runs SET state=$2 WHERE id=$1',[f.plannerRun,state]);
  await pool.query("UPDATE planning_requests SET state='awaiting_supervision' WHERE id=$1",[f.request]);
  await reconcilePendingStrategyIntents();expect(await requests(f.goal)).toHaveLength(1);
});
it('does not emit progress for repeated waits or allow automatic demand to consume a pending intent slot',async()=>{
  const f=await fixture();await f.record();await f.finish();
  const first=(await transitions(f.goal))[0];expect(await createPlanningDemands(f.goal)).toEqual([]);
  // Restore ownership to exercise no-op waits without changing the consumer.
  await pool.query("UPDATE planning_requests SET state='preparing' WHERE id=$1",[f.request]);
  await reconcilePendingStrategyIntents();await reconcilePendingStrategyIntents();
  const last=(await transitions(f.goal))[0];expect(last.updated_at).toEqual(first.updated_at);expect(last.receipt).toEqual(first.receipt);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_TRANSITION_CHANGED'")).rowCount).toBe(0);
});
it('skips sixteen administratively disabled waits before the bounded selection of an eligible intent',async()=>{
  const waiting=[];
  for(let i=0;i<16;i++){
    const f=await fixture();await f.record();await f.finish();
    await pool.query("UPDATE settings SET value=jsonb_set(value,'{enabled}','false') WHERE id=$1",['management:'+f.goal]);
    waiting.push(f);
  }
  const eligible=await fixture();await eligible.record();await eligible.finish();
  await reconcilePendingStrategyIntents();expect(await requests(eligible.goal)).toHaveLength(2);
  for(const f of waiting)expect(await requests(f.goal)).toHaveLength(1);
});
it('recovers the explicitly focused pending intent beyond eight rows without deleting other cases',async()=>{
  const f=await fixture(),{receipt}=await f.record(),others=[];
  for(let i=0;i<9;i++){
    const review=await f.manager(),id=randomUUID(),caseId=randomUUID(),scope='test/other-'+i;
    await pool.query(`INSERT INTO delivery_transitions(id,goal_id,review_run_id,scope,state,proposal,receipt)
      VALUES($1,$2,$3,$4,'strategy_revalidation','{}',$5)`,[id,f.goal,review,scope,
      JSON.stringify({code:'SOURCE_CHANGED',intent:{binding:{caseId}}})]);
    others.push({id,caseId,scope});
  }
  const focal=await readDeliveryTransitions(pool,f.goal,{caseId:f.caseId,scope:f.scope});
  expect(focal).toHaveLength(8);expect(focal[0].id).toBe(receipt.transitionId);
  for(const other of others)expect((await readDeliveryTransitions(pool,f.goal,other))[0].id).toBe(other.id);
  expect(await transitions(f.goal)).toHaveLength(10);
});
