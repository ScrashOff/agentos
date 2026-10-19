// Server-only PostgreSQL integration: canonical administrative recovery and real manager review roundtrip.
// Only the remote inspection/stop boundary is replaced by deterministic receipts.
import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,migrate,tx} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement,managementSnapshot,recordManagementReviews} from '../server/management.js';
import {readManagerCases,projectManagerCases} from '../server/manager-material-consumption.js';
import {selectManagerFocus,selectManagerCaseWindow} from '../server/manager-focus.js';
import {projectManagerReview} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {installPlanningDemand} from '../server/planning-demand.js';
import {installSupervision} from '../server/supervision.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installNativeSupervision,reserveNativeSupervisor,registerNativeSupervisor,startDetachedNativeRun,
  reconcileNativeSupervisor,nativeSupervisorLockNamespace,nativeSupervisorLockKey,nativeSupervisorCgroup,
  type NativeReservation,type NativeUnitObservation} from '../server/native-supervision.js';
import {cancelTask,completeRun,writeArtifact,validateTask} from '../server/service.js';
import {operatorRecoveryCode} from '../server/operator-recovery-contract.js';
import {requestOperatorOpenCodeRecovery,consumeOperatorOpenCodeRecovery,
  reviewStoppedOperatorOpenCodeRecovery} from '../server/operator-opencode-recovery.js';
import {operatorOpenCodeRecoveryCompatibleSources} from '../server/operator-opencode-recovery-contract.js';
import {reconcileStoppedDetachedOpenCode} from '../server/reconcile-stopped-opencode.js';

const release={candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
// The owner predates the controller under test. Compatibility is selected from
// the reviewed contract, not widened by changing production hashes in a test.
const ownerRelease={candidate:'/opt/agentos/releases/managed-def7aad1-987b-4f72-ad2d-0ac3a49ca078',digest:'d'.repeat(64)};
const held:PoolClient[]=[],oldConfig={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installNativeSupervision(pool);await installBacklogPlanner();await installManagement();
  await installSupervision();await installPlanningDemand();await installTaskExecutionMetadata();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query(`DELETE FROM settings WHERE id IN('maintenance','runtime-memory-policy','artifact-recovery',
    'operator-opencode-recovery-scan:dispatch','operator-opencode-recovery-scan:review','operator-opencode-recovery-scan:handoff')`);
  await pool.query(`INSERT INTO settings(id,value) VALUES('maintenance','{"draining":false}')`);
  for(const name of ['agentos-workers','agentos-backlog','agentos-cycles','agentos-opencode'])
    await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
      ['worker-readiness:'+name,JSON.stringify(release)]);
  config.bridgeHost='test-only';config.remoteExecution=true;
});
afterEach(async()=>{
  for(const client of held.splice(0)){await client.query('SELECT pg_advisory_unlock_all()');client.release();}
  Object.assign(config,oldConfig);vi.restoreAllMocks();
});
afterAll(()=>pool.end());

async function fixture(options:{frozenPlanningRequestId?:string;goalId?:string;scope?:string}={}){
  const goal=options.goalId??randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),context=randomUUID();
  const project='recovery-'+goal,scope=options.scope??'opencode-recovery/base',workspace='opencode-recovery/'+run;
  const planningRequest=randomUUID(),sourceRevision='b'.repeat(64);
  const historicalInputs=[['planning-query.mjs','// immutable helper from the old attempt'],
    ['planning-context.json',JSON.stringify({requestId:planningRequest,sourceRevision})]].map(([name,text])=>({
      id:randomUUID(),name,digest:createHash('sha256').update(text).digest('hex'),base64:Buffer.from(text).toString('base64')}));
  const execution={kind:'opencode',managed:true,readOnlySource:true,workKind:'planning',baseWorkspace:scope,
    workspace,writePaths:['qa-protocol/','evidence/'],planningRequestId:planningRequest,inputs:historicalInputs};
  const criteria=[{id:'plan',label:'Bound exact scope plan',type:'artifact',name:'scope-plan.json'}];
  if(!options.goalId)await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1::uuid,'Bound planning recovery','Preserve the original delivery gates',$2,$1::text,'active')`,[goal,project]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('glm','GLM fixture','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'fixture-glm') ON CONFLICT(id) DO NOTHING`);
  if(!options.goalId)await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', ['workforce:'+goal,JSON.stringify({enabled:true,
    glmAccountId:'glm',glmTarget:12,glmRequestLimit:6,museTarget:0,workerAccounts:[],coordinatorAccountId:'sol',coordinatorModel:'gpt-6.1-sol'})]);
  if(!options.goalId)await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1::uuid,$2,$1::text,'Bound planner','Keep immutable planning authority','qa.backend','queued','work',$3,$4,1)`,
    [task,goal,JSON.stringify(criteria),JSON.stringify({...execution,
      planningRequestId:options.frozenPlanningRequestId??planningRequest})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,checkpoint,instruction_snapshot)
    VALUES($1,$2,1,$3,'opencode',1,'queued','{"retained":true}',$4)`,[run,task,fence,JSON.stringify({schema:1,
      task:{id:task,goal_id:goal,title:'Bound planner',description:'Keep immutable planning authority',criteria,
        execution:{...execution,planningRequestId:options.frozenPlanningRequestId??planningRequest}}})]);
  // INSERT's canonical trigger freezes the task, ignoring caller-supplied
  // snapshots. Change only the current task contract after that real freeze.
  if(options.frozenPlanningRequestId){
    await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1',[task,JSON.stringify(execution)]);
    expect((await pool.query("SELECT instruction_snapshot#>>'{task,execution,planningRequestId}' frozen FROM runs WHERE id=$1",[run])).rows[0].frozen)
      .toBe(options.frozenPlanningRequestId);
  }
  await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)',
    [context,run,JSON.stringify({taskId:task,runId:run}),'Frozen planning context']);
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,task_id)
    VALUES($1,$2,$3,'glm',$4,'running','BOUND_TEST',$5,$6)`,[planningRequest,goal,scope,sourceRevision,
      JSON.stringify({tasks:[],artifacts:[],opportunities:{consumers:[]},coverage:[],repairConsumption:[],deferredDeliveries:[]}),task]);
  // The final obligation remains open. Tests must not pass merely because a
  // lone failed planner makes the entire goal inactive and bypasses admission.
  const consumer=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1::uuid,$2,$1::text,'Final delivery','Preserve independent final acceptance','engineering.integration',
      'recovery_blocked','integrate',$3,$4)`,[consumer,goal,
    JSON.stringify([{id:'verified',label:'Real final delivery',type:'json_field',name:'verification.json',field:'passed',equals:true}]),
    JSON.stringify({kind:'harness',baseWorkspace:scope,workspace:'opencode-recovery/consumer-'+consumer})]);
  await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2),($3,$2)',['remote-slot',run,'workspace:'+workspace]);
  await pool.query('INSERT INTO opencode_jobs(run_id) VALUES($1)',[run]);
  const reservation=(await tx(db=>reserveNativeSupervisor(db,run,ownerRelease)))!;
  expect(reservation).toBeTruthy();
  const owner=await pool.connect();held.push(owner);
  await owner.query('SELECT pg_advisory_lock($1::int,$2::int)',[nativeSupervisorLockNamespace,nativeSupervisorLockKey(run)]);
  const backend=(await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  expect(await tx(db=>registerNativeSupervisor(db,reservation,{unit:reservation.unit,pid:12345,
    cwd:reservation.candidate,cgroup:nativeSupervisorCgroup(run)},backend))).toBe(true);
  expect(await tx(db=>startDetachedNativeRun(db,reservation,'opencode-native-12345'))).toBeTruthy();
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,context]);
  const request={requestId:randomUUID(),runtime:'opencode',runId:run,runFence:fence,contextVersion:context,
    expectedAttempt:1,planningRequestId:planningRequest,intent:'recover_current_attempt',
    operatorConfirmed:true,reasonCode:'operator_requested_recovery',
    evidenceRefs:[{kind:'operator_observation',referenceId:randomUUID(),sha256:'c'.repeat(64)}]};
  return {goal,task,run,fence,context,project,scope,workspace,planningRequest,sourceRevision,reservation,owner,request,consumer};
}

const stoppedOwner=(reservation:NativeReservation):NativeUnitObservation=>({unit:reservation.unit,checkedAt:new Date().toISOString(),
  activeState:'inactive',loadState:'not-found',mainPid:0,pendingJob:false,cgroupEmpty:true});
async function current(f:Awaited<ReturnType<typeof fixture>>){
  return {run:(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0],
    task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0],
    job:(await pool.query('SELECT * FROM opencode_jobs WHERE run_id=$1',[f.run])).rows[0],
    planning:(await pool.query('SELECT * FROM planning_requests WHERE id=$1',[f.planningRequest])).rows[0],
    leases:(await pool.query('SELECT resource FROM leases WHERE run_id=$1 ORDER BY resource',[f.run])).rows};
}

type Fixture=Awaited<ReturnType<typeof fixture>>;
function liveIdentity(f:Fixture){
  expect(Object.keys(operatorOpenCodeRecoveryCompatibleSources).length).toBeGreaterThan(0);
  return {version:1,runtime:'opencode',runId:f.run,runFence:f.fence,contextVersion:f.context,
    workspace:f.workspace,state:'running',stateDigest:'9'.repeat(64),checkedAt:new Date().toISOString(),
    stateIdentityFields:{runFence:false,contextVersion:false},
    process:{unit:'agentos-run-'+f.run+'.service',mainPid:22345,startTicks:'1234',activeState:'active',subState:'running',
      controlGroup:'/system.slice/agentos-run-'+f.run+'.service'},
    owner:{unit:f.reservation.unit+'.service',mainPid:12345,startTicks:'5678',activeState:'active',subState:'running',
      controlGroup:nativeSupervisorCgroup(f.run),candidate:f.reservation.candidate,
      sourceHashes:Object.fromEntries(Object.entries(operatorOpenCodeRecoveryCompatibleSources).map(([file,hashes])=>[file,hashes[0]]))}};
}
function terminalInspection(f:Fixture){
  const checkedAt=new Date().toISOString(),physical={unit:'agentos-run-'+f.run,stopped:true,
    mainPid:0,populated:false,activeState:'inactive',loadState:'not-found',checkedAt};
  return {runId:f.run,runtime:'opencode',workspace:f.workspace,checkedAt,
    // The real old actor has no fence/context fields in state.json. Its canonical
    // reservation and persisted intent supply authority; do not invent fields.
    state:{runId:f.run,runtime:'opencode',workspace:f.workspace,state:'failed',code:'OPENCODE_STOPPED',stopped:true},
    process:physical,bridgeReceipt:{digest:createHash('sha256').update(JSON.stringify(physical)).digest('hex'),proof:physical}};
}
const request=(f:Fixture,inspect=async()=>liveIdentity(f))=>
  requestOperatorOpenCodeRecovery(f.task,f.request,{channel:'api-token'},inspect,release.candidate);
const consume=(f:Fixture,stop=async(_payload:{requestId:string;identity:any}):Promise<unknown>=>({observed:true}),
  inspect=async()=>liveIdentity(f))=>consumeOperatorOpenCodeRecovery(f.run,{inspect,stop,expectedController:release.candidate});
async function confirmStop(f:Fixture){
  await f.owner.query('SELECT pg_advisory_unlock_all()');
  expect(await tx(db=>reconcileNativeSupervisor(db,f.reservation,stoppedOwner(f.reservation)))).toBe('reconciling');
  expect((await current(f)).run.error_code).toBe(operatorRecoveryCode);
  expect(await reconcileStoppedDetachedOpenCode(f.reservation,stoppedOwner(f.reservation),terminalInspection(f))).toBe(true);
}

async function administrativeFixture(options:{goalId?:string;scope?:string}={}){
  const f=await fixture(options);await request(f);await consume(f);await confirmStop(f);
  const review=await reviewStoppedOperatorOpenCodeRecovery(f.run,async()=>terminalInspection(f));
  expect(review?.caseId).toBeTruthy();
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO NOTHING',[
    'management:'+f.goal,JSON.stringify({enabled:true,intervalSeconds:120,enabledAt:new Date(0).toISOString()})]);
  return {...f,caseId:review!.caseId};
}
type AdministrativeFixture=Awaited<ReturnType<typeof administrativeFixture>>;
async function cases(f:AdministrativeFixture){return readManagerCases(pool,f.goal);}
async function administrative(f:AdministrativeFixture){return (await cases(f)).find(c=>c.id===f.caseId)!;}
async function preserved(f:AdministrativeFixture){return {actor:await current(f),
  cases:(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY id',[f.task])).rows,
  artifacts:(await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows,
  acceptances:(await pool.query('SELECT * FROM acceptances WHERE task_id=$1 ORDER BY id',[f.task])).rows};}
async function deliveryCases(f:AdministrativeFixture,count=1){
  const ids:string[]=[];
  for(let i=0;i<count;i++){
    const task=randomUUID(),run=randomUUID(),fence=randomUUID(),caseId=randomUUID();ids.push(caseId);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
      VALUES($1::uuid,$2,$1::text,'Independent final delivery','Preserve an unrelated original delivery obligation',
        'engineering.integration','failed','integrate',$3,$4,1)`,[task,f.goal,
      JSON.stringify([{id:'delivery',label:'Final delivery',type:'artifact',name:'verification.json'}]),
      JSON.stringify({kind:'harness',baseWorkspace:'other/delivery-'+task,workspace:'other/delivery-'+task})]);
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at)
      VALUES($1,$2,1,$3,'harness',1,'failed','FUNCTIONAL_TEST_FAILED',now())`,[run,task,fence]);
    await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,request_key,created_at)
      VALUES($1::uuid,$2,$3,$4,'open','Unrelated delivery remains to be reviewed','[]',$1::text,now()-interval '1 hour')`,[caseId,f.goal,task,run]);
  }
  return ids;
}
async function managerReview(f:AdministrativeFixture,focus:any,options:{invalid?:boolean;record?:boolean;accept?:boolean}={}){
  const rows=await cases(f),selected=selectManagerCaseWindow(rows,focus);
  const policy=(await pool.query('SELECT value FROM settings WHERE id=$1',['workforce:'+f.goal])).rows[0].value;
  const operation=await managementSnapshot(pool,f.goal,policy,focus);
  const snapshot={cases:projectManagerCases(selected),operation};
  const projection=projectManagerReview(snapshot,'',0,180000);expect(projection.withinBudget).toBe(true);
  const projected=expandSharedManagerContext(JSON.parse(projection.description));
  expect(projected.operation.focus).toEqual(focus);
  // Match the real manager contract marker so the backlog INSERT trigger does
  // not append a second instruction after the final projected JSON document.
  const description='MANAGEMENT_V1\nReview only the canonical current administrative recovery and retain all final delivery gates.\n'+projection.description;
  expect(description.lastIndexOf('\n{"cases":')).toBeGreaterThan(0);
  const task=randomUUID(),run=randomUUID(),fence=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1::uuid,$2,$1::text,'Actual manager review',$3,'research.requirements','running','coordinate',$4,1)`,[
    task,f.goal,description,JSON.stringify([{id:'review',label:'Actual structured manager result',type:'artifact',name:'coordination.json'}])]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'harness',1,'running')`,[run,task,fence]);
  const frozen=(await pool.query("SELECT instruction_snapshot#>>'{task,description}' description FROM runs WHERE id=$1",[run])).rows[0].description;
  expect(frozen).toBe(description);
  const management={summary:'Review the exact stopped administrative planner without authorizing another execution.',
    capacityReason:'Unrelated deliveries retain their original dependencies and final acceptance requirements.',
    ...(options.invalid?{}:{actions:[]})};
  await writeArtifact(run,fence,'coordination.json',JSON.stringify({management}));
  await completeRun(run,fence,'Actual manager fixture result',{});
  if(options.accept!==false){await validateTask(task);
    expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1',[run])).rows[0]?.passed).toBe(true);}
  if(options.record!==false)await recordManagementReviews();
  const events=(await pool.query("SELECT id::text,payload FROM events WHERE kind='MANAGEMENT_REVIEWED' AND entity_id=$1 AND payload->>'runId'=$2",[f.goal,run])).rows;
  return {task,run,fence,focus,snapshot,projected,description,events};
}

it('gives the exact stopped administrative episode one immediate review among 28 delivery cases without changing execution authority',async()=>{
  const f=await administrativeFixture(),deliveries=await deliveryCases(f,28),before=await preserved(f);
  const rows=await cases(f),entry=rows.find(c=>c.id===f.caseId)!;
  expect(entry.administrativeRecoveryReview).toMatchObject({version:1,authority:'review_only',
    goalId:f.goal,caseId:f.caseId,taskId:f.task,runId:f.run,runFence:f.fence,contextVersion:f.context,
    attempt:1,planningRequestId:f.planningRequest,requestId:f.request.requestId,
    priorityConsumed:false,actionValidationRequired:true});
  const prior=selectManagerFocus(rows.filter(c=>deliveries.includes(c.id)),null)!;
  const focus=selectManagerFocus(rows,prior)!;
  expect(focus.caseId).toBe(f.caseId);
  expect(focus).toHaveProperty('administrativeRecoveryEpisodeDigest',entry.administrativeRecoveryReview.episodeDigest);
  expect(await preserved(f)).toEqual(before);
});

it('consumes the one-time priority through the real frozen manager review and canonical MANAGEMENT_REVIEWED writer',async()=>{
  const f=await administrativeFixture(),deliveries=await deliveryCases(f,2),before=await preserved(f);
  const rows=await cases(f),focus=selectManagerFocus(rows,null)!;
  expect(focus.caseId).toBe(f.caseId);
  const result=await managerReview(f,focus);
  expect(result.projected.cases.find((c:any)=>c.id===f.caseId).administrativeRecoveryReview)
    .toEqual(rows.find(c=>c.id===f.caseId)!.administrativeRecoveryReview);
  expect(result.events).toHaveLength(1);
  expect(result.events[0].payload.contextRequestFocus).toEqual(focus);
  expect(result.events[0].payload.receipts).not.toContainEqual(expect.objectContaining({code:'MANAGEMENT_FORMAT'}));
  const reread=await cases(f),entry=reread.find(c=>c.id===f.caseId)!;
  expect(entry.administrativeRecoveryReview).toMatchObject({priorityConsumed:true,lastCompletedReviewWasAdministrative:true,
    episodeDigest:rows.find(c=>c.id===f.caseId)!.administrativeRecoveryReview.episodeDigest});
  const next=selectManagerFocus(reread,focus)!;expect(deliveries).toContain(next.caseId);
  expect(await preserved(f)).toEqual(before);
});

it.each(['missing-intent-event','missing-readiness','missing-native-stop','wrong-native-fence','missing-bridge',
  'missing-stop-event','foreign-readiness','forged-case-cause','changed-contract',
  'wrong-context','wrong-request','live-run','lease','cancelled','new-attempt','closed-case'] as const)
('does not lend administrative priority for %s',async mode=>{
  const f=await administrativeFixture();expect((await administrative(f)).administrativeRecoveryReview).toBeDefined();
  if(mode==='missing-intent-event')await pool.query('DELETE FROM events WHERE kind=$1 AND entity_id=$2',[operatorRecoveryCode,f.task]);
  if(mode==='missing-readiness')await pool.query("DELETE FROM events WHERE kind='OPENCODE_RECOVERY_REVIEW_READY' AND entity_id=$1",[f.task]);
  if(mode==='missing-native-stop')await pool.query("UPDATE runs SET checkpoint=checkpoint-'nativeStopReceipt' WHERE id=$1",[f.run]);
  if(mode==='wrong-native-fence')await pool.query("UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{nativeStopReceipt,runFence}',to_jsonb($2::text)) WHERE id=$1",[f.run,randomUUID()]);
  if(mode==='missing-bridge')await pool.query("UPDATE runs SET checkpoint=checkpoint#-'{nativeStopReceipt,bridgeReceiptDigest}' WHERE id=$1",[f.run]);
  if(mode==='missing-stop-event')await pool.query("DELETE FROM events WHERE kind='DETACHED_RUN_STOP_RECONCILED' AND entity_id=$1",[f.task]);
  if(mode==='foreign-readiness')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{caseId}',to_jsonb($2::text)) WHERE kind='OPENCODE_RECOVERY_REVIEW_READY' AND entity_id=$1",[f.task,randomUUID()]);
  if(mode==='forged-case-cause'){
    const evidence=(await pool.query('SELECT evidence FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0].evidence;
    for(const e of evidence)if(e.kind===operatorRecoveryCode)e.requestId=randomUUID();
    await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[f.caseId,JSON.stringify(evidence)]);
  }
  if(mode==='changed-contract')await pool.query("UPDATE tasks SET description='Different current source obligation' WHERE id=$1",[f.task]);
  if(mode==='wrong-context'){
    const context=randomUUID();await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Another context')",[context,f.run]);
    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.run,context]);
  }
  if(mode==='wrong-request')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{planningRequestId}',to_jsonb($2::text)) WHERE id=$1",[f.task,randomUUID()]);
  if(mode==='live-run')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
  if(mode==='lease')await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',['retained-owner:'+f.run,f.run]);
  if(mode==='cancelled'){
    expect((await current(f)).task.state).toBe('recovery_blocked');
    await cancelTask(f.task);expect((await current(f)).task.state).toBe('cancelled');
    expect((await pool.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND entity_id=$1",[f.task])).rowCount).toBe(1);
  }
  if(mode==='new-attempt')await pool.query('UPDATE tasks SET attempt_count=attempt_count+1 WHERE id=$1',[f.task]);
  if(mode==='closed-case')await pool.query("UPDATE supervision_cases SET state='resolved' WHERE id=$1",[f.caseId]);
  const before=await preserved(f),entry=await administrative(f);
  expect(entry?.administrativeRecoveryReview).toBeUndefined();expect(await preserved(f)).toEqual(before);
});

it.each(['invalid-format','unaccepted','unrecorded','unrelated-focus'] as const)
('does not consume administrative priority from a %s review',async mode=>{
  const f=await administrativeFixture(),deliveries=await deliveryCases(f),rows=await cases(f);
  const focus=mode==='unrelated-focus'?selectManagerFocus(rows.filter(c=>deliveries.includes(c.id)),null)!:selectManagerFocus(rows,null)!;
  const result=await managerReview(f,focus,{invalid:mode==='invalid-format',accept:mode!=='unaccepted',record:mode!=='unrecorded'});
  if(mode==='invalid-format')expect(result.events[0].payload.receipts).toContainEqual(expect.objectContaining({code:'MANAGEMENT_FORMAT'}));
  if(mode==='unaccepted'||mode==='unrecorded')expect(result.events).toHaveLength(0);
  expect((await administrative(f)).administrativeRecoveryReview.priorityConsumed).toBe(false);
});

it('does not rearm an already served episode from chatter or a later acceptance revocation',async()=>{
  const f=await administrativeFixture(),deliveries=await deliveryCases(f);
  const focus=selectManagerFocus(await cases(f),null)!,result=await managerReview(f,focus);
  const old=(await administrative(f)).administrativeRecoveryReview;expect(old.priorityConsumed).toBe(true);
  await pool.query('UPDATE runs SET heartbeat_at=clock_timestamp() WHERE id=$1',[f.run]);
  // Metadata arrival does not change the administrative cause. This fixture
  // inserts only an evidence record; it grants no acceptance or runtime write.
  await pool.query(`INSERT INTO artifacts(id,run_id,name,mime,size,digest)
    VALUES($1,$2,'late-observation.txt','text/plain',1,$3)`,[randomUUID(),f.run,'e'.repeat(64)]);
  const after=(await administrative(f)).administrativeRecoveryReview;
  expect(after.episodeDigest).toBe(old.episodeDigest);expect(after.priorityConsumed).toBe(true);
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,false,'[]','latest-revocation',clock_timestamp()+interval '1 second')`,[randomUUID(),result.task,result.run]);
  // A later revoked acceptance does not undo the fact that this episode was
  // given its one accepted review. Action appliers still own current authority.
  expect((await administrative(f)).administrativeRecoveryReview.priorityConsumed).toBe(true);
  expect(deliveries).toContain(selectManagerFocus(await cases(f),focus)!.caseId);
});

it('alternates two administrative episodes with real delivery review instead of starving the original work',async()=>{
  const a=await administrativeFixture(),b=await administrativeFixture({goalId:a.goal,scope:'opencode-recovery/second'});
  const deliveries=await deliveryCases(a),rows=await cases(a);
  const initial=selectManagerFocus(rows,null)!;
  expect([a.caseId,b.caseId]).toContain(initial.caseId);
  const selected=initial.caseId===a.caseId?a:b,remaining=selected===a?b:a;
  await managerReview(selected,initial);
  const after=await cases(a),next=selectManagerFocus(after,initial)!;
  expect(deliveries).toContain(next.caseId);
  expect(after.find(c=>c.id===remaining.caseId)!.administrativeRecoveryReview.priorityConsumed).toBe(false);
  await managerReview(a,next);
  const last=selectManagerFocus(await cases(a),next)!;
  expect(last.caseId).toBe(remaining.caseId);
  expect(last).toHaveProperty('administrativeRecoveryEpisodeDigest',
    (await administrative(remaining)).administrativeRecoveryReview.episodeDigest);
  for(const f of [a,b]){
    expect((await current(f)).run.state).toBe('failed');
    expect((await current(f)).planning.state).toBe('awaiting_supervision');
    expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.task])).rowCount).toBe(1);
  }
});
