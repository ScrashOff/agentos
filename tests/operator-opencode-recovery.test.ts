// Server-only PostgreSQL integration: real ownership, locks and guarded writers.
// Only the remote inspection/stop boundary is replaced by deterministic receipts.
import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,migrate,tx} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installPlanningDemand,scopeSourceRevision,assignPlanningRequest} from '../server/planning-demand.js';
import {installSupervision,recordSupervisionDecisions,applySupervisionDecisions} from '../server/supervision.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installNativeSupervision,reserveNativeSupervisor,registerNativeSupervisor,startDetachedNativeRun,
  reconcileNativeSupervisor,nativeSupervisorLockNamespace,nativeSupervisorLockKey,nativeSupervisorCgroup,
  type NativeReservation,type NativeUnitObservation} from '../server/native-supervision.js';
import {heartbeat,cancelTask,completeRun,failRun,writeArtifact,validateTask} from '../server/service.js';
import {createPlanningDemands,reconcilePlanningDemand} from '../server/planning-demand-runtime.js';
import {operatorRecoveryCode,hasOperatorRecoveryIntent} from '../server/operator-recovery-contract.js';
import {requestOperatorOpenCodeRecovery,consumeOperatorOpenCodeRecovery,
  keepOperatorOpenCodePlannerInSupervision,operatorOpenCodeRecoveryOwnsScope,
  reviewStoppedOperatorOpenCodeRecovery,reconcileOperatorOpenCodeRecoveries} from '../server/operator-opencode-recovery.js';
import {operatorOpenCodeRecoveryCompatibleSources} from '../server/operator-opencode-recovery-contract.js';
import {reconcileStoppedDetachedOpenCode} from '../server/reconcile-stopped-opencode.js';
import {requestRecoveryReview} from '../server/recovery-supervision.js';
import {recoverOpenCodeRun} from '../server/opencode-recovery.js';
import {handoffOperatorOpenCodePlanner,reconcileOperatorOpenCodePlannerHandoffs} from '../server/operator-opencode-planner-handoff.js';
import {WorkspacePreparationWorker} from '../server/workspace-preparation.js';
import {accountBridge} from '../server/bridge.js';
import {operatorOpenCodeReviewIsFresh,staleOperatorOpenCodeReview} from '../server/operator-opencode-review.js';

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

async function fixture(options:{frozenPlanningRequestId?:string;largeContextBytes?:number}={}){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),context=randomUUID();
  const project='recovery-'+goal,scope='opencode-recovery/base',workspace='opencode-recovery/'+run;
  const planningRequest=randomUUID(),sourceRevision='b'.repeat(64);
  const historicalInputs=[['planning-query.mjs','// immutable helper from the old attempt'],
    ['planning-context.json',JSON.stringify({requestId:planningRequest,sourceRevision})]].map(([name,text])=>({
      id:randomUUID(),name,digest:createHash('sha256').update(text).digest('hex'),base64:Buffer.from(text).toString('base64')}));
  if(options.largeContextBytes){
    // Preserve the canonical per-input limit while making the frozen document
    // large: historical planners embed several separately valid inputs.
    for(let left=options.largeContextBytes,index=0;left>0;index++){
      const bytes=Buffer.alloc(Math.min(left,7*1024*1024),120);left-=bytes.length;
      historicalInputs.push({id:randomUUID(),name:'historical-padding-'+index+'.txt',
        digest:createHash('sha256').update(bytes).digest('hex'),base64:bytes.toString('base64')});
    }
  }
  const execution={kind:'opencode',managed:true,readOnlySource:true,workKind:'planning',baseWorkspace:scope,
    workspace,writePaths:['qa-protocol/','evidence/'],planningRequestId:planningRequest,inputs:historicalInputs};
  const criteria=[{id:'plan',label:'Bound exact scope plan',type:'artifact',name:'scope-plan.json'}];
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1::uuid,'Bound planning recovery','Preserve the original delivery gates',$2,$1::text,'active')`,[goal,project]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('glm','GLM fixture','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'fixture-glm') ON CONFLICT(id) DO NOTHING`);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', ['workforce:'+goal,JSON.stringify({enabled:true,
    glmAccountId:'glm',glmTarget:12,glmRequestLimit:6,museTarget:0,workerAccounts:[],coordinatorAccountId:'sol',coordinatorModel:'gpt-6.1-sol'})]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,'scoped-planner','Bound planner','Keep immutable planning authority','qa.backend','queued','work',$3,$4,1)`,
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
    VALUES($1,$2,'final-consumer','Final delivery','Preserve independent final acceptance','engineering.integration',
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
async function noNewAttemptOrCancellation(f:Fixture){
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.task])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",[f.run])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.task])).rowCount).toBe(0);
}
async function startSupervisionReview(f:Fixture,caseId:string){
  await pool.query("INSERT INTO settings(id,value) VALUES($1,'{\"enabled\":true}') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    ['management:'+f.goal]);
  const coordinator=randomUUID(),decisionRun=randomUUID(),decisionFence=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Review stopped planner','Review exact operator evidence','research.requirements','running','coordinate',$4,1)`,
    [coordinator,f.goal,'review-'+coordinator,JSON.stringify([{id:'decision',label:'Structured supervision decision',type:'artifact',name:'coordination.json'}])]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,instruction_snapshot)
    VALUES($1,$2,1,$3,'harness',1,'running',$4)`,[decisionRun,coordinator,decisionFence,
      JSON.stringify({schema:1,task:{id:coordinator,goal_id:f.goal,title:'Review stopped planner',description:'Frozen case review',execution:{}}})]);
  const action={caseId,kind:'revise',
    diagnosis:'The old scanner repeats its first catalog page because it reads the wrong cursor field.',
    instruction:'Create one fresh scoped planning request with the current signed helper and catalog; read selection.nextOffset and preserve final delivery criteria and old evidence.'};
  return {caseId,decisionRun,decisionFence,coordinator,action};
}
async function publishSupervisionReview(review:Awaited<ReturnType<typeof startSupervisionReview>>){
  await writeArtifact(review.decisionRun,review.decisionFence,'coordination.json',JSON.stringify({supervision:{actions:[review.action]}}));
  await completeRun(review.decisionRun,review.decisionFence,'Supervised current-catalog handoff',{});await validateTask(review.coordinator);
  expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC LIMIT 1',[review.decisionRun])).rows[0]?.passed).toBe(true);
}
async function supervisedRevision(f:Fixture,apply=true){
  const owner=await reviewStoppedOperatorOpenCodeRecovery(f.run,async()=>terminalInspection(f));expect(owner?.caseId).toBeTruthy();
  const review=await startSupervisionReview(f,owner!.caseId);await publishSupervisionReview(review);
  await recordSupervisionDecisions();
  expect((await pool.query('SELECT state,decision_run_id FROM supervision_cases WHERE id=$1',[review.caseId])).rows[0])
    .toMatchObject({state:'decided',decision_run_id:review.decisionRun});
  if(apply){
    await applySupervisionDecisions(async()=>terminalInspection(f));
    expect((await pool.query('SELECT state,applied_attempt FROM supervision_cases WHERE id=$1',[review.caseId])).rows[0])
      .toMatchObject({state:'applied',applied_attempt:1});
  }
  return review;
}
const handoff=(f:Fixture,inspect=async()=>terminalInspection(f))=>
  handoffOperatorOpenCodePlanner(f.run,inspect,async()=>stoppedOwner(f.reservation));

it('persists one explicit intent under concurrent requests while preserving frozen authority, artifacts and real owner leases',async()=>{
  const f=await fixture();await writeArtifact(f.run,f.fence,'partial.json','{"preserved":true}');
  const before=await current(f),artifacts=(await pool.query('SELECT * FROM artifacts WHERE run_id=$1',[f.run])).rows;
  const results=await Promise.all(Array.from({length:6},()=>request(f)));
  expect(results.filter(r=>!r.duplicate)).toHaveLength(1);
  for(const result of results)expect(result).toMatchObject({state:'reconciling',runtime:'opencode',phase:'requested',
    code:operatorRecoveryCode,stopConfirmed:false,newAttemptQueued:false});
  const after=await current(f);
  expect(after.run).toMatchObject({state:'reconciling',error_code:operatorRecoveryCode,fence:f.fence,context_id:f.context,finished_at:null});
  expect(after.run.instruction_snapshot).toEqual(before.run.instruction_snapshot);
  expect(after.run.checkpoint.retained).toBe(true);
  expect(after.run.checkpoint.operatorRecoveryStop.externalEvidence).toEqual({operatorSupplied:true,contextOnly:true,verifiedByServer:false});
  expect(after.run.checkpoint.operatorRecoveryStop.sourceRevision).toBe(f.sourceRevision);
  expect(after.task.execution).toEqual(before.task.execution);expect(after.task.criteria).toEqual(before.task.criteria);
  expect(after.task.attempt_count).toBe(1);expect(after.job).toEqual(before.job);expect(after.leases).toEqual(before.leases);
  expect(after.planning.state).toBe('awaiting_supervision');expect(after.planning.snapshot).toEqual(before.planning.snapshot);
  expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1',[f.run])).rows).toEqual(artifacts);
  expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2',[f.task,operatorRecoveryCode])).rowCount).toBe(1);
  expect(await hasOperatorRecoveryIntent(pool,after.run)).toBe(true);
  await noNewAttemptOrCancellation(f);
});

it('returns only the original historical receipt after pause and never retargets another request',async()=>{
  const f=await fixture();await request(f);await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  const inspect=vi.fn(async()=>liveIdentity(f));expect(await request(f,inspect)).toMatchObject({duplicate:true,runId:f.run});
  expect(inspect).not.toHaveBeenCalled();const before=await current(f);
  await expect(requestOperatorOpenCodeRecovery(f.task,{...f.request,requestId:randomUUID()},
    {channel:'api-token'},inspect,release.candidate)).rejects.toThrow();
  expect(await current(f)).toEqual(before);expect(inspect).not.toHaveBeenCalled();
});

it.each(['fence','context','owner-gone','owner-token','owner-pid','cancel','pause','autonomy','supervision',
  'source-revision','contract','finished','adoption'])
('fails closed if %s changes while the initial remote identity is inspected',async mode=>{
  const f=await fixture();let afterRace:Awaited<ReturnType<typeof current>>|undefined;
  const inspect=async()=>{
    if(mode==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
    if(mode==='context'){
      const replacement=randomUUID();await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Replacement')",[replacement,f.run]);
      await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.run,replacement]);
    }
    if(mode==='owner-gone')await f.owner.query('SELECT pg_advisory_unlock_all()');
    if(mode==='owner-token')await pool.query("UPDATE opencode_jobs SET supervision=jsonb_set(supervision,'{launchToken}',to_jsonb($2::text)) WHERE run_id=$1",[f.run,randomUUID()]);
    if(mode==='owner-pid')await pool.query("UPDATE opencode_jobs SET supervision=jsonb_set(supervision,'{owner,pid}','12346') WHERE run_id=$1",[f.run]);
    if(mode==='cancel')await cancelTask(f.task);
    if(mode==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
    if(mode==='autonomy')await pool.query("INSERT INTO settings(id,value) VALUES($1,'{\"enabled\":false}')",['autonomy:'+f.project]);
    if(mode==='supervision')await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['supervision:'+f.goal]);
    if(mode==='source-revision')await pool.query('UPDATE planning_requests SET source_revision=$2 WHERE id=$1',[f.planningRequest,'e'.repeat(64)]);
    if(mode==='contract')await pool.query("UPDATE tasks SET description='Changed operator instructions' WHERE id=$1",[f.task]);
    if(mode==='finished')await completeRun(f.run,f.fence,'A concurrent final callback',{});
    if(mode==='adoption')await pool.query("DELETE FROM settings WHERE id='worker-readiness:agentos-backlog'");
    afterRace=await current(f);return liveIdentity(f);
  };
  await expect(request(f,inspect)).rejects.toThrow();expect(afterRace).toBeDefined();
  expect(await current(f)).toEqual(afterRace);
  expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2',[f.task,operatorRecoveryCode])).rowCount).toBe(0);
});

it('rejects changes to frozen instructions without changing the actor or authorizing administrative recovery',async()=>{
  const f=await fixture(),before=await current(f);
  await expect(pool.query("UPDATE runs SET instruction_snapshot=jsonb_set(instruction_snapshot,'{task,description}','\"Changed frozen instructions\"') WHERE id=$1",[f.run]))
    .rejects.toThrow('INSTRUCTION_SNAPSHOT_IMMUTABLE');
  expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2',[f.task,operatorRecoveryCode])).rowCount).toBe(0);
});

it.each(['stale','wrong-fence','wrong-context','wrong-owner-pid','wrong-workspace','unreviewed-owner','wrong-unit','wrong-attempt','missing-confirmation','extra-command'])
('rejects %s without a durable intent or physical dispatch',async mode=>{
  const f=await fixture(),proof=liveIdentity(f),raw:any={...f.request};
  if(mode==='stale')proof.checkedAt='2000-01-01T00:00:00Z';
  if(mode==='wrong-fence')proof.runFence=randomUUID();
  if(mode==='wrong-context')proof.contextVersion=randomUUID();
  if(mode==='wrong-owner-pid')proof.owner.mainPid++;
  if(mode==='wrong-workspace')proof.workspace='opencode-recovery/other';
  if(mode==='unreviewed-owner')proof.owner.sourceHashes[Object.keys(proof.owner.sourceHashes)[0]]='f'.repeat(64);
  if(mode==='wrong-unit')proof.process.unit='agentos-run-'+randomUUID()+'.service';
  if(mode==='wrong-attempt')raw.expectedAttempt=2;
  if(mode==='missing-confirmation')raw.operatorConfirmed=false;
  if(mode==='extra-command')raw.command='kill arbitrary process';
  const before=await current(f);
  await expect(requestOperatorOpenCodeRecovery(f.task,raw,{channel:'api-token'},async()=>proof,release.candidate)).rejects.toThrow();
  expect(await current(f)).toEqual(before);
});

it('requires authenticated operator authority before any inspection or mutation',async()=>{
  const f=await fixture(),before=await current(f),inspect=vi.fn(async()=>liveIdentity(f));
  await expect(requestOperatorOpenCodeRecovery(f.task,f.request,{channel:'model-tool'} as any,inspect,release.candidate))
    .rejects.toMatchObject({code:'OPERATOR_REQUIRED'});
  expect(inspect).not.toHaveBeenCalled();expect(await current(f)).toEqual(before);
});

it('reports an inspection transport failure without persisting intent, leaking its payload or dispatching a stop',async()=>{
  const f=await fixture(),before=await current(f);
  const telemetry=vi.spyOn(console,'warn').mockImplementation(()=>{});
  const privateMarker='PRIVATE_INSPECTOR_PAYLOAD_MUST_NOT_ESCAPE';
  const inspect=vi.fn(async()=>{throw Object.assign(Error(privateMarker),{
    code:'ETIMEDOUT',stderr:privateMarker,stdout:privateMarker});});
  const failure=await request(f,inspect).then(()=>null,error=>error);
  expect(failure).toMatchObject({code:'OPENCODE_RECOVERY_INSPECTION_UNAVAILABLE',status:503});
  expect(String(failure.message)+JSON.stringify(failure)).not.toContain(privateMarker);
  expect(telemetry).toHaveBeenCalled();
  const logs=telemetry.mock.calls.map(call=>JSON.parse(String(call[0])));
  for(const record of logs){
    expect(Object.keys(record).sort()).toEqual(['code','elapsedMs','event','phase','phaseElapsedMs','requestId','runId','version']);
    expect(record).toMatchObject({event:'OPENCODE_RECOVERY_PREFLIGHT',version:1,requestId:f.request.requestId,runId:f.run});
    expect(record.elapsedMs).toBeGreaterThanOrEqual(0);expect(record.phaseElapsedMs).toBeGreaterThanOrEqual(0);
  }
  expect(JSON.stringify(logs)).not.toContain(privateMarker);
  expect(logs.some(record=>record.code===failure.code)).toBe(true);
  expect(inspect).toHaveBeenCalledOnce();expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2',[f.task,operatorRecoveryCode])).rowCount).toBe(0);
  const stop=vi.fn(async()=>({observed:true})),again=vi.fn(async()=>liveIdentity(f));
  expect(await consume(f,stop,again)).toBe(false);
  expect(stop).not.toHaveBeenCalled();expect(again).not.toHaveBeenCalled();
  await noNewAttemptOrCancellation(f);
});

it('rechecks the unchanged ten-second identity lifetime after waiting for the real transaction lock',async()=>{
  const f=await fixture(),before=await current(f);
  await f.owner.query('SELECT pg_advisory_lock(4280001)');
  let proof:ReturnType<typeof liveIdentity>|undefined;
  const inspect=vi.fn(async()=>{
    proof={...liveIdentity(f),checkedAt:new Date(Date.now()-9000).toISOString()};
    return proof;
  });
  const pending=request(f,inspect).then(value=>({value,error:null}),error=>({value:null,error}));
  let waiting=false;
  try{
    const deadline=Date.now()+3000;
    while(Date.now()<deadline){
      waiting=!!(await pool.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory'
        AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
        AND classid=0 AND objid=4280001 AND objsubid=1 AND NOT granted LIMIT 1`)).rowCount;
      if(waiting&&proof)break;
      await new Promise(resolve=>setTimeout(resolve,20));
    }
    expect(waiting).toBe(true);expect(proof).toBeDefined();
    const expiresAt=Date.parse(proof!.checkedAt)+10001;
    if(Date.now()<=expiresAt)await new Promise(resolve=>setTimeout(resolve,expiresAt-Date.now()+20));
  }finally{
    await f.owner.query('SELECT pg_advisory_unlock(4280001)');
    // Settle the tested request even if an assertion fails, before another test
    // can truncate this database. The real owner lock remains held as usual.
    await pending;
  }
  const result=await pending;
  expect(result.value).toBeNull();
  expect(result.error).toMatchObject({code:'OPENCODE_RECOVERY_OBSERVATION_EXPIRED',status:409});
  expect(inspect).toHaveBeenCalledOnce();expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2',[f.task,operatorRecoveryCode])).rowCount).toBe(0);
  const stop=vi.fn(async()=>({observed:true}));expect(await consume(f,stop)).toBe(false);
  expect(stop).not.toHaveBeenCalled();await noNewAttemptOrCancellation(f);
},15000);

it('rolls back a real database error during intent persistence without claiming the commit was confirmed',async()=>{
  const f=await fixture(),before=await current(f),privateMarker='PRIVATE_DATABASE_EVENT_MUST_NOT_ESCAPE';
  const telemetry=vi.spyOn(console,'warn').mockImplementation(()=>{});
  // Test-database-only fault at the real event writer, after the guarded UPDATEs.
  // A temporary function keeps this failure injection out of application code.
  await f.owner.query(`CREATE FUNCTION pg_temp.operator_recovery_test_failure() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN IF NEW.kind='OPERATOR_RECOVERY_REQUESTED' AND NEW.entity_id=TG_ARGV[0] THEN
      RAISE EXCEPTION 'PRIVATE_DATABASE_EVENT_MUST_NOT_ESCAPE' USING ERRCODE='57014';
    END IF; RETURN NEW; END $$`);
  await f.owner.query(`CREATE TRIGGER operator_recovery_test_failure BEFORE INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION pg_temp.operator_recovery_test_failure('${f.task}')`);
  const inspect=vi.fn(async()=>liveIdentity(f));
  let failure:any;
  try{failure=await request(f,inspect).then(()=>null,error=>error);}
  finally{await f.owner.query('DROP TRIGGER operator_recovery_test_failure ON events');
    await f.owner.query('DROP FUNCTION pg_temp.operator_recovery_test_failure()');}
  expect(failure).toMatchObject({code:'OPENCODE_RECOVERY_COMMIT_UNCONFIRMED',status:503});
  expect(String(failure.message)+JSON.stringify(failure)+JSON.stringify(telemetry.mock.calls)).not.toContain(privateMarker);
  expect(inspect).toHaveBeenCalledOnce();expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2',[f.task,operatorRecoveryCode])).rowCount).toBe(0);
  const stop=vi.fn(async()=>({observed:true}));expect(await consume(f,stop)).toBe(false);
  expect(stop).not.toHaveBeenCalled();await noNewAttemptOrCancellation(f);
});

it('preserves the original contract digest for one real 17 MiB frozen planning context',async()=>{
  // One large fixture exercises TOAST/detoast, without replicating a production
  // table's churn or changing the other recovery fixtures into stress tests.
  const f=await fixture({largeContextBytes:12*1024*1024+768*1024});
  const before=(await pool.query(`SELECT octet_length(r.instruction_snapshot::text) snapshot_bytes,
    encode(sha256(convert_to(r.instruction_snapshot::text,'UTF8')),'hex') snapshot_digest,
    encode(sha256(convert_to(jsonb_build_array(t.description,t.criteria,t.resources,t.allowed_accounts,
      t.required_memories,t.recovery,m.control,t.execution->'planningRequestId',m.input_digests,r.instruction_snapshot)::text,'UTF8')),'hex') contract_digest
    FROM runs r JOIN tasks t ON t.id=r.task_id JOIN task_execution_metadata m ON m.task_id=t.id
    WHERE r.id=$1`,[f.run])).rows[0];
  expect(Number(before.snapshot_bytes)).toBeGreaterThanOrEqual(17*1024*1024);
  const leases=(await pool.query('SELECT resource FROM leases WHERE run_id=$1 ORDER BY resource',[f.run])).rows;
  const inspect=vi.fn(async(row:any)=>{
    expect(row.contract_digest).toBe(before.contract_digest);
    expect(row).not.toHaveProperty('instruction_snapshot');
    return liveIdentity(f);
  });
  expect(await requestOperatorOpenCodeRecovery(f.task,f.request,{channel:'api-token'},inspect,release.candidate))
    .toMatchObject({phase:'requested',stopConfirmed:false,newAttemptQueued:false});
  expect(inspect).toHaveBeenCalledOnce();
  const after=(await pool.query(`SELECT state,checkpoint#>>'{operatorRecoveryStop,contractDigest}' contract_digest,
    encode(sha256(convert_to(instruction_snapshot::text,'UTF8')),'hex') snapshot_digest
    FROM runs WHERE id=$1`,[f.run])).rows[0];
  expect(after).toEqual({state:'reconciling',contract_digest:before.contract_digest,snapshot_digest:before.snapshot_digest});
  expect((await pool.query('SELECT resource FROM leases WHERE run_id=$1 ORDER BY resource',[f.run])).rows).toEqual(leases);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OPENCODE_RECOVERY_STOP_DISPATCHED' AND payload->>'runId'=$1",[f.run])).rowCount).toBe(0);
  await noNewAttemptOrCancellation(f);
},30000);

it.each(['foreign-context','frozen-planning-request'])('rejects an apparently matching %s which is not owned by this canonical attempt',async mode=>{
  const f=await fixture(mode==='frozen-planning-request'?{frozenPlanningRequestId:randomUUID()}:{}),raw={...f.request},proof=liveIdentity(f);
  if(mode==='foreign-context'){
    const otherRun=randomUUID(),foreignContext=randomUUID();
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
      VALUES($1,$2,1,$3,'harness',1,'failed')`,[otherRun,f.consumer,randomUUID()]);
    await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Different actor context')",[foreignContext,otherRun]);
    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.run,foreignContext]);
    raw.contextVersion=foreignContext;proof.contextVersion=foreignContext;
  }
  const before=await current(f),inspect=vi.fn(async()=>proof);
  await expect(requestOperatorOpenCodeRecovery(f.task,raw,{channel:'api-token'},inspect,release.candidate)).rejects.toThrow();
  expect(await current(f)).toEqual(before);expect(inspect).not.toHaveBeenCalled();
});

it('dispatches once under consumer concurrency even though the legacy owner interprets a withdrawn heartbeat as running',async()=>{
  const f=await fixture();await request(f);const before=await current(f);
  // This is the actual old OpenCode fallback, not the new Harness heartbeat loop.
  expect((await heartbeat(f.run,f.fence))?.state??'running').toBe('running');
  const stop=vi.fn(async(payload:{requestId:string;identity:any})=>{
    expect(payload).toMatchObject({requestId:f.request.requestId,identity:{runId:f.run,runFence:f.fence,contextVersion:f.context}});
    const during=await current(f);expect(during.run.checkpoint.operatorOpenCodeRecovery.phase).toBe('stopping');
    expect(during.leases).toEqual(before.leases);return {stopped:true};
  });
  const results=await Promise.all(Array.from({length:6},()=>consume(f,stop)));
  expect(results.filter(Boolean)).toHaveLength(1);expect(stop).toHaveBeenCalledOnce();
  const after=await current(f);expect(after.run.state).toBe('reconciling');expect(after.job).toEqual(before.job);
  expect(after.run.checkpoint.operatorOpenCodeRecovery).toMatchObject({phase:'uncertain',observationReceived:true});
  expect(after.run.checkpoint.operatorRecoveryStop).toEqual(before.run.checkpoint.operatorRecoveryStop);
  expect(after.run.checkpoint.nativeStopReceipt).toBeUndefined();expect(after.leases).toEqual(before.leases);
  const inspect=vi.fn(async()=>liveIdentity(f));expect(await consume(f,stop,inspect)).toBe(false);
  expect(inspect).not.toHaveBeenCalled();expect(stop).toHaveBeenCalledOnce();await noNewAttemptOrCancellation(f);
});

it.each(['pid-reused','context','fence','source-revision','owner-token','pause','cancel'])
('does not signal after %s changes between administrative intent and dispatch',async mode=>{
  const f=await fixture();await request(f);const proof=liveIdentity(f);
  if(mode==='pid-reused')proof.process.startTicks='1235';
  if(mode==='context')await pool.query('UPDATE runs SET context_id=NULL WHERE id=$1',[f.run]);
  if(mode==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
  if(mode==='source-revision')await pool.query('UPDATE planning_requests SET source_revision=$2 WHERE id=$1',[f.planningRequest,'e'.repeat(64)]);
  if(mode==='owner-token')await pool.query("UPDATE opencode_jobs SET supervision=jsonb_set(supervision,'{launchToken}',to_jsonb($2::text)) WHERE run_id=$1",[f.run,randomUUID()]);
  if(mode==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='cancel')await cancelTask(f.task);
  const before=await current(f),stop=vi.fn(async()=>({observed:true}));
  expect(await consume(f,stop,async()=>proof)).toBe(false);expect(stop).not.toHaveBeenCalled();expect(await current(f)).toEqual(before);
});

it('keeps leases after an uncertain transport and refuses another signal or closure while the owner lock is alive',async()=>{
  const f=await fixture();await request(f);const before=await current(f);
  const stop=vi.fn(async()=>{throw Error('SSH transport ended after delivery may have happened');});
  expect(await consume(f,stop)).toBe(true);expect(await consume(f,stop)).toBe(false);expect(stop).toHaveBeenCalledOnce();
  expect(await tx(db=>reconcileNativeSupervisor(db,f.reservation,stoppedOwner(f.reservation)))).toBe('observing');
  expect(await reconcileStoppedDetachedOpenCode(f.reservation,stoppedOwner(f.reservation),terminalInspection(f))).toBe(false);
  const after=await current(f);expect(after.run.state).toBe('reconciling');expect(after.run.finished_at).toBeNull();
  expect(after.run.checkpoint.operatorOpenCodeRecovery).toMatchObject({phase:'uncertain',observationReceived:false});
  expect(after.leases).toEqual(before.leases);expect(after.job).toEqual(before.job);await noNewAttemptOrCancellation(f);
});

it('rechecks a pause that races with the final inspection before committing physical dispatch',async()=>{
  const f=await fixture();await request(f);let afterPause:Awaited<ReturnType<typeof current>>|undefined;
  const inspect=async()=>{await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
    afterPause=await current(f);return liveIdentity(f);};
  const stop=vi.fn(async()=>({observed:true}));expect(await consume(f,stop,inspect)).toBe(false);
  expect(afterPause).toBeDefined();expect(stop).not.toHaveBeenCalled();expect(await current(f)).toEqual(afterPause);
});

it.each(['sandbox-alive','stale-proof','wrong-workspace','missing-terminal','wrong-token','missing-bridge','bridge-before-request','bridge-wrong-unit','terminal-not-stopped'])
('keeps resources when %s prevents independent stop confirmation',async mode=>{
  const f=await fixture();await request(f);await consume(f);await f.owner.query('SELECT pg_advisory_unlock_all()');
  await tx(db=>reconcileNativeSupervisor(db,f.reservation,stoppedOwner(f.reservation)));
  const before=await current(f),proof:any=terminalInspection(f);
  if(mode==='sandbox-alive')proof.process.stopped=false;
  if(mode==='stale-proof')proof.checkedAt='2000-01-01T00:00:00Z';
  if(mode==='wrong-workspace'){proof.workspace='opencode-recovery/other';proof.state.workspace=proof.workspace;}
  if(mode==='missing-terminal')delete proof.state;
  if(mode==='missing-bridge')delete proof.bridgeReceipt;
  if(mode==='bridge-before-request')proof.bridgeReceipt={...proof.bridgeReceipt,proof:{...proof.bridgeReceipt.proof,checkedAt:'2000-01-01T00:00:00Z'}};
  if(mode==='bridge-wrong-unit')proof.bridgeReceipt={...proof.bridgeReceipt,proof:{...proof.bridgeReceipt.proof,unit:'agentos-run-'+randomUUID()}};
  if(mode==='terminal-not-stopped')proof.state.stopped=false;
  const reservation=mode==='wrong-token'?{...f.reservation,launchToken:randomUUID()}:f.reservation;
  if(mode==='wrong-token')expect(await reconcileStoppedDetachedOpenCode(reservation,stoppedOwner(reservation),proof)).toBe(false);
  else await expect(reconcileStoppedDetachedOpenCode(reservation,stoppedOwner(reservation),proof)).rejects.toThrow();
  expect(await current(f)).toEqual(before);expect(before.job.supervision.resources.state).toBe('reserved');
});

it('closes only after both owners stop, then gives supervision one recoverable failure without inventing cancellation or acceptance',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const stopped=await current(f);
  expect(stopped.run).toMatchObject({state:'failed',error_code:operatorRecoveryCode});expect(stopped.run.finished_at).not.toBeNull();
  expect(stopped.run.checkpoint.nativeStopReceipt).toMatchObject({runFence:f.fence,actorCode:'OPENCODE_STOPPED'});
  expect(stopped.leases).toHaveLength(0);expect(stopped.job.state).toBe('finished');expect(stopped.job.supervision.resources.state).toBe('released');
  expect(stopped.planning.state).toBe('awaiting_supervision');
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
  await Promise.all(Array.from({length:3},()=>reconcileOperatorOpenCodeRecoveries({reviewInspect:async()=>terminalInspection(f)})));
  const cases=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
  expect(cases).toHaveLength(1);expect(cases[0]).toMatchObject({source_run_id:f.run,state:'open'});
  expect(cases[0].evidence).toContainEqual(expect.objectContaining({kind:operatorRecoveryCode,requestId:f.request.requestId,
    administrative:true,gameFailureClaimed:false,stopConfirmed:true,contextOnly:true}));
  expect(await request(f)).toMatchObject({duplicate:true,stopConfirmed:true,newAttemptQueued:false});
  await noNewAttemptOrCancellation(f);
});

it.each(['wrong-process-unit','active-process','unknown-process-load','wrong-owner-unit','active-owner','pending-owner-job','live-owner-cgroup','missing-bridge-digest','wrong-fence'])
('does not report stopConfirmed from a contradictory %s historical receipt',async mode=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  expect(await request(f)).toMatchObject({duplicate:true,stopConfirmed:true});
  const receipt=structuredClone((await current(f)).run.checkpoint.nativeStopReceipt);
  if(mode==='wrong-process-unit')receipt.process.unit='agentos-run-'+randomUUID();
  if(mode==='active-process')receipt.process.activeState='active';
  if(mode==='unknown-process-load')delete receipt.process.loadState;
  if(mode==='wrong-owner-unit')receipt.supervisor.unit='agentos-native-supervisor-'+randomUUID();
  if(mode==='active-owner')receipt.supervisor.activeState='active';
  if(mode==='pending-owner-job')receipt.supervisor.pendingJob=true;
  if(mode==='live-owner-cgroup')receipt.supervisor.cgroupEmpty=false;
  if(mode==='missing-bridge-digest')delete receipt.bridgeReceiptDigest;
  if(mode==='wrong-fence')receipt.runFence=randomUUID();
  await pool.query("UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{nativeStopReceipt}',$2::jsonb) WHERE id=$1",[f.run,JSON.stringify(receipt)]);
  const before=await current(f),inspect=vi.fn(async()=>liveIdentity(f));
  expect(await request(f,inspect)).toMatchObject({duplicate:true,stopConfirmed:false,newAttemptQueued:false});
  expect(inspect).not.toHaveBeenCalled();expect(await current(f)).toEqual(before);
});

it('finds the canonical scope owner behind more than eight newer requests with no administrative intent',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  for(let i=0;i<9;i++)await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,demand_epoch,state,reason,snapshot,task_id,created_at)
    VALUES($1,$2,$3,'glm',$4,$5,'awaiting_supervision','Other historical request','{}',$6,now()+($5::int*interval '1 second'))`,
    [randomUUID(),f.goal,f.scope,'e'.repeat(64),i+1,f.consumer]);
  expect(await operatorOpenCodeRecoveryOwnsScope(pool,f.goal,f.scope)).toBe(true);
  expect(await createPlanningDemands(f.goal)).toEqual([]);
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1 AND scope=$2',[f.goal,f.scope])).rowCount).toBe(10);
  expect((await current(f)).planning.state).toBe('awaiting_supervision');await noNewAttemptOrCancellation(f);
});

it('the automatic review consumer enriches a competing generic case exactly once',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const caseId=randomUUID();await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
    VALUES($1,$2,$3,$4,'Generic stopped planner review','[]')`,[caseId,f.goal,f.task,f.run]);
  for(let i=0;i<2;i++)await reconcileOperatorOpenCodeRecoveries({reviewInspect:async()=>terminalInspection(f)});
  const cases=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
  expect(cases).toHaveLength(1);expect(cases[0].id).toBe(caseId);
  expect(cases[0].evidence).toContainEqual(expect.objectContaining({kind:operatorRecoveryCode,requestId:f.request.requestId,
    administrative:true,contextOnly:true,stopConfirmed:true}));
  expect(cases[0].evidence.filter((entry:any)=>entry.kind===operatorRecoveryCode)).toHaveLength(1);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='SUPERVISION_EVIDENCE_RECORDED'",[f.task])).rowCount).toBe(1);
});

it.each(['in-flight','decided'])('requires a new review when administrative evidence arrives after the %s generic review was frozen',async phase=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const caseId=randomUUID();await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
    VALUES($1,$2,$3,$4,'Competing generic review before administrative evidence','[]')`,[caseId,f.goal,f.task,f.run]);
  const stale=await startSupervisionReview(f,caseId);
  if(phase==='decided'){
    await publishSupervisionReview(stale);await recordSupervisionDecisions();
    expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[caseId])).rows[0].state).toBe('decided');
  }
  const frozen=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[stale.decisionRun])).rows[0].instruction_snapshot;
  await reconcileOperatorOpenCodeRecoveries({reviewInspect:async()=>terminalInspection(f)});
  const readiness=(await pool.query(`SELECT e.id::text id,r.created_at<e.created_at stale FROM events e JOIN runs r ON r.id=$2
    WHERE e.kind='OPENCODE_RECOVERY_REVIEW_READY' AND e.entity_id=$1 AND e.payload->>'caseId'=$3`,[f.task,stale.decisionRun,caseId])).rows;
  expect(readiness).toHaveLength(1);expect(readiness[0].stale).toBe(true);
  if(phase==='in-flight'){await publishSupervisionReview(stale);await recordSupervisionDecisions();}
  await applySupervisionDecisions(async()=>terminalInspection(f));
  const rejected=(await pool.query('SELECT state,decision_run_id FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
  expect(rejected).toMatchObject({state:'open',decision_run_id:null});expect(await handoff(f)).toBeNull();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_APPLIED' AND entity_id=$1 AND payload->>'decisionRunId'=$2",
    [f.task,stale.decisionRun])).rowCount).toBe(0);
  if(phase==='decided')expect((await pool.query(`SELECT 1 FROM events WHERE entity_id=$1
    AND kind='DELIVERY_SOURCE_DECISION_REJECTED' AND payload->>'code'='OPENCODE_RECOVERY_REVIEW_STALE'
    AND payload->>'decisionRunId'=$2`,[f.task,stale.decisionRun])).rowCount).toBe(1);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[stale.decisionRun])).rows[0].instruction_snapshot).toEqual(frozen);
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1',[f.goal])).rowCount).toBe(1);
  const fresh=await supervisedRevision(f);expect(fresh.caseId).toBe(caseId);expect(fresh.decisionRun).not.toBe(stale.decisionRun);
  expect(await handoff(f)).toMatchObject({caseId,decisionRunId:fresh.decisionRun,sourceRunId:f.run});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OPENCODE_RECOVERY_REVIEW_READY' AND entity_id=$1",[f.task])).rowCount).toBe(1);
});

it('uses PostgreSQL precision for authority when JavaScript collection cannot distinguish reviews in the same millisecond',async()=>{
  const evidence=[{kind:operatorRecoveryCode,runtime:'opencode',observedAt:'2026-10-17T03:00:00.123400Z'}];
  // Collection permits both rounded timestamps; neither grants application.
  expect(operatorOpenCodeReviewIsFresh(evidence,'2026-10-17T03:00:00.123200Z')).toBe(true);
  expect(operatorOpenCodeReviewIsFresh(evidence,'2026-10-17T03:00:00.123600Z')).toBe(true);
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const owner=await reviewStoppedOperatorOpenCodeRecovery(f.run,async()=>terminalInspection(f));
  const review=await startSupervisionReview(f,owner!.caseId);
  const c=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[owner!.caseId])).rows[0];
  for(const [delta,expected] of [[-1,false],[1,true]] as const){
    await pool.query(`UPDATE runs SET created_at=(SELECT created_at FROM events WHERE kind='OPENCODE_RECOVERY_REVIEW_READY'
      AND entity_id=$2 AND payload->>'caseId'=$3 ORDER BY id DESC LIMIT 1)+($4::int*interval '1 microsecond') WHERE id=$1`,
      [review.decisionRun,f.task,c.id,delta]);
    const phase=await staleOperatorOpenCodeReview(pool,c,review.decisionRun);
    if(expected)expect(phase).toBeNull();else expect(phase).toMatchObject({reviewIsFresh:false});
  }
  expect(await handoff(f)).toBeNull();await noNewAttemptOrCancellation(f);
});

it('creates one new administrative generation after an unaware generic revision was already applied, preserving its history',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const oldCase=randomUUID();await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
    VALUES($1,$2,$3,$4,'Generic revision raced before administrative attachment','[]')`,[oldCase,f.goal,f.task,f.run]);
  const oldReview=await startSupervisionReview(f,oldCase);await publishSupervisionReview(oldReview);
  await recordSupervisionDecisions();await applySupervisionDecisions(async()=>terminalInspection(f));
  const old=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[oldCase])).rows[0];
  expect(old).toMatchObject({state:'applied',decision_run_id:oldReview.decisionRun,applied_attempt:1});
  expect(await handoff(f)).toBeNull();
  const runBefore=(await current(f)).run;
  for(let i=0;i<2;i++)await reconcileOperatorOpenCodeRecoveries({reviewInspect:async()=>terminalInspection(f)});
  const cases=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
  expect(cases).toHaveLength(2);expect(cases.find(c=>c.id===oldCase)).toEqual(old);
  const administrative=cases.find(c=>c.id!==oldCase);
  expect(administrative).toMatchObject({state:'open',parent_case_id:oldCase,source_run_id:f.run});
  expect(administrative.evidence).toContainEqual(expect.objectContaining({kind:operatorRecoveryCode,runtime:'opencode',requestId:f.request.requestId}));
  expect(await handoff(f)).toBeNull();expect((await current(f)).run).toEqual(runBefore);
  const fresh=await supervisedRevision(f);expect(fresh.caseId).toBe(administrative.id);
  expect(await handoff(f)).toMatchObject({caseId:administrative.id,decisionRunId:fresh.decisionRun});
  expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[oldCase])).rows[0]).toEqual(old);
  await noNewAttemptOrCancellation(f);
});

it('automatic dispatch visits a third intent while the first two identities cannot be freshly confirmed',async()=>{
  const fs:Fixture[]=[];for(let i=0;i<3;i++){const f=await fixture();await request(f);fs.push(f);}
  fs.sort((a,b)=>a.run.localeCompare(b.run));
  const stop=vi.fn(async(_payload:{requestId:string;identity:any})=>({delivered:true}));
  const inspect=async(row:any)=>{const proof=liveIdentity(fs.find(f=>f.run===row.id)!);
    if(row.id!==fs[2].run)proof.checkedAt='2000-01-01T00:00:00Z';return proof;};
  await reconcileOperatorOpenCodeRecoveries({inspect,stop,expectedController:release.candidate});
  expect(stop).not.toHaveBeenCalled();
  await reconcileOperatorOpenCodeRecoveries({inspect,stop,expectedController:release.candidate});
  expect(stop).toHaveBeenCalledOnce();expect(stop.mock.calls[0][0].identity.runId).toBe(fs[2].run);
  for(const f of fs.slice(0,2))expect((await current(f)).run.checkpoint.operatorOpenCodeRecovery.phase).toBe('requested');
  expect((await current(fs[2])).run.checkpoint.operatorOpenCodeRecovery.phase).toBe('uncertain');
  for(const f of fs){expect((await current(f)).leases).toHaveLength(2);await noNewAttemptOrCancellation(f);}
});

it('automatic review visits an eligible third stopped intent without discarding paused obligations',async()=>{
  const fs:Fixture[]=[];for(let i=0;i<3;i++){const f=await fixture();await request(f);await consume(f);await confirmStop(f);fs.push(f);}
  fs.sort((a,b)=>a.run.localeCompare(b.run));
  for(const f of fs.slice(0,2))await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  const reviewInspect=async(runId:string)=>terminalInspection(fs.find(f=>f.run===runId)!);
  for(let i=0;i<2;i++)await reconcileOperatorOpenCodeRecoveries({reviewInspect});
  expect((await pool.query('SELECT task_id FROM supervision_cases')).rows).toEqual([{task_id:fs[2].task}]);
  for(const f of fs){expect((await current(f)).planning.state).toBe('awaiting_supervision');await noNewAttemptOrCancellation(f);}
});

it('keeps the exact planning obligation owned before and after closure rather than replacing it with a fresh planner',async()=>{
  const f=await fixture();await request(f);
  expect(await keepOperatorOpenCodePlannerInSupervision(randomUUID(),f.run)).toBe(false);
  expect(await keepOperatorOpenCodePlannerInSupervision(f.planningRequest,randomUUID())).toBe(false);
  expect(await operatorOpenCodeRecoveryOwnsScope(pool,f.goal,f.scope)).toBe(true);
  await reconcilePlanningDemand();expect((await current(f)).planning.state).toBe('awaiting_supervision');
  await consume(f);await confirmStop(f);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
  const revision=await scopeSourceRevision(pool,f.goal,f.scope);
  await pool.query(`UPDATE tasks SET criteria=criteria||$2::jsonb WHERE id=$1`,[f.consumer,
    JSON.stringify([{id:'additional-proof',label:'New material delivery obligation',type:'artifact',name:'independent-proof.json'}])]);
  expect(await scopeSourceRevision(pool,f.goal,f.scope)).not.toBe(revision);
  expect(await createPlanningDemands(f.goal)).toEqual([]);
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1 AND scope=$2',[f.goal,f.scope])).rowCount).toBe(1);
  // Exercise the generic retry path too: an old observation can project this
  // request back to running, but the canonical intent must regain ownership.
  await pool.query("UPDATE planning_requests SET state='running' WHERE id=$1",[f.planningRequest]);
  await reconcilePlanningDemand();expect((await current(f)).planning.state).toBe('awaiting_supervision');
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(0);
  await reviewStoppedOperatorOpenCodeRecovery(f.run,async()=>terminalInspection(f));
  await reconcilePlanningDemand();
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1 AND scope=$2',[f.goal,f.scope])).rowCount).toBe(1);
  expect((await current(f)).planning.state).toBe('awaiting_supervision');await noNewAttemptOrCancellation(f);
});

it('rejects generic planner replay before creating a new context or remounting historical inputs',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const before=await current(f),contexts=(await pool.query('SELECT * FROM contexts WHERE run_id=$1',[f.run])).rows;
  await expect(recoverOpenCodeRun(f.run,f.fence,terminalInspection(f)))
    .rejects.toMatchObject({code:'OPENCODE_RECOVERY_PLANNER_CONTEXT_REQUIRED'});
  // Even a supplied case ID cannot turn old embedded inputs into a fresh plan.
  await expect(recoverOpenCodeRun(f.run,f.fence,{...terminalInspection(f),supervisionResumeCaseId:randomUUID()},{prepareSupervised:true}))
    .rejects.toMatchObject({code:'OPENCODE_RECOVERY_PLANNER_CONTEXT_REQUIRED'});
  expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT * FROM contexts WHERE run_id=$1',[f.run])).rows).toEqual(contexts);
  expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  await noNewAttemptOrCancellation(f);
});

it('hands an accepted and applied revision to exactly one new current-source request while preserving the stopped attempt',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const review=await supervisedRevision(f),before=await current(f),revision=await scopeSourceRevision(pool,f.goal,f.scope);
  const results=await Promise.all(Array.from({length:4},()=>handoff(f)));
  expect(results.filter(Boolean)).toHaveLength(1);
  const next=results.find(Boolean)!;
  expect(next).toMatchObject({sourceRunId:f.run,sourceFence:f.fence,contextVersion:f.context,
    caseId:review.caseId,decisionRunId:review.decisionRun,sourceRevision:revision});
  const requests=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 AND scope=$2 ORDER BY created_at,id',[f.goal,f.scope])).rows;
  expect(requests).toHaveLength(2);
  const prior=requests.find(r=>r.id===f.planningRequest),successor=requests.find(r=>r.id!==f.planningRequest);
  expect(prior).toMatchObject({state:'superseded',source_revision:f.sourceRevision});
  expect(prior.receipt.operatorRecoverySuccessor.requestId).toBe(successor.id);
  expect(successor).toMatchObject({id:next.requestId,state:'pending',task_id:null,source_revision:revision});
  expect(successor.snapshot.strategy).toMatchObject({reviewRunId:review.decisionRun,cause:review.action.diagnosis,hypothesis:review.action.instruction});
  expect((await current(f)).run).toEqual(before.run);
  expect((await current(f)).task.execution.inputs).toEqual(before.task.execution.inputs);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OPENCODE_PLANNER_RECOVERY_HANDOFF' AND entity_id=$1",[f.task])).rowCount).toBe(1);
  expect(await handoff(f)).toBeNull();await noNewAttemptOrCancellation(f);
});

it('does not transfer a merely decided revision before application',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);await supervisedRevision(f,false);
  const before=await current(f),inspect=vi.fn(async()=>terminalInspection(f));
  expect(await handoff(f,inspect)).toBeNull();expect(inspect).not.toHaveBeenCalled();expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1',[f.goal])).rowCount).toBe(1);
});

it('automatic handoff reaches an applied third decision while two earlier obligations still await supervision',async()=>{
  const fs:Fixture[]=[];for(let i=0;i<3;i++){const f=await fixture();await request(f);await consume(f);await confirmStop(f);fs.push(f);}
  fs.sort((a,b)=>a.run.localeCompare(b.run));await supervisedRevision(fs[2]);
  const inspect=async(runId:string)=>terminalInspection(fs.find(f=>f.run===runId)!);
  const owner=async(reservation:NativeReservation)=>stoppedOwner(reservation);
  for(let i=0;i<2;i++)await reconcileOperatorOpenCodePlannerHandoffs(inspect,owner);
  for(const f of fs.slice(0,2))expect((await current(f)).planning.state).toBe('awaiting_supervision');
  const final=await current(fs[2]);expect(final.planning.state).toBe('superseded');
  expect(final.planning.receipt.operatorRecoverySuccessor.requestId).toBeTruthy();
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OPENCODE_PLANNER_RECOVERY_HANDOFF'",[])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1',[fs[2].goal])).rowCount).toBe(2);
});

it('admits a successor through real workspace preparation and mounts the current catalog helper instead of historical bytes',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const source=randomUUID(),sourceRun=randomUUID(),sourceFence=randomUUID(),sourceWorkspace='opencode-recovery/source-'+source;
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,$3,'Accepted source','Independent immutable source for planning','qa.backend','running','work',$4,$5,1)`,
    [source,f.goal,'source-'+source,JSON.stringify([{id:'source',label:'Source evidence',type:'artifact',name:'source-summary.json'}]),
      JSON.stringify({kind:'harness',workKind:'diagnostic',baseWorkspace:f.scope,workspace:sourceWorkspace})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'harness',1,'running')`,[sourceRun,source,sourceFence]);
  await writeArtifact(sourceRun,sourceFence,'source-summary.json','{"scope":"immutable accepted fixture"}');
  await completeRun(sourceRun,sourceFence,'Source evidence',{});await validateTask(source);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[source])).rows[0].state).toBe('accepted');
  await supervisedRevision(f);const old=await current(f),next=await handoff(f);expect(next?.requestId).toBeTruthy();
  const prepare=vi.spyOn(accountBridge,'request').mockResolvedValue({confirmed:true});
  expect(await assignPlanningRequest(next!.requestId)).toBeNull();
  const worker=new WorkspacePreparationWorker();try{await worker.tick();}finally{await worker.close();}
  expect(prepare).toHaveBeenCalledOnce();expect(prepare.mock.calls[0]).toEqual([
    'worker.prepare',{workspace:'opencode-recovery/o-plan-'+next!.requestId,sourceWorkspace},{timeout:180000}]);
  const taskId=await assignPlanningRequest(next!.requestId);expect(taskId).toBeTruthy();
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[taskId])).rows[0];
  expect(task.execution).toMatchObject({kind:'opencode',workKind:'planning',planningRequestId:next!.requestId,
    baseWorkspace:f.scope,sourceWorkspace});
  const reader=task.execution.inputs.find((input:any)=>input.name==='planning-query.mjs');expect(reader).toBeTruthy();
  expect(reader.digest).not.toBe(old.task.execution.inputs.find((input:any)=>input.name==='planning-query.mjs').digest);
  const bytes=Buffer.from(reader.base64,'base64');expect(createHash('sha256').update(bytes).digest('hex')).toBe(reader.digest);
  expect(bytes.toString()).toContain('selection.nextOffset');expect(task.description).toContain(next!.requestId);
  expect((await current(f)).run).toEqual(old.run);
  expect((await current(f)).task.execution.inputs).toEqual(old.task.execution.inputs);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[taskId])).rowCount).toBe(0);
});

it('honors a real cancellation of the recovery obligation before the accepted revision is applied',async()=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);
  const review=await supervisedRevision(f,false);
  expect((await current(f)).task.state).toBe('recovery_blocked');
  await cancelTask(f.task);
  const before=await current(f);expect(before.task.state).toBe('cancelled');
  expect((await pool.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND entity_id=$1",[f.task])).rowCount).toBe(1);
  await applySupervisionDecisions(async()=>terminalInspection(f));
  expect(await handoff(f)).toBeNull();expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[review.caseId])).rows[0].state).not.toBe('applied');
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1',[f.goal])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OPENCODE_PLANNER_RECOVERY_HANDOFF' AND entity_id=$1",[f.task])).rowCount).toBe(0);
});

it.each(['owner-lock','lease','pause','source-race'])('does not create a successor when %s changes its authority',async mode=>{
  const f=await fixture();await request(f);await consume(f);await confirmStop(f);await supervisedRevision(f);
  if(mode==='owner-lock')await f.owner.query('SELECT pg_advisory_lock($1::int,$2::int)',[nativeSupervisorLockNamespace,nativeSupervisorLockKey(f.run)]);
  if(mode==='lease')await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',['late-owner:'+f.run,f.run]);
  if(mode==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  const before=await current(f);
  const inspect=async()=>{
    if(mode==='source-race')await pool.query(`UPDATE tasks SET criteria=criteria||$2::jsonb WHERE id=$1`,[f.consumer,
      JSON.stringify([{id:'raced-obligation',label:'Concurrent material requirement',type:'artifact',name:'raced-proof.json'}])]);
    return terminalInspection(f);
  };
  // The guarded API may either refuse before inspection or raise the existing
  // supervised-resume authority error. Neither outcome may create a successor.
  const outcome=await handoff(f,inspect).catch(()=>null);expect(outcome).toBeNull();
  expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT 1 FROM planning_requests WHERE goal_id=$1',[f.goal])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OPENCODE_PLANNER_RECOVERY_HANDOFF' AND entity_id=$1",[f.task])).rowCount).toBe(0);
});

it.each(['reconciling','confirmed'])('late callbacks cannot overwrite the %s recovery boundary',async phase=>{
  const f=await fixture();await request(f);if(phase==='confirmed'){await consume(f);await confirmStop(f);}
  const before=await current(f);
  await expect(completeRun(f.run,f.fence,'Late model final result',{tokens:1})).rejects.toMatchObject({code:'STALE_RUN'});
  await expect(writeArtifact(f.run,f.fence,'late.json','{"passed":true}')).rejects.toMatchObject({code:'STALE_RUN'});
  await failRun(f.run,f.fence,'UNRELATED_CALLBACK','Late provider exception',true);
  expect(await current(f)).toEqual(before);
  expect((await pool.query("SELECT 1 FROM artifacts WHERE run_id=$1 AND name='late.json'",[f.run])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM outbox WHERE run_id=$1',[f.run])).rowCount).toBe(0);await noNewAttemptOrCancellation(f);
});

it('does not preserve forged administrative authority after its canonical event is absent',async()=>{
  const f=await fixture();await request(f);await pool.query('DELETE FROM events WHERE kind=$1 AND entity_id=$2',[operatorRecoveryCode,f.task]);
  expect(await hasOperatorRecoveryIntent(pool,(await current(f)).run)).toBe(false);
  const stop=vi.fn(async()=>({observed:true}));expect(await consume(f,stop)).toBe(false);expect(stop).not.toHaveBeenCalled();
  expect(await keepOperatorOpenCodePlannerInSupervision(f.planningRequest,f.run)).toBe(false);
  await f.owner.query('SELECT pg_advisory_unlock_all()');await tx(db=>reconcileNativeSupervisor(db,f.reservation,stoppedOwner(f.reservation)));
  expect((await current(f)).run.error_code).toBe('SUPERVISOR_LOST');
  expect(await requestRecoveryReview(f.task,f.run,operatorRecoveryCode,'Unproven administrative origin',{stopProof:terminalInspection(f)})).toBeNull();
});
