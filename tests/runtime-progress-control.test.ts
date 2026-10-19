import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,migrate,tx,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installSupervision} from '../server/supervision.js';
import {heartbeat,cancelTask} from '../server/service.js';
import {RunHeartbeat} from '../server/run-heartbeat.js';
import {ContinuationSupervisor} from '../server/continuation.js';
import {reconcileStoppedDetachedHarness} from '../server/harness-supervision.js';
import {installNativeSupervision,reserveNativeSupervisor,registerNativeSupervisor,startDetachedNativeRun,
  reconcileNativeSupervisor,nativeSupervisorLockNamespace,nativeSupervisorLockKey,nativeSupervisorCgroup,
  type NativeReservation,type NativeUnitObservation} from '../server/native-supervision.js';
import {reconcileRuntimeProgress} from '../server/runtime-progress-control.js';
import {runtimeProgressCode,hasRuntimeProgressStop,type RuntimeProgressProof} from '../server/runtime-progress-contract.js';
import {requestRecoveryReview} from '../server/recovery-supervision.js';

const release={candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
const held:PoolClient[]=[],oldConfig={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installNativeSupervision(pool);await installSupervision();await installTaskExecutionMetadata();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id IN('maintenance','runtime-memory-policy','artifact-recovery')");
  config.bridgeHost='test-only';config.remoteExecution=true;
});
afterEach(async()=>{
  for(const c of held.splice(0)){await c.query('SELECT pg_advisory_unlock_all()');c.release();}
  Object.assign(config,oldConfig);vi.restoreAllMocks();
});
afterAll(()=>pool.end());

async function fixture(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),project='progress-'+goal;
  const workspace='progress-test/'+run,base=Date.now();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1,'Stall regression','No models invoked',$2,$3,'active')`,[goal,project,goal]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',
    ['workforce:'+goal,JSON.stringify({enabled:true,detachedHarnessEnabled:true})]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
    VALUES($1,$2,'bounded-task','Bounded task','Preserve criteria and evidence','qa.backend','queued',
      '[{"id":"real-result","type":"artifact","name":"result.json"}]',$3,1)`,
    [task,goal,JSON.stringify({kind:'harness',workspace,baseWorkspace:'progress-test/base',writePaths:['evidence/']})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,checkpoint)
    VALUES($1,$2,1,$3,'harness',1,'queued','{"preserved":true}')`,[run,task,fence]);
  await pool.query('INSERT INTO leases VALUES($1,$2),($3,$2)',['slot:'+run,run,'workspace:'+workspace]);
  await pool.query('INSERT INTO opencode_jobs(run_id) VALUES($1)',[run]);
  const reservation=(await tx(db=>reserveNativeSupervisor(db,run,release)))!;
  expect(reservation).toBeTruthy();
  const owner=await pool.connect();held.push(owner);
  await owner.query('SELECT pg_advisory_lock($1::int,$2::int)',[nativeSupervisorLockNamespace,nativeSupervisorLockKey(run)]);
  const backend=(await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  expect(await tx(db=>registerNativeSupervisor(db,reservation,
    {unit:reservation.unit,pid:12345,cwd:reservation.candidate,cgroup:nativeSupervisorCgroup(run)},backend))).toBe(true);
  expect(await tx(db=>startDetachedNativeRun(db,reservation,'harness-native-12345'))).toBeTruthy();
  const proof=():RuntimeProgressProof=>({version:1,policy:'failed-evidence-poll-v1',runId:run,workspace,
    checkedAt:new Date().toISOString(),stalled:true,reason:'Repeated exact failed evidence',
    evidence:{completedPolls:4,firstStartedAt:new Date(base-125000).toISOString(),
      firstCompletedAt:new Date(base-120000).toISOString(),lastCompletedAt:new Date(base-10000).toISOString(),
      firstSequence:1,lastSequence:8,callIds:['a','b','c','d'],pendingPollId:null,pendingPollSequence:null,
      commandDigest:'b'.repeat(64),resultDigest:'c'.repeat(64),
      file:{path:'evidence/test.log',digest:'d'.repeat(64),size:100,mtimeMs:base-180000,ctimeMs:base-180000,dev:'1',ino:'22'},
      failure:{kind:'explicit-test-failure',matches:1,digest:'e'.repeat(64)},
      trace:{digest:'f'.repeat(64),size:1000,mtimeMs:base-1000}},
    process:{unit:'agentos-run-'+run+'.service',mainPid:22345,activeState:'active',subState:'running',startTicks:'1234',
      controlGroup:'/system.slice/agentos-run-'+run+'.service'},
    state:{runtime:'harness',state:'running',runFence:fence,digest:'9'.repeat(64)}});
  const sandbox=()=>({runId:run,workspace,checkedAt:new Date().toISOString(),
    process:{unit:'agentos-run-'+run,stopped:true,activeState:'inactive',mainPid:0,populated:false}});
  return {goal,task,run,fence,project,workspace,reservation,owner,proof,sandbox};
}
function ownerStopped(reservation:NativeReservation):NativeUnitObservation{
  return {unit:reservation.unit,checkedAt:new Date().toISOString(),activeState:'inactive',loadState:'not-found',
    mainPid:0,pendingJob:false,cgroupEmpty:true};
}
async function current(f:Awaited<ReturnType<typeof fixture>>){
  return {run:(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0],
    task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0],
    job:(await pool.query('SELECT * FROM opencode_jobs WHERE run_id=$1',[f.run])).rows[0],
    leases:(await pool.query('SELECT resource FROM leases WHERE run_id=$1 ORDER BY resource',[f.run])).rows};
}

it('withdraws heartbeat authority once, preserves leases and evidence, and uses the actor heartbeat to request abort',async()=>{
  const f=await fixture(),before=await current(f),abort=vi.fn();
  const pulse=new RunHeartbeat(()=>heartbeat(f.run,f.fence),abort);
  await pulse.tick();expect(abort).not.toHaveBeenCalled();
  const inspect=vi.fn(async()=>f.proof());
  expect(await reconcileRuntimeProgress(inspect)).toMatchObject({inspected:1,stopRequests:1});
  expect(inspect).toHaveBeenCalledTimes(2);expect(inspect).toHaveBeenCalledWith(f.run,f.workspace);
  await pulse.tick();expect(abort).toHaveBeenCalledTimes(1);await pulse.close();
  expect(await heartbeat(f.run,f.fence)).toBeNull();
  const after=await current(f);
  expect(after.run.state).toBe('reconciling');expect(after.run.error_code).toBe(runtimeProgressCode);
  expect(after.run.fence).toBe(before.run.fence);expect(after.run.context_id).toBe(before.run.context_id);
  expect(after.run.instruction_snapshot).toEqual(before.run.instruction_snapshot);
  expect(after.run.checkpoint.preserved).toBe(true);
  expect(hasRuntimeProgressStop(after.run.checkpoint,f.run,f.fence)).toBe(true);
  expect(after.run.finished_at).toBeNull();expect(after.task.attempt_count).toBe(1);
  expect(after.task.criteria).toEqual(before.task.criteria);expect(after.task.execution).toEqual(before.task.execution);
  expect(after.leases).toEqual(before.leases);expect(after.job).toEqual(before.job);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.task])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='CANCEL_REQUESTED'",[f.task])).rowCount).toBe(0);
  expect(await reconcileRuntimeProgress(inspect)).toMatchObject({inspected:0,stopRequests:0});
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='RUNTIME_PROGRESS_STOP_REQUESTED'",[f.task])).rowCount).toBe(1);
});

it.each(['not-stalled','stale','short','file-changed','process-changed','result-changed','duplicate-calls','cancel','paused','autonomy','owner-gone','workspace-changed'])
('does not interrupt on incomplete, changed or unauthorized evidence: %s',async mode=>{
  const f=await fixture(),before=await current(f);let n=0;
  const inspect=vi.fn(async()=>{
    const proof=f.proof();n++;
    if(mode==='not-stalled')return {stalled:false};
    if(mode==='stale')proof.checkedAt='2020-01-01T00:00:00Z';
    if(mode==='short')proof.evidence.firstCompletedAt=proof.evidence.lastCompletedAt;
    if(mode==='duplicate-calls')proof.evidence.callIds=['a','a','c','d'];
    if(n===2){
      if(mode==='file-changed')proof.evidence.file.digest='a'.repeat(64);
      if(mode==='process-changed')proof.process.startTicks='9999';
      if(mode==='result-changed')proof.evidence.resultDigest='a'.repeat(64);
      if(mode==='cancel')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
      if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
      if(mode==='autonomy')await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":false}')",['autonomy:'+f.project]);
      if(mode==='owner-gone')await f.owner.query('SELECT pg_advisory_unlock_all()');
      if(mode==='workspace-changed')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"progress-test/changed\"') WHERE id=$1",[f.task]);
    }
    return proof;
  });
  expect((await reconcileRuntimeProgress(inspect)).stopRequests).toBe(0);
  const after=await current(f);
  expect(after.run).toEqual(before.run);expect(after.leases).toEqual(before.leases);expect(after.job).toEqual(before.job);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='RUNTIME_PROGRESS_STOP_REQUESTED'",[f.task])).rowCount).toBe(0);
});

it('retains the typed failure and leases after only the native supervisor is proven stopped',async()=>{
  const f=await fixture();await reconcileRuntimeProgress(async()=>f.proof());
  await f.owner.query('SELECT pg_advisory_unlock_all()');
  expect(await tx(db=>reconcileNativeSupervisor(db,f.reservation,ownerStopped(f.reservation)))).toBe('reconciling');
  const state=await current(f);
  expect(state.run.state).toBe('reconciling');expect(state.run.error_code).toBe(runtimeProgressCode);
  expect(state.job.state).toBe('orphaned');expect(state.leases).toHaveLength(2);
  expect(hasRuntimeProgressStop(state.run.checkpoint,f.run,f.fence)).toBe(true);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.task])).rowCount).toBe(0);
});

it.each(['owner-alive','sandbox-alive','stale-supervisor','stale-sandbox','wrong-workspace','wrong-token'])
('never releases leases without both fresh stop proofs: %s',async mode=>{
  const f=await fixture();await reconcileRuntimeProgress(async()=>f.proof());
  if(mode!=='owner-alive')await f.owner.query('SELECT pg_advisory_unlock_all()');
  const supervisor=ownerStopped(f.reservation),sandbox=f.sandbox();
  if(mode==='sandbox-alive')sandbox.process.stopped=false;
  if(mode==='stale-supervisor')supervisor.checkedAt='2020-01-01T00:00:00Z';
  if(mode==='stale-sandbox')sandbox.checkedAt='2020-01-01T00:00:00Z';
  if(mode==='wrong-workspace')sandbox.workspace='progress-test/other';
  const reservation=mode==='wrong-token'?{...f.reservation,launchToken:randomUUID()}:f.reservation;
  const before=await current(f),action=reconcileStoppedDetachedHarness(reservation,supervisor,async()=>sandbox);
  if(['sandbox-alive','stale-sandbox','wrong-workspace'].includes(mode))await expect(action).rejects.toThrow('STOP_UNCONFIRMED');
  else expect(await action).toBe(false);
  expect(await current(f)).toEqual(before);
});

it('after both processes stop, routes the first stalled failure to supervision instead of a blind retry, including a single-task failed goal',async()=>{
  const f=await fixture();await reconcileRuntimeProgress(async()=>f.proof());
  await f.owner.query('SELECT pg_advisory_unlock_all()');
  expect(await reconcileStoppedDetachedHarness(f.reservation,ownerStopped(f.reservation),async()=>f.sandbox())).toBe(true);
  const stopped=await current(f);
  expect(stopped.run.state).toBe('failed');expect(stopped.run.error_code).toBe(runtimeProgressCode);
  expect(stopped.leases).toHaveLength(0);expect(stopped.job.state).toBe('finished');
  const inspect=vi.fn(async()=>f.sandbox());await new ContinuationSupervisor(inspect).scan();
  expect(inspect).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT state,source_run_id FROM supervision_cases WHERE task_id=$1',[f.task])).rows)
    .toEqual([{state:'open',source_run_id:f.run}]);
  const after=await current(f);
  expect(after.task.state).toBe('recovery_blocked');expect(after.task.attempt_count).toBe(1);
  expect(after.run).toEqual(stopped.run);expect(after.task.criteria).toEqual(stopped.task.criteria);
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.task])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.task])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM outbox WHERE run_id=$1',[f.run])).rowCount).toBe(0);
});

it('explicit cancellation remains cancelled after physical reconciliation and is never supervised as a retry',async()=>{
  const f=await fixture();await reconcileRuntimeProgress(async()=>f.proof());await cancelTask(f.task);
  await f.owner.query('SELECT pg_advisory_unlock_all()');
  expect(await reconcileStoppedDetachedHarness(f.reservation,ownerStopped(f.reservation),async()=>f.sandbox())).toBe(true);
  await new ContinuationSupervisor(async()=>f.sandbox()).scan();
  const after=await current(f);
  expect(after.run.state).toBe('cancelled');expect(after.task.state).toBe('cancelled');expect(after.leases).toHaveLength(0);
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(0);
});

it.each(['no-proof','active-no-proof','stale-proof','wrong-workspace','wrong-error','wrong-attempt','paused','autonomy-off','manual','fulfilled-generation'])
('does not reactivate a failed goal without the exact stopped progress review: %s',async mode=>{
  const f=await fixture();await reconcileRuntimeProgress(async()=>f.proof());
  await f.owner.query('SELECT pg_advisory_unlock_all()');
  expect(await reconcileStoppedDetachedHarness(f.reservation,ownerStopped(f.reservation),async()=>f.sandbox())).toBe(true);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('failed');
  const stopProof=f.sandbox();
  if(mode==='active-no-proof')await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
  if(mode==='stale-proof')stopProof.checkedAt='2020-01-01T00:00:00Z';
  if(mode==='wrong-workspace')stopProof.workspace='progress-test/other';
  if(mode==='wrong-error')await pool.query("UPDATE runs SET error_code='MODEL_TIMEOUT' WHERE id=$1",[f.run]);
  if(mode==='wrong-attempt')await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);
  if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='autonomy-off')await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":false}')",['autonomy:'+f.project]);
  if(mode==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);
  if(mode==='fulfilled-generation')await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,request_key)
    VALUES($1,$2,$3,$4,'applied','Historical completed decision','[]','recovery:'||encode(sha256(convert_to($5,'UTF8')),'hex'))`,
    [randomUUID(),f.goal,f.task,f.run,runtimeProgressCode]);
  const before=await current(f),goalBefore=(await pool.query('SELECT * FROM goals WHERE id=$1',[f.goal])).rows[0];
  const cases=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
  expect(await requestRecoveryReview(f.task,f.run,runtimeProgressCode,'Explicit verified failure',['no-proof','active-no-proof'].includes(mode)?{}:{stopProof})).toBeNull();
  expect(await current(f)).toEqual(before);
  expect((await pool.query('SELECT * FROM goals WHERE id=$1',[f.goal])).rows[0]).toEqual(goalBefore);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows).toEqual(cases);
});

it.each(['matching','different-source','different-cause'])('keeps a failed objective actionable only for its exact pending progress case: %s',async mode=>{
  const f=await fixture();await reconcileRuntimeProgress(async()=>f.proof());
  await f.owner.query('SELECT pg_advisory_unlock_all()');
  await reconcileStoppedDetachedHarness(f.reservation,ownerStopped(f.reservation),async()=>f.sandbox());
  let sourceRun=f.run;
  if(mode==='different-source'){
    sourceRun=randomUUID();await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
      VALUES($1,$2,0,$3,'harness',1,'failed')`,[sourceRun,f.task,randomUUID()]);
  }
  const caseId=randomUUID();
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,request_key)
    VALUES($1,$2,$3,$4,'open','Existing review','[]','recovery:'||encode(sha256(convert_to($5,'UTF8')),'hex'))`,
    [caseId,f.goal,f.task,sourceRun,mode==='different-cause'?'OTHER_FAILURE':runtimeProgressCode]);
  const before=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
  const result=await requestRecoveryReview(f.task,f.run,runtimeProgressCode,'Use existing exact owner',{stopProof:f.sandbox()});
  expect(result).toEqual(mode==='matching'?{caseId,state:'open'}:null);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe(mode==='matching'?'active':'failed');
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows).toEqual([before]);
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual({state:'failed',attempt_count:1});
});
