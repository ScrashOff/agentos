import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,migrate,tx,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installSupervision} from '../server/supervision.js';
import {heartbeat,cancelTask,confirmStopped,completeRun,writeArtifact} from '../server/service.js';
import {RunHeartbeat} from '../server/run-heartbeat.js';
import {ContinuationSupervisor} from '../server/continuation.js';
import {reconcileStoppedDetachedHarness} from '../server/harness-supervision.js';
import {installNativeSupervision,reserveNativeSupervisor,registerNativeSupervisor,startDetachedNativeRun,
  reconcileNativeSupervisor,nativeSupervisorLockNamespace,nativeSupervisorLockKey,nativeSupervisorCgroup,
  type NativeReservation,type NativeUnitObservation} from '../server/native-supervision.js';
import {requestOperatorRecovery} from '../server/operator-recovery.js';
import {operatorRecoveryCode,operatorRecoveryCompatibleSources,operatorRecoveryDigest,operatorRecoveryNextAction,hasOperatorRecoveryIntent} from '../server/operator-recovery-contract.js';
import {requestRecoveryReview} from '../server/recovery-supervision.js';
import {makeApp} from '../server/app.js';
import {releaseRuntimeMemory} from '../server/runtime-memory.js';

const release={candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
const held:PoolClient[]=[],oldConfig={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installNativeSupervision(pool);await installSupervision();await installTaskExecutionMetadata();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id IN('maintenance','runtime-memory-policy','artifact-recovery')");
  await pool.query(`INSERT INTO settings(id,value) VALUES('maintenance','{"draining":false}')`);
  for(const name of ['agentos-workers','agentos-backlog','agentos-cycles','agentos-opencode'])
    await pool.query(`INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value`,
      ['worker-readiness:'+name,JSON.stringify(release)]);
  config.bridgeHost='test-only';config.remoteExecution=true;
});
afterEach(async()=>{
  for(const c of held.splice(0)){await c.query('SELECT pg_advisory_unlock_all()');c.release();}
  Object.assign(config,oldConfig);vi.restoreAllMocks();
});
afterAll(()=>pool.end());

async function fixture(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),project='progress-'+goal;
  const workspace='progress-test/'+run;
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
  const proof=()=>({version:1,runId:run,runFence:fence,workspace,runtime:'harness',state:'running',
    checkedAt:new Date().toISOString(),stateDigest:'9'.repeat(64),
    process:{unit:'agentos-run-'+run+'.service',mainPid:22345,activeState:'active',subState:'running',startTicks:'1234',
      controlGroup:'/system.slice/agentos-run-'+run+'.service'},
    owner:{unit:reservation.unit+'.service',mainPid:12345,startTicks:'5678',activeState:'active',subState:'running',
      controlGroup:nativeSupervisorCgroup(run),candidate:release.candidate,
      sourceHashes:Object.fromEntries(Object.entries(operatorRecoveryCompatibleSources).map(([file,hashes])=>[file,hashes[0]]))}});
  const sandbox=()=>({runId:run,workspace,checkedAt:new Date().toISOString(),
    process:{unit:'agentos-run-'+run,stopped:true,activeState:'inactive',mainPid:0,populated:false}});
  const request={requestId:randomUUID(),runId:run,runFence:fence,expectedAttempt:1,intent:'recover_current_attempt',
    operatorConfirmed:true,reasonCode:'operator_requested_recovery',evidenceRefs:[{kind:'operator_observation',referenceId:randomUUID(),sha256:'a'.repeat(64)}]};
  return {goal,task,run,fence,project,workspace,reservation,owner,proof,sandbox,request};
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

const request=(f:Awaited<ReturnType<typeof fixture>>,inspect=async()=>f.proof())=>
  requestOperatorRecovery(f.task,f.request,{channel:'api-token'},inspect,release.candidate);

it('persists one explicit intent before withdrawing heartbeat and preserves the exact task, artifacts, snapshot and reservation',async()=>{
 const f=await fixture();await writeArtifact(f.run,f.fence,'partial.json','{"retained":true}');
 const artifacts=(await pool.query('SELECT * FROM artifacts WHERE run_id=$1',[f.run])).rows,before=await current(f),abort=vi.fn();
 const pulse=new RunHeartbeat(()=>heartbeat(f.run,f.fence),abort);await pulse.tick();expect(abort).not.toHaveBeenCalled();
 const results=await Promise.all([request(f),request(f)]);
 expect(results.filter(r=>!r.duplicate)).toHaveLength(1);
 expect(results[0]).toMatchObject({state:'reconciling',code:operatorRecoveryCode,stopConfirmed:false,newAttemptQueued:false});
 await pulse.tick();expect(abort).toHaveBeenCalledOnce();await pulse.close();
 await confirmStopped(f.run,f.fence); // Old actor's abort handler is deliberately a no-op for reconciling.
 const after=await current(f);
 expect(after.run.state).toBe('reconciling');expect(after.run.error_code).toBe(operatorRecoveryCode);
 expect(after.run.instruction_snapshot).toEqual(before.run.instruction_snapshot);expect(after.run.fence).toBe(before.run.fence);
 expect(after.run.checkpoint.preserved).toBe(true);expect(after.run.finished_at).toBeNull();
 expect(after.run.checkpoint.operatorRecoveryStop.externalEvidence).toEqual({operatorSupplied:true,contextOnly:true,verifiedByServer:false});
 expect(after.task.execution).toEqual(before.task.execution);expect(after.task.criteria).toEqual(before.task.criteria);
 expect(after.task.attempt_count).toBe(1);expect(after.job).toEqual(before.job);expect(after.leases).toEqual(before.leases);
 expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1',[f.run])).rows).toEqual(artifacts);
 expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='CANCEL_REQUESTED'",[f.task])).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2',[f.task,operatorRecoveryCode])).rowCount).toBe(1);
 expect(await hasOperatorRecoveryIntent(pool,after.run)).toBe(true);
});

it.each(['cancel','pause','autonomy','supervision','finished','accepted','owner-gone','token','workspace','revision','contract','pending','adoption'])
('fails closed when %s changes during server inspection',async mode=>{
 const f=await fixture();const inspect=async()=>{
  if(mode==='cancel')await cancelTask(f.task);
  if(mode==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='autonomy')await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":false}')",['autonomy:'+f.project]);
  if(mode==='supervision')await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['supervision:'+f.goal]);
  if(mode==='finished')await completeRun(f.run,f.fence,'Finished concurrently',{});
  if(mode==='accepted')await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[f.task]);
  if(mode==='owner-gone')await f.owner.query('SELECT pg_advisory_unlock_all()');
  if(mode==='token')await pool.query("UPDATE opencode_jobs SET supervision=jsonb_set(supervision,'{launchToken}',to_jsonb($2::text)) WHERE run_id=$1",[f.run,randomUUID()]);
  if(mode==='workspace')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"progress-test/other\"') WHERE id=$1",[f.task]);
  if(mode==='revision')await pool.query("UPDATE tasks SET description='Changed operator contract' WHERE id=$1",[f.task]);
  if(mode==='contract')await pool.query("UPDATE tasks SET criteria='[]' WHERE id=$1",[f.task]);
  if(mode==='pending')await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,'Concurrent review','[]')",[randomUUID(),f.goal,f.task,f.run]);
  if(mode==='adoption')await pool.query("DELETE FROM settings WHERE id='worker-readiness:agentos-backlog'");
  return f.proof();
 };
 await expect(request(f,inspect)).rejects.toThrow('A tentativa ou sua autoridade mudou');
 const after=await current(f);expect(after.run.checkpoint.operatorRecoveryStop).toBeUndefined();
 expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1 AND kind=$2',[f.task,operatorRecoveryCode])).rowCount).toBe(0);
});

it.each(['stale','wrong-fence','wrong-pid','wrong-workspace','unreviewed-owner','opencode','manual','muse','request-attempt'])
('refuses %s without treating time or operator references as physical proof',async mode=>{
 const f=await fixture(),proof=f.proof();
 if(mode==='stale')proof.checkedAt='2000-01-01T00:00:00Z';
 if(mode==='wrong-fence')proof.runFence=randomUUID();
 if(mode==='wrong-pid')proof.owner.mainPid++;
 if(mode==='wrong-workspace')proof.workspace='progress-test/other';
 if(mode==='unreviewed-owner')proof.owner.sourceHashes['server/run-heartbeat.ts']='f'.repeat(64);
 if(mode==='opencode')await pool.query("UPDATE runs SET runtime='opencode' WHERE id=$1",[f.run]);
 if(mode==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);
 if(mode==='muse')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workforceLane}','\"muse\"') WHERE id=$1",[f.task]);
 if(mode==='request-attempt')f.request.expectedAttempt=2;
 const before=await current(f);await expect(request(f,async()=>proof)).rejects.toThrow();expect(await current(f)).toEqual(before);
});

it('returns only the original idempotent receipt after policies change and never retargets another request',async()=>{
 const f=await fixture();await request(f);
 await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 const inspect=vi.fn(async()=>f.proof());expect(await request(f,inspect)).toMatchObject({duplicate:true,runId:f.run});
 expect(inspect).not.toHaveBeenCalled();
 await expect(requestOperatorRecovery(f.task,{...f.request,requestId:randomUUID()},{channel:'api-token'},inspect,release.candidate)).rejects.toThrow();
});

it('hashes JSONB-equivalent objects identically while preserving array order',()=>{
 expect(operatorRecoveryDigest({b:2,a:{z:3,c:4}})).toBe(operatorRecoveryDigest({a:{c:4,z:3},b:2}));
 expect(operatorRecoveryDigest([1,2])).not.toBe(operatorRecoveryDigest([2,1]));
});

it.each(['owner-alive','sandbox-alive','stale-proof','wrong-token'])('retains uncertain resources when %s prevents double stop',async mode=>{
 const f=await fixture();await request(f);if(mode!=='owner-alive')await f.owner.query('SELECT pg_advisory_unlock_all()');
 const proof=f.sandbox();if(mode==='sandbox-alive')proof.process.stopped=false;if(mode==='stale-proof')proof.checkedAt='2000-01-01T00:00:00Z';
 const reservation=mode==='wrong-token'?{...f.reservation,launchToken:randomUUID()}:f.reservation,before=await current(f);
 const action=reconcileStoppedDetachedHarness(reservation,ownerStopped(reservation),async()=>proof);
 if(mode==='sandbox-alive'||mode==='stale-proof')await expect(action).rejects.toThrow();else expect(await action).toBe(false);
 expect(await current(f)).toEqual(before);
});

it('preserves administrative origin after double stop and opens one supervised next step without retry or negative acceptance',async()=>{
 const f=await fixture();await request(f);await f.owner.query('SELECT pg_advisory_unlock_all()');
 expect(await reconcileStoppedDetachedHarness(f.reservation,ownerStopped(f.reservation),async()=>f.sandbox())).toBe(true);
 const stopped=await current(f);expect(stopped.run.error_code).toBe(operatorRecoveryCode);expect(stopped.run.state).toBe('failed');
 expect(stopped.leases).toHaveLength(0);expect(stopped.job.state).toBe('finished');
 expect(stopped.job.supervision.resources.state).toBe('reserved');
 expect(await tx(db=>releaseRuntimeMemory(db,f.reservation,{...f.sandbox(),process:{...f.sandbox().process,populated:true}}))).toBe(false);
 expect(await tx(db=>releaseRuntimeMemory(db,f.reservation,f.sandbox()))).toBe(true);
 expect(await request(f)).toMatchObject({duplicate:true,stopConfirmed:true});
 await new ContinuationSupervisor(async()=>f.sandbox()).scan();
 await new ContinuationSupervisor(async()=>f.sandbox()).scan();
 const cases=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
 expect(cases).toHaveLength(1);expect(cases[0]).toMatchObject({source_run_id:f.run,state:'open'});
 expect(cases[0].evidence).toContainEqual(expect.objectContaining({kind:operatorRecoveryCode,administrative:true,gameFailureClaimed:false}));
 expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
 expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.task])).rowCount).toBe(1);
 expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.task])).rowCount).toBe(0);
 expect((await pool.query('SELECT 1 FROM outbox WHERE run_id=$1',[f.run])).rowCount).toBe(0);
});

it('adds the administrative context once when a same-run generic review wins the race after physical stop',async()=>{
 const f=await fixture();await request(f);await f.owner.query('SELECT pg_advisory_unlock_all()');
 await reconcileStoppedDetachedHarness(f.reservation,ownerStopped(f.reservation),async()=>f.sandbox());
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
 const caseId=randomUUID();await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
  VALUES($1,$2,$3,$4,'Stopped technical run','[]')`,[caseId,f.goal,f.task,f.run]);
 for(let i=0;i<2;i++)expect(await requestRecoveryReview(f.task,f.run,operatorRecoveryCode,'Administrative context',
  {stopProof:f.sandbox()})).toMatchObject({caseId,state:'open'});
 const cases=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows;
 expect(cases).toHaveLength(1);expect(cases[0].decision).toBeNull();
 expect(cases[0].evidence).toEqual([expect.objectContaining({kind:operatorRecoveryCode,nextAction:operatorRecoveryNextAction,
  administrative:true,operatorSupplied:true,contextOnly:true,verifiedByServer:false,gameFailureClaimed:false})]);
 expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='SUPERVISION_EVIDENCE_RECORDED'",[f.task])).rowCount).toBe(1);
});

it('explicit cancel after request wins and cannot become operator recovery',async()=>{
 const f=await fixture();await request(f);await cancelTask(f.task);await f.owner.query('SELECT pg_advisory_unlock_all()');
 await reconcileStoppedDetachedHarness(f.reservation,ownerStopped(f.reservation),async()=>f.sandbox());
 await new ContinuationSupervisor(async()=>f.sandbox()).scan();
 expect((await current(f)).run.state).toBe('cancelled');expect((await current(f)).task.state).toBe('cancelled');
 expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(0);
});

it('does not preserve a forged checkpoint/code or allow recovery without the durable server intent',async()=>{
 const f=await fixture();await request(f);await pool.query('DELETE FROM events WHERE kind=$1 AND entity_id=$2',[operatorRecoveryCode,f.task]);
 expect(await hasOperatorRecoveryIntent(pool,(await current(f)).run)).toBe(false);
 await f.owner.query('SELECT pg_advisory_unlock_all()');await tx(db=>reconcileNativeSupervisor(db,f.reservation,ownerStopped(f.reservation)));
 expect((await current(f)).run.error_code).toBe('SUPERVISOR_LOST');
 expect(await requestRecoveryReview(f.task,f.run,operatorRecoveryCode,'Not authorized',{stopProof:f.sandbox()})).toBeNull();
});

it('requires normal API authentication and explicit closed-schema operator confirmation',async()=>{
 const app=makeApp(),id=randomUUID(),url='/api/tasks/'+id+'/interrupt-for-recovery';
 try{
  expect((await app.inject({method:'POST',url,headers:{host:'localhost'},payload:{}})).statusCode).toBe(401);
  expect((await app.inject({method:'POST',url,headers:{authorization:'Bearer '+config.apiToken},payload:{}})).statusCode).toBe(400);
  const f=await fixture();
  expect((await app.inject({method:'POST',url:'/api/tasks/'+f.task+'/interrupt-for-recovery',headers:{authorization:'Bearer '+config.apiToken},
   payload:{...f.request,operatorConfirmed:false}})).statusCode).toBe(400);
  expect((await current(f)).run.state).toBe('running');
 }finally{await app.close();}
});
