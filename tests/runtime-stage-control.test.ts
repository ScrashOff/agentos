import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision} from '../server/supervision.js';
import {installManagement} from '../server/management.js';
import {reconcileRuntimeStage} from '../server/runtime-stage-control.js';
import {readLiveFailureReview} from '../server/manager-live-failure.js';
import {checkedRuntimeStageSnapshot,stageDigest,stageFactsDigest} from '../server/runtime-stage-contract.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
// @ts-expect-error Remote ESM has no generated declarations.
import {partialReportObservation} from '../bridge/runtime-stage-observation.mjs';
const original={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installSupervision();await installManagement();await installTaskExecutionMetadata();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id LIKE 'runtime-stage:%' OR id LIKE 'runtime-guidance:%'");
  config.bridgeHost='test-only';config.remoteExecution=true;
});
afterAll(async()=>{Object.assign(config,original);await pool.end();});
async function fixture(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),context=randomUUID();
  const workspace='stage-test/'+run;
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key,state) VALUES($1::uuid,'Stage review','Preserve the real contract','stage-test',$1::text,'active')",[goal]);
  for(const prefix of ['management:','supervision:','workforce:'])
    await pool.query('INSERT INTO settings VALUES($1,$2)',[prefix+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,'current','Current live worker','Authorized unchanged criteria','qa.backend','running','work',
      '[{"id":"verified","type":"artifact","name":"verification.json"}]',$3,1)`,
    [task,goal,JSON.stringify({kind:'harness',profile:'code',workspace,baseWorkspace:'stage-test/base'})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,started_at,checkpoint) VALUES($1,$2,1,$3,'harness',1,'running',now(),'{\"preserved\":true}')",[run,task,fence]);
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','{}')",[context,run]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,context]);
  await pool.query('INSERT INTO leases VALUES($1,$2)',['workspace:'+workspace,run]);
  await event(pool,'RUNTIME_GUIDANCE_AVAILABLE',task,{goalId:goal,taskId:task,runId:run,runFence:fence,contextVersion:context});
  const base=Date.now();
  const snapshot=(calls=8,last=base-90000)=>({version:1,policy:'runtime-stage-observation-v1',authority:'review_only',
    runId:run,runFence:fence,workspace,checkedAt:new Date().toISOString(),available:true,
    trace:{digest:'a'.repeat(64),size:900,lastSequence:20+calls},observations:[{category:'unproductive_wait',
      evidenceDigest:'b'.repeat(64),callId:'call_8',toolName:'bash',
      wait:{commandDigest:'c'.repeat(64),completedCalls:calls,firstCompletedAt:new Date(base-360000).toISOString(),lastCompletedAt:new Date(last).toISOString()},
      reports:[{path:'data/logs/browser-desktop.log',digest:'d'.repeat(64),size:0,mtimeMs:base-400000,passed:null,exitCode:null}]}]});
  return {goal,task,run,fence,context,workspace,base,snapshot};
}
async function state(f:Awaited<ReturnType<typeof fixture>>){return {
  task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0],
  run:(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0],
  leases:(await pool.query('SELECT * FROM leases WHERE run_id=$1 ORDER BY resource',[f.run])).rows,
  artifacts:(await pool.query('SELECT * FROM artifacts WHERE run_id=$1',[f.run])).rows,
  acceptances:(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows,
  cases:(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows,
};}
async function events(f:Awaited<ReturnType<typeof fixture>>){return (await pool.query(
  "SELECT id::text,payload FROM events WHERE entity_id=$1 AND kind='RUNTIME_STAGE_OBSERVATION' ORDER BY id",[f.task])).rows;}
async function review(f:Awaited<ReturnType<typeof fixture>>,eventId:string,kind:'guide'|'no-action',phase='consumed'){
  const task=randomUUID(),run=randomUUID(),actionId=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1::uuid,$2,$1::text,'Manager','MANAGEMENT_V1 authorized actual fixture','qa.backend','accepted','coordinate','[]',1)`,[task,f.goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,1,$3,'analytical',1,'submitted',now())",[run,task,randomUUID()]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','fixture')",[randomUUID(),task,run]);
  const identity={goalId:f.goal,taskId:f.task,runId:f.run,runFence:f.fence,contextVersion:f.context};
  const cause=(await pool.query('SELECT payload FROM events WHERE id=$1',[eventId])).rows[0].payload.semanticCauseDigest;
  const receipt=kind==='guide'?{kind:'guide',code:'PROPOSED',actionId}:{...identity,code:'LIVE_FAILURE_NO_ACTION',
    eventId,failureKey:stageDigest([f.goal,f.task,f.run,f.fence,f.context,cause]),decision:'continue_observing',
    reason:'The current observation is still inconclusive; preserve the live actor and inspect the next bounded material window.'};
  await pool.query('INSERT INTO management_actions VALUES($1,0,$2)',[run,JSON.stringify(receipt)]);
  if(kind==='guide')await pool.query('INSERT INTO settings VALUES($1,$2)',['runtime-guidance:'+actionId,
    JSON.stringify({...identity,actionId,expectedAttempt:1,state:phase,facts:[{evidenceRef:'event:'+eventId}]})]);
  return {run,actionId};
}
function reportSnapshot(f:Awaited<ReturnType<typeof fixture>>,enriched:boolean){
  const text=JSON.stringify({suiteExecuted:true,exitCode:1,totals:{cases:2,passed:1,failed:1},
    cases:[{name:'rejects invalid input as expected',ok:true,detail:'expected rejection'},
      {name:'recovery preserves exactly-once effect',ok:false,detail:'actual=2 expected=1'}],
    command:'PRIVATE_COMMAND_MUST_NOT_APPEAR',logTail:['PRIVATE_LOG_MUST_NOT_APPEAR']});
  const path='evidence/protocol-report.json',digest=stageDigest(text);
  const observation=partialReportObservation(path,{text,digest,size:Buffer.byteLength(text),mtimeMs:f.base-200000},f.base-300000);
  if(!enriched){delete observation.reports[0].facts;observation.evidenceDigest=stageDigest([path,digest]);}
  return {...f.snapshot(),observations:[observation]};
}

it('atomically deduplicates concurrent observations, exposes a nonfatal manager focus and preserves every worker authority',async()=>{
  const f=await fixture(),before=await state(f);
  expect((await pool.query('SELECT version,workspace,base_workspace,control FROM task_execution_metadata WHERE task_id=$1',[f.task])).rows[0])
    .toMatchObject({version:1,workspace:f.workspace,base_workspace:'stage-test/base',control:{kind:'harness'}});
  await Promise.all([reconcileRuntimeStage(async()=>f.snapshot()),reconcileRuntimeStage(async()=>f.snapshot()),reconcileRuntimeStage(async()=>f.snapshot())]);
  expect(await events(f)).toHaveLength(1);expect(await state(f)).toEqual(before);
  const focus=(await readLiveFailureReview(pool,f.goal)).focus;
  expect(focus).toMatchObject({authority:'review_only',nonfatal:true,category:'unproductive_wait',runId:f.run,runFence:f.fence});
  expect(focus?.stageEvidence).toMatchObject({verification:'unverified_observation',evidenceDigest:'b'.repeat(64),
    reports:[{path:'data/logs/browser-desktop.log',digest:'d'.repeat(64),passed:null,exitCode:null}],
    wait:{commandDigest:'c'.repeat(64),completedCalls:8}});
  expect(focus?.statement).toContain('not proof');
  await reconcileRuntimeStage(async()=>f.snapshot(50));expect(await events(f)).toHaveLength(1);
});
it.each(['missing','obsolete'])('never falls back to the large execution document with %s metadata',async(kind)=>{
  const f=await fixture();
  if(kind==='missing')await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[f.task]);
  else await pool.query('UPDATE task_execution_metadata SET version=0 WHERE task_id=$1',[f.task]);
  let inspections=0;await reconcileRuntimeStage(async()=>{inspections++;return f.snapshot();});
  expect(inspections).toBe(0);expect(await events(f)).toHaveLength(0);
  expect((await state(f)).run.state).toBe('running');
});
it.each(['fence','workspace','expired','future','cancel','contract_changed','closed','unavailable'])('refuses %s observation without altering the worker',async(kind)=>{
  const f=await fixture();
  const result=await reconcileRuntimeStage(async()=>{
    const snapshot:any=f.snapshot();
    if(kind==='fence')snapshot.runFence=randomUUID();
    if(kind==='workspace')snapshot.workspace='other/workspace';
    if(kind==='expired')snapshot.checkedAt=new Date(Date.now()-60000).toISOString();
    if(kind==='future')snapshot.checkedAt=new Date(Date.now()+60000).toISOString();
    if(kind==='cancel')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
    if(kind==='contract_changed')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"stage-test/other\"') WHERE id=$1",[f.task]);
    if(kind==='closed')await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[f.run]);
    if(kind==='unavailable')return {available:false};
    return snapshot;
  });
  expect(result.events).toBe(0);expect(await events(f)).toHaveLength(0);
  expect((await pool.query('SELECT count(*)::int n FROM leases WHERE run_id=$1',[f.run])).rows[0].n).toBe(1);
});
it('partial negative reports request review and never create canonical artifacts or acceptances',async()=>{
  const f=await fixture(),before=await state(f),snapshot:any=f.snapshot();
  snapshot.observations=[{category:'stage_failure',evidenceDigest:'f'.repeat(64),callId:'report_1',toolName:'stage_report',
    reports:[{path:'evidence/protocol-report.json',digest:'e'.repeat(64),size:1200,mtimeMs:f.base-200000,passed:false,exitCode:1}]}];
  await reconcileRuntimeStage(async()=>snapshot);
  const focus=(await readLiveFailureReview(pool,f.goal)).focus;
  expect(focus?.statement).toContain('provenance');
  expect(focus?.stageEvidence).toMatchObject({verification:'unverified_observation',category:'stage_failure',
    reports:[{path:'evidence/protocol-report.json',digest:'e'.repeat(64),passed:false,exitCode:1}]});
  expect(await state(f)).toEqual(before);
  await reconcileRuntimeStage(async()=>({...snapshot,checkedAt:new Date().toISOString(),observations:[]}));
  expect((await readLiveFailureReview(pool,f.goal)).focus).toBeNull();expect(await events(f)).toHaveLength(1);
});
it.each([false,true])('enriches the same report as a new immutable cause without retaining the old active cause (truncated=%s)',async(truncated)=>{
  const f=await fixture(),before=await state(f);
  await reconcileRuntimeStage(async()=>reportSnapshot(f,false));
  const old=(await events(f))[0],oldFocus=(await readLiveFailureReview(pool,f.goal)).focus;
  const disposition=await review(f,old.id,'no-action');
  const receipt=(await pool.query('SELECT receipt FROM management_actions WHERE run_id=$1',[disposition.run])).rows[0].receipt;
  await event(pool,'MANAGEMENT_REVIEWED',f.goal,{runId:disposition.run,contextRequestFocus:oldFocus,receipts:[receipt]});
  expect((await readLiveFailureReview(pool,f.goal)).focus).toBeNull();
  const snapshot={...reportSnapshot(f,true),truncated};
  const checked=checkedRuntimeStageSnapshot(snapshot);
  expect(checked.observations[0].reports[0].facts?.observedCounts).toEqual({total:2,examined:2,passed:1,failed:1,unknown:0});
  await Promise.all([reconcileRuntimeStage(async()=>snapshot),reconcileRuntimeStage(async()=>snapshot)]);
  const all=await events(f);expect(all).toHaveLength(2);expect(all[0]).toEqual(old);
  expect(all[1].payload).toMatchObject({authority:'review_only',acceptance:false,generation:0});
  expect(all[1].payload.semanticCauseDigest).not.toBe(old.payload.semanticCauseDigest);
  const ledger=(await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-stage:'+f.run])).rows[0].value;
  expect(ledger.activeEventIds).toEqual([all[1].id]);expect(Object.keys(ledger.episodes)).toHaveLength(2);
  const focus=(await readLiveFailureReview(pool,f.goal)).focus;
  expect(focus?.eventId).toBe(all[1].id);expect(focus?.failureKey).not.toBe(oldFocus?.failureKey);
  expect(focus?.stageEvidence?.verification).toBe('unverified_observation');
  expect(focus?.stageEvidence?.reports[0].facts?.failedCases).toMatchObject([
    {index:1,name:'recovery preserves exactly-once effect',ok:false,detailClass:'assertion_mismatch'},
  ]);
  expect(JSON.stringify(all)).not.toContain('PRIVATE_');
  expect(await state(f)).toEqual(before);
});
it('binds current file bytes to new facts and retires only their directly observed path',async()=>{
  const f=await fixture(),before=await state(f),first=reportSnapshot(f,true);
  const other=structuredClone(first.observations[0]);other.reports[0].path='evidence/other-report.json';
  other.evidenceDigest=stageDigest([other.reports[0].path,other.reports[0].digest,stageFactsDigest(other.reports[0].facts)]);
  await reconcileRuntimeStage(async()=>({...first,observations:[...first.observations,other]}));
  const old=await events(f),fresh=structuredClone(first),report=fresh.observations[0].reports[0];
  report.digest='f'.repeat(64);report.facts.failedCases[0].detailClass='timeout';
  fresh.observations[0].evidenceDigest=stageDigest([report.path,report.digest,stageFactsDigest(report.facts)]);
  fresh.checkedAt=new Date().toISOString();
  await reconcileRuntimeStage(async()=>({...fresh,truncated:true}));
  const all=await events(f);expect(all).toHaveLength(3);expect(all.slice(0,2)).toEqual(old);
  const ledger=(await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-stage:'+f.run])).rows[0].value;
  expect(ledger.activeEventIds.sort()).toEqual([all[1].id,all[2].id].sort());
  expect((await readLiveFailureReview(pool,f.goal)).pendingFocuses.map(x=>x.eventId).sort()).toEqual([all[1].id,all[2].id].sort());
  expect(await state(f)).toEqual(before);
});
it('refuses detached, malformed or raw report facts before emitting an event',async()=>{
  const f=await fixture(),before=await state(f);
  for(const mutate of [
    (s:any)=>s.observations[0].reports[0].facts.observedCounts.failed=2,
    (s:any)=>s.observations[0].reports[0].facts.failedCases[0].detail='PRIVATE_DETAIL',
    (s:any)=>s.observations[0].reports[0].facts.failedCases[0].index=2,
    (s:any)=>s.observations[0].reports[0].facts.failedCases[0].detailClass='acceptance_proven',
    (s:any)=>s.observations[0].reports[0].facts.inconsistency=true,
    (s:any)=>s.observations[0].reports[0].digest='f'.repeat(64),
    (s:any)=>s.observations[0].reports[0].facts.failedCases[0].name='a materially different case',
  ]){
    const snapshot=reportSnapshot(f,true);mutate(snapshot);
    expect(()=>checkedRuntimeStageSnapshot(snapshot)).toThrow();
    expect((await reconcileRuntimeStage(async()=>snapshot)).events).toBe(0);
  }
  expect(await events(f)).toHaveLength(0);expect(await state(f)).toEqual(before);
});
it.each(['proposed','dispatching','queued_next_step','uncertain'])('does not issue a second episode while guidance is %s',async(phase)=>{
  const f=await fixture();await reconcileRuntimeStage(async()=>f.snapshot());const first=(await events(f))[0];
  await review(f,first.id,'guide',phase);
  await reconcileRuntimeStage(async()=>f.snapshot(30,f.base-10000));
  await reconcileRuntimeStage(async()=>f.snapshot(50,f.base-5000));
  expect(await events(f)).toHaveLength(1);
});
it.each(['guide','no-action'] as const)('revisits an unchanged wait only after a new completed window following %s',async(kind)=>{
  const f=await fixture();await reconcileRuntimeStage(async()=>f.snapshot());const first=(await events(f))[0];
  await review(f,first.id,kind);
  await reconcileRuntimeStage(async()=>f.snapshot(8)); // Fix post-disposition baseline.
  await reconcileRuntimeStage(async()=>f.snapshot(10,f.base-5000)); // Age without enough completed calls is insufficient.
  expect(await events(f)).toHaveLength(1);
  await Promise.all([reconcileRuntimeStage(async()=>f.snapshot(12,f.base-5000)),reconcileRuntimeStage(async()=>f.snapshot(12,f.base-5000))]);
  const all=await events(f);expect(all).toHaveLength(2);
  expect(all[1].payload).toMatchObject({generation:1,previousEventId:first.id,acceptance:false});
  expect(all[1].payload.semanticCauseDigest).not.toBe(first.payload.semanticCauseDigest);
  expect((await readLiveFailureReview(pool,f.goal)).focus?.eventId).toBe(all[1].id);
  expect((await state(f)).task.state).toBe('running');
});
it('new completed calls after an inbox splice rearm even when the trace window count restarts',async()=>{
  const f=await fixture();await reconcileRuntimeStage(async()=>f.snapshot(50));const first=(await events(f))[0];
  await review(f,first.id,'guide');await reconcileRuntimeStage(async()=>f.snapshot(50));
  const snapshot=f.snapshot(4,f.base-5000);
  snapshot.observations[0].wait.firstCompletedAt=new Date(f.base-85000).toISOString();
  await reconcileRuntimeStage(async()=>snapshot);
  expect(await events(f)).toHaveLength(2);
});
it('preserves episode history across an absent sample instead of reopening identical evidence',async()=>{
  const f=await fixture();await reconcileRuntimeStage(async()=>f.snapshot());const first=(await events(f))[0];
  await reconcileRuntimeStage(async()=>({...f.snapshot(),observations:[]}));
  expect((await readLiveFailureReview(pool,f.goal)).focus).toBeNull();
  await reconcileRuntimeStage(async()=>f.snapshot());expect(await events(f)).toHaveLength(1);
  expect((await readLiveFailureReview(pool,f.goal)).focus?.eventId).toBe(first.id);
});
it('a truncated observation does not infer that an omitted current cause disappeared',async()=>{
  const f=await fixture();await reconcileRuntimeStage(async()=>f.snapshot());const first=(await events(f))[0];
  await reconcileRuntimeStage(async()=>({...f.snapshot(),truncated:true,observations:[]}));
  expect((await readLiveFailureReview(pool,f.goal)).focus?.eventId).toBe(first.id);
  expect(await events(f)).toHaveLength(1);
});
it.each(['proposed','dispatching','queued_next_step','uncertain'])('suppresses all other stage causes while this run has %s guidance',async(phase)=>{
  const f=await fixture(),snapshot:any=f.snapshot();
  snapshot.observations.push({category:'stage_failure',evidenceDigest:'f'.repeat(64),callId:'report_2',toolName:'stage_report',
    reports:[{path:'evidence/test.json',digest:'e'.repeat(64),size:50,mtimeMs:f.base-200000,passed:false,exitCode:1}]});
  await reconcileRuntimeStage(async()=>snapshot);const first=(await events(f))[0];
  await review(f,first.id,'guide',phase);
  expect((await readLiveFailureReview(pool,f.goal)).pendingFocuses).toHaveLength(0);
  expect(await events(f)).toHaveLength(2);
  expect((await state(f)).task.state).toBe('running');
});
it('rejects malformed evidence paths, lone age, fabricated wait counts and successful reports',async()=>{
  const f=await fixture();
  for(const mutate of [
    (s:any)=>s.observations[0].reports[0].path='evidence/../auth.json',
    (s:any)=>s.observations[0].wait.completedCalls=1,
    (s:any)=>s.observations[0].wait.firstCompletedAt=s.observations[0].wait.lastCompletedAt,
    (s:any)=>{s.observations[0].category='stage_failure';delete s.observations[0].wait;s.observations[0].reports[0].passed=true;},
  ]){const snapshot:any=f.snapshot();mutate(snapshot);expect(()=>checkedRuntimeStageSnapshot(snapshot)).toThrow();}
});
