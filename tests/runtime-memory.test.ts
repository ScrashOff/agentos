import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate, tx, event } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import { installNativeSupervision, reserveNativeSupervisor, reconcileNativeSupervisor,
  finishNativeSupervisor, startDetachedNativeRun, registerNativeSupervisor,
  nativeSupervisorLockNamespace, nativeSupervisorLockKey, nativeSupervisorCgroup,
  type NativeReservation } from '../server/native-supervision.js';
import { runtimeMemoryOccupancy, releaseRuntimeMemory, reconcileRuntimeMemory } from '../server/runtime-memory.js';

const release = {candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate(); await installNativeSupervision(pool);
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE'); await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='runtime-memory-policy' OR id LIKE 'worker-readiness:%'");
  await pool.query("INSERT INTO settings VALUES('remote-capacity','{\"maxRuns\":12}') ON CONFLICT(id) DO UPDATE SET value=excluded.value");
});
afterAll(()=>pool.end());
async function enableHigh() {
  await pool.query("INSERT INTO settings VALUES('runtime-memory-policy',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",[
    JSON.stringify({version:1,enabled:true,maxOpenCode:12,maxElevated:2,inventoryConfirmed:true,hostRuntimeBudgetMiB:24144})]);
  for(const service of ['agentos-workers','agentos-backlog','agentos-cycles','agentos-opencode'])
    await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
      ['worker-readiness:'+service,JSON.stringify(release)]);
}
async function fixture(oom=false,runtime='opencode',sourceWorkspace?:string) {
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),old=randomUUID();
  const workspace='memory-tests/'+run;
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Memory fixture','No runtime','memory-tests',$2)",[goal,goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
    VALUES($1,$2,$3,'Fixture','Contract remains intact','qa.backend','queued','[]',$4,$5)`,
    [task,goal,task,JSON.stringify({kind:runtime,workspace:sourceWorkspace??workspace,writePaths:['evidence/']}),oom?2:1]);
  if(oom) {
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,instruction_snapshot)
      VALUES($1,$2,1,$3,'opencode',1,'failed','OPENCODE_SANDBOX_OOM',$4)`,
      [old,task,randomUUID(),JSON.stringify({schema:1,task:{execution:{kind:'opencode',workspace:sourceWorkspace??workspace}}})]);
    await event(pool,'RUNTIME_TERMINATION',task,{runId:old,source:'systemd-journal',unit:'agentos-run-'+old+'.service',result:'oom-kill'});
    await event(pool,'OPENCODE_STOPPED',task,{runId:old,confirmed:true,proof:{unit:'agentos-run-'+old,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
  }
  if(sourceWorkspace)await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}',$2::jsonb) WHERE id=$1",[task,JSON.stringify(workspace)]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,checkpoint)
    VALUES($1,$2,$3,$4,$5,1,'queued',$6)`,[run,task,oom?2:1,fence,runtime,
    JSON.stringify(oom?{kind:'opencode_recovery',previousRunId:old,preservedWorkspace:workspace}:{})]);
  await pool.query('INSERT INTO leases VALUES($1,$2),($3,$2)',['remote-slot',run,'workspace:'+workspace]);
  await pool.query('INSERT INTO opencode_jobs(run_id) VALUES($1)',[run]);
  return {goal,task,run,fence,old,workspace};
}
const reserve=(f:{run:string})=>tx(db=>reserveNativeSupervisor(db,f.run,release));
const proof=(r:NativeReservation)=>({runId:r.runId,executionRunId:r.runId,workspace:r.resources!.workspace,
  checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+r.runId,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
const stopSupervisor=(r:NativeReservation)=>({unit:r.unit,checkedAt:new Date().toISOString(),activeState:'inactive',loadState:'not-found',mainPid:0,pendingJob:false,cgroupEmpty:true});
async function fail(r:NativeReservation) {await pool.query("UPDATE runs SET state='failed' WHERE id=$1",[r.runId]);}

it('reserves before any native launch, without changing the task contract or attempt',async()=>{
  const f=await fixture(); const before=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  const r=(await reserve(f))!;
  expect(r.resources).toMatchObject({version:1,runId:f.run,runFence:f.fence,launchToken:r.launchToken,memoryMiB:1800,state:'reserved',workspace:f.workspace});
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT state,started_at FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual({state:'queued',started_at:null});
  expect((await pool.query("SELECT 1 FROM events WHERE kind IN ('RUN_STARTED','OPENCODE_LAUNCH_ATTEMPTED')")).rowCount).toBe(0);
});

it('atomically caps concurrent proven OOM retries at two and refills after a confirmed stop',async()=>{
  await enableHigh(); const fixtures=await Promise.all([fixture(true),fixture(true),fixture(true)]);
  const result=await Promise.all(fixtures.map(reserve)); const admitted=result.filter((r):r is NativeReservation=>!!r);
  expect(admitted).toHaveLength(2); expect(admitted.every(r=>r.resources?.memoryMiB===3072)).toBe(true);
  expect(await runtimeMemoryOccupancy(pool)).toEqual({total:2,openCode:2,elevated:2,memoryMiB:6144,openCodeMiB:6144});
  const blocked=fixtures[result.findIndex(r=>!r)];
  const frozen=(await pool.query('SELECT state,attempt_count,criteria,execution FROM tasks WHERE id=$1',[blocked.task])).rows[0];
  await fail(admitted[0]); expect(await tx(db=>releaseRuntimeMemory(db,admitted[0],proof(admitted[0])))).toBe(true);
  expect((await reserve(blocked))?.resources?.memoryMiB).toBe(3072);
  expect((await pool.query('SELECT state,attempt_count,criteria,execution FROM tasks WHERE id=$1',[blocked.task])).rows[0]).toEqual(frozen);
});

it('limits twelve residents even when more queued tasks already own leases',async()=>{
  const fixtures=[];for(let i=0;i<15;i++)fixtures.push(await fixture());
  expect((await pool.query("SELECT 1 FROM leases WHERE resource='remote-slot'")).rowCount).toBe(15);
  const result=await Promise.all(fixtures.map(reserve)); expect(result.filter(Boolean)).toHaveLength(12);
  expect(await runtimeMemoryOccupancy(pool)).toEqual({total:12,openCode:12,elevated:0,memoryMiB:21600,openCodeMiB:21600});
});

it('never exceeds 24144 MiB for two elevated and ten normal reservations',async()=>{
  await enableHigh(); await reserve(await fixture(true)); await reserve(await fixture(true));
  const fixtures=[]; for(let i=0;i<11;i++)fixtures.push(await fixture());
  expect((await Promise.all(fixtures.map(reserve))).filter(Boolean)).toHaveLength(10);
  expect(await runtimeMemoryOccupancy(pool)).toEqual({total:12,openCode:12,elevated:2,memoryMiB:24144,openCodeMiB:24144});
});

it('rolls back claims without leaking capacity and admits a given run only once',async()=>{
  const f=await fixture();
  await expect(tx(async db=>{expect(await reserveNativeSupervisor(db,f.run,release)).not.toBeNull();throw Error('fixture rollback');})).rejects.toThrow('fixture rollback');
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(0);
  expect((await Promise.all(Array.from({length:8},()=>reserve(f)))).filter(Boolean)).toHaveLength(1);
});

it('retains memory through terminal status, lost owner and invalid/stale sandbox observations',async()=>{
  const r=(await reserve(await fixture()))!;await fail(r);
  await tx(db=>finishNativeSupervisor(db,r));
  for(const bad of [
    {...proof(r),checkedAt:new Date(Date.now()-61000).toISOString()},
    {...proof(r),runId:randomUUID()}, {...proof(r),workspace:'memory-tests/other'},
    {...proof(r),executionRunId:randomUUID()},
    {...proof(r),process:{...proof(r).process,populated:true}},
    {...proof(r),process:{...proof(r).process,activeState:'active'}},
    {...proof(r),process:{...proof(r).process,unit:r.unit}},
  ]) expect(await tx(db=>releaseRuntimeMemory(db,r,bad))).toBe(false);
  expect(await tx(db=>releaseRuntimeMemory(db,{...r,runFence:randomUUID()},proof(r)))).toBe(false);
  expect(await tx(db=>releaseRuntimeMemory(db,{...r,launchToken:randomUUID()},proof(r)))).toBe(false);
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(1);
  expect(await tx(db=>releaseRuntimeMemory(db,r,proof(r)))).toBe(true);
  expect(await tx(db=>releaseRuntimeMemory(db,r,proof(r)))).toBe(true);
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(0);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='RUNTIME_MEMORY_RELEASED'")).rowCount).toBe(1);
});

it('does not release a live execution even with an inactive sandbox snapshot',async()=>{
  const r=(await reserve(await fixture()))!;
  await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[r.runId]);
  expect(await tx(db=>releaseRuntimeMemory(db,r,proof(r)))).toBe(false);
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(1);
});

it('requires the exact reserved allocation at registration and at launch',async()=>{
  const f=await fixture(),r=(await reserve(f))!,owner=await pool.connect();
  try{
    await owner.query('SELECT pg_advisory_lock($1::int,$2::int)',[nativeSupervisorLockNamespace,nativeSupervisorLockKey(f.run)]);
    const pid=(await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
    const own={unit:r.unit,pid:12345,cwd:r.candidate,cgroup:nativeSupervisorCgroup(f.run)};
    expect(await tx(db=>registerNativeSupervisor(db,{...r,resources:undefined},own,pid))).toBe(false);
    expect(await tx(db=>registerNativeSupervisor(db,r,own,pid))).toBe(true);
    expect(await tx(db=>startDetachedNativeRun(db,{...r,resources:undefined},'opencode-native-12345'))).toBeNull();
    expect(await tx(db=>startDetachedNativeRun(db,{...r,resources:{...r.resources!,memoryMiB:3072}},'opencode-native-12345'))).toBeNull();
    expect((await tx(db=>startDetachedNativeRun(db,r,'opencode-native-12345')))?.id).toBe(f.run);
  }finally{await owner.query('SELECT pg_advisory_unlock_all()');owner.release();}
});

it('fences never-started launches before releasing and reclaims with a new reservation',async()=>{
  const f=await fixture(),r=(await reserve(f))!;
  expect(await tx(db=>reconcileNativeSupervisor(db,r,stopSupervisor(r)))).toBe('queued');
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(0);
  expect(await tx(db=>startDetachedNativeRun(db,r,'opencode-native-12345'))).toBeNull();
  await pool.query("UPDATE opencode_jobs SET supervision=supervision-'nextLaunchAt' WHERE run_id=$1",[f.run]);
  const next=(await reserve(f))!;
  expect(next.resources?.id).not.toBe(r.resources?.id);expect(next.launchToken).not.toBe(r.launchToken);
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(1);
});

it('counts legacy resident Harness and uncertain OpenCode without reserving never-started legacy queue entries',async()=>{
  const harness=await fixture(false,'harness'),uncertain=await fixture(),queued=await fixture();
  await pool.query("UPDATE runs SET state='running',started_at=now() WHERE id=$1",[harness.run]);
  await pool.query("UPDATE opencode_jobs SET state='orphaned' WHERE run_id=$1",[uncertain.run]);
  expect(await runtimeMemoryOccupancy(pool)).toEqual({total:2,openCode:1,elevated:0,memoryMiB:3600,openCodeMiB:1800});
  expect((await reserve(queued))?.resources?.memoryMiB).toBe(1800);
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(3);
});

it('requires explicit current-release adoption and inventory before elevating a proven OOM retry',async()=>{
  const f=await fixture(true); expect(await reserve(f)).toBeNull();
  await enableHigh(); await pool.query("UPDATE settings SET value=jsonb_set(value,'{digest}','\"old\"') WHERE id='worker-readiness:agentos-backlog'");
  expect(await reserve(f)).toBeNull(); await enableHigh();
  await pool.query("UPDATE settings SET value='{\"maxRuns\":26}' WHERE id='remote-capacity'");
  expect((await reserve(f))?.resources?.memoryMiB).toBe(3072);
});

it.each(['termination','stop','wrong_task','wrong_workspace','wrong_error','wrong_checkpoint'])('does not elevate a retry with absent or mismatched %s proof',async(kind)=>{
  await enableHigh();const f=await fixture(true,'opencode',kind==='wrong_workspace'?'memory-tests/other':undefined);
  if(kind==='termination') await pool.query("DELETE FROM events WHERE kind='RUNTIME_TERMINATION'");
  if(kind==='stop') await pool.query("DELETE FROM events WHERE kind='OPENCODE_STOPPED'");
  if(kind==='wrong_task') {const other=await fixture(true);await pool.query("UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{previousRunId}',$2::jsonb) WHERE id=$1",[f.run,JSON.stringify(other.old)]);}
  if(kind==='wrong_error') await pool.query("UPDATE runs SET error_code='RUNTIME_EXITED' WHERE id=$1",[f.old]);
  if(kind==='wrong_checkpoint') await pool.query("UPDATE runs SET checkpoint='{}' WHERE id=$1",[f.run]);
  expect((await reserve(f))?.resources?.memoryMiB).toBe(1800);
});

it('reconciles finished jobs only after fresh sandbox inspection and preserves failures for later observation',async()=>{
  const a=(await reserve(await fixture()))!,b=(await reserve(await fixture()))!;
  await fail(a);await fail(b);await tx(db=>finishNativeSupervisor(db,a));await tx(db=>finishNativeSupervisor(db,b));
  expect(await reconcileRuntimeMemory(async id=>{if(id===a.runId)throw Error('SSH unavailable');return proof(b);})).toBe(1);
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(1);
  expect(await reconcileRuntimeMemory(async()=>proof(a))).toBe(1);
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(0);
});

it('does not starve later stopped reservations when the first four inspections fail',async()=>{
  const reservations:NativeReservation[]=[];
  for(let i=0;i<7;i++){const r=(await reserve(await fixture()))!;await fail(r);await tx(db=>finishNativeSupervisor(db,r));reservations.push(r);}
  const seen:string[]=[],blocked=new Set(reservations.slice(0,4).map(r=>r.runId));
  let active=0,peak=0;
  expect(await reconcileRuntimeMemory(async id=>{
    seen.push(id);active++;peak=Math.max(peak,active);
    await new Promise(resolve=>setTimeout(resolve,2));active--;
    if(blocked.has(id))throw Error('Observation unavailable');
    return proof(reservations.find(r=>r.runId===id)!);
  })).toBe(3);
  expect(seen).toHaveLength(7);expect(peak).toBeLessThanOrEqual(2);
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(4);
});

it('charges the aggregate resident budget separately from the OpenCode count',async()=>{
  await enableHigh();
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{hostRuntimeBudgetMiB}','7200') WHERE id='runtime-memory-policy'");
  for(let i=0;i<2;i++){const f=await fixture(false,'harness');await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);}
  expect(await reserve(await fixture())).not.toBeNull();expect(await reserve(await fixture())).not.toBeNull();
  expect(await reserve(await fixture())).toBeNull();
  expect(await runtimeMemoryOccupancy(pool)).toEqual({total:4,openCode:2,elevated:0,memoryMiB:7200,openCodeMiB:3600});
});

it('serializes mixed Harness and OpenCode launch claims under the same aggregate memory budget',async()=>{
  await enableHigh();
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{hostRuntimeBudgetMiB}','7200') WHERE id='runtime-memory-policy'");
  const jobs=[];
  for(let i=0;i<8;i++) {
    const f=await fixture(false,i%2?'opencode':'harness');jobs.push(f);
    if(i%2===0)await pool.query('INSERT INTO settings VALUES($1,$2)',
      ['workforce:'+f.goal,'{"enabled":true,"detachedHarnessEnabled":true}']);
  }
  const result=await Promise.all(jobs.map(reserve));
  expect(result.filter(Boolean)).toHaveLength(4);
  expect((await runtimeMemoryOccupancy(pool)).memoryMiB).toBe(7200);
  for(const reservation of result.filter((r):r is NativeReservation=>!!r))
    expect(reservation.resources?.memoryMiB??1800).toBe(1800);
  expect(await reserve(jobs[result.findIndex(r=>!r)])).toBeNull();
});

it('releases a cancelled never-started reservation only after stopped supervisor proof fences its launch',async()=>{
  const r=(await reserve(await fixture()))!;
  await pool.query("UPDATE runs SET state='cancelled' WHERE id=$1",[r.runId]);
  expect(await tx(db=>reconcileNativeSupervisor(db,r,{...stopSupervisor(r),pendingJob:true}))).toBe('observing');
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(1);
  expect(await tx(db=>reconcileNativeSupervisor(db,r,stopSupervisor(r)))).toBe('finished');
  expect((await runtimeMemoryOccupancy(pool)).total).toBe(0);
  expect(await tx(db=>startDetachedNativeRun(db,r,'opencode-native-12345'))).toBeNull();
});

it('retains source identity when an artifact-collection attempt sits between OOM and native recovery',async()=>{
  await enableHigh();const f=await fixture(true),collection=randomUUID();
  await pool.query("UPDATE tasks SET attempt_count=3 WHERE id=$1",[f.task]);
  await pool.query("UPDATE runs SET attempt=3,checkpoint=jsonb_set(checkpoint,'{previousRunId}',$2::jsonb) WHERE id=$1",[f.run,JSON.stringify(collection)]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,checkpoint)
    VALUES($1,$2,2,$3,'opencode',1,'failed','RESULT_FORMAT',$4)`,[collection,f.task,randomUUID(),JSON.stringify({kind:'artifact_recovery',sourceRunId:f.old})]);
  const r=await reserve(f);expect(r?.resources?.memoryMiB).toBe(3072);expect(r?.resources?.oomSource?.runId).toBe(f.old);
});
