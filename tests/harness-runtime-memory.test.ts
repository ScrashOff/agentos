import {afterAll,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {recordHarnessTerminationEvidence,runtimeMemoryOccupancy,releaseRuntimeMemory,reconcileRuntimeMemory} from '../server/runtime-memory.js';
import {installNativeSupervision,reserveNativeSupervisor,registerNativeSupervisor,startDetachedNativeRun,
  nativeSupervisorLockNamespace,nativeSupervisorLockKey,nativeSupervisorCgroup,type NativeReservation} from '../server/native-supervision.js';

const release={candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installNativeSupervision(pool);});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='runtime-memory-policy' OR id LIKE 'worker-readiness:%'");
});
afterAll(()=>pool.end());
async function highPolicy(){
  await pool.query("INSERT INTO settings VALUES('runtime-memory-policy',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",[
    JSON.stringify({version:1,enabled:true,maxOpenCode:12,maxElevated:2,inventoryConfirmed:true,hostRuntimeBudgetMiB:24144})]);
  for(const name of ['agentos-workers','agentos-backlog','agentos-cycles','agentos-opencode'])
    await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',['worker-readiness:'+name,JSON.stringify(release)]);
}
async function source(code='RUNTIME_EXITED',provider='codex',runtime='harness'){
  const goal=randomUUID(),task=randomUUID(),old=randomUUID(),fence=randomUUID(),account='memory-'+randomUUID(),project='harness-memory-'+goal;
  const workspace='harness-memory/'+task;
  await pool.query("INSERT INTO accounts(id,label,provider,model) VALUES($1,'Fixture',$2,'test-model')",[account,provider]);
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Memory fixture','No runtime',$2,$3)",[goal,project,goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
    VALUES($1,$2,$3,'Fixture','Immutable source','qa.backend','failed','[]',$4,1)`,
    [task,goal,task,JSON.stringify({kind:runtime,workspace,writePaths:['evidence/']})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,runtime,agent_version,state,error_code)
    VALUES($1,$2,1,$3,$4,$6,1,'failed',$5)`,[old,task,fence,account,code,runtime]);
  await pool.query('INSERT INTO settings VALUES($1,$2),($3,$4)',[
    'supervision:'+goal,'{"enabled":true}','workforce:'+goal,'{"enabled":true,"detachedHarnessEnabled":true}']);
  const checkedAt=new Date().toISOString();
  const proof={version:1,runId:old,runFence:fence,workspace,runtime:'harness',state:'failed',code,
    stateDigest:'b'.repeat(64),checkedAt,termination:{source:'systemd-journal',unit:'agentos-run-'+old+'.service',result:'oom-kill'},
    process:{unit:'agentos-run-'+old,stopped:true,mainPid:0,populated:false,activeState:'inactive',checkedAt}};
  return {goal,task,old,fence,account,project,workspace,proof};
}
type Source=Awaited<ReturnType<typeof source>>;
async function next(f:Source,record=true){
  if(record)expect(await recordHarnessTerminationEvidence(f.old,f.fence,f.proof)).toMatchObject({recorded:true});
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='queued',attempt_count=2,execution=jsonb_set(execution,'{continuation}',$2::jsonb) WHERE id=$1",
    [f.task,JSON.stringify({state:'queued',runId:f.old})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,runtime,agent_version,state,checkpoint)
    VALUES($1,$2,2,$3,$4,'harness',1,'queued',$5)`,[run,f.task,fence,f.account,
    JSON.stringify({kind:'automatic_recovery',strategy:'targeted_repair',sourceRunId:f.old,preservedWorkspace:f.workspace})]);
  await pool.query('INSERT INTO opencode_jobs(run_id) VALUES($1)',[run]);
  return {...f,run,runFence:fence};
}
const reserve=(f:{run:string})=>tx(db=>reserveNativeSupervisor(db,f.run,release));
async function nextOpenCode(oom=true){
  const f=await source(oom?'OPENCODE_SANDBOX_OOM':'RUNTIME_EXITED','opencode','opencode'),run=randomUUID(),fence=randomUUID();
  if(oom){
    await event(pool,'RUNTIME_TERMINATION',f.task,{runId:f.old,source:'systemd-journal',unit:'agentos-run-'+f.old+'.service',result:'oom-kill'});
    await event(pool,'OPENCODE_STOPPED',f.task,{runId:f.old,confirmed:true,proof:f.proof.process});
  }
  await pool.query("UPDATE tasks SET state='queued',attempt_count=2 WHERE id=$1",[f.task]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,runtime,agent_version,state,checkpoint)
    VALUES($1,$2,2,$3,$4,'opencode',1,'queued',$5)`,[run,f.task,fence,f.account,
    JSON.stringify({kind:'opencode_recovery',previousRunId:f.old,preservedWorkspace:f.workspace})]);
  await pool.query('INSERT INTO opencode_jobs(run_id) VALUES($1)',[run]);
  return {...f,run,runFence:fence};
}
const physical=(r:NativeReservation)=>({runId:r.runId,executionRunId:r.runId,workspace:r.resources!.workspace,
  checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+r.runId,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});

it('records a legacy OOM as append-only evidence, idempotently, without changing run or task',async()=>{
  const f=await source(),before=(await pool.query('SELECT row_to_json(r) value FROM runs r WHERE id=$1',[f.old])).rows[0].value;
  const task=(await pool.query('SELECT row_to_json(t) value FROM tasks t WHERE id=$1',[f.task])).rows[0].value;
  const a=await recordHarnessTerminationEvidence(f.old,f.fence,f.proof),b=await recordHarnessTerminationEvidence(f.old,f.fence,f.proof);
  expect(a).toMatchObject({recorded:true});expect(b).toMatchObject({recorded:false,receiptEventId:a!.receiptEventId});
  expect((await pool.query('SELECT row_to_json(r) value FROM runs r WHERE id=$1',[f.old])).rows[0].value).toEqual(before);
  expect((await pool.query('SELECT row_to_json(t) value FROM tasks t WHERE id=$1',[f.task])).rows[0].value).toEqual(task);
  expect((await pool.query("SELECT kind FROM events WHERE entity_id=$1 ORDER BY id",[f.task])).rows.map(r=>r.kind))
    .toEqual(['RUNTIME_TERMINATION','HARNESS_STOPPED','HARNESS_TERMINATION_EVIDENCE_RECORDED']);
});

it.each(['stale','future','fence','workspace','pid','populated','active','unit','journal','result','digest','error','runtime','identity'])
('rejects untrusted or mismatched legacy %s evidence without events',async kind=>{
  const f=await source(),proof=structuredClone(f.proof);
  if(kind==='stale')proof.checkedAt=new Date(Date.now()-61000).toISOString();
  if(kind==='future')proof.checkedAt=new Date(Date.now()+6000).toISOString();
  if(kind==='fence')proof.runFence=randomUUID();
  if(kind==='workspace')proof.workspace='other/workspace';
  if(kind==='pid')proof.process.mainPid=12;
  if(kind==='populated')proof.process.populated=true;
  if(kind==='active')proof.process.activeState='active';
  if(kind==='unit')proof.process.unit='agentos-run-'+randomUUID();
  if(kind==='journal')proof.termination.source='model';
  if(kind==='result')proof.termination.result='exit-code';
  if(kind==='digest')proof.stateDigest='invalid';
  if(kind==='error')proof.code='HARNESS_SANDBOX_OOM';
  if(kind==='runtime')proof.runtime='opencode';
  if(kind==='identity')proof.runId=randomUUID();
  expect(await recordHarnessTerminationEvidence(f.old,f.fence,proof)).toBeNull();
  expect((await pool.query('SELECT 1 FROM events WHERE entity_id=$1',[f.task])).rowCount).toBe(0);
});

it.each(['live','paused','cancelled','manual','autonomy','supervision','new-attempt','fence'])
('cannot record a recovery authorization across %s authority',async mode=>{
  const f=await source();
  if(mode==='live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.old]);
  if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='cancelled')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.old});
  if(mode==='manual')await pool.query("UPDATE tasks SET recovery=$2 WHERE id=$1",[f.task,JSON.stringify({mode:'manual'})]);
  if(mode==='autonomy')await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":false}')",['autonomy:'+f.project]);
  if(mode==='supervision')await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['supervision:'+f.goal]);
  if(mode==='new-attempt')await next(f,false);
  expect(await recordHarnessTerminationEvidence(f.old,mode==='fence'?randomUUID():f.fence,f.proof)).toBeNull();
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='RUNTIME_TERMINATION'",[f.task])).rowCount).toBe(0);
});

it('reserves3072 only for the exact frozen Harness recovery and retains the legacy error',async()=>{
  await highPolicy();const f=await next(await source()),r=(await reserve(f))!;
  expect(r.resources).toMatchObject({runtime:'harness',memoryMiB:3072,runId:f.run,runFence:f.runFence,
    launchToken:r.launchToken,workspace:f.workspace,oomSource:{runId:f.old,attempt:1}});
  expect((await pool.query('SELECT error_code FROM runs WHERE id=$1',[f.old])).rows[0].error_code).toBe('RUNTIME_EXITED');
  expect(await runtimeMemoryOccupancy(pool)).toEqual({total:1,openCode:0,elevated:1,memoryMiB:3072,openCodeMiB:0});
});

it.each(['termination','stop','weak-stop','event-fence','event-workspace','checkpoint','snapshot-source','wrong-source','muse'])
('does not elevate absent or divergent %s provenance',async mode=>{
  await highPolicy();const f=await source('RUNTIME_EXITED',mode==='muse'?'muse':'codex');
  expect(await recordHarnessTerminationEvidence(f.old,f.fence,f.proof)).toMatchObject({recorded:true});
  const originalSource=f.old;
  if(mode==='snapshot-source')f.old=randomUUID(); // frozen continuation differs from the checkpoint
  const n=await next(f,false);
  if(mode==='snapshot-source')await pool.query("UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{sourceRunId}',$2::jsonb) WHERE id=$1",[n.run,JSON.stringify(originalSource)]);
  if(mode==='termination')await pool.query("DELETE FROM events WHERE kind='RUNTIME_TERMINATION'");
  if(mode==='stop')await pool.query("DELETE FROM events WHERE kind='HARNESS_STOPPED'");
  if(mode==='weak-stop')await pool.query("UPDATE events SET payload=payload-'proof' WHERE kind='HARNESS_STOPPED'");
  if(mode==='event-fence')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{runFence}',$1::jsonb) WHERE kind='RUNTIME_TERMINATION'",[JSON.stringify(randomUUID())]);
  if(mode==='event-workspace')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{workspace}','\"other/workspace\"') WHERE kind='HARNESS_STOPPED'");
  if(mode==='checkpoint')await pool.query("UPDATE runs SET checkpoint='{}' WHERE id=$1",[n.run]);
  if(mode==='wrong-source')await pool.query("UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{sourceRunId}',$2::jsonb) WHERE id=$1",[n.run,JSON.stringify(randomUUID())]);
  expect((await reserve(n))?.resources?.memoryMiB??1800).toBe(1800);
});

it('waits for policy and all four matching controller receipts instead of silently repeating low memory',async()=>{
  const f=await next(await source());expect(await reserve(f)).toBeNull();await highPolicy();
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{digest}','\"old\"') WHERE id='worker-readiness:agentos-cycles'");
  expect(await reserve(f)).toBeNull();await highPolicy();expect((await reserve(f))?.resources?.memoryMiB).toBe(3072);
});

it('atomically shares the two elevated slots across OpenCode and Harness',async()=>{
  await highPolicy();const a=await next(await source()),b=await next(await source()),c=await next(await source());
  const oc=(await reserve(await nextOpenCode()))!;
  expect(oc.resources?.memoryMiB).toBe(3072);
  const reservations=await Promise.all([reserve(a),reserve(b),reserve(c)]),admitted=reservations.filter((r):r is NativeReservation=>!!r);
  expect(admitted).toHaveLength(1);expect((await runtimeMemoryOccupancy(pool)).elevated).toBe(2);
  await pool.query("UPDATE runs SET state='failed' WHERE id=$1",[admitted[0].runId]);
  expect(await tx(db=>releaseRuntimeMemory(db,admitted[0],physical(admitted[0])))).toBe(true);
  expect((await reserve([a,b,c][reservations.findIndex(r=>!r)]))?.resources?.memoryMiB).toBe(3072);
});

it('keeps the twelve OpenCode cap separate while applying the aggregate budget to Harness',async()=>{
  await highPolicy();
  for(let i=0;i<12;i++)expect(await reserve(await nextOpenCode(false))).not.toBeNull();
  expect(await reserve(await nextOpenCode(false))).toBeNull();
  expect((await reserve(await next(await source(),false)))?.resources?.memoryMiB).toBe(1800);
  expect(await reserve(await next(await source(),false))).toBeNull(); // 23400+1800 exceeds24144
  expect(await runtimeMemoryOccupancy(pool)).toEqual({total:13,openCode:12,elevated:0,memoryMiB:23400,openCodeMiB:21600});
});

it('retains Harness allocation after terminal status and releases only after physical stop',async()=>{
  await highPolicy();const r=(await reserve(await next(await source())))!;
  await pool.query("UPDATE runs SET state='failed' WHERE id=$1",[r.runId]);
  expect((await runtimeMemoryOccupancy(pool)).memoryMiB).toBe(3072);
  expect(await tx(db=>releaseRuntimeMemory(db,r,{...physical(r),process:{...physical(r).process,populated:true}}))).toBe(false);
  let observed:any;
  expect(await reconcileRuntimeMemory(async(_id,native,envelope)=>{observed={native,envelope};return physical(r);})).toBe(1);
  expect(observed.native).toBe(false);expect(observed.envelope.resources.runtime).toBe('harness');
  expect((await runtimeMemoryOccupancy(pool)).memoryMiB).toBe(0);
});

it('revalidates Harness resources at owner registration and start, then forwards only the bound envelope',async()=>{
  await highPolicy();const f=await next(await source()),r=(await reserve(f))!,owner=await pool.connect();
  try{
    await owner.query('SELECT pg_advisory_lock($1::int,$2::int)',[nativeSupervisorLockNamespace,nativeSupervisorLockKey(f.run)]);
    const pid=(await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
    const self={unit:r.unit,pid:12345,cwd:r.candidate,cgroup:nativeSupervisorCgroup(f.run)};
    expect(await tx(db=>registerNativeSupervisor(db,{...r,resources:undefined},self,pid))).toBe(false);
    expect(await tx(db=>registerNativeSupervisor(db,r,self,pid))).toBe(true);
    expect(await tx(db=>startDetachedNativeRun(db,{...r,resources:undefined},'harness-native-12345'))).toBeNull();
    expect(await tx(db=>startDetachedNativeRun(db,{...r,resources:{...r.resources!,launchToken:randomUUID()}},'harness-native-12345'))).toBeNull();
    const run=await tx(db=>startDetachedNativeRun(db,r,'harness-native-12345'));
    expect(run).toMatchObject({id:f.run,memoryReservation:r.resources,launchToken:r.launchToken});
  }finally{await owner.query('SELECT pg_advisory_unlock_all()');owner.release();}
});

it('allows a normal legacy Harness reservation without a new memory envelope but charges its residency',async()=>{
  const f=await next(await source(),false),r=(await reserve(f))!;
  const {resources:unused,...legacy}=r;
  await pool.query('UPDATE opencode_jobs SET supervision=$2 WHERE run_id=$1',[f.run,JSON.stringify(legacy)]);
  expect(await runtimeMemoryOccupancy(pool)).toEqual({total:1,openCode:0,elevated:0,memoryMiB:1800,openCodeMiB:0});
  expect(unused?.memoryMiB).toBe(1800);
});

// These are the two typed owner events already emitted by old Harness bridges.
// No administrative ingestion or mutation of the historical error is needed.
async function providerLabelWithOom(){
  const f=await source('PROVIDER_ERROR');
  await event(pool,'RUNTIME_TERMINATION',f.task,{runId:f.old,runFence:f.fence,workspace:f.workspace,
    source:'systemd-journal',unit:'agentos-run-'+f.old+'.service',result:'oom-kill'});
  await event(pool,'HARNESS_STOPPED',f.task,{runId:f.old,runFence:f.fence,workspace:f.workspace,
    confirmed:true,proof:f.proof.process});
  return f;
}

it('uses confirmed owner OOM proof behind a legacy provider label without rewriting that label or history',async()=>{
  await highPolicy();const f=await providerLabelWithOom(),n=await next(f,false);
  const old=(await pool.query('SELECT row_to_json(r) value FROM runs r WHERE id=$1',[f.old])).rows[0].value;
  const history=(await pool.query('SELECT id,kind,payload FROM events WHERE entity_id=$1 ORDER BY id',[f.task])).rows;
  const r=(await reserve(n))!;
  expect(r.resources).toMatchObject({runtime:'harness',memoryMiB:3072,runId:n.run,runFence:n.runFence,
    workspace:f.workspace,oomSource:{runId:f.old,attempt:1,terminationEventId:String(history[0].id),
      stopEventId:String(history[1].id),provenance:'legacy_label_with_confirmed_oom',
      originalDatabaseCode:'PROVIDER_ERROR',journalConfirmedOom:true}});
  expect((await pool.query('SELECT row_to_json(r) value FROM runs r WHERE id=$1',[f.old])).rows[0].value).toEqual(old);
  expect((await pool.query('SELECT id,kind,payload FROM events WHERE id=ANY($1::bigint[]) ORDER BY id',
    [history.map(row=>row.id)])).rows).toEqual(history);
  expect(old.error_code).toBe('PROVIDER_ERROR');
  expect((await runtimeMemoryOccupancy(pool)).elevated).toBe(1);
});

it.each(['no-events','termination','stop','termination-run','termination-fence','termination-workspace',
  'termination-unit','termination-source','termination-result','stop-run','stop-fence','stop-workspace',
  'stop-confirmed','stop-unit','stop-stopped','stop-pid','stop-populated','stop-active','event-task','cancelled','checkpoint','snapshot-source'])
('keeps a legacy provider label at normal memory when %s proof is absent or mismatched',async mode=>{
  await highPolicy();const f=await providerLabelWithOom(),originalSource=f.old;
  if(mode==='snapshot-source')f.old=randomUUID();
  const n=await next(f,false);
  if(mode==='no-events')await pool.query('DELETE FROM events WHERE entity_id=$1',[f.task]);
  else if(mode==='termination'||mode==='stop')await pool.query('DELETE FROM events WHERE entity_id=$1 AND kind=$2',
    [f.task,mode==='termination'?'RUNTIME_TERMINATION':'HARNESS_STOPPED']);
  else if(mode.startsWith('termination-')||mode.startsWith('stop-')){
    const kind=mode.startsWith('termination-')?'RUNTIME_TERMINATION':'HARNESS_STOPPED';
    const row=(await pool.query('SELECT id,payload FROM events WHERE entity_id=$1 AND kind=$2',[f.task,kind])).rows[0];
    const p=row.payload,field=mode.slice(mode.indexOf('-')+1);
    if(field==='run')p.runId=randomUUID();
    if(field==='fence')p.runFence=randomUUID();
    if(field==='workspace')p.workspace='other/workspace';
    if(field==='source')p.source='model';
    if(field==='result')p.result='exit-code';
    if(field==='confirmed')p.confirmed=false;
    if(field==='unit'){
      if(kind==='RUNTIME_TERMINATION')p.unit='agentos-run-'+randomUUID()+'.service';
      else p.proof.unit='agentos-run-'+randomUUID();
    }
    if(field==='pid')p.proof.mainPid=7;
    if(field==='stopped')p.proof.stopped=false;
    if(field==='populated')p.proof.populated=true;
    if(field==='active')p.proof.activeState='active';
    await pool.query('UPDATE events SET payload=$2 WHERE id=$1',[row.id,JSON.stringify(p)]);
  }else if(mode==='event-task')await pool.query('UPDATE events SET entity_id=$2 WHERE entity_id=$1',[f.task,randomUUID()]);
  else if(mode==='cancelled')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.old});
  else if(mode==='checkpoint')await pool.query("UPDATE runs SET checkpoint='{}' WHERE id=$1",[n.run]);
  else if(mode==='snapshot-source')await pool.query("UPDATE runs SET checkpoint=jsonb_set(checkpoint,'{sourceRunId}',$2::jsonb) WHERE id=$1",
    [n.run,JSON.stringify(originalSource)]);
  const r=await reserve(n);
  expect(r?.resources?.memoryMiB).toBe(1800);
  expect(r?.resources?.oomSource).toBeUndefined();
  expect((await pool.query('SELECT error_code FROM runs WHERE id=$1',[originalSource])).rows[0].error_code).toBe('PROVIDER_ERROR');
});

it('requires existing policy, all controllers and available elevated capacity for a legacy label too',async()=>{
  const f=await next(await providerLabelWithOom(),false);
  expect(await reserve(f)).toBeNull();
  await highPolicy();
  await pool.query("DELETE FROM settings WHERE id='worker-readiness:agentos-backlog'");
  expect(await reserve(f)).toBeNull();
  await highPolicy();
  expect((await reserve(await nextOpenCode()))?.resources?.memoryMiB).toBe(3072);
  expect((await reserve(await next(await source())))?.resources?.memoryMiB).toBe(3072);
  expect(await reserve(f)).toBeNull();
  expect((await runtimeMemoryOccupancy(pool)).elevated).toBe(2);
});
