import { beforeAll, beforeEach, afterAll, afterEach, it, expect, vi } from 'vitest';
import { randomUUID,createHash } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import {
  installBacklogPlanner,
  applyBacklog,
  reconcileDependencyWaits,
  reconcileBacklog,
  musePendingLimit,
} from '../server/backlog.js';
import { accountBridge } from '../server/bridge.js';
import { writeArtifact, completeRun, validateTask } from '../server/service.js';
import { executeRuntime } from '../server/runtime.js';
import { config } from '../server/config.js';
import {backlogDeliveryContext} from '../server/backlog-handoffs.js';
beforeAll(async () => {
  await migrate();
  await installBacklogPlanner();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
});
afterEach(() => vi.restoreAllMocks());
afterAll(async () => {
  await pool.end();
});
it('publishes backlog capacity from small metadata without counting blocked, delayed or terminal tasks as ready', async () => {
  const f = await fixture();
  const add = async (key: string, state: string, kind: string | null, delay = false) => {
    const id = randomUUID();
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,stage,state,criteria,execution,next_attempt_at)
      VALUES($1,$2,$3,'Task','Task','qa.backend','implement',$4,'[]',$5,
        CASE WHEN $6 THEN now()+interval '1 hour' ELSE NULL END)`,
      [id,f.goal,key,state,JSON.stringify(kind === null ? [] : {kind,inputs:[{content:'x'.repeat(2*1024*1024)}]}),delay]);
    return id;
  };
  await add('active','running','opencode');
  await add('ready','ready','opencode');
  const blocked = await add('dependency','ready','opencode');
  await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[blocked,f.consumer]);
  await add('later','retry_scheduled','opencode',true);
  await add('accepted','accepted','opencode');
  await add('harness','running','harness');
  await add('nonobject','ready',null);
  await reconcileBacklog();
  const value=(await pool.query('SELECT value FROM settings WHERE id=$1',['backlog-status:'+f.goal])).rows[0].value;
  expect(value.glm).toEqual({target:12,active:1,ready:1,blocked:1,pending:4,pendingLimit:24});
  expect(value.plannerRunning).toBe(false);
});
it('does not publish misleading backlog availability when a pending task projection is missing', async () => {
  const f = await fixture();
  await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[f.consumer]);
  await expect(reconcileBacklog()).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
  expect((await pool.query('SELECT 1 FROM settings WHERE id=$1',['backlog-status:'+f.goal])).rowCount).toBe(0);
});
async function fixture() {
  const goal = randomUUID(),
    consumer = randomUUID(),
    coordinator = randomUUID(),
    run = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Goal','Complete delivery','test',$2)",
    [goal, 'goal-' + goal],
  );
  await pool.query('INSERT INTO settings VALUES($1,$2)', [
    'workforce:' + goal,
    JSON.stringify({ enabled: true, glmTarget: 12, glmAccountId: 'glm' }),
  ]);
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency) VALUES('glm','GLM','openai-compatible','glm-5.3-flash','[\"glm-5.3-flash\"]',true,20)",
  );
  for (const [id, key, stage] of [
    [consumer, 'verify', 'verify'],
    [coordinator, 'coordinate', 'coordinate'],
  ])
    await pool.query(
      "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,stage,state,criteria,execution) VALUES($1,$2,$3,'Task','Task','qa.backend',$4,'blocked_dependencies','[]',$5)",
      [
        id,
        goal,
        key,
        stage,
        JSON.stringify({
          baseWorkspace: 'test/source',
          workspace: 'test/current',
          kind: 'harness',
        }),
      ],
    );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'accepted','harness')",
    [run, coordinator, randomUUID()],
  );
  vi.spyOn(accountBridge, 'request').mockResolvedValue({});
  const proposal = {
    reason: 'Missing negative-path coverage',
    tasks: [
      {
        coverageKey: 'negative-path',
        title: 'Validate negative path',
        description:
          'Implement and execute the missing negative path regression against the candidate.',
        baseWorkspace: 'test/source',
        deliverToTaskId: consumer,
      },
    ],
  };
  return { goal, consumer, run, proposal };
}
async function acceptedSource(goal: string, key = 'accepted-diagnostic', workKind = 'diagnostic') {
  const id = randomUUID(),
    run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count)
     VALUES($1,$2,$3,'Source','Source','qa.backend','running','[]',$4,1)`,
    [
      id,
      goal,
      key,
      JSON.stringify({
        baseWorkspace: 'test/source',
        workspace: 'test/' + key,
        kind: 'opencode',
        workKind,
      }),
    ],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'running','harness')",
    [run, id, fence],
  );
  return { id, run, fence };
}
async function acceptSource(source: Awaited<ReturnType<typeof acceptedSource>>) {
  await completeRun(source.run, source.fence, 'Fixture source delivery', {});
  await validateTask(source.id);
}
async function enableMuse(f: Awaited<ReturnType<typeof fixture>>, target=6) {
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('muse','Muse','muse','muse-spark-1.3','["muse-spark-1.3"]',true,6,true,'muse')`);
  await pool.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1',[
    'workforce:'+f.goal,JSON.stringify({museTarget:target,workerAccounts:['muse']})]);
  return {...f.proposal,tasks:f.proposal.tasks.map(p=>({...p,lane:'muse'}))};
}
it('keeps parked failed history without letting it exhaust the runnable GLM reserve',async()=>{
 const f=await fixture();
 for(let i=0;i<24;i++)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
 VALUES($1,$2,$3,'Preserved failed attempt','Requires supervision','qa.backend','recovery_blocked','[]',$4)`,
 [randomUUID(),f.goal,'parked-'+i,JSON.stringify({kind:'opencode',workspace:'test/parked-'+i})]);
 const proposals=Array.from({length:24},(_,i)=>({...f.proposal.tasks[0],coverageKey:'distinct-boundary-'+i,
 title:'Independent boundary '+i,description:'Execute the distinct missing boundary case '+i+' against the current immutable candidate.'}));
 expect(await applyBacklog(f.goal,f.run,{reason:'Runnable reserve beside preserved failed history',tasks:proposals.slice(0,12)})).toHaveLength(12);
 expect(await applyBacklog(f.goal,f.run,{reason:'Fill the bounded distinct runnable reserve',tasks:proposals.slice(12)})).toHaveLength(12);
 const rejectionRun=randomUUID(),coordinator=(await pool.query('SELECT task_id FROM runs WHERE id=$1',[f.run])).rows[0].task_id;
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,2,$3,1,'accepted','harness')",
 [rejectionRun,coordinator,randomUUID()]);
 expect(await applyBacklog(f.goal,rejectionRun,{reason:'Respect the full runnable reserve',tasks:[{...proposals[0],coverageKey:'another-boundary'}]})).toEqual([]);
 expect((await pool.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1',[rejectionRun])).rows[0].receipts[0].code).toBe('PENDING_LIMIT');
 expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE goal_id=$1 AND state='recovery_blocked'",[f.goal])).rows[0].n).toBe(24);
});
it('admits useful independent work beside blocked DAG branches and counts those branches once their input is accepted',async()=>{
 const f=await fixture(),parent=await acceptedSource(f.goal,'pending-input');
 await pool.query('UPDATE settings SET value=value||\'{"glmTarget":1}\'::jsonb WHERE id=$1',['workforce:'+f.goal]);
 const blocked=[];
 for(let i=0;i<24;i++){
  const id=randomUUID();blocked.push(id);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
   VALUES($1,$2,$3,'Future dependent work','Wait for the actual input','qa.backend','blocked_dependencies','[]',$4)`,
   [id,f.goal,'future-'+i,JSON.stringify({kind:'opencode',baseWorkspace:'test/source',workspace:'test/future-'+i})]);
  await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[id,parent.id]);
 }
 const before=(await pool.query('SELECT id,state,criteria,execution FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[blocked])).rows;
 expect(await applyBacklog(f.goal,f.run,f.proposal)).toHaveLength(1);
 expect((await pool.query('SELECT id,state,criteria,execution FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[blocked])).rows).toEqual(before);
 await acceptSource(parent);
 const next=randomUUID(),coordinator=(await pool.query('SELECT task_id FROM runs WHERE id=$1',[f.run])).rows[0].task_id;
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,2,$3,1,'accepted','harness')",
  [next,coordinator,randomUUID()]);
 expect(await applyBacklog(f.goal,next,{reason:'Respect the newly eligible bounded reserve',tasks:[{...f.proposal.tasks[0],coverageKey:'next-independent'}]})).toEqual([]);
 expect((await pool.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1',[next])).rows[0].receipts[0].code).toBe('PENDING_LIMIT');
 expect((await pool.query('SELECT count(*)::int n FROM tasks WHERE id=ANY($1::uuid[])',[blocked])).rows[0].n).toBe(24);
});
it('creates independent GLM and Muse work from one backlog contract with real Harness output paths',async()=>{
  const f=await fixture(),muse=await enableMuse(f),source=await acceptedSource(f.goal);
  await writeArtifact(source.run,source.fence,'protocol-report.json',JSON.stringify({suiteExecuted:true,exitCode:1,command:'node test.mjs',cases:['real negative']}));
  await acceptSource(source);
  const ids=await applyBacklog(f.goal,f.run,{reason:'Two distinct acceptance obligations',tasks:[
    {...f.proposal.tasks[0],coverageKey:'glm-negative',sourceTaskId:source.id},
    {...muse.tasks[0],coverageKey:'muse-boundary',sourceTaskId:source.id},
  ]});
  expect(ids).toHaveLength(2);
  const tasks=(await pool.query('SELECT * FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY model',[ids])).rows;
  const m=tasks.find(t=>t.account_id==='muse'),g=tasks.find(t=>t.account_id==='glm');
  expect(m.execution).toMatchObject({kind:'harness',workforceLane:'muse',readOnlySource:true,testFile:'data/qa-protocol/muse-boundary.test.mjs'});
  expect(m.execution.writePaths).toBeUndefined();
  expect(m.execution.inputs).toHaveLength(1);
  expect(m.allowed_accounts).toEqual(['muse']);
  expect(m.execution.workspace).not.toBe(g.execution.workspace);
  expect(m.description).toContain('MUSE_DELIVERY_V1');
  expect(m.description).toContain('cases como array não vazio');
  expect(m.description).toContain('limitations como array');
  expect(m.criteria).toContainEqual(expect.objectContaining({name:'test-source.mjs'}));
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.consumer])).rows[0].state).toBe('blocked_dependencies');
});
it('preserves a negative Muse repair as rejected evidence and keeps the consumer gate closed',async()=>{
  const f=await fixture(),p=await enableMuse(f);
  await pool.query("UPDATE tasks SET stage='integrate' WHERE id=$1",[f.consumer]);
  const [id]=await applyBacklog(f.goal,f.run,{...p,tasks:p.tasks.map(t=>({...t,workKind:'repair'}))});
  const t=(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
  expect(t.description).toContain('MUSE_REPAIR_V1');
  expect(t.description).toContain('data/repair-candidate');
  expect(t.criteria).toContainEqual(expect.objectContaining({id:'repair_verified',name:'repair-report.json'}));
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[id]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,account_id,model) VALUES($1,$2,1,$3,1,'running','harness','muse','muse-spark-1.3')",[run,id,fence]);
  for(const [name,content] of Object.entries({'protocol-report.json':JSON.stringify({suiteExecuted:true,exitCode:1,command:'node '+t.execution.testFile,cases:['defect reproduced']}),'runtime-protocol-check.json':JSON.stringify({verified:true}),'protocol-tests.log':'real red regression','changes.patch':'','test-source.mjs':'import assert from "node:assert"; assert.fail("real defect");','repair-report.json':JSON.stringify({fixVerified:false,limitations:['hypothesis refuted']}),'implementation.patch':'','repair-tests.log':'before and after remain red'}))
    await writeArtifact(run,fence,name,content);
  await completeRun(run,fence,'Hypothesis refuted; no verified fix',{});
  const acceptance=await validateTask(id);
  expect(acceptance.passed).toBe(false);
  expect(acceptance.checks).toContainEqual(expect.objectContaining({id:'repair_verified',passed:false}));
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.consumer])).rows[0].state).toBe('blocked_dependencies');
});
it('persists an invalid Muse protocol report and rejects acceptance through the real controller pipeline',async()=>{
  const f=await fixture(),p=await enableMuse(f),[id]=await applyBacklog(f.goal,f.run,p);
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[id]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,account_id,model) VALUES($1,$2,1,$3,1,'running','harness','muse','muse-spark-1.3')",[run,id,fence]);
  const original=JSON.stringify({suiteExecuted:true,exitCode:'0',command:'node '+task.execution.testFile});
  const supplied=[['protocol-report.json',original],['protocol-tests.log','stdout of actual assigned fixture'],['test-source.mjs','throw new Error("fixture")'],['changes.patch',''],['runtime-protocol-check.json','{"verified":true}']].map(([name,content])=>{
    const bytes=Buffer.from(content);return{name,base64:bytes.toString('base64'),digest:createHash('sha256').update(bytes).digest('hex'),mime:'text/plain'};
  });
  vi.mocked(accountBridge.request).mockResolvedValue({summary:'Unverified protocol delivery',evidence:{stopped:true},artifacts:supplied,usage:{}});
  config.remoteExecution=true;
  try{
    const result=await executeRuntime({run:{id:run,task_id:id},task,account:{bridge_id:'muse',model:'muse-spark-1.3'},
      context:{content:'',manifest:{selected:[],dependencyArtifacts:[],executionInputs:[]}},signal:new AbortController().signal,onEvent:async()=>{}});
    expect(result.artifacts.filter(a=>a.name==='runtime-protocol-check.json')).toHaveLength(1);
    expect(result.artifacts.find(a=>a.name==='protocol-report.json')!.content.toString()).toBe(original);
    for(const a of result.artifacts)await writeArtifact(run,fence,a.name,a.content,a.mime);
    await completeRun(run,fence,result.summary,result.usage);
    const verdict=await validateTask(id);
    expect(verdict.passed).toBe(false);
    expect(verdict.checks).toContainEqual(expect.objectContaining({id:'protocol_verified',passed:false}));
    expect((await pool.query('SELECT state FROM runs WHERE id=$1',[run])).rows[0].state).toBe('submitted');
    expect((await pool.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1 AND name=$2',[run,'protocol-report.json'])).rows[0].n).toBe(1);
    expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.consumer])).rows[0].state).toBe('blocked_dependencies');
  }finally{config.remoteExecution=false;}
});
it('accepts a real negative Muse diagnostic delivery while preserving the independent final QA rejection',async()=>{
  const f=await fixture(),p=await enableMuse(f),qaRun=randomUUID(),qaFence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1,criteria=$2 WHERE id=$1",[f.consumer,
    JSON.stringify([{id:'final_qa',label:'Independent game QA',type:'json_field',name:'verification.json',field:'passed',equals:true}])]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,account_id,model) VALUES($1,$2,1,$3,1,'running','harness','muse','muse-spark-1.3')",[qaRun,f.consumer,qaFence]);
  await writeArtifact(qaRun,qaFence,'verification.json',JSON.stringify({passed:false,defect:'genuine unresolved candidate defect'}));
  await completeRun(qaRun,qaFence,'Final QA remains red',{});
  expect((await validateTask(f.consumer)).passed).toBe(false);
  const [id]=await applyBacklog(f.goal,f.run,p),task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[id]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,account_id,model) VALUES($1,$2,1,$3,1,'running','harness','muse','muse-spark-1.3')",[run,id,fence]);
  const supplied=[['protocol-report.json',JSON.stringify({suiteExecuted:true,exitCode:1,command:'node '+task.execution.testFile,cases:['observed unresolved defect'],limitations:[]})],
    ['protocol-tests.log','Regression exited 1: observed unresolved defect'],['test-source.mjs','throw new Error("observed unresolved defect");'],['changes.patch','']].map(([name,content])=>{
      const bytes=Buffer.from(content);return{name,base64:bytes.toString('base64'),digest:createHash('sha256').update(bytes).digest('hex'),mime:'text/plain'};
    });
  vi.mocked(accountBridge.request).mockResolvedValue({summary:'Executed diagnostic; defect remains',evidence:{stopped:true},artifacts:supplied,usage:{}});
  config.remoteExecution=true;
  try{
    const result=await executeRuntime({run:{id:run,task_id:id},task,account:{bridge_id:'muse',model:'muse-spark-1.3'},
      context:{content:'',manifest:{selected:[],dependencyArtifacts:[],executionInputs:[]}},signal:new AbortController().signal,onEvent:async()=>{}});
    for(const a of result.artifacts)await writeArtifact(run,fence,a.name,a.content,a.mime);
    await completeRun(run,fence,result.summary,result.usage);
    expect((await validateTask(id)).passed).toBe(true);
    expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC LIMIT 1',[qaRun])).rows[0].passed).toBe(false);
    expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.consumer])).rows[0].state).toBe('blocked_dependencies');
    expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
  }finally{config.remoteExecution=false;}
});
it('holds separate lane reserves and allows planning while the Muse account is full',async()=>{
  const f=await fixture(),p=await enableMuse(f,1);
  const mtask=randomUUID(),run=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,criteria,execution,attempt_count)
   VALUES($1,$2,'busy-muse','Busy','Live fixture','qa.backend','muse','muse-spark-1.3','running','work','[]','{"kind":"harness","workforceLane":"muse"}',1)`,[mtask,f.goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,account_id,model) VALUES($1,$2,1,$3,1,'running','harness','muse','muse-spark-1.3')",[run,mtask,randomUUID()]);
  await pool.query("UPDATE accounts SET max_concurrency=1 WHERE id='muse'");
  expect(await applyBacklog(f.goal,f.run,p)).toHaveLength(1);
  const extra={...p,tasks:p.tasks.map(t=>({...t,coverageKey:'over-muse-reserve'}))};
  expect(await applyBacklog(f.goal,f.run,extra)).toHaveLength(1);
  expect(await applyBacklog(f.goal,f.run,{...extra,tasks:extra.tasks.map(t=>({...t,coverageKey:'third-muse-reserve'}))})).toEqual([]);
  expect(await applyBacklog(f.goal,f.run,{...f.proposal,tasks:f.proposal.tasks.map(t=>({...t,coverageKey:'independent-glm-reserve'}))})).toHaveLength(1);
  expect(musePendingLimit({museTarget:0})).toBe(0);
});
it('keeps serialized original Muse DAG stages outside the independent backlog reserve',async()=>{
  const f=await fixture(),p=await enableMuse(f,6);
  for(let i=0;i<12;i++)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,criteria,execution)
   VALUES($1,$2,$3,'Original serial stage','Await implementation','qa.backend','muse','muse-spark-1.3','blocked_dependencies','verify','[]','{"kind":"harness","workforceLane":"muse","readOnlySource":true}')`,[randomUUID(),f.goal,'serial-'+i]);
  const plan={...p,tasks:Array.from({length:12},(_,i)=>({...p.tasks[0],coverageKey:'distinct-obligation-'+i}))};
  expect(await applyBacklog(f.goal,f.run,plan)).toHaveLength(12);
  expect(await applyBacklog(f.goal,f.run,{...p,tasks:p.tasks.map(t=>({...t,coverageKey:'over-12-independent'}))})).toEqual([]);
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE goal_id=$1 AND model='muse-spark-1.3'",[f.goal])).rows[0].n).toBe(24);
});
it.each(['exhausted','auth','unauthorized','disabled','model'])('rejects unavailable Muse route: %s',async(reason)=>{
  const f=await fixture(),p=await enableMuse(f);
  if(reason==='exhausted')await pool.query("UPDATE accounts SET quota='{\"ordinaryUsageAllowed\":false}' WHERE id='muse'");
  if(reason==='auth')await pool.query("UPDATE accounts SET connection_status='reconnect_required' WHERE id='muse'");
  if(reason==='unauthorized')await pool.query("UPDATE settings SET value=value||'{\"workerAccounts\":[]}'::jsonb WHERE id=$1",['workforce:'+f.goal]);
  if(reason==='disabled')await pool.query("UPDATE settings SET value=value||'{\"museTarget\":0}'::jsonb WHERE id=$1",['workforce:'+f.goal]);
  if(reason==='model')await pool.query("UPDATE accounts SET models='[]' WHERE id='muse'");
  expect(await applyBacklog(f.goal,f.run,p)).toEqual([]);
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE goal_id=$1 AND model LIKE 'muse-%'",[f.goal])).rows[0].n).toBe(0);
});
it('does not duplicate the same coverage across GLM and Muse',async()=>{
  const f=await fixture(),p=await enableMuse(f);
  expect(await applyBacklog(f.goal,f.run,f.proposal)).toHaveLength(1);
  expect(await applyBacklog(f.goal,f.run,p)).toEqual([]);
});
it('does not recreate a source-bound hypothesis with a new title and coverage key through real backlog admission',async()=>{
  const f=await fixture(),source=await acceptedSource(f.goal);
  await pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1',[f.consumer,JSON.stringify([
    {id:'delivery',label:'Verified delivery',type:'json_field',name:'verification.json',field:'passed',equals:true}])]);
  await writeArtifact(source.run,source.fence,'protocol-report.json',JSON.stringify({suiteExecuted:true,exitCode:1,
    command:'node real-test.mjs',cases:['specific failing input']}));
  await acceptSource(source);
  const plan={...f.proposal,tasks:f.proposal.tasks.map(p=>({...p,sourceTaskId:source.id}))};
  const [first]=await applyBacklog(f.goal,f.run,plan);expect(first).toBeTruthy();
  const prepareCalls=()=>vi.mocked(accountBridge.request).mock.calls.filter(call=>call[0]==='worker.prepare').length;
  expect(prepareCalls()).toBe(1);
  const coordinator=(await pool.query('SELECT task_id FROM runs WHERE id=$1',[f.run])).rows[0].task_id;
  const review=async(attempt:number)=>{
    const id=randomUUID();
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,$3,$4,1,'accepted','harness')",
      [id,coordinator,attempt,randomUUID()]);
    return id;
  };
  const renamedRun=await review(2);
  const before=(await pool.query('SELECT id,criteria,description,execution FROM tasks WHERE goal_id=$1 ORDER BY id',[f.goal])).rows;
  const renamed={...plan,tasks:plan.tasks.map(p=>({...p,coverageKey:'fresh-uuid-same-hypothesis',title:'A different title for the same hypothesis'}))};
  expect(await applyBacklog(f.goal,renamedRun,renamed)).toEqual([]);
  const receipt=(await pool.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1',[renamedRun])).rows[0].receipts;
  expect(receipt).toContainEqual(expect.objectContaining({code:'OBLIGATION_REUSE_REQUIRED',status:'ignored',taskId:first}));
  expect(prepareCalls()).toBe(1);
  expect((await pool.query('SELECT receipts FROM workforce_backlog_plans WHERE run_id=$1',[f.run])).rows[0].receipts)
    .toContainEqual(expect.objectContaining({code:'TASK_CREATED',status:'applied',taskId:first}));
  expect((await pool.query('SELECT id,criteria,description,execution FROM tasks WHERE goal_id=$1 ORDER BY id',[f.goal])).rows).toEqual(before);
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2',[f.consumer,first])).rowCount).toBe(1);
  expect(await applyBacklog(f.goal,await review(3),{...renamed,tasks:renamed.tasks.map(p=>({...p,
    description:p.description+' Investigate the distinct timeout boundary with a captured slow-response fixture.'}))})).toHaveLength(1);
  expect(prepareCalls()).toBe(2);
});
it('rechecks latest source acceptance and cycle creation after workspace preparation',async()=>{
  const f=await fixture(),p=await enableMuse(f),source=await acceptedSource(f.goal);
  await acceptSource(source);
  const plan={...p,tasks:p.tasks.map(t=>({...t,sourceTaskId:source.id}))};
  vi.mocked(accountBridge.request).mockImplementationOnce(async()=>{
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,false,'[]','latest-rejection',now()+interval '1 second')",[randomUUID(),source.id,source.run]);return{};
  });
  expect(await applyBacklog(f.goal,f.run,plan)).toEqual([]);
  await pool.query("DELETE FROM acceptances WHERE reviewer='latest-rejection'");
  vi.mocked(accountBridge.request).mockImplementationOnce(async()=>{
    await pool.query('INSERT INTO dependencies VALUES($1,$2)',[source.id,f.consumer]);return{};
  });
  expect(await applyBacklog(f.goal,f.run,plan)).toEqual([]);
});
it('preserves the full candidate of a legacy accepted Muse repair without demanding scratch implementation.patch',async()=>{
  const f=await fixture(),p=await enableMuse(f),source=await acceptedSource(f.goal,'legacy-muse-repair','repair');
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{kind}','\"harness\"') WHERE id=$1",[source.id]);
  await acceptSource(source);
  const [id]=await applyBacklog(f.goal,f.run,{...p,tasks:p.tasks.map(t=>({...t,sourceTaskId:source.id}))});
  const t=(await pool.query('SELECT description,execution FROM tasks WHERE id=$1',[id])).rows[0];
  expect(t.execution.sourceWorkspace).toBe('test/legacy-muse-repair');
  expect(t.description).not.toContain('SOURCE_REPAIR_DELIVERY_V1');
});
it('serializes concurrent Muse reserve admissions without changing live actors',async()=>{
  const f=await fixture(),p=await enableMuse(f,1);
  const proposals=Array.from({length:3},(_,i)=>({...p,tasks:p.tasks.map(t=>({...t,coverageKey:'parallel-muse-'+i}))}));
  const admitted=await Promise.all(proposals.map(x=>applyBacklog(f.goal,f.run,x)));
  expect(admitted.flat()).toHaveLength(2);
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE goal_id=$1 AND execution->>'workforceLane'='muse'",[f.goal])).rows[0].n).toBe(2);
});
it('persists useful backlog exactly once and makes it a dependency of pending acceptance', async () => {
  const f = await fixture();
  const before=(await pool.query('SELECT description FROM tasks WHERE id=$1',[f.consumer])).rows[0].description;
  const first = await applyBacklog(f.goal, f.run, f.proposal);
  expect(first).toHaveLength(1);
  expect(await applyBacklog(f.goal, f.run, f.proposal)).toEqual([]);
  expect(
    (
      await pool.query('SELECT * FROM dependencies WHERE task_id=$1 AND depends_on=$2', [
        f.consumer,
        first[0],
      ])
    ).rowCount,
  ).toBe(1);
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [first[0]])).rows[0];
  expect(task.execution.kind).toBe('opencode');
  expect(task.state).toBe('ready');
  expect(task.execution.writePaths).toEqual(['qa-protocol/', 'evidence/']);
  const consumer=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.consumer])).rows[0];
  expect(consumer.description).toBe(before);
  expect(backlogDeliveryContext(consumer).references).toEqual([expect.objectContaining({taskId:first[0],plannerRunId:f.run,coverageKey:'negative-path'})]);
});
it('assigns a scoped GLM repair to an implementation consumer and keeps final QA readonly', async () => {
  const f = await fixture();
  const proposal = {
    ...f.proposal,
    tasks: f.proposal.tasks.map((t) => ({ ...t, workKind: 'repair' })),
  };
  expect(await applyBacklog(f.goal, f.run, proposal)).toEqual([]);
  await pool.query("UPDATE tasks SET stage='integrate' WHERE id=$1", [f.consumer]);
  const ids = await applyBacklog(f.goal, f.run, proposal);
  expect(ids).toHaveLength(1);
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [ids[0]])).rows[0];
  expect(task.execution.writePaths).toEqual(['qa-protocol/', 'evidence/']);
  expect(task.description).toContain('data/repair-candidate');
  expect(task.criteria.some((c: any) => c.name === 'implementation.patch')).toBe(true);
  expect(
    backlogDeliveryContext((await pool.query('SELECT * FROM tasks WHERE id=$1', [f.consumer])).rows[0]).content,
  ).toContain('implementation.patch');
});

it('derives the base from the consumer and accepts only an authenticated accepted-source alias', async () => {
  const f = await fixture(),
    sourceDelivery = await acceptedSource(f.goal),
    source = sourceDelivery.id;
  await acceptSource(sourceDelivery);
  const alias = {
    ...f.proposal,
    tasks: f.proposal.tasks.map((p) => ({
      ...p,
      sourceTaskId: source,
      baseWorkspace: 'test/accepted-diagnostic',
    })),
  };
  const ids = await applyBacklog(f.goal, f.run, alias);
  const task = (await pool.query('SELECT execution FROM tasks WHERE id=$1', [ids[0]])).rows[0];
  expect(task.execution).toMatchObject({
    baseWorkspace: 'test/source',
    sourceWorkspace: 'test/accepted-diagnostic',
  });
  const derived = {
    ...f.proposal,
    tasks: f.proposal.tasks.map((p) => ({
      ...p,
      coverageKey: 'derived-scope',
      baseWorkspace: undefined,
    })),
  };
  expect(await applyBacklog(f.goal, f.run, derived)).toHaveLength(1);
  await pool.query("UPDATE tasks SET state='cancelled' WHERE id=$1", [source]);
  await expect(
    applyBacklog(f.goal, f.run, {
      ...alias,
      tasks: alias.tasks.map((p) => ({ ...p, coverageKey: 'cancelled-source' })),
    }),
  ).rejects.toThrow('BACKLOG_SCOPE');
});

it('delivers actual diagnostic artifacts and accepted peer repairs with immutable provenance', async () => {
  const f = await fixture(),
    source = await acceptedSource(f.goal),
    peer = await acceptedSource(f.goal, 'peer-repair', 'repair');
  const contract = JSON.stringify({
    vendor: 'genuine captured module',
    defect: 'missing export',
    exitCode: 1,
  });
  await writeArtifact(
    source.run,
    source.fence,
    'vendor-contract.json',
    contract,
    'application/json',
  );
  await writeArtifact(
    source.run,
    source.fence,
    'protocol-tests.log',
    'Actual negative test output',
  );
  await acceptSource(source);
  await writeArtifact(peer.run, peer.fence, 'implementation.patch', 'Actual accepted peer patch');
  await acceptSource(peer);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.consumer, peer.id]);
  const ids = await applyBacklog(f.goal, f.run, {
    ...f.proposal,
    tasks: f.proposal.tasks.map((p) => ({ ...p, sourceTaskId: source.id })),
  });
  expect(ids).toHaveLength(1);
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [ids[0]])).rows[0];
  expect(task.execution.inputs.map((a: any) => a.name).sort()).toEqual([
    'implementation.patch',
    'protocol-tests.log',
    'vendor-contract.json',
  ]);
  expect(
    Buffer.from(
      task.execution.inputs.find((a: any) => a.name === 'vendor-contract.json').base64,
      'base64',
    ).toString(),
  ).toBe(contract);
  expect(task.execution.backlogContext).toMatchObject({
    version: 1,
    sourceTaskId: source.id,
    sourceAttempt: 1,
    consumerTaskId: f.consumer,
    omitted: [],
  });
  expect(task.execution.backlogContext.provenance.map((a: any) => a.role)).toEqual([
    'accepted_source',
    'accepted_source',
    'accepted_repair',
  ]);
  expect(task.execution.backlogContext.provenance.every((a: any) => a.digest.length === 64)).toBe(
    true,
  );
  expect(task.description).toContain('/home/agent/inputs/<artifactId>/<name>');
  const receipt = (
    await pool.query(
      "SELECT payload FROM events WHERE kind='BACKLOG_TASK_CREATED' AND payload->>'taskId'=$1",
      [ids[0]],
    )
  ).rows[0].payload;
  expect(receipt.contextArtifacts).toEqual(task.execution.backlogContext.provenance);
  expect(JSON.stringify(receipt)).not.toContain('base64');
});

it('delivers an accepted OpenCode implementation and requires its materialization before new diagnostics', async () => {
  const f = await fixture(),
    source = await acceptedSource(f.goal, 'accepted-repair', 'repair');
  const patch =
    'diff --git a/server.mjs b/server.mjs\n--- a/server.mjs\n+++ b/server.mjs\n@@ -1 +1 @@\n-memory\n+durable\n';
  await writeArtifact(source.run, source.fence, 'implementation.patch', patch);
  await acceptSource(source);
  const ids = await applyBacklog(f.goal, f.run, {
    ...f.proposal,
    tasks: f.proposal.tasks.map((p) => ({ ...p, sourceTaskId: source.id })),
  });
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [ids[0]])).rows[0];
  const input = task.execution.inputs.find((a: any) => a.name === 'implementation.patch');
  expect(Buffer.from(input.base64, 'base64').toString()).toBe(patch);
  expect(task.description).toContain('SOURCE_REPAIR_DELIVERY_V1');
  expect(task.description).toContain('data/accepted-source');
  expect(task.description).toContain('CANDIDATE_ROOT');
  expect(task.description).toContain('se já aplicada, comprove pelos hashes');
  expect(task.execution.backlogContext.sourceTaskId).toBe(source.id);
});

it('resolves the exact consumer workspace alias within its registered scope', async () => {
  const f = await fixture(),
    source = await acceptedSource(f.goal);
  await acceptSource(source);
  const ids = await applyBacklog(f.goal, f.run, {
    ...f.proposal,
    tasks: f.proposal.tasks.map((p) => ({
      ...p,
      sourceTaskId: source.id,
      baseWorkspace: 'test/current',
    })),
  });
  expect(ids).toHaveLength(1);
  expect(
    (await pool.query('SELECT execution FROM tasks WHERE id=$1', [ids[0]])).rows[0].execution,
  ).toMatchObject({ baseWorkspace: 'test/source', sourceWorkspace: 'test/accepted-diagnostic' });
  await expect(
    applyBacklog(f.goal, f.run, {
      ...f.proposal,
      tasks: f.proposal.tasks.map((p) => ({
        ...p,
        coverageKey: 'external-alias',
        baseWorkspace: 'other/private',
      })),
    }),
  ).rejects.toThrow('BACKLOG_SCOPE');
});

it('fences a consumer whose workspace or source permissions changed while the copy was prepared', async () => {
  const f = await fixture();
  vi.spyOn(accountBridge, 'request').mockImplementation(async () => {
    await pool.query(
      'UPDATE tasks SET execution=execution||\'{"workspace":"other/private"}\'::jsonb WHERE id=$1',
      [f.consumer],
    );
    return {} as any;
  });
  expect(await applyBacklog(f.goal, f.run, f.proposal)).toEqual([]);
  expect(
    (await pool.query('SELECT count(*) n FROM workforce_backlog_items WHERE goal_id=$1', [f.goal]))
      .rows[0].n,
  ).toBe('0');
});

it('does not authorize source transport from a stale or unaccepted attempt', async () => {
  const f = await fixture(),
    source = await acceptedSource(f.goal);
  await acceptSource(source);
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [source.id]);
  expect(
    await applyBacklog(f.goal, f.run, {
      ...f.proposal,
      tasks: f.proposal.tasks.map((p) => ({ ...p, sourceTaskId: source.id })),
    }),
  ).toEqual([]);
  expect(
    (await pool.query('SELECT count(*) n FROM workforce_backlog_items WHERE goal_id=$1', [f.goal]))
      .rows[0].n,
  ).toBe('0');
});

it('keeps descriptive coverage keys bounded within artifact-name limits', async () => {
  const f = await fixture(),
    coverageKey = 'wild-bandito-server-restart-durable-identity-settlement-repair-v1';
  expect(coverageKey.length).toBeGreaterThan(60);
  const ids = await applyBacklog(f.goal, f.run, {
    ...f.proposal,
    tasks: f.proposal.tasks.map((p) => ({ ...p, coverageKey })),
  });
  expect(ids).toHaveLength(1);
  const task = (await pool.query('SELECT execution FROM tasks WHERE id=$1', [ids[0]])).rows[0];
  expect(task.execution.testFile).toBe('qa-protocol/' + coverageKey + '.test.mjs');
  expect((coverageKey + '.test.mjs').length).toBeLessThanOrEqual(101);
  await expect(
    applyBacklog(f.goal, f.run, {
      ...f.proposal,
      tasks: f.proposal.tasks.map((p) => ({ ...p, coverageKey: 'a'.repeat(81) })),
    }),
  ).rejects.toThrow();
});

it('updates a rejected plan receipt after compatible revalidation without duplicating work', async () => {
  const f = await fixture();
  await pool.query(
    "INSERT INTO workforce_backlog_plans(run_id,goal_id,state,reason) VALUES($1,$2,'rejected','Old path alias rejected')",
    [f.run, f.goal],
  );
  const ids = await applyBacklog(f.goal, f.run, f.proposal);
  expect(ids).toHaveLength(1);
  expect(
    (
      await pool.query(
        'SELECT state,task_ids,validation_version FROM workforce_backlog_plans WHERE run_id=$1',
        [f.run],
      )
    ).rows[0],
  ).toEqual({ state: 'applied', task_ids: ids, validation_version: 2 });
  expect(await applyBacklog(f.goal, f.run, f.proposal)).toEqual([]);
  expect(
    (await pool.query('SELECT task_ids FROM workforce_backlog_plans WHERE run_id=$1', [f.run]))
      .rows[0].task_ids,
  ).toEqual(ids);
});
it('does not add prerequisites to an active consumer or escape the authorized project', async () => {
  const f = await fixture();
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [f.consumer]);
  expect(await applyBacklog(f.goal, f.run, f.proposal)).toEqual([]);
  f.proposal.tasks[0].baseWorkspace = 'other/private';
  await expect(applyBacklog(f.goal, f.run, f.proposal)).rejects.toThrow('BACKLOG_SCOPE');
});
it('publishes a rejection receipt for every proposal instead of claiming an empty plan was applied', async () => {
  const f = await fixture();
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [f.consumer]);
  expect(await applyBacklog(f.goal, f.run, f.proposal)).toEqual([]);
  const plan = (
    await pool.query('SELECT state,receipts FROM workforce_backlog_plans WHERE run_id=$1', [f.run])
  ).rows[0];
  expect(plan.state).toBe('rejected');
  expect(plan.receipts).toEqual([
    {
      index: 0,
      coverageKey: 'negative-path',
      lane: 'glm',
      consumerTaskId: f.consumer,
      status: 'rejected',
      code: 'CONSUMER_LIVE',
    },
  ]);
  expect(
    (await pool.query('SELECT * FROM dependencies WHERE task_id=$1', [f.consumer])).rowCount,
  ).toBe(0);
  expect(accountBridge.request).not.toHaveBeenCalled();
});
it('reports partial creation and distinguishes a deliberate empty plan from ignored proposals', async () => {
  const f = await fixture();
  const rejected = {
    ...f.proposal.tasks[0],
    coverageKey: 'missing-consumer',
    deliverToTaskId: randomUUID(),
  };
  expect(
    await applyBacklog(f.goal, f.run, { ...f.proposal, tasks: [rejected, f.proposal.tasks[0]] }),
  ).toHaveLength(1);
  const plan = (
    await pool.query('SELECT state,receipts FROM workforce_backlog_plans WHERE run_id=$1', [f.run])
  ).rows[0];
  expect(plan.state).toBe('partial');
  expect(plan.receipts.map((r: any) => [r.index, r.status, r.code])).toEqual([
    [0, 'rejected', 'CONSUMER_NOT_FOUND'],
    [1, 'applied', 'TASK_CREATED'],
  ]);
  await pool.query('DELETE FROM workforce_backlog_plans WHERE run_id=$1', [f.run]);
  await applyBacklog(f.goal, f.run, { reason: 'No independent useful work remains', tasks: [] });
  expect(
    (
      await pool.query('SELECT state,receipts FROM workforce_backlog_plans WHERE run_id=$1', [
        f.run,
      ])
    ).rows[0],
  ).toEqual({ state: 'no_work', receipts: [] });
});
it('serializes planner deliveries and bounds the queue separately from the execution target', async () => {
  const f = await fixture();
  await pool.query('UPDATE settings SET value=value||\'{"glmTarget":1}\'::jsonb WHERE id=$1', [
    'workforce:' + f.goal,
  ]);
  const results = await Promise.all([
    applyBacklog(f.goal, f.run, f.proposal),
    applyBacklog(f.goal, f.run, {
      ...f.proposal,
      tasks: [{ ...f.proposal.tasks[0], coverageKey: 'second-path' }],
    }),
  ]);
  expect(results.flat()).toHaveLength(2);
  expect(
    (await pool.query('SELECT * FROM workforce_backlog_items WHERE goal_id=$1', [f.goal])).rows,
  ).toHaveLength(2);
  expect(
    await applyBacklog(f.goal, f.run, {
      ...f.proposal,
      tasks: [{ ...f.proposal.tasks[0], coverageKey: 'third-path' }],
    }),
  ).toEqual([]);
});
it('persists the next task while a GLM execution already occupies its entire session target', async () => {
  const f = await fixture();
  await pool.query('UPDATE settings SET value=value||\'{"glmTarget":1}\'::jsonb WHERE id=$1', [
    'workforce:' + f.goal,
  ]);
  const busy = await acceptedSource(f.goal, 'active-existing');
  const ids = await applyBacklog(f.goal, f.run, f.proposal);
  expect(ids).toHaveLength(1);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [busy.run])).rows[0].state).toBe(
    'running',
  );
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [ids[0]])).rows[0].state).toBe(
    'ready',
  );
  expect((await pool.query('SELECT * FROM outbox')).rowCount).toBe(0);
});
it('automatically re-evaluates failed dependency waits after recovery, without resuming paused goals', async () => {
  const f = await fixture();
  const parent = (
    await pool.query("SELECT id FROM tasks WHERE goal_id=$1 AND stage='coordinate'", [f.goal])
  ).rows[0].id;
  await pool.query('INSERT INTO dependencies VALUES($1,$2)', [f.consumer, parent]);
  await pool.query("UPDATE tasks SET state='blocked_failed' WHERE id=$1", [f.consumer]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1", [parent]);
  expect(await reconcileDependencyWaits()).toBe(0);
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [parent]);
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1", [f.goal]);
  expect(await reconcileDependencyWaits()).toBe(0);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.goal]);
  expect(await reconcileDependencyWaits()).toBe(1);
  expect(
    (await pool.query('SELECT state FROM tasks WHERE id=$1', [f.consumer])).rows[0].state,
  ).toBe('blocked_dependencies');
});
