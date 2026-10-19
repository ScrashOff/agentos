import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from 'vitest';
import { installTaskExecutionMetadata } from '../server/task-execution-metadata.js';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { seedRegistry } from '../server/registry.js';
import {
  buildWorkforceHealth,
  persistWorkforceHealth,
  readWorkforceHealth,
  workforcePlanningWake,
  type WorkforceHealthInput,
} from '../server/workforce-health.js';
import { completeRun, schedule, startRun, validateTask } from '../server/service.js';

const now = Date.parse('2026-10-06T19:00:00Z');
const policy = {
  enabled: true,
  glmAccountId: 'glm',
  glmTarget: 12,
  glmRequestLimit: 6,
  workerAccounts: [],
  coordinatorAccountId: 'sol',
  coordinatorModel: 'gpt-6.1-sol',
};
const base = (): WorkforceHealthInput => ({
  goalState: 'active',
  policy,
  tasks: [
    {
      id: 'one',
      state: 'ready',
      execution: { kind: 'opencode' },
      account_id: 'glm',
      model: 'glm-5.3-flash',
      resources: ['remote-slot', 'workspace:one'],
    },
  ],
  account: {
    id: 'glm',
    enabled: true,
    bridge_id: 'glm',
    model: 'glm-5.3-flash',
    models: ['glm-5.3-flash'],
    max_concurrency: 12,
    allow_unknown: true,
    quota: {},
  },
  accountOccupied: 0,
  globalOccupied: 0,
  goalOccupied: 0,
  limits: { maxRuns: 20, remoteSlots: 18, browserSlots: 10 },
  leases: {},
  remoteExecution: true,
  bridgeConfigured: true,
});
it('counts transferred Harness GLM tasks in the same pool while retaining dependencies and uncertain occupancy',()=>{
  const input=base();input.policy={...input.policy,glmHarnessEnabled:true};
  input.tasks.push({id:'migrated',state:'ready',account_id:'glm',model:'glm-5.3-flash',
    execution:{kind:'harness',workforceLane:'glm'},resources:['remote-slot','workspace:migrated']},
    {id:'blocked',state:'blocked_dependencies',account_id:'glm',model:'glm-5.3-flash',dependency_blocked:true,
      execution:{kind:'harness',workforceLane:'glm'}},
    {id:'uncertain',state:'recovery_blocked',has_live_run:true,live_state:'reconciling',account_id:'glm',model:'glm-5.3-flash',
      execution:{kind:'harness',workforceLane:'glm'}});
  expect(buildWorkforceHealth(input,now).glm).toMatchObject({ready:2,blocked:1,confirming:1,occupied:1});
});
it('admits GLM by the executor of each task instead of the OpenCode maintenance flag',()=>{
  const input=base();input.policy={...input.policy,glmHarnessEnabled:true};
  input.tasks.push({id:'harness',state:'ready',account_id:'glm',model:'glm-5.3-flash',execution:{kind:'harness',workforceLane:'glm'}});
  input.admission={glm:{allowed:true,service:'agentos-opencode'},glmHarness:{allowed:false,service:'agentos-workers'},muse:{allowed:false,service:'agentos-workers'}};
  const native=buildWorkforceHealth(input,now);
  expect(native.glm).toMatchObject({ready:2,admittedReady:1,admissionBlocked:1,available:1,reserveDeficit:11});
  expect(native.glm.executors).toContainEqual({kind:'harness',service:'agentos-workers',allowed:false,ready:1,occupied:0,available:0});
  input.admission.glm.allowed=false;input.admission.glmHarness!.allowed=true;
  const harness=buildWorkforceHealth(input,now);
  expect(harness.glm).toMatchObject({admittedReady:1,admissionBlocked:1,available:1});
  expect(harness.fingerprint).not.toBe(native.fingerprint);
  input.admission.glmHarness!.allowed=false;
  expect(buildWorkforceHealth(input,now)).toMatchObject({needsPlanning:false,refillPossible:false,status:'rollout',glm:{available:0,admittedReady:0}});
});
it('keeps uncertain actors occupied while their executor is draining and never calls a queued Harness task available through OpenCode',()=>{
  const input=base();input.policy={...input.policy,glmHarnessEnabled:true};
  input.tasks=[{id:'waiting',state:'ready',account_id:'glm',execution:{kind:'harness',workforceLane:'glm'}},
    {id:'uncertain',state:'recovery_blocked',has_live_run:true,live_state:'reconciling',account_id:'glm',execution:{kind:'harness',workforceLane:'glm'}}];
  input.admission={glm:{allowed:true,service:'agentos-opencode'},glmHarness:{allowed:false,service:'agentos-workers'},muse:{allowed:false,service:'agentos-workers'}};
  expect(buildWorkforceHealth(input,now)).toMatchObject({status:'rollout',refillPossible:false,glm:{ready:1,occupied:1,admittedReady:0,admissionBlocked:1,available:0}});
});
it('separates reserve occupancy from parked history while retaining uncertain live actors',()=>{
 const input=base();
 input.tasks.push(...Array.from({length:24},(_,i)=>({id:'parked-'+i,state:'recovery_blocked',execution:{kind:'opencode'}})));
 input.tasks.push({id:'uncertain',state:'recovery_blocked',has_live_run:true,live_state:'reconciling',execution:{kind:'opencode'}});
 const h=buildWorkforceHealth(input,now);
 expect(h.glm).toMatchObject({pending:2,totalPending:26,ready:1,parked:24,confirming:1,occupied:1,pendingLimit:24});
 expect(h.bottlenecks).toContainEqual({code:'SUPERVISION_OR_EVIDENCE',count:24});
});
it('keeps blocked DAG branches visible without exhausting the reserve for independent work',()=>{
 const input=base();
 input.tasks.push(...Array.from({length:60},(_,i)=>({id:'future-'+i,state:'blocked_dependencies',dependency_blocked:true,
  execution:{kind:'opencode'}})));
 input.tasks.push({id:'uncertain',state:'recovery_blocked',has_live_run:true,live_state:'reconciling',dependency_blocked:true,
  execution:{kind:'opencode'}});
 const before=structuredClone(input),health=buildWorkforceHealth(input,now);
 expect(health.glm).toMatchObject({pending:2,totalPending:62,blocked:60,ready:1,occupied:1,available:1,pendingLimit:24});
 expect(health.bottlenecks).toContainEqual({code:'DEPENDENCIES',count:60});
 expect(input).toEqual(before);
});
function withMuse(input=base(),count=6):WorkforceHealthInput {
  input.policy={...input.policy,museTarget:6,workerAccounts:['muse']};
  input.museAccounts=[{account:{id:'muse',provider:'muse',enabled:true,bridge_id:'muse',
    model:'muse-spark-1.3',models:['muse-spark-1.3'],max_concurrency:6,allow_unknown:true,quota:{}},occupied:0}];
  input.tasks.push(...Array.from({length:count},(_,i)=>({id:'muse-'+i,state:'ready',
    model:'muse-spark-1.3',account_id:'muse',allowed_accounts:['muse'],execution:{kind:'harness',workforceLane:'muse',workKind:'diagnostic',protocolDelivery:'backlog-v1'},
    resources:['remote-slot','workspace:muse-'+i]})));
  return input;
}
it('reports independent Muse reserve and readiness without treating its target as existing work',()=>{
  const input=withMuse(base(),0);
  const health=buildWorkforceHealth(input,now);
  expect(health.muse).toMatchObject({target:6,pendingLimit:12,running:0,ready:0,available:0,
    deficit:6,reserveDeficit:6,needsPlanning:true,status:'backlog_missing'});
  input.tasks.push({id:'muse-parked',state:'recovery_blocked',model:'muse-spark-1.3',execution:{kind:'harness',workforceLane:'muse'}});
  expect(buildWorkforceHealth(input,now).muse).toMatchObject({parked:1,ready:0,status:'blocked'});
});
it('estimates twelve GLM plus six Muse admissions within shared global and remote slots',()=>{
  const input=withMuse();
  input.tasks=input.tasks.filter(t=>t.id.startsWith('muse-'));
  input.tasks.unshift(...Array.from({length:12},(_,i)=>({id:'glm-'+i,state:'ready',model:'glm-5.3-flash',account_id:'glm',
    execution:{kind:'opencode'},resources:['remote-slot','workspace:glm-'+i]})));
  const h=buildWorkforceHealth(input,now);
  expect(h.glm.available).toBe(12);
  expect(h.muse).toMatchObject({ready:6,available:6,freeSlots:6,reserveDeficit:0});
  input.globalOccupied=4;input.goalOccupied=4;input.leases={'remote-slot':4};
  const limited=buildWorkforceHealth(input,now);
  expect(limited.glm.available+limited.muse.available).toBe(14);
  expect(limited.muse.available).toBe(2);
});
it('shows native rollout blocking separately while adopted Muse remains available',()=>{
  const input=withMuse();
  input.admission={glm:{allowed:false,service:'agentos-opencode'},muse:{allowed:true,service:'agentos-workers'}};
  const h=buildWorkforceHealth(input,now);
  expect(h.glm).toMatchObject({ready:1,available:0,freeSlots:0,admission:{allowed:false,service:'agentos-opencode'}});
  expect(h.glm.reason).toContain('aguarda atualização');
  expect(h.muse.available).toBe(6);
  expect(h.refillPossible).toBe(true);
  input.admission.muse.allowed=false;
  const allBlocked=buildWorkforceHealth(input,now);
  expect(allBlocked.muse).toMatchObject({ready:6,available:0,status:'rollout'});
  expect(allBlocked.refillPossible).toBe(false);
});
it('counts uncertain Muse actors and actual account occupancy while preserving shared leases',()=>{
  const input=withMuse();
  input.tasks[1]={...input.tasks[1],state:'reconciling',has_live_run:true,live_state:'reconciling'};
  input.tasks[2]={...input.tasks[2],state:'blocked_dependencies',dependency_blocked:true};
  input.museAccounts![0].occupied=5;
  input.globalOccupied=5;input.goalOccupied=1;
  input.leases={'remote-slot':5,'workspace:muse-2':1};
  const h=buildWorkforceHealth(input,now);
  expect(h.muse).toMatchObject({confirming:1,occupied:1,blocked:1,ready:4,freeSlots:1,available:1});
  expect(input.leases).toEqual({'remote-slot':5,'workspace:muse-2':1});
});
it.each(['auth','quota','unknown','scope','model','browser'])('does not advertise Muse refill when %s forbids admission',reason=>{
  const input=withMuse();
  const account=input.museAccounts![0].account;
  if(reason==='auth')account.connection_status='reconnect_required';
  if(reason==='quota')account.quota={ordinaryUsageAllowed:false};
  if(reason==='unknown')account.allow_unknown=false;
  if(reason==='scope')input.policy.workerAccounts=[];
  if(reason==='model')account.models=[];
  if(reason==='browser'){
    input.tasks.filter(t=>t.model==='muse-spark-1.3').forEach(t=>t.resources!.push('browser'));
    input.leases={browser:10};
  }
  expect(buildWorkforceHealth(input,now).muse.available).toBe(0);
});

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await installTaskExecutionMetadata();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
});
afterEach(() => {
  config.remoteExecution = false;
  config.bridgeHost = '';
});
it('reads real Muse reserve identity apart from original DAG stages and respects maintenance admission',async()=>{
  config.remoteExecution=true;config.bridgeHost='test-bridge';
  const goal=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Muse health','Useful work','muse-health',$2)",[goal,goal]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('glm','GLM','openai','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'glm'),
    ('muse','Muse','muse','muse-spark-1.3','["muse-spark-1.3"]',true,6,true,'muse')`);
  const p={...policy,museTarget:6,workerAccounts:['muse']};
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['workforce:'+goal,JSON.stringify(p)]);
  await pool.query("INSERT INTO settings VALUES('capacity','{\"maxRuns\":20,\"browserSlots\":10}') ON CONFLICT(id) DO UPDATE SET value=excluded.value");
  await pool.query("INSERT INTO settings VALUES('remote-capacity','{\"maxRuns\":18}') ON CONFLICT(id) DO UPDATE SET value=excluded.value");
  for(let i=0;i<6;i++)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,criteria,resources,execution)
    VALUES($1,$2,$3,'Independent Muse diagnostic','Useful task','qa.backend','muse','muse-spark-1.3','ready','work','[]',$4,$5)`,
    [randomUUID(),goal,'muse-'+i,JSON.stringify(['remote-slot','workspace:muse-'+i]),JSON.stringify({kind:'harness',workforceLane:'muse'})]);
  try{
    expect((await readWorkforceHealth(pool,goal,p)).muse).toMatchObject({ready:6,available:6,pendingLimit:12});
    for(let i=0;i<12;i++)await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,criteria,resources,execution)
      VALUES($1,$2,$3,'Original DAG stage','Stage waiting for implementation','qa.backend','muse','muse-spark-1.3','blocked_dependencies','verify','[]',$4,$5)`,
      [randomUUID(),goal,'dag-'+i,JSON.stringify(['remote-slot','workspace:dag-'+i]),JSON.stringify({kind:'harness',workforceLane:'muse',readOnlySource:true})]);
    await pool.query(`UPDATE tasks SET execution=execution||$2::jsonb WHERE goal_id=$1 AND key='muse-0'`,
      [goal,JSON.stringify({workKind:'diagnostic',testFile:'data/qa-protocol/reserve.test.mjs',protocolDelivery:'backlog-v1',
        backlogContext:{sourceTaskId:randomUUID(),provenance:Array.from({length:100},()=>({digest:'f'.repeat(64),notes:'private artifact body must not be projected'}))}})]);
    const reserved=await readWorkforceHealth(pool,goal,p);
    expect(reserved.muse).toMatchObject({pending:1,totalPending:18,reserveReady:1,reserveDeficit:5,pendingLimit:12});
    await pool.query(`UPDATE tasks SET execution=execution-'protocolDelivery'-'testFile' WHERE goal_id=$1 AND key='muse-0'`,[goal]);
    expect((await readWorkforceHealth(pool,goal,p)).muse).toMatchObject({pending:1,reserveReady:1});
    await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{backlogContext}','[]') WHERE goal_id=$1 AND key='muse-0'`,[goal]);
    expect((await readWorkforceHealth(pool,goal,p)).muse).toMatchObject({pending:0,reserveReady:0});
    await pool.query("INSERT INTO settings VALUES('maintenance','{\"owner\":\"foreign-maintenance\",\"draining\":true}') ON CONFLICT(id) DO UPDATE SET value=excluded.value");
    const blocked=await readWorkforceHealth(pool,goal,p);
    expect(blocked.muse).toMatchObject({available:0,status:'rollout',admission:{allowed:false,service:'agentos-workers'}});
    expect(blocked.refillPossible).toBe(false);
  }finally{await pool.query("DELETE FROM settings WHERE id='maintenance'");}
});
afterAll(async () => {
  await pool.end();
});

it('separates twelve sessions from six requests and identifies deterministic ready work', () => {
  const input = base();
  input.tasks = Array.from({ length: 13 }, (_, i) => ({
    ...input.tasks[0],
    id: String(i),
    resources: ['remote-slot', 'workspace:' + i],
  }));
  const health = buildWorkforceHealth(input, now);
  expect(health.glm).toMatchObject({
    target: 12,
    requestLimit: 6,
    ready: 13,
    available: 12,
    deficit: 12,
  });
  expect(health).toMatchObject({
    refillPossible: true,
    needsPlanning: false,
    replenishment: 'scheduler',
  });
});

it('reports no useful backlog rather than fabricating twelve runnable sessions from parked items', () => {
  const input = base();
  input.tasks = [
    { ...input.tasks[0], state: 'running', has_live_run: true, live_state: 'running' },
    { ...input.tasks[0], id: 'wait', state: 'recovery_blocked' },
    { ...input.tasks[0], id: 'dep', state: 'blocked_dependencies', dependency_blocked: true },
    {
      ...input.tasks[0],
      id: 'delay',
      state: 'retry_scheduled',
      next_attempt_at: new Date(now + 10000),
    },
  ];
  const health = buildWorkforceHealth(input, now);
  expect(health.glm).toMatchObject({
    running: 1,
    ready: 0,
    available: 0,
    blocked: 1,
    parked: 1,
    delayed: 1,
    deficit: 11,
  });
  expect(health).toMatchObject({
    status: 'blocked',
    needsPlanning: true,
    replenishment: 'supervisor',
  });
});

it('includes quota/configuration waits when actual dependencies and account observations permit admission', () => {
  const input = base();
  input.tasks = ['waiting_quota', 'waiting_configuration', 'blocked_failed'].map((state, i) => ({
    ...input.tasks[0],
    id: String(i),
    state,
    resources: ['remote-slot', 'workspace:' + i],
  }));
  expect(buildWorkforceHealth(input, now).glm.available).toBe(3);
  input.leases = { browser: 10, 'workspace:0': 1 };
  input.tasks[1].resources!.push('browser');
  const health = buildWorkforceHealth(input, now);
  expect(health.glm.available).toBe(1);
  expect(health.bottlenecks).toContainEqual({ code: 'RESOURCE_LEASE', count: 2 });
});

it('keeps uncertain leases occupied and never treats auth or remote host exclusivity as backlog shortage', () => {
  const input = base();
  input.tasks.push({
    ...input.tasks[0],
    id: 'uncertain',
    state: 'reconciling',
    live_state: 'reconciling',
    has_live_run: true,
  });
  input.leases = { 'host:agentos-remote': 1 };
  expect(buildWorkforceHealth(input, now).glm).toMatchObject({
    confirming: 1,
    occupied: 1,
    available: 0,
  });
  expect(buildWorkforceHealth(input, now).bottlenecks).toContainEqual({
    code: 'HOST_EXCLUSIVE',
    count: 1,
  });
  input.account.connection_status = 'reconnect_required';
  expect(buildWorkforceHealth(input, now)).toMatchObject({
    status: 'configuration',
    refillPossible: false,
  });
});

it('coalesces planning on semantic changes and ignores heartbeat time and failed attempt counters', () => {
  const input = base();
  input.tasks[0].state = 'recovery_blocked';
  const first = buildWorkforceHealth(input, now);
  input.tasks[0].attempt_count = 90;
  const later = buildWorkforceHealth(input, now + 5000);
  expect(later.fingerprint).toBe(first.fingerprint);
  expect(workforcePlanningWake(first, {}, now)).toBe(true);
  expect(
    workforcePlanningWake(
      later,
      { workforceFingerprint: first.fingerprint, requestedAt: new Date(now).toISOString() },
      now + 600000,
    ),
  ).toBe(false);
  input.tasks.push({ ...input.tasks[0], id: 'new-source', state: 'accepted', attempt_count: 1 });
  const changed = buildWorkforceHealth(input, now + 5000);
  expect(
    workforcePlanningWake(
      changed,
      { workforceFingerprint: first.fingerprint, requestedAt: new Date(now).toISOString() },
      now + 5000,
    ),
  ).toBe(false);
  expect(
    workforcePlanningWake(
      changed,
      { workforceFingerprint: first.fingerprint, requestedAt: new Date(now).toISOString() },
      now + 120000,
    ),
  ).toBe(true);
  input.goalState = 'paused';
  expect(buildWorkforceHealth(input, now)).toMatchObject({
    needsPlanning: false,
    refillPossible: false,
    status: 'inactive',
  });
});

it('uses the real scheduler to admit twelve GLM runs and replace a released vacancy without invoking a model', async () => {
  config.remoteExecution = true;
  config.bridgeHost = 'test-bridge';
  const goal = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Pool fixture','Independent tasks','pool-test',$2)",
    [goal, goal],
  );
  await pool.query(
    'INSERT INTO settings(id,value) VALUES(\'capacity\',\'{"maxRuns":20,"browserSlots":10}\') ON CONFLICT(id) DO UPDATE SET value=excluded.value',
  );
  await pool.query(
    "INSERT INTO settings(id,value) VALUES('remote-capacity','{\"maxRuns\":18}') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
  );
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
    'workforce:' + goal,
    JSON.stringify(policy),
  ]);
  await pool.query(
    "INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,bridge_id,max_concurrency) VALUES('glm','GLM','openai','glm-5.3-flash','[\"glm-5.3-flash\"]',true,true,'glm',12)",
  );
  for (let i = 0; i < 13; i++) {
    const id = randomUUID(),
      workspace = 'pool-test/task-' + i;
    await pool.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,resources,criteria,execution)
      VALUES($1,$2,$3,'Independent regression','Useful independent fixture','qa.backend','glm','glm-5.3-flash','ready','work',$4,'[]',$5)`,
      [
        id,
        goal,
        'task-' + i,
        JSON.stringify(['remote-slot', 'workspace:' + workspace]),
        JSON.stringify({
          kind: 'opencode',
          workspace,
          baseWorkspace: 'pool-test/base',
          workforceLane: 'glm',
        }),
      ],
    );
  }
  const first = await schedule();
  expect(first).toHaveLength(12);
  expect(
    (await pool.query("SELECT count(*)::int count FROM opencode_jobs WHERE state='queued'")).rows[0]
      .count,
  ).toBe(12);
  const full = await readWorkforceHealth(pool, goal, policy);
  expect(full.glm).toMatchObject({
    queued: 12,
    occupied: 12,
    ready: 1,
    available: 0,
    requestLimit: 6,
  });
  expect(full.refillPossible).toBe(false);
  const source = await startRun(first[0], 'fixture-only-no-runtime');
  await completeRun(source.id, source.fence, 'Completed fixture', {});
  await validateTask(source.task_id);
  const released = await persistWorkforceHealth(pool, goal, policy);
  expect(released.glm).toMatchObject({ queued: 11, ready: 1, available: 1 });
  const replacement = await schedule();
  expect(replacement).toHaveLength(1);
  expect((await readWorkforceHealth(pool, goal, policy)).glm).toMatchObject({
    occupied: 12,
    ready: 0,
  });
  expect(
    (await pool.query("SELECT count(*)::int count FROM events WHERE kind='RUN_STARTED'")).rows[0]
      .count,
  ).toBe(1);
});
