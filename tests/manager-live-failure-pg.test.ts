import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installSupervision,detectSupervisionCases,wakeSupervisors} from '../server/supervision.js';
import {installManagement,managementSnapshot,recordManagementReviews} from '../server/management.js';
import {readLiveFailureReview} from '../server/manager-live-failure.js';
import {readManagementMateriality} from '../server/management-materiality.js';
import {schedule,startRun,writeArtifact,completeRun,validateTask} from '../server/service.js';
beforeAll(async()=>{await migrate();await installBacklogPlanner();await installSupervision();await installManagement();});
beforeEach(async()=>{config.bridgeHost='test-only';config.remoteExecution=true;
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
 await pool.query("UPDATE settings SET value='{\"draining\":false}' WHERE id='maintenance'");});
afterAll(async()=>{config.bridgeHost='';config.remoteExecution=false;await pool.end();});
async function liveFixture(){
 const f=await fixture();const task=randomUUID(),run=randomUUID(),fence=randomUUID(),context=randomUUID();
 await pool.query('INSERT INTO settings VALUES($1,$2)', ['management:'+f.goal,JSON.stringify({enabled:true,intervalSeconds:120,enabledAt:new Date(0).toISOString()})]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count)
   VALUES($1,$2,'live-canary','Live worker','Complete the unchanged bounded worker contract.','qa.backend','running','work','muse','muse-spark-1.3',$3,$4,1)`,
   [task,f.goal,JSON.stringify(f.criteria),JSON.stringify({kind:'harness',profile:'code',workspace:'supervision-test/live',baseWorkspace:'supervision-test/game'})]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,started_at) VALUES($1,$2,1,$3,'harness',1,'running',now())",[run,task,fence]);
 await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','{}')",[context,run]);
 await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,context]);
 const identity={goalId:f.goal,taskId:task,runId:run,runFence:fence,contextVersion:context};
 await event(pool,'RUNTIME_GUIDANCE_AVAILABLE',task,identity);
 const payload={...identity,callId:'call_1',toolName:'bash',category:'missing_resource',semanticCauseDigest:'a'.repeat(64)};
 const failure=(await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_TOOL_FAILURE',$1,$2) RETURNING id::text",[task,JSON.stringify(payload)])).rows[0].id;
 return {...f,liveTask:task,liveRun:run,liveFence:fence,liveContext:context,failure,payload};
}
async function review(f:any,management:any){
 await wakeSupervisors();const ids=await schedule();expect(ids).toHaveLength(1);
 const run=await startRun(ids[0],'live-failure-review-fixture');
 await writeArtifact(run.id,run.fence,'coordination.json',JSON.stringify({management}));
 await completeRun(run.id,run.fence,'Actual accepted bounded manager fixture',{});await validateTask(f.coordinator);
 await recordManagementReviews();return run.id;
}
const report=(actions:any[]=[],extra:any={})=>({summary:'Review the bounded current tool failure without treating a negative test as fatal.',
 capacityReason:'Preserve the live actor and all existing delivery criteria.',actions,...extra});
it('reads an exactly bound current event, exposes factual hint, and does not mutate task contract',async()=>{
 const f=await liveFixture();const before=(await pool.query('SELECT criteria,execution,required_memories FROM tasks WHERE id=$1',[f.liveTask])).rows[0];
 const value=await readLiveFailureReview(pool,f.goal);expect(value.focus).toMatchObject({kind:'live_failure',taskId:f.liveTask,runId:f.liveRun,eventId:f.failure,nonfatal:true});
 expect((await pool.query('SELECT criteria,execution,required_memories FROM tasks WHERE id=$1',[f.liveTask])).rows[0]).toEqual(before);
 const snapshot=await managementSnapshot(pool,f.goal,(await pool.query('SELECT value FROM settings WHERE id=$1',['workforce:'+f.goal])).rows[0].value,value.focus);
 expect(snapshot.liveFailureReview?.evidenceRef).toBe('event:'+f.failure);
});
it.each(['foreign_goal','foreign_fence','foreign_context','completed','cancelled','unavailable'])('refuses %s event before wake authority',async(kind)=>{
 const f=await liveFixture();
 if(kind.startsWith('foreign_')){const key=kind==='foreign_goal'?'goalId':kind==='foreign_fence'?'runFence':'contextVersion';
   await pool.query('UPDATE events SET payload=$2 WHERE id=$1',[f.failure,JSON.stringify({...f.payload,[key]:randomUUID()})]);}
 if(kind==='completed'){await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1",[f.liveRun]);await pool.query("UPDATE tasks SET state='submitted' WHERE id=$1",[f.liveTask]);}
 if(kind==='cancelled')await event(pool,'CANCEL_REQUESTED',f.liveTask,{runId:f.liveRun});
 if(kind==='unavailable')await pool.query("DELETE FROM events WHERE entity_id=$1 AND kind='RUNTIME_GUIDANCE_AVAILABLE'",[f.liveTask]);
 expect((await readLiveFailureReview(pool,f.goal)).focus).toBeNull();
});
it('current failure wins one manager focus over old integration, keeps its open case, and repeated cause is materiality-stable',async()=>{
 const f=await liveFixture();await detectSupervisionCases();
 const caseId=(await pool.query("SELECT id FROM supervision_cases WHERE task_id=$1 AND state='open'",[f.task])).rows[0].id;
 const materiality=await readManagementMateriality(pool,f.goal);
 await event(pool,'HARNESS_TOOL_FAILURE',f.liveTask,{...f.payload,callId:'call_2'});
 expect(await readManagementMateriality(pool,f.goal)).toBe(materiality);
 await wakeSupervisors();
 const manager=(await pool.query('SELECT state,description FROM tasks WHERE id=$1',[f.coordinator])).rows[0];
 expect(manager.state).toBe('ready');expect(manager.description).toContain('"kind":"live_failure"');expect(manager.description).toContain(f.liveTask);
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[caseId])).rows[0].state).toBe('open');
 await wakeSupervisors();expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rows[0].n).toBe(1);
});
it('accepted exactly bound guide owns the cause through queued/uncertain/consumed/effectverified; rejection reopens demand',async()=>{
 const f=await liveFixture();const focus=(await readLiveFailureReview(pool,f.goal)).focus!;
 await review(f,report([{kind:'guide',taskId:f.liveTask,expectedAttempt:1,runId:f.liveRun,runFence:f.liveFence,contextVersion:f.liveContext,
   instruction:'Inspect the actual failed local tool result and continue the unchanged task after the smallest necessary repair.',
   reason:'The concrete current event requires a bounded advisory review.',facts:[{evidenceRef:focus.evidenceRef,statement:focus.statement}]}]));
 const action=(await pool.query("SELECT id,value FROM settings WHERE id LIKE 'runtime-guidance:%' AND value->>'runId'=$1",[f.liveRun])).rows[0];
 expect(action).toBeTruthy();
 for(const state of ['proposed','queued_next_step','uncertain','consumed','effectverified']){
   await pool.query("UPDATE settings SET value=jsonb_set(value,'{state}',$2::jsonb) WHERE id=$1",[action.id,JSON.stringify(state)]);
   expect((await readLiveFailureReview(pool,f.goal)).focus).toBeNull();
 }
 await pool.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"rejected\"') WHERE id=$1",[action.id]);
 expect((await readLiveFailureReview(pool,f.goal)).focus?.failureKey).toBe(focus.failureKey);
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.liveTask])).rows[0].state).toBe('running');
});
it.each(['declined','context_requested'])('accepted %s review does not irreversibly suppress the cause',async(kind)=>{
 const f=await liveFixture();const focus=(await readLiveFailureReview(pool,f.goal)).focus!;
 await review(f,kind==='declined'?report([{kind:'guide',taskId:f.liveTask}]):report([],{contextRequests:[{kind:'task_metadata',taskId:f.liveTask,expectedAttempt:1,part:'outcome',offset:0}]}));
 expect((await readLiveFailureReview(pool,f.goal)).focus?.failureKey).toBe(focus.failureKey);
});
it('admitted due review keeps pending live identity after accepted context request despite wake debounce',async()=>{
 const f=await liveFixture();await detectSupervisionCases();
 const first=(await readLiveFailureReview(pool,f.goal)).focus!;
 await review(f,report([],{contextRequests:[{kind:'task_metadata',taskId:f.liveTask,expectedAttempt:1,part:'outcome',offset:0}]}));
 const state=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 expect(state.liveFailureRequestedKeys).toContain(first.failureKey);
 const wakes=(await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rows[0].n;
 await wakeSupervisors(Date.parse(state.nextReviewAt)+1);
 const next=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 expect(next.focus).toMatchObject({kind:'live_failure',failureKey:first.failureKey,eventId:first.eventId,taskId:f.liveTask,runId:f.liveRun,runFence:f.liveFence,contextVersion:f.liveContext,expectedAttempt:1});
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rows[0].n).toBe(wakes+1);
 await wakeSupervisors(Date.parse(next.requestedAt)+2);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rows[0].n).toBe(wakes+1);
});
it('exact accepted no-action releases affinity to the older stopped case without accepting live worker',async()=>{
 const f=await liveFixture();await detectSupervisionCases();
 const first=(await readLiveFailureReview(pool,f.goal)).focus!;
 await review(f,report([],{liveFailureDisposition:{failureKey:first.failureKey,eventId:first.eventId,decision:'continue_observing',reason:'The controlled negative is intentional; retain the evidence and continue observing the original live actor without treating it as accepted.'}}));
 const state=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 expect((await readLiveFailureReview(pool,f.goal)).focus).toBeNull();
 await wakeSupervisors(Date.parse(state.nextReviewAt)+1);
 const next=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 expect(next.focus.taskId).toBe(f.task);expect(next.focus.kind).not.toBe('live_failure');
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.liveTask])).rows[0].state).toBe('running');
});
it('unseen second cause wins once, then context continuation keeps second affinity without wake spam',async()=>{
 const f=await liveFixture();const first=(await readLiveFailureReview(pool,f.goal)).focus!;
 const request=report([],{contextRequests:[{kind:'task_metadata',taskId:f.liveTask,expectedAttempt:1,part:'outcome',offset:0}]});
 await review(f,request);
 const secondId=(await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_TOOL_FAILURE',$1,$2) RETURNING id::text",[f.liveTask,JSON.stringify({...f.payload,callId:'second_cause',semanticCauseDigest:'b'.repeat(64)})])).rows[0].id;
 await wakeSupervisors();let state=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 expect(state.focus.eventId).toBe(secondId);expect(state.focus.failureKey).not.toBe(first.failureKey);expect(state.liveFailureRequestedKeys).toContain(state.focus.failureKey);
 const secondKey=state.focus.failureKey;const wakes=(await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rows[0].n;
 await wakeSupervisors();expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rows[0].n).toBe(wakes);
 const ids=await schedule();expect(ids).toHaveLength(1);const run=await startRun(ids[0],'two-cause-manager');await writeArtifact(run.id,run.fence,'coordination.json',JSON.stringify({management:request}));await completeRun(run.id,run.fence,'Actual accepted context request for second current cause',{});await validateTask(f.coordinator);await recordManagementReviews();
 state=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 await wakeSupervisors(Date.parse(state.nextReviewAt)+1);state=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 expect(state.focus.failureKey).toBe(secondKey);expect(state.focus.eventId).toBe(secondId);
});
it('accepted explicitly bounded no-action disposes only its exact cause without accepting the worker',async()=>{
 const f=await liveFixture();const focus=(await readLiveFailureReview(pool,f.goal)).focus!;
 await review(f,report([],{liveFailureDisposition:{failureKey:focus.failureKey,eventId:focus.eventId,decision:'continue_observing',
   reason:'This is an intentional controlled negative test; retain the evidence and let the same live worker continue its original contract.'}}));
 expect((await readLiveFailureReview(pool,f.goal)).focus).toBeNull();
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.liveTask])).rows[0].state).toBe('running');
});
it('target closure during snapshot preparation rejects the stale wake instead of steering a replacement',async()=>{
 const f=await liveFixture();await wakeSupervisors(Date.now(),async(db,goal,policy,focus)=>{
   const snapshot=await managementSnapshot(db,goal,policy,focus);
   await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1",[f.liveRun]);
   await pool.query("UPDATE tasks SET state='submitted' WHERE id=$1",[f.liveTask]);return snapshot;
 });
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('accepted');
});
it('a valid no-action for the second unseen cause does not dispose the older unresolved cause',async()=>{
 const f=await liveFixture();const first=(await readLiveFailureReview(pool,f.goal)).focus!;
 await event(pool,'HARNESS_TOOL_FAILURE',f.liveTask,{...f.payload,callId:'call_2',category:'tool_error',semanticCauseDigest:'b'.repeat(64)});
 const pending=await readLiveFailureReview(pool,f.goal);const second=pending.pendingFocuses.find(p=>p.failureKey!==first.failureKey)!;
 await pool.query('INSERT INTO settings VALUES($1,$2)', ['management-state:'+f.goal,JSON.stringify({liveFailureRequestedKeys:[first.failureKey],wakeMateriality:await readManagementMateriality(pool,f.goal)})]);
 await review(f,report([],{liveFailureDisposition:{failureKey:second.failureKey,eventId:second.eventId,decision:'continue_observing',
   reason:'The second tool error is the intended negative probe; leave that worker running and keep the first unresolved cause for investigation.'}}));
 const after=await readLiveFailureReview(pool,f.goal);expect(after.pendingKeys).toEqual([first.failureKey]);
});
it('an older passed manager acceptance cannot suppress a cause after its latest acceptance is false',async()=>{
 const f=await liveFixture();const focus=(await readLiveFailureReview(pool,f.goal)).focus!;
 const managerRun=await review(f,report([],{liveFailureDisposition:{failureKey:focus.failureKey,eventId:focus.eventId,decision:'continue_observing',
   reason:'This observed controlled negative test is intentional; preserve the source and continue without changing the acceptance contract.'}}));
 expect((await readLiveFailureReview(pool,f.goal)).focus).toBeNull();
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,false,'[]','latest-negative-fixture',now()+interval '1 second')",[randomUUID(),f.coordinator,managerRun]);
 expect((await readLiveFailureReview(pool,f.goal)).focus?.failureKey).toBe(focus.failureKey);
});

async function fixture(taskDescription='Authorized task') {
  const goal = randomUUID(),
    task = randomUUID(),
    coordinator = randomUUID(),
    source = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Supervise','Complete real work','supervision-test',$2)",
    [goal, goal],
  );
  const policy = {
    enabled: true,
    glmAccountId: 'glm',
    glmTarget: 12,
    museTarget: 6,
    workerAccounts: ['muse'],
    coordinatorAccountId: 'sol',
    coordinatorModel: 'gpt-6.1-sol',
  };
  await pool.query('INSERT INTO settings VALUES($1,$2),($3,\'{"enabled":true}\')', [
    'workforce:' + goal,
    JSON.stringify(policy),
    'supervision:' + goal,
  ]);
  for (const [id, provider, model] of [
    ['sol', 'codex', 'gpt-6.1-sol'],
    ['muse', 'muse', 'muse-spark-1.3'],
    ['glm', 'openai', 'glm-5.3-flash'],
  ])
    await pool.query(
      'INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id) VALUES($1,$1,$2,$3,$4,true,true,12,$1)',
      [id, provider, model, JSON.stringify([model])],
    );
  const criteria = [
    {
      id: 'test',
      label: 'Actual verification',
      type: 'json_field',
      name: 'verification.json',
      field: 'passed',
      equals: true,
    },
  ];
  for (const [id, key, state, stage] of [
    [task, 'fix', 'failed', 'integrate'],
    [source, 'source', 'accepted', 'work'],
    [coordinator, 'coordinate', 'accepted', 'coordinate'],
  ])
    await pool.query(
      "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count) VALUES($1,$2,$3,$3,$11,'qa.backend',$4,$5,$6,$7,$8,$9,$10)",
      [
        id,
        goal,
        key,
        state,
        stage,
        id === coordinator ? 'sol' : 'muse',
        id === coordinator ? 'gpt-6.1-sol' : 'muse-spark-1.3',
        JSON.stringify(
          id === coordinator
            ? [{ id: 'coord', label: 'Plan', type: 'artifact', name: 'coordination.json' }]
            : criteria,
        ),
        id === coordinator
          ? null
          : JSON.stringify({
              kind: 'harness',
              profile: 'code',
              workspace: 'supervision-test/' + key,
              baseWorkspace: 'supervision-test/game',
            }),
        id === task ? 3 : 0,
        id === task ? taskDescription : 'Authorized task',
      ],
    );
  let run = '',
    fence = '';
  for (let attempt = 1; attempt <= 3; attempt++) {
    run = randomUUID();
    fence = randomUUID();
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,summary,finished_at) VALUES($1,$2,$3,$4,'harness',1,'failed','MODEL_TIMEOUT','Known failure needing another approach',now())",
      [run, task, attempt, fence],
    );
  }
  const proof = () => ({
    runId: run,
    workspace: 'supervision-test/fix',
    checkedAt: new Date().toISOString(),
    process: {
      unit: 'agentos-run-' + run,
      stopped: true,
      mainPid: 0,
      populated: false,
      activeState: 'inactive',
    },
  });
  return { goal, task, coordinator, source, run, fence, criteria, proof };
}
