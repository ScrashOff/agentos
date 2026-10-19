import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx,lock,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {hash} from '../server/memory.js';
import {ContinuationSupervisor,resumeFailedHarness,setContinuation} from '../server/continuation.js';
import {installSupervision,resumeSupervisedWork} from '../server/supervision.js';
import {appliedSupervisionOwnsContinuation,requestRecoveryReview} from '../server/recovery-supervision.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installSupervision();
});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());

async function fixture(applied=true){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID(),account=randomUUID();
  const project='continuation-owner-'+goal,workspace=project+'/candidate';
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,quota,enabled,allow_unknown)
    VALUES($1,'Original GLM route','openai','glm-5.3-flash','["glm-5.3-flash"]','{"ordinaryUsageAllowed":false}',true,true)`,[account]);
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1::uuid,'Preserve auxiliary result','Consume the later accepted repair',$2,$1::text,'active')`,[goal,project]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  const execution={kind:'opencode',workspace,baseWorkspace:project+'/base',managed:true,workKind:'diagnostic',inputs:[]};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,execution,criteria,attempt_count,account_id,model)
    VALUES($1,$2,'rtp','RTP readiness','Original accepted auxiliary instructions','qa.backend','submitted','work',$3,
      '[{"id":"executed","type":"json_field","artifact":"result.json","path":"passed","equals":true}]',3,$4,'glm-5.3-flash')`,
    [task,goal,JSON.stringify(execution),account]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id,model,instruction_snapshot)
    SELECT $1,id,3,$3,'opencode',1,'submitted',$4,'glm-5.3-flash',
      jsonb_build_object('schema',1,'revisionId',instruction_revision_id,'task',to_jsonb(tasks)) FROM tasks WHERE id=$2`,
    [run,task,randomUUID(),account]);
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,true,'[{"id":"executed","type":"json_field","passed":true}]','real-contract-fixture')`,[randomUUID(),task,run]);
  await pool.query(`UPDATE tasks SET description='Consume the authorized later repair',execution=execution||$2::jsonb WHERE id=$1`,
    [task,JSON.stringify({repairFeedback:{sourceRunId:randomUUID()},inputs:[{id:randomUUID(),name:'repair.diff',digest:hash('real patch fixture'),base64:Buffer.from('real patch fixture').toString('base64')}],
      ...(applied?{supervision:{caseId,sourceRunId:run}}:{})})]);
  if(applied)await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,applied_attempt)
    VALUES($1,$2,$3,$4,'applied','Consume the pinned later repair','[]',3)`,[caseId,goal,task,run]);
  return {goal,task,run,caseId,account,project,workspace};
}
function stopped(f:Awaited<ReturnType<typeof fixture>>){
  return {runId:f.run,runtime:'opencode',workspace:f.workspace,checkedAt:new Date().toISOString(),
    state:{state:'submitted'},candidate:{patch:''},
    process:{unit:'agentos-run-'+f.run,activeState:'inactive',stopped:true,mainPid:0,populated:false}};
}
async function history(f:Awaited<ReturnType<typeof fixture>>){
  return {runs:(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows,
    acceptances:(await pool.query('SELECT * FROM acceptances WHERE task_id=$1 ORDER BY id',[f.task])).rows};
}

it('retains the applied exact decision during a real quota refusal and keeps generic scans from reopening its positive auxiliary check',async()=>{
  const f=await fixture(),before=await history(f);
  const revised=(await pool.query('SELECT description,execution,criteria,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0];
  const inspect=vi.fn(async()=>stopped(f));
  expect(await resumeSupervisedWork(inspect)).toEqual([]);
  expect(inspect).toHaveBeenCalledTimes(1);
  expect((await pool.query("SELECT payload->>'code' code FROM events WHERE entity_id=$1 AND kind='SUPERVISION_RESUME_DEFERRED'",[f.task])).rows)
    .toEqual([{code:'ACCOUNT_UNAVAILABLE'}]);
  const genericInspect=vi.fn(async()=>stopped(f));
  await new ContinuationSupervisor(genericInspect).scan();
  await new ContinuationSupervisor(genericInspect).scan();
  expect(genericInspect).not.toHaveBeenCalled();
  expect((await pool.query('SELECT state,applied_attempt,source_run_id FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0])
    .toEqual({state:'applied',applied_attempt:3,source_run_id:f.run});
  expect((await pool.query('SELECT description,execution,criteria,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual(revised);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='SUPERVISION_REQUESTED'",[f.task])).rowCount).toBe(0);
  expect(await history(f)).toEqual(before);
});

it('rechecks ownership atomically when a decision is applied during the generic stop inspection',async()=>{
  const f=await fixture(false),before=await history(f);
  const inspect=vi.fn(async()=>{
    await tx(async db=>{
      await lock(db);
      await db.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,applied_attempt)
        VALUES($1,$2,$3,$4,'applied','Authorized during inspection','[]',3)`,[f.caseId,f.goal,f.task,f.run]);
      await db.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision}',$2::jsonb) WHERE id=$1",
        [f.task,JSON.stringify({caseId:f.caseId,sourceRunId:f.run})]);
    });
    return stopped(f);
  });
  await new ContinuationSupervisor(inspect).scan();
  expect(inspect).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0].state).toBe('applied');
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_REQUESTED' AND entity_id=$1",[f.task])).rowCount).toBe(0);
  expect((await pool.query("SELECT execution->'continuation'->>'reasonCode' code FROM tasks WHERE id=$1",[f.task])).rows[0].code).toBeNull();
  expect(await history(f)).toEqual(before);
});

it.each(['attempt','source','directive','case-state','current-attempt'])('does not suppress a required review when the applied binding is stale: %s',async mode=>{
  const f=await fixture();
  if(mode==='attempt')await pool.query('UPDATE supervision_cases SET applied_attempt=2 WHERE id=$1',[f.caseId]);
  if(mode==='source')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision,sourceRunId}',to_jsonb($2::text)) WHERE id=$1",[f.task,randomUUID()]);
  if(mode==='directive')await pool.query("UPDATE tasks SET execution=execution-'supervision' WHERE id=$1",[f.task]);
  if(mode==='case-state')await pool.query("UPDATE supervision_cases SET state='decided' WHERE id=$1",[f.caseId]);
  if(mode==='current-attempt')await pool.query('UPDATE tasks SET attempt_count=4 WHERE id=$1',[f.task]);
  expect(await appliedSupervisionOwnsContinuation(pool,f.task,f.run)).toBe(false);
  if(mode!=='current-attempt'){
    const review=await requestRecoveryReview(f.task,f.run,'NO_FAILED_CHECK','A stale decision needs reconciliation');
    if(mode==='case-state')expect(review?.caseId).toBe(f.caseId);
    else {
      expect(review?.caseId).toBeTruthy();expect(review?.caseId).not.toBe(f.caseId);
      expect((await pool.query('SELECT parent_case_id FROM supervision_cases WHERE id=$1',[review!.caseId])).rows[0].parent_case_id).toBe(f.caseId);
      expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0].state).toBe('applied');
    }
  }
});

it.each(['cancel','paused','disabled','autonomy-off','manual','live','qa','integrity'])('does not claim unsafe or unsupported supervised ownership: %s',async mode=>{
  const f=await fixture();
  if(mode==='cancel')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
  if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='disabled')await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['supervision:'+f.goal]);
  if(mode==='autonomy-off')await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":false}')",['autonomy:'+f.project]);
  if(mode==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);
  if(mode==='live')await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[f.run]);
  if(mode==='qa')await pool.query("UPDATE tasks SET stage='verify',execution=execution||'{\"readOnlySource\":true}'::jsonb WHERE id=$1",[f.task]);
  if(mode==='integrity')await pool.query("UPDATE runs SET error_code='ARTIFACT_CORRUPT' WHERE id=$1",[f.run]);
  expect(await appliedSupervisionOwnsContinuation(pool,f.task,f.run)).toBe(false);
  if(['cancel','paused','disabled','autonomy-off','manual','live'].includes(mode)){
    expect(await requestRecoveryReview(f.task,f.run,'NO_FAILED_CHECK','Must not resume unsafe task')).toBeNull();
    expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0].state).toBe('applied');
  }
});

it('keeps new technical review requests available even when an exact prior decision exists',async()=>{
  const f=await fixture(),before=await history(f);
  const review=await requestRecoveryReview(f.task,f.run,'VERIFICATION_ROUTING_FAILED','A different verified failure needs another decision');
  expect(review?.caseId).toBeTruthy();expect(review?.caseId).not.toBe(f.caseId);
  expect((await pool.query('SELECT state,parent_case_id FROM supervision_cases WHERE id=$1',[review!.caseId])).rows[0])
    .toEqual({state:'open',parent_case_id:f.caseId});
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0].state).toBe('applied');
  expect((await requestRecoveryReview(f.task,f.run,'VERIFICATION_ROUTING_FAILED','Repeated observation'))?.caseId).toBe(review!.caseId);
  expect(await history(f)).toEqual(before);
});

it('does not treat an absent decision as authority to reuse a previously accepted run',async()=>{
  const f=await fixture(false),before=await history(f);
  await new ContinuationSupervisor(async()=>stopped(f)).scan();
  expect((await pool.query('SELECT state,source_run_id FROM supervision_cases WHERE task_id=$1',[f.task])).rows)
    .toEqual([{state:'open',source_run_id:f.run}]);
  expect(await history(f)).toEqual(before);
});

it('does not advertise a fulfilled technical generation as a pending review on later scans',async()=>{
  const f=await fixture(false);
  const review=await requestRecoveryReview(f.task,f.run,'VERIFICATION_ROUTING_FAILED','Resolve the routing cause once');
  expect(review?.state).toBe('open');
  await pool.query("UPDATE supervision_cases SET state='applied' WHERE id=$1",[review!.caseId]);
  const before=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[review!.caseId])).rows[0];
  expect(await requestRecoveryReview(f.task,f.run,'VERIFICATION_ROUTING_FAILED','Same cause observed again')).toBeNull();
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows).toEqual([before]);
});

async function failedHarness(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID(),fence=randomUUID();
  const project='pending-continuation-'+goal,workspace=project+'/candidate';
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('pending-repair','Fixture','codex','gpt-6-luna','["gpt-6-luna"]',true,5,true,'test-route')`);
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1::uuid,'Preserve supervisor ownership','Repair only after the current decision',$2,$1::text,'active')`,[goal,project]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,execution,criteria,attempt_count,allowed_accounts)
    VALUES($1,$2,'repair','Repair','Keep real failures and artifacts','qa.backend','failed',$3,'[]',184,'["pending-repair"]')`,
    [task,goal,JSON.stringify({kind:'harness',profile:'code',workspace,baseWorkspace:project+'/base'})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at)
    VALUES($1,$2,184,$3,'harness',1,'failed','RUNTIME_EXITED',now())`,[run,task,fence]);
  const proof=()=>({runId:run,workspace,checkedAt:new Date().toISOString(),candidate:{patch:'preserved patch'},
    process:{unit:'agentos-run-'+run,stopped:true,activeState:'inactive',mainPid:0,populated:false}});
  const pending=async(state='open',hold=true)=>{
    await tx(async db=>{
      await lock(db);
      await db.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
        VALUES($1,$2,$3,$4,$5,'Resolve the existing failure','[]')`,[caseId,goal,task,run,state]);
      // Use the real hold trigger, including its current ownership marker.
      if(hold)await db.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[task]);
    });
  };
  return {goal,task,run,caseId,fence,proof,pending};
}
async function pendingHistory(f:Awaited<ReturnType<typeof failedHarness>>){
  return {task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0],
    runs:(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows,
    cases:(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY id',[f.task])).rows,
    artifacts:(await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows,
    queued:(await pool.query("SELECT * FROM events WHERE entity_id=$1 AND kind='CONTINUATION_QUEUED' ORDER BY id",[f.task])).rows};
}

it.each(['open','decided','delegated','waiting_evidence'])('leaves pending %s supervision intact without inspection, checkpoints or a false queue receipt',async state=>{
  const f=await failedHarness();await f.pending(state);const before=await pendingHistory(f);
  const inspect=vi.fn(async()=>f.proof());
  // Directly test the setter as well as the scan; the latter may honor nextAt.
  expect(await setContinuation(f.task,f.run,'checking','Generic inspection',0)).toBe(false);
  await new ContinuationSupervisor(inspect).scan();
  expect(inspect).not.toHaveBeenCalled();
  await expect(resumeFailedHarness(f.run,f.fence,f.proof())).rejects.toThrow('SUPERVISION_PENDING');
  expect(await pendingHistory(f)).toEqual(before);
});

it('rechecks a pending case committed during stop inspection before generating a retry checkpoint',async()=>{
  const f=await failedHarness();let committed:any;
  const inspect=vi.fn(async()=>{await f.pending();committed=await pendingHistory(f);return f.proof();});
  await new ContinuationSupervisor(inspect).scan();
  expect(inspect).toHaveBeenCalledTimes(1);
  expect(await pendingHistory(f)).toEqual(committed);
  expect(committed.queued).toEqual([]);expect(committed.artifacts).toEqual([]);
  expect(committed.runs).toHaveLength(1);expect(committed.runs[0].checkpoint).toBeNull();
});

it('still collects evidence for a pending packaging decision without falling through to generic retry',async()=>{
  const f=await failedHarness();await pool.query("UPDATE runs SET error_code='RESULT_FORMAT' WHERE id=$1",[f.run]);
  // Case insertion precedes its first continuation projection. The scan's
  // collecting update must run the real trigger and still allow evidence I/O.
  await f.pending('open',false);
  const before=await pendingHistory(f);
  const inspect=vi.fn(async()=>f.proof());
  const collect=vi.fn(async()=>{throw Error('COLLECTION_FAILED');});
  await new ContinuationSupervisor(inspect,collect).scan();
  expect(collect).toHaveBeenCalledTimes(1);expect(inspect).not.toHaveBeenCalled();
  expect((await pendingHistory(f)).queued).toEqual([]);
  expect((await pendingHistory(f)).cases).toEqual(before.cases);
});

it('queues the ordinary failed attempt when no supervisor has claimed it',async()=>{
  const f=await failedHarness(),inspect=vi.fn(async()=>f.proof());
  await new ContinuationSupervisor(inspect).scan();
  expect(inspect).toHaveBeenCalledTimes(1);
  const after=await pendingHistory(f);
  expect(after.task.state).toBe('retry_scheduled');expect(after.task.attempt_count).toBe(184);
  expect(after.runs).toHaveLength(1);expect(after.runs[0].checkpoint.sourceRunId).toBe(f.run);
  expect(after.artifacts).toHaveLength(2);expect(after.queued).toHaveLength(1);expect(after.cases).toEqual([]);
});
