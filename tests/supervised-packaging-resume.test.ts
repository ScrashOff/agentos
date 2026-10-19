import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {hash} from '../server/memory.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
import {ContinuationSupervisor} from '../server/continuation.js';
import {adoptRecoveredArtifacts} from '../server/artifact-recovery.js';
import {installSupervision,applySupervisionDecisions,resumeSupervisedWork} from '../server/supervision.js';
import {appliedSupervisionOwnsContinuation} from '../server/recovery-supervision.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installTaskExecutionMetadata();await installSupervision();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query(`INSERT INTO settings(id,value) VALUES('artifact-recovery','{"enabled":true}')
    ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value`);
});
afterAll(async()=>{await pool.query("DELETE FROM settings WHERE id='artifact-recovery'");await pool.end();});

async function fixture(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID(),account=randomUUID(),fence=randomUUID();
  const project='supervised-packaging-'+goal,workspace=project+'/candidate';
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES($1,'Test only','codex','gpt-6-luna','["gpt-6-luna"]',true,5,true,'test-only')`,[account]);
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1,'Packaging recovery','Consume existing files',$2,$3,'active')`,[goal,project,goal]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,execution,criteria,
    attempt_count,account_id,model,allowed_accounts)
    VALUES($1,$2,'consumer','Consumer','Preserve the original tests and artifacts','qa.backend','failed','integrate',$3,
      '[{"id":"passed","type":"json_field","name":"verification.json","field":"passed","equals":true}]',
      77,$4,'gpt-6-luna',$5)`,[task,goal,JSON.stringify({kind:'harness',profile:'code',workspace,baseWorkspace:project+'/base'}),
      account,JSON.stringify([account])]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at,account_id,model)
    VALUES($1,$2,77,$3,'harness',1,'failed','RESULT_FORMAT',now(),'previous-route','previous-model')`,[run,task,fence]);
  const action={caseId,kind:'revise',diagnosis:'The existing files need a different packaging step.',
    instruction:'Inspect the preserved files and package the real evidence using the original criteria. Do not mark failing tests as successful.'};
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision)
    VALUES($1,$2,$3,$4,'decided','Consume accepted repair with original criteria','[]',$5)`,
    [caseId,goal,task,run,JSON.stringify({actions:[action]})]);
  const coordinator=randomUUID(),decisionRun=randomUUID(),decisionFence=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,'supervisor','Supervisor','Decide the next approach','qa.backend','running','coordinate',
      '[{"id":"decision","type":"artifact","name":"coordination.json"}]',1)`,[coordinator,goal]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'harness',1,'running')`,[decisionRun,coordinator,decisionFence]);
  await writeArtifact(decisionRun,decisionFence,'coordination.json',JSON.stringify({supervision:{actions:[action]}}),'application/json');
  await completeRun(decisionRun,decisionFence,'Authorized fixture decision',{});await validateTask(coordinator);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1 AND state='failed'",[goal]);
  await pool.query('UPDATE supervision_cases SET decision_run_id=$2 WHERE id=$1',[caseId,decisionRun]);
  const proof=()=>({runId:run,workspace,checkedAt:new Date().toISOString(),candidate:{patch:'preserved patch'},
    process:{unit:'agentos-run-'+run,stopped:true,activeState:'inactive',mainPid:0,populated:false}});
  const delivery=()=>({...proof(),candidate:{candidateDigest:'fixture'},summary:'Preserved delivery',
    artifacts:[{name:'verification.json',mime:'application/json',base64:Buffer.from('{"passed":true}').toString('base64'),
      digest:hash('{"passed":true}')},{name:'candidate.json',mime:'application/json',
      base64:Buffer.from('{"candidateDigest":"fixture"}').toString('base64'),digest:hash('{"candidateDigest":"fixture"}')},
      {name:'changes.patch',mime:'text/plain',base64:Buffer.from('preserved patch').toString('base64'),digest:hash('preserved patch')}]});
  return {goal,task,run,caseId,account,fence,project,workspace,proof,delivery};
}
async function apply(f:Awaited<ReturnType<typeof fixture>>){
  await applySupervisionDecisions(async()=>f.proof());
  expect((await pool.query('SELECT state,applied_attempt FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0])
    .toEqual({state:'applied',applied_attempt:77});
  // This is the real BEFORE UPDATE trigger responsible for the live dead end.
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('recovering_artifacts');
}
async function current(f:Awaited<ReturnType<typeof fixture>>){
  return {task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0],
    runs:(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows,
    cases:(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY id',[f.task])).rows};
}

it('resumes a real applied revision projected to recovering_artifacts without another collection or supervisor generation',async()=>{
  const f=await fixture();await apply(f);const before=await current(f);
  expect(await appliedSupervisionOwnsContinuation(pool,f.task,f.run)).toBe(true);
  const collect=vi.fn(async()=>f.delivery()),inspect=vi.fn(async()=>f.proof());
  await new ContinuationSupervisor(inspect,collect).scan();
  expect(collect).not.toHaveBeenCalled();expect(inspect).not.toHaveBeenCalled();
  expect(await resumeSupervisedWork(inspect)).toEqual([f.task]);
  expect(inspect).toHaveBeenCalledTimes(1);
  const after=await current(f);
  expect(after.task.state).toBe('retry_scheduled');expect(after.task.attempt_count).toBe(77);
  expect(after.task.criteria).toEqual(before.task.criteria);expect(after.task.description).toBe(before.task.description);
  expect(after.task.instruction_revision_id).toBe(before.task.instruction_revision_id);
  expect(after.task.execution.supervision).toEqual(before.task.execution.supervision);
  expect(after.runs).toHaveLength(1);expect(after.runs[0].id).toBe(f.run);
  expect(after.runs[0].state).toBe('failed');expect(after.runs[0].error_code).toBe('RESULT_FORMAT');
  expect(after.runs[0].checkpoint.sourceRunId).toBe(f.run);
  expect(after.cases).toEqual(before.cases);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE entity_id=$1 AND kind='SUPERVISION_RESUMED'",[f.task])).rows[0].n).toBe(1);
  expect(await resumeSupervisedWork(inspect)).toEqual([]);
});

it('does not let a collector that started before the decision adopt stale output after the decision commits',async()=>{
  const f=await fixture(),inspect=vi.fn(async()=>f.proof());
  const collect=vi.fn(async()=>{await apply(f);return f.delivery();});
  await new ContinuationSupervisor(inspect,collect).scan();
  expect(collect).toHaveBeenCalledTimes(1);expect(inspect).not.toHaveBeenCalled();
  const after=await current(f);
  expect(after.task.state).toBe('recovering_artifacts');expect(after.runs).toHaveLength(1);
  expect(after.cases[0].state).toBe('applied');
  expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='ARTIFACTS_RECOVERED'",[f.task])).rowCount).toBe(0);
  expect(await resumeSupervisedWork(async()=>f.proof())).toEqual([f.task]);
});

it('keeps valid collection available when no applied decision owns the failure',async()=>{
  const f=await fixture();await pool.query('DELETE FROM supervision_cases WHERE id=$1',[f.caseId]);
  const result=await adoptRecoveredArtifacts(f.run,f.fence,f.delivery());
  expect(result.runId).not.toBe(f.run);
  expect((await pool.query('SELECT attempt,state FROM runs WHERE id=$1',[result.runId])).rows[0]).toEqual({attempt:78,state:'submitted'});
  expect((await current(f)).runs).toHaveLength(2);
});

it('retains ownership on a real quota refusal and does not oscillate back to collection',async()=>{
  const f=await fixture();await apply(f);
  await pool.query("UPDATE accounts SET quota='{\"ordinaryUsageAllowed\":false}' WHERE id=$1",[f.account]);
  const before=await current(f),inspect=vi.fn(async()=>f.proof()),collect=vi.fn(async()=>f.delivery());
  expect(await resumeSupervisedWork(inspect)).toEqual([]);expect(inspect).toHaveBeenCalledTimes(1);
  expect((await pool.query("SELECT payload->>'code' code FROM events WHERE kind='SUPERVISION_RESUME_DEFERRED' AND entity_id=$1",[f.task])).rows)
    .toEqual([{code:'ACCOUNT_UNAVAILABLE'}]);
  await new ContinuationSupervisor(inspect,collect).scan();
  expect(collect).not.toHaveBeenCalled();expect(await current(f)).toEqual(before);
});

it.each(['wrong-run','wrong-attempt','no-directive','pending','paused','cancelled','manual','integrity','native','non-packaging','qa'])
('does not claim the packaging projection without eligible exact authority: %s',async mode=>{
  const f=await fixture();await apply(f);
  if(mode==='wrong-run')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision,sourceRunId}',to_jsonb($2::text)) WHERE id=$1",[f.task,randomUUID()]);
  if(mode==='wrong-attempt')await pool.query('UPDATE supervision_cases SET applied_attempt=76 WHERE id=$1',[f.caseId]);
  if(mode==='no-directive')await pool.query("UPDATE tasks SET execution=execution-'supervision' WHERE id=$1",[f.task]);
  if(mode==='pending')await pool.query("UPDATE supervision_cases SET state='decided' WHERE id=$1",[f.caseId]);
  if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='cancelled')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
  if(mode==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);
  if(mode==='integrity')await pool.query("UPDATE runs SET error_code='ARTIFACT_CORRUPT' WHERE id=$1",[f.run]);
  if(mode==='native')await pool.query("UPDATE runs SET runtime='opencode' WHERE id=$1",[f.run]);
  if(mode==='non-packaging')await pool.query("UPDATE runs SET error_code='MODEL_TIMEOUT' WHERE id=$1",[f.run]);
  if(mode==='qa')await pool.query("UPDATE tasks SET stage='verify',execution=execution||'{\"readOnlySource\":true}'::jsonb WHERE id=$1",[f.task]);
  const before=await current(f),inspect=vi.fn(async()=>f.proof());
  expect(await appliedSupervisionOwnsContinuation(pool,f.task,f.run)).toBe(false);
  expect(await resumeSupervisedWork(inspect)).toEqual([]);expect(inspect).not.toHaveBeenCalled();
  expect(await current(f)).toEqual(before);
});

it.each(['stale-stop','running-stop','wrong-workspace','cancellation-during-inspection','dependency-during-inspection'])
('rechecks physical and semantic guards before scheduling: %s',async mode=>{
  const f=await fixture();await apply(f);const before=await current(f);
  const inspect=vi.fn(async()=>{
    const proof=f.proof();
    if(mode==='stale-stop')proof.checkedAt='2020-01-01T00:00:00Z';
    if(mode==='running-stop')proof.process.stopped=false;
    if(mode==='wrong-workspace')proof.workspace='another/workspace';
    if(mode==='cancellation-during-inspection')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
    if(mode==='dependency-during-inspection'){
      const parent=randomUUID();
      await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria)
        VALUES($1,$2,'new-parent','Required source','Still incomplete','qa.backend','ready','[]')`,[parent,f.goal]);
      await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.task,parent]);
    }
    return proof;
  });
  expect(await resumeSupervisedWork(inspect)).toEqual([]);expect(inspect).toHaveBeenCalledTimes(1);
  expect(await current(f)).toEqual(before);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SUPERVISION_RESUMED' AND entity_id=$1",[f.task])).rowCount).toBe(0);
});
