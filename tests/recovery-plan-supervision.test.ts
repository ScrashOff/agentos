import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installSupervision,detectSupervisionCases,recordSupervisionDecisions,applySupervisionDecisions} from '../server/supervision.js';
import {recoverStoppedTasks} from '../server/recovery.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
beforeAll(async()=>{await migrate();await installBacklogPlanner();await installSupervision();await installManagement();});
beforeEach(async()=>{config.bridgeHost='test-only';config.remoteExecution=true;
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{config.bridgeHost='';config.remoteExecution=false;await pool.end();});
async function fixture(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),coordinator=randomUUID();
  const execution={kind:'harness',profile:'code',workspace:'plan-recovery/integration',baseWorkspace:'plan-recovery/base'};
  const criteria=[{id:'ready',type:'json_field',label:'Integration ready',name:'integration.json',field:'ready',equals:true}];
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Integration','Keep every criterion','plan-recovery',$2)",[goal,goal]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode) VALUES($1,'{}','test','execute')",[goal]);
  await pool.query('INSERT INTO settings VALUES($1,$2),($3,$4)',[
    'supervision:'+goal,JSON.stringify({enabled:true}),'workforce:'+goal,JSON.stringify({enabled:true,glmAccountId:'glm',glmTarget:12,museTarget:6,workerAccounts:['muse'],coordinatorAccountId:'sol',coordinatorModel:'gpt-6.1-sol'})]);
  for(const [id,provider,model] of [['sol','codex','gpt-6.1-sol'],['muse','muse','muse-spark-1.3'],['glm','openai','glm-5.3-flash']])
    await pool.query('INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id) VALUES($1,$1,$2,$3,$4,true,true,12,$1)',[id,provider,model,JSON.stringify([model])]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count)
    VALUES($1,$2,'integrate','Integration','Preserve the original integration contract.','qa.backend','submitted','integrate','muse','muse-spark-1.3',$3,$4,1)`,[task,goal,JSON.stringify(criteria),JSON.stringify(execution)]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria)
    VALUES($1,$2,'coordinate','Sol manager','Review the bounded failed contract.','qa.backend','accepted','coordinate','sol','gpt-6.1-sol',$3)`,[coordinator,goal,JSON.stringify([{id:'coord',label:'Plan',type:'artifact',name:'coordination.json'}])]);
  const snapshot={schema:1,task:{id:task,goal_id:goal,execution},revisionId:null};
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,summary,finished_at,instruction_snapshot)
    VALUES($1,$2,1,$3,'harness',1,'submitted','Integration cannot compose required evidence yet.',now(),$4)`,[run,task,randomUUID(),JSON.stringify(snapshot)]);
  await pool.query('INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,\'validator\')',
    [randomUUID(),task,run,JSON.stringify([{id:'ready',label:'Integration ready',type:'json_field',passed:false}])]);
  const proof=()=>({runId:run,workspace:execution.workspace,checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
  return {goal,task,run,coordinator,execution,criteria,proof};
}
it('first structural recovery opens one current case and accepted Sol revise preserves the original delivery contract',async()=>{
  const f=await fixture();await recoverStoppedTasks();
  expect((await pool.query('SELECT status FROM recoveries WHERE source_run_id=$1',[f.run])).rows[0].status).toBe('blocked');
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('recovery_blocked');
  expect(await detectSupervisionCases(async()=>f.proof())).toHaveLength(1);
  expect(await detectSupervisionCases(async()=>f.proof())).toEqual([]);
  const c=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0];
  expect(c.source_run_id).toBe(f.run);expect(c.evidence[0].kind).toBe('PLAN_RECOVERY_BLOCKED');
  const decisionRun=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[f.coordinator]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[decisionRun,f.coordinator,fence]);
  await writeArtifact(decisionRun,fence,'coordination.json',JSON.stringify({supervision:{actions:[{caseId:c.id,kind:'revise',
    diagnosis:'Composition has one unresolved binding; inspect its actual patch before broad tests.',
    instruction:'Reproduce the exact missing composition binding, preserve existing modules, resolve that bounded conflict and rerun original integration criteria.'}]}}));
  await completeRun(decisionRun,fence,'Accepted scoped Sol instruction',{});await validateTask(f.coordinator);
  await recordSupervisionDecisions();await applySupervisionDecisions(async()=>f.proof());
  const t=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  expect(t.criteria).toEqual(f.criteria);expect(t.execution.workspace).toBe(f.execution.workspace);
  expect(t.state).not.toBe('accepted');expect(t.attempt_count).toBe(1);
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[c.id])).rows[0].state).toBe('applied');
});
it.each(['live','foreign_source','positive','human_review','manual','disabled','user_owner','cancelled','stale_snapshot','bad_stop','auth','quota'])
  ('does not escalate %s as structural recovery',async(kind)=>{
    const f=await fixture();await recoverStoppedTasks();
    if(kind==='live'){
      await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'running')",[randomUUID(),f.task,randomUUID()]);
      await pool.query("UPDATE tasks SET state='running',attempt_count=2 WHERE id=$1",[f.task]);
    }
    if(kind==='foreign_source')await pool.query('UPDATE recoveries SET target_task_id=$2 WHERE source_run_id=$1',[f.run,f.coordinator]);
    if(kind==='positive')await pool.query('UPDATE acceptances SET passed=true WHERE run_id=$1',[f.run]);
    if(kind==='human_review')await pool.query(`UPDATE acceptances SET checks='[{"type":"review","passed":false}]' WHERE run_id=$1`,[f.run]);
    if(kind==='manual')await pool.query(`UPDATE tasks SET recovery='{"mode":"manual"}' WHERE id=$1`,[f.task]);
    if(kind==='disabled')await pool.query(`UPDATE settings SET value='{"enabled":false}' WHERE id=$1`,['supervision:'+f.goal]);
    if(kind==='user_owner')await pool.query(`UPDATE tasks SET execution=execution||'{"continuation":{"owner":"user"}}' WHERE id=$1`,[f.task]);
    if(kind==='cancelled')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
    if(kind==='auth'||kind==='quota')await pool.query('UPDATE runs SET error_code=$2 WHERE id=$1',[f.run,kind==='auth'?'AUTH_REQUIRED':'QUOTA_EXCEEDED']);
    if(kind==='stale_snapshot')await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','"plan-recovery/foreign"') WHERE id=$1`,[f.task]);
    const proof=()=>kind==='bad_stop'?{...f.proof(),process:{...f.proof().process,stopped:false}}:f.proof();
    expect(await detectSupervisionCases(async()=>proof())).toEqual([]);
    expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(0);
  });
it('revalidates current recovery while stop inspection is in flight',async()=>{
  const f=await fixture();await recoverStoppedTasks();
  expect(await detectSupervisionCases(async()=>{await pool.query("UPDATE recoveries SET status='scheduled' WHERE source_run_id=$1",[f.run]);return f.proof();})).toEqual([]);
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(0);
});
it('one structurally blocked attempt after an applied decision bypasses ordinary two-attempt cooldown',async()=>{
  const f=await fixture();
  const previous=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,0,$3,'harness',1,'failed',now())",[previous,f.task,randomUUID()]);
  await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,applied_attempt) VALUES($1,$2,$3,$4,'applied','Previous explicit decision','[]',0)",[randomUUID(),f.goal,f.task,previous]);
  await recoverStoppedTasks();expect(await detectSupervisionCases(async()=>f.proof())).toHaveLength(1);
  expect((await pool.query("SELECT source_run_id FROM supervision_cases WHERE task_id=$1 AND state='open'",[f.task])).rows[0].source_run_id).toBe(f.run);
});
it('first confirmed functional process failure is structurally escalated without replaying it',async()=>{
  const f=await fixture();
  await pool.query("UPDATE runs SET state='failed',error_code='TOOL_FAILED' WHERE id=$1",[f.run]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[f.task]);
  await event(pool,'HARNESS_STOPPED',f.task,{runId:f.run,confirmed:true});
  await recoverStoppedTasks();expect(await detectSupervisionCases(async()=>f.proof())).toHaveLength(1);
  expect((await pool.query('SELECT attempt_count,state FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual({attempt_count:1,state:'recovery_blocked'});
});
it('concurrent detectors acquire only one case for the same blocked source run',async()=>{
  const f=await fixture();await recoverStoppedTasks();
  const results=await Promise.all(Array.from({length:3},()=>detectSupervisionCases(async()=>f.proof())));
  expect(results.flat()).toHaveLength(1);
  expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1 AND source_run_id=$2',[f.task,f.run])).rowCount).toBe(1);
});
