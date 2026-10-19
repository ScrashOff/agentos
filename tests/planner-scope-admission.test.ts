import {beforeAll,beforeEach,afterAll,afterEach,it,expect} from 'vitest';
import { installTaskExecutionMetadata } from '../server/task-execution-metadata.js';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installPlanningDemand} from '../server/planning-demand.js';
import {schedule} from '../server/service.js';

let goal:string;
const original={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installTaskExecutionMetadata();await installPlanningDemand();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id IN('maintenance','storage:agentos-workspaces')");
  await pool.query("UPDATE settings SET value='{\"maxRuns\":20,\"browserSlots\":4}' WHERE id='capacity'");
  config.bridgeHost='test-only';config.remoteExecution=true;
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('glm-scope','Fixture GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,16,true,'glm-scope')`);
  goal=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Scope admission','Preserve live ownership','scope-admission',$2)",[goal,goal]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+goal,JSON.stringify({enabled:true,glmTarget:12,
    glmAccountId:'glm-scope',plannerMaxActive:5,workerAccounts:['glm-scope']})]);
});
afterEach(()=>{Object.assign(config,original);});
afterAll(()=>pool.end());
async function candidate(scope:string,priority:number,kind='planning'){
  const id=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,account_id,model,execution,priority,resources)
    VALUES($1,$2,$3,'Exact scoped task','Preserve source and acceptance','research.requirements','ready','work',$4,'glm-scope','glm-5.3-flash',$5,$6,'[]')`,
    [id,goal,'scope-'+id,JSON.stringify([{id:'proof',type:'artifact',name:'proof.json',label:'Required proof'}]),
      JSON.stringify({kind:'opencode',workforceLane:'glm',workKind:kind,workspace:'scope-admission/run-'+id,baseWorkspace:'scope-admission/'+scope}),priority]);
  return id;
}
async function actor(id:string,state:string){
  const run=randomUUID();
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id)
    VALUES($1,$2,1,$3,'opencode',1,$4,'glm-scope')`,[run,id,randomUUID(),state]);
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[id]);return run;
}
const task=(id:string)=>pool.query('SELECT * FROM tasks WHERE id=$1',[id]).then(r=>r.rows[0]);

it.each(['queued','running','cancelling','reconciling'])('isolates a %s same-scope actor while admitting another scope in the same native batch',async(state)=>{
  const owner=await candidate('occupied',120),ownerRun=await actor(owner,state);
  const busy=await candidate('occupied',100),available=await candidate('available',50);
  const originalTask=await task(busy),originalActor=(await pool.query('SELECT * FROM runs WHERE id=$1',[ownerRun])).rows[0];
  const admitted=await schedule('agentos-opencode');expect(admitted).toHaveLength(1);
  expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[admitted[0]])).rows[0].task_id).toBe(available);
  const blocked=await task(busy);expect(blocked.state).toBe('waiting_resource');expect(blocked.reason).toContain('planejador ativo neste escopo');
  for(const field of ['attempt_count','execution','criteria','instruction_revision_id','description','next_attempt_at'])expect(blocked[field]).toEqual(originalTask[field]);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[ownerRun])).rows[0]).toEqual(originalActor);
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[busy])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM outbox o JOIN runs r ON r.id=o.run_id WHERE r.task_id=$1',[busy])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM task_instruction_revisions WHERE task_id=$1',[busy])).rows[0].n).toBe(0);
  expect(await schedule('agentos-opencode')).toEqual([]);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE entity_id=$1 AND kind='TASK_WAITING' AND payload->>'code'='SCOPED_PLANNER_SCOPE_BUSY'",[busy])).rows[0].n).toBe(1);
  // Releasing the real actor, rather than a timeout, permits the waiting task.
  await pool.query("UPDATE runs SET state='failed',finished_at=now() WHERE id=$1",[ownerRun]);
  await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[owner]);
  const resumed=await schedule('agentos-opencode');expect(resumed).toHaveLength(1);
  expect((await pool.query('SELECT task_id,attempt FROM runs WHERE id=$1',[resumed[0]])).rows[0]).toEqual({task_id:busy,attempt:1});
});

it('keeps the objective planning ceiling and admits ordinary work after the guarded planner',async()=>{
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{plannerMaxActive}','1') WHERE id=$1",['workforce:'+goal]);
  await actor(await candidate('occupied',120),'running');
  const planner=await candidate('different-scope',100),worker=await candidate('worker',50,'diagnostic');
  const admitted=await schedule('agentos-opencode');expect(admitted).toHaveLength(1);
  expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[admitted[0]])).rows[0].task_id).toBe(worker);
  expect((await task(planner)).state).toBe('waiting_resource');expect((await task(planner)).attempt_count).toBe(0);
});
