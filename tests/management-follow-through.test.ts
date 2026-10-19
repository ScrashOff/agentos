import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {installSupervision} from '../server/supervision.js';
import {installDeliveryTransitions} from '../server/delivery-transitions.js';
import {readManagementFollowThrough} from '../server/management-follow-through.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installSupervision();await installDeliveryTransitions();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');});
afterAll(async()=>{await pool.end();});
async function fixture(state='ready',expectedArtifact='verification.json') {
  const goal=randomUUID(),task=randomUUID(),manager=randomUUID(),review=randomUUID(),transition=randomUUID();
  await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('management-proof','Test','test','[]','test',1,'Test fixture')");
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Delivery','Keep strict acceptance','management-proof',$2)",[goal,goal]);
  for(const [id,stage,taskState] of [[task,'integrate',state],[manager,'coordinate','accepted']])
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
      VALUES($1,$2,$3,'Delivery','Actual criteria','management-proof',$4,$5,$6)`,[id,goal,id,taskState,stage,
      JSON.stringify([{id:'verified',label:'Actual proof',type:'json_field',name:'verification.json',field:'passed',equals:true}])]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'test',1,'submitted')",[review,manager,randomUUID()]);
  await pool.query(`INSERT INTO delivery_transitions(id,goal_id,review_run_id,task_id,expected_attempt,state,proposal,receipt)
    VALUES($1,$2,$3,$4,0,'tracking',$5,'{"code":"TRANSITION_BOUND"}')`,[transition,goal,review,task,JSON.stringify({expectedArtifact})]);
  const inspect=()=>readManagementFollowThrough(pool,goal,review);
  const addCase=async(caseState='open')=>{
    const id=randomUUID();await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,state,reason,evidence)
      VALUES($1,$2,$3,$4,'Exact failed criterion','[]')`,[id,goal,task,caseState]);return id;};
  return {goal,task,manager,review,transition,inspect,addCase};
}

it.each(['TRANSITION_STALE','TRANSITION_BINDING_REQUIRED','TRANSITION_AUTHORITY'])('does not count %s as an executable response',async(code)=>{
  const f=await fixture();await pool.query("UPDATE delivery_transitions SET task_id=NULL,state='rejected',receipt=$2 WHERE id=$1",
    [f.transition,JSON.stringify({code})]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_TRANSITION_REJECTED',satisfied:false,owner:'supervisor'});
});
it('binds a real pending criterion and refuses arbitrary output text without touching the task',async()=>{
  const f=await fixture(),before=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_DISPATCH_OWNED',satisfied:true,taskId:f.task,criterionIds:['verified']});
  await pool.query('UPDATE delivery_transitions SET proposal=$2 WHERE id=$1',[f.transition,JSON.stringify({expectedArtifact:'some-narrative.json'})]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_EXPECTED_ARTIFACT_UNBOUND',satisfied:false});
  expect((await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0]).toEqual(before);
});
it('requires a decision for the exact open case even if a nextTransition was successfully bound',async()=>{
  const f=await fixture('recovery_blocked'),caseId=await f.addCase();
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_CASE_ACTION_REQUIRED',satisfied:false,caseId,taskId:f.task});
  await pool.query("UPDATE supervision_cases SET state='decided',decision_run_id=$2 WHERE id=$1",[caseId,f.review]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_SUPERVISION_OWNED',satisfied:true,caseId});
});
it.each(['waiting_quota','waiting_resource','retry_scheduled'])('keeps %s as a qualified wait without functional failure or acceptance',async(state)=>{
  const f=await fixture(state);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_WAIT_CAPACITY',satisfied:true,owner:'scheduler',state});
  expect((await pool.query('SELECT 1 FROM supervision_cases')).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM acceptances')).rowCount).toBe(0);
});
it('keeps a real actor unchanged and does not reinterpret its heartbeat age as stagnation',async()=>{
  const f=await fixture('running'),run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET attempt_count=1 WHERE id=$1",[f.task]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,heartbeat_at)
    VALUES($1,$2,1,$3,'test',1,'running',now()-interval '4 hours')`,[run,f.task,fence]);
  const before=(await pool.query('SELECT * FROM runs WHERE id=$1',[run])).rows[0];
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_WAIT_ACTOR',satisfied:true,owner:'consumer'});
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[run])).rows[0]).toEqual(before);
});
it('directs an accepted supporting artifact to its consumer instead of declaring further progress',async()=>{
  const f=await fixture('accepted');
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_CONSUMER_REQUIRED',satisfied:false,taskId:f.task});
});
it('reports exact failed dependencies and refuses a stale dependency wait with no blocker',async()=>{
  const f=await fixture('blocked_dependencies');
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_DEPENDENCY_ACTION_REQUIRED',satisfied:false,blockedTaskIds:[]});
  const parent=randomUUID();await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria)
    VALUES($1,$2,$3,'Prerequisite','Exact failed source','management-proof','recovery_blocked','[]')`,[parent,f.goal,parent]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.task,parent]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_DEPENDENCY_ACTION_REQUIRED',satisfied:false,blockedTaskIds:[parent]});
  await pool.query("UPDATE tasks SET state='ready' WHERE id=$1",[parent]);
  expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_WAIT_DEPENDENCIES',satisfied:true,blockedTaskIds:[parent]});
});
