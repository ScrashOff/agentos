import {beforeAll,beforeEach,afterEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {readScheduleCandidates,schedule} from '../server/service.js';
import {quotaFor} from '../server/accounts.js';

const initial={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
let goal:string;
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id IN('maintenance','storage:agentos-workspaces')");
  await pool.query(`UPDATE settings SET value='{"maxRuns":3,"browserSlots":1}' WHERE id='capacity'`);
  config.bridgeHost='test-only';config.remoteExecution=true;
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('remote','Fixture','qwen','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'remote')`);
  goal=await makeGoal();
});
afterEach(()=>{config.bridgeHost=initial.bridgeHost;config.remoteExecution=initial.remoteExecution;});
afterAll(()=>pool.end());
async function makeGoal(){
  const id=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Delivery','Keep original proof','delivery-priority',$1::text)",[id]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['workforce:'+id,JSON.stringify({enabled:true,
    deliveryFirstEnabled:true,glmAccountId:'remote',glmTarget:12,glmHarnessEnabled:true,
    workerAccounts:['remote'],coordinatorAccountId:'remote',coordinatorModel:'gpt-6.1-sol'})]);
  return id;
}
async function task(options:any={}){
  const id=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,
    criteria,execution,priority,resources,created_at,next_attempt_at)
    VALUES($1::uuid,$2,$1::text,'Authorized task','Unchanged contract','qa.backend',$3,$4,$5,'glm-5.3-flash',$6,$7,$8,$9,
      now()-($10::int*interval '1 minute'),$11)`,
    [id,options.goal??goal,options.state??'ready',options.stage??'work',options.account??'remote',
      JSON.stringify([{id:'proof',type:'artifact',name:'proof.json',label:'Original proof'}]),
      JSON.stringify({kind:'harness',workKind:options.workKind??'diagnostic',workforceLane:'glm',
        workspace:'delivery-priority/'+id,baseWorkspace:'delivery-priority/base',writePaths:['src/']}),
      options.priority??10,JSON.stringify(options.resources??[]),options.ageMinutes??0,options.nextAttemptAt??null]);
  return id;
}
const state=async(id:string)=>(await pool.query('SELECT state FROM tasks WHERE id=$1',[id])).rows[0].state;
const depends=async(child:string,parent:string)=>{await pool.query('INSERT INTO dependencies VALUES($1,$2)',[child,parent]);};

it('inherits delivery urgency through two dependency levels and admits the executable prerequisite first',async()=>{
  await pool.query(`UPDATE settings SET value='{"maxRuns":1,"browserSlots":1}' WHERE id='capacity'`);
  const delivery=await task({stage:'integrate',priority:70}),repair=await task({priority:20,workKind:'repair'}),
    proof=await task({priority:5}),unrelated=await task({priority:79});
  await depends(delivery,repair);await depends(repair,proof);
  const rows=await readScheduleCandidates();
  expect(rows.find(r=>r.id===proof)).toMatchObject({inherited_priority:110,effective_priority:110});
  const before=(await pool.query('SELECT * FROM dependencies ORDER BY task_id,depends_on')).rows;
  const dispatched=await schedule();expect(dispatched).toHaveLength(1);
  expect(await state(proof)).toBe('queued');expect(await state(unrelated)).toBe('waiting_resource');
  expect(await state(delivery)).toBe('blocked_dependencies');expect(await state(repair)).toBe('blocked_dependencies');
  expect((await pool.query('SELECT * FROM dependencies ORDER BY task_id,depends_on')).rows).toEqual(before);
  const event=(await pool.query("SELECT payload FROM events WHERE entity_id=$1 AND kind='RUN_QUEUED'",[proof])).rows[0];
  expect(event.payload.dispatchPriority).toMatchObject({base:5,inherited:110,effective:110});
});

it('inherits the maximum useful downstream priority and stops at accepted or closed nodes',async()=>{
  const leaf=await task({priority:5}),a=await task({stage:'verify',priority:50}),b=await task({stage:'review',priority:70});
  await depends(a,leaf);await depends(b,leaf);
  expect((await readScheduleCandidates()).find(r=>r.id===leaf).inherited_priority).toBe(110);
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[b]);
  expect((await readScheduleCandidates()).find(r=>r.id===leaf).inherited_priority).toBe(90);
  await pool.query("UPDATE tasks SET state='superseded' WHERE id=$1",[a]);
  expect((await readScheduleCandidates()).find(r=>r.id===leaf).inherited_priority).toBeNull();
});

it('keeps cross-goal edges from lending authority or priority to another objective',async()=>{
  const other=await makeGoal(),leaf=await task({goal:other,priority:5}),delivery=await task({stage:'integrate',priority:79});
  await depends(delivery,leaf);
  expect((await readScheduleCandidates()).find(r=>r.id===leaf).inherited_priority).toBeNull();
});

it('terminates even with a corrupt dependency cycle and admits no member of that cycle',async()=>{
  const delivery=await task({stage:'verify',priority:60}),leaf=await task();
  await depends(delivery,leaf);await depends(leaf,delivery);
  expect(await readScheduleCandidates()).toHaveLength(2);
  expect(await schedule()).toEqual([]);
  expect(await state(leaf)).toBe('blocked_dependencies');
});

it('lets an older ready task outrank fresh analysis without changing its stored priority',async()=>{
  const old=await task({priority:40,ageMinutes:100}),fresh=await task({priority:55});
  const rows=await readScheduleCandidates();expect(rows.map(r=>r.id)).toEqual([old,fresh]);
  expect(rows[0]).toMatchObject({priority:40,age_priority:20,effective_priority:60});
  expect((await pool.query('SELECT priority FROM tasks WHERE id=$1',[old])).rows[0].priority).toBe(40);
  await pool.query("UPDATE tasks SET reason='Observed again',updated_at=now() WHERE id=$1",[old]);
  expect((await readScheduleCandidates())[0].age_priority).toBe(20);
});

it('does not count provider cooldown or a prior active attempt as time waiting for admission',async()=>{
  const cooling=await task({ageMinutes:200,nextAttemptAt:new Date(Date.now()+60000)}),recent=await task({ageMinutes:200});
  await pool.query('UPDATE tasks SET attempt_count=1 WHERE id=$1',[recent]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,finished_at)
    VALUES($1,$2,1,'failed',$3,'harness',1,now())`,[randomUUID(),recent,randomUUID()]);
  const rows=await readScheduleCandidates();
  expect(rows.find(r=>r.id===cooling).age_priority).toBe(0);
  expect(rows.find(r=>r.id===recent).age_priority).toBe(0);
});

it('borrows every available slot when a critical prerequisite has no quota',async()=>{
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,quota_status,remaining_percent,allow_unknown,bridge_id,quota)
    VALUES('exhausted','Fixture exhausted','qwen','glm-5.3-flash','["glm-5.3-flash"]',true,12,'exhausted',0,true,'exhausted',$1)`,
    [JSON.stringify({status:'ok',ordinaryUsageAllowed:false,groups:[{label:'Qwen',windows:[{remainingPercent:0}]}]})]);
  // Bridge admission derives quota from the provider observation, replacing
  // the denormalized status columns. Unknown quota is deliberately allowed.
  expect(quotaFor((await pool.query("SELECT * FROM accounts WHERE id='exhausted'")).rows[0],'glm-5.3-flash'))
    .toMatchObject({status:'exhausted',remaining:0});
  const delivery=await task({stage:'integrate',priority:70}),prerequisite=await task({account:'exhausted',priority:5});
  // Keep this fixture's authorized route explicit as well as its account pin.
  await pool.query('UPDATE tasks SET allowed_accounts=$2 WHERE id=$1',[prerequisite,JSON.stringify(['exhausted'])]);
  await depends(delivery,prerequisite);
  const other=await Promise.all(Array.from({length:3},()=>task({priority:30})));
  expect(await schedule()).toHaveLength(3);
  expect(await state(prerequisite)).toBe('waiting_quota');expect(await state(delivery)).toBe('blocked_dependencies');
  expect(await Promise.all(other.map(state))).toEqual(['queued','queued','queued']);
});

it('preserves resource leases and borrows capacity without bypassing a blocked critical browser',async()=>{
  const delivery=await task({stage:'integrate',priority:70}),prerequisite=await task({priority:5,resources:['browser']}),other=await task({priority:30});
  await depends(delivery,prerequisite);
  const holder=await task({state:'running'}),run=randomUUID();
  await pool.query('UPDATE tasks SET attempt_count=1 WHERE id=$1',[holder]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,account_id,model)
    VALUES($1,$2,1,'running',$3,'harness',1,'remote','glm-5.3-flash')`,[run,holder,randomUUID()]);
  await pool.query("INSERT INTO leases VALUES('browser',$1)",[run]);
  expect(await schedule()).toHaveLength(1);expect(await state(other)).toBe('queued');
  expect(await state(prerequisite)).toBe('waiting_resource');
  expect((await pool.query("SELECT run_id FROM leases WHERE resource='browser'")).rows).toEqual([{run_id:run}]);
});

it('leaves the existing base-priority ordering intact when delivery-first is disabled',async()=>{
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{deliveryFirstEnabled}','false') WHERE id=$1",['workforce:'+goal]);
  const delivery=await task({stage:'integrate',priority:50}),leaf=await task({priority:5,ageMinutes:200}),other=await task({priority:70});
  await depends(delivery,leaf);
  const rows=await readScheduleCandidates();expect(rows.map(r=>r.id)).toEqual([other,delivery,leaf]);
  expect(rows.every(r=>r.age_priority===0&&r.inherited_priority===null)).toBe(true);
});
