import {beforeAll,beforeEach,afterAll,afterEach,it,expect} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {readScheduleCandidates,schedule} from '../server/service.js';

let goalId:string;
const initial={bridgeHost:config.bridgeHost,remoteExecution:config.remoteExecution};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id IN('maintenance','storage:agentos-workspaces')");
  await pool.query("UPDATE settings SET value='{"+'"maxRuns":4,"browserSlots":1'+"}' WHERE id='capacity'");
  config.bridgeHost='test-only';config.remoteExecution=true;
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
    VALUES('remote','Fixture account','qwen','glm-5.3-flash','["glm-5.3-flash"]',true,16,true,'remote')`);
  goalId=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Projection','Original objective','schedule-projection',$2)",[goalId,goalId]);
});
afterEach(()=>{config.bridgeHost=initial.bridgeHost;config.remoteExecution=initial.remoteExecution;});
afterAll(()=>pool.end());

async function task(options:any={}){
  const id=randomUUID();
  const execution=Object.hasOwn(options,'execution')?options.execution:{kind:'harness',workforceLane:'glm',
    workspace:'schedule-projection/work',baseWorkspace:'schedule-projection/base',profile:'code',writePaths:['src/']};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,
    execution,priority,resources,next_attempt_at,allowed_accounts)
    VALUES($1,$2,$13,'Original task',$3,'qa.backend',$4,$5,$6,'glm-5.3-flash',$7,$8,$9,$10,$11,$12)`,
    [id,goalId,options.description??'Original authorization',options.state??'ready',options.stage??'work',
      options.accountId??'remote',JSON.stringify([{id:'proof',type:'artifact',name:'proof.json',label:'Original proof'}]),
      execution===null?null:JSON.stringify(execution),options.priority??50,JSON.stringify(options.resources??[]),
      options.nextAttemptAt??null,JSON.stringify(options.allowedAccounts??[]),id]);
  return id;
}
const row=async(id:string)=>(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];

it('examines bounded waiting metadata and freezes the entire authorized contract only on actual admission',async()=>{
  const bytes=Buffer.from('Original immutable evidence. '.repeat(30000));
  const input={id:randomUUID(),name:'source.txt',digest:createHash('sha256').update(bytes).digest('hex'),base64:bytes.toString('base64')};
  const parent=await task({state:'running'});
  const execution={kind:'harness',workforceLane:'glm',workspace:'schedule-projection/consumer',
    baseWorkspace:'schedule-projection/base',profile:'code',writePaths:['src/'],inputs:[input]};
  const blocked=await task({execution,description:'Exact contract '.repeat(20000)});
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[blocked,parent]);
  const original=await row(blocked),candidates=await readScheduleCandidates();
  expect(JSON.stringify(candidates).length).toBeLessThan(2000);
  expect(candidates).toHaveLength(1);
  expect(candidates[0]).toMatchObject({id:blocked,
    execution:{kind:'harness',workforceLane:'glm'}});
  expect(candidates[0]).not.toHaveProperty('description');expect(candidates[0]).not.toHaveProperty('criteria');
  expect(await schedule()).toEqual([]);
  expect((await row(blocked)).state).toBe('blocked_dependencies');
  expect((await row(blocked)).execution).toEqual(original.execution);
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[parent]);
  const [runId]=await schedule();expect(runId).toBeDefined();
  const snapshot=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[runId])).rows[0].instruction_snapshot;
  expect(snapshot.task.execution).toEqual(original.execution);
  expect(snapshot.task.description).toBe(original.description);expect(snapshot.task.criteria).toEqual(original.criteria);
  expect(snapshot.executionInputs).toContainEqual({id:input.id,name:input.name,digest:input.digest,size:bytes.length,source:'execution_input'});
});

it('preserves dependency failure, future retry and storage precedence without materializing their inputs',async()=>{
  const parent=await task({state:'failed'}),failed=await task(),pendingParent=await task({state:'running'}),blocked=await task();
  await pool.query('INSERT INTO dependencies VALUES($1,$2),($3,$4)',[failed,parent,blocked,pendingParent]);
  const delayed=await task({state:'retry_scheduled',nextAttemptAt:new Date(Date.now()+60000)}),before=await row(delayed);
  expect(await schedule()).toEqual([]);
  expect((await row(failed)).state).toBe('blocked_failed');expect((await row(blocked)).state).toBe('blocked_dependencies');
  expect(await row(delayed)).toEqual(before);
  await pool.query("INSERT INTO settings VALUES('storage:agentos-workspaces','{\"state\":\"blocked\"}')");
  expect(await schedule()).toEqual([]);
  expect((await row(blocked)).state).toBe('waiting_resource');
  expect((await row(blocked)).reason).toContain('Volume sem capacidade');
  expect(await row(delayed)).toEqual(before);
});

it('propagates dependency failure from an earlier candidate within the same scheduler transaction',async()=>{
  const failed=await task({state:'failed'}),middle=await task({state:'blocked_dependencies',priority:90}),last=await task({priority:80});
  await pool.query('INSERT INTO dependencies VALUES($1,$2),($3,$1)',[middle,failed,last]);
  await schedule();
  expect((await row(middle)).state).toBe('blocked_failed');
  expect((await row(last)).state).toBe('blocked_failed');
});

it('keeps delivery priority, analytical null and native lane filtering intact',async()=>{
  const analytical=await task({execution:null,stage:'coordinate',priority:100});
  const integration=await task({stage:'integrate',priority:50});
  const repair=await task({execution:{kind:'harness',workforceLane:'glm',workKind:'repair'},priority:50});
  const native=await task({execution:{kind:'opencode',workforceLane:'glm'},priority:55});
  await pool.query('INSERT INTO settings VALUES($1,$2)',[
    'workforce:'+goalId,JSON.stringify({enabled:true,deliveryFirstEnabled:true,detachedHarnessEnabled:true})]);
  const all=await readScheduleCandidates();
  expect(all.map(t=>t.id)).toEqual([analytical,integration,repair,native]);
  expect(all[0].execution).toBeNull();
  expect((await readScheduleCandidates(pool,'agentos-opencode')).map(t=>t.id)).toEqual([integration,repair,native]);
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{detachedHarnessEnabled}','false') WHERE id=$1",['workforce:'+goalId]);
  expect((await readScheduleCandidates(pool,'agentos-opencode')).map(t=>t.id)).toEqual([native]);
});

it('does not starve an available route behind many exhausted high-priority tasks and retains exclusive leases',async()=>{
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id,quota_status,remaining_percent)
    VALUES('empty','Exhausted fixture','qwen','glm-5.3-flash','["glm-5.3-flash"]',true,16,true,'empty','exhausted',0)`);
  await pool.query("UPDATE accounts SET quota=$1 WHERE id='empty'",[JSON.stringify({status:'ok',groups:[{label:'Qwen',windows:[{remainingPercent:0}]}]})]);
  for(let i=0;i<18;i++)await task({priority:100,accountId:'empty'});
  const available=await task({priority:20,resources:['browser']}),other=await task({priority:10,resources:['browser']});
  const runs=await schedule();expect(runs).toHaveLength(1);
  expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[runs[0]])).rows[0].task_id).toBe(available);
  expect((await row(other)).state).toBe('waiting_resource');
  expect((await row(other)).reason).toContain('Recurso exclusivo');
  expect((await pool.query("SELECT run_id FROM leases WHERE resource='browser'")).rows).toEqual([{run_id:runs[0]}]);
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE account_id='empty' AND state='waiting_quota'")).rows[0].n).toBe(18);
});

it('enforces the GLM active pool using projected runtime lanes',async()=>{
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+goalId,JSON.stringify({enabled:true,
    glmAccountId:'remote',glmTarget:1,glmHarnessEnabled:true,workerAccounts:['remote'],
    coordinatorAccountId:'remote',coordinatorModel:'gpt-6.1-sol'})]);
  const first=await task({priority:60}),second=await task({priority:50});
  expect(await schedule()).toHaveLength(1);expect((await row(first)).state).toBe('queued');
  expect((await row(second)).state).toBe('waiting_resource');expect((await row(second)).reason).toContain('vagas GLM');
  expect(await schedule()).toEqual([]);
});
