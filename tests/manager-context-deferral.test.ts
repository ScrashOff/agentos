import {beforeAll,beforeEach,afterAll,afterEach,it,expect,vi} from 'vitest';
import {randomBytes,randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement,managementSnapshot} from '../server/management.js';
import {installSupervision,wakeSupervisors,closeSupervisorJobs} from '../server/supervision.js';
import {heartbeat} from '../server/service.js';
import {readContextCaseBindings,contextCaseBindingsMatch,contextCaseSourceFingerprint,
 selectContextDeferrals,addContextDeferral,managerContextDeferralMilliseconds,type ContextCaseBinding} from '../server/manager-context-deferral.js';

beforeAll(async()=>{await migrate();await installBacklogPlanner();await installSupervision();await installManagement();});
beforeEach(async()=>{config.bridgeHost='test-only';config.remoteExecution=true;
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterEach(()=>{vi.restoreAllMocks();config.bridgeHost='';config.remoteExecution=false;});
afterAll(async()=>{await closeSupervisorJobs();await pool.end();});

function pureCase(){
 const entry={id:randomUUID(),task_id:randomUUID(),source_run_id:randomUUID(),current_run_id:randomUUID(),
  current_attempt:3,current_run_state:'failed',pending_dependencies:0,fresh_handoffs:0,evidence:[{kind:'UNKNOWN',value:'required'}]};
 const binding:ContextCaseBinding={case_id:entry.id,case_contract:'a'.repeat(64),task_id:entry.task_id,task_contract:'b'.repeat(64),
  source_run_id:entry.source_run_id,instruction_revision_id:null,run_id:entry.current_run_id,run_contract:'c'.repeat(64),artifact_binding:[]};
 return {entry,binding};
}
it('does not count a context refusal as service and releases an unchanged case after a finite two-minute delay',()=>{
 const {entry,binding}=pureCase(),now=Date.now();
 const ledger=addContextDeferral({version:1,entries:[]},entry,binding,'version','a'.repeat(64),151000,150000,now);
 const before=structuredClone({entry,binding,ledger});
 expect(selectContextDeferrals([entry],[binding],ledger,'version',now+1).eligible).toEqual([]);
 expect(selectContextDeferrals([entry],[binding],ledger,'version',now+managerContextDeferralMilliseconds).eligible).toEqual([entry]);
 expect(selectContextDeferrals([entry],[binding],ledger,'changed-version',now+1).eligible).toEqual([entry]);
 expect({entry,binding,ledger}).toEqual(before);
 expect(ledger).not.toHaveProperty('reviewedAt');expect(ledger).not.toHaveProperty('lastReviewed');
});
it('revokes only the affected deferral on material evidence, run, revision, task contract or artifact changes',()=>{
 const {entry,binding}=pureCase(),now=Date.now();
 const ledger=addContextDeferral({version:1,entries:[]},entry,binding,'v','b'.repeat(64),151000,150000,now);
 for(const changed of [{...entry,evidence:[...entry.evidence,{kind:'NEW_MATERIAL_EVIDENCE'}]},
   {...entry,current_attempt:4},{...entry,current_run_id:randomUUID()}])
  expect(selectContextDeferrals([changed],[binding],ledger,'v',now+1).eligible).toEqual([changed]);
 for(const changed of [{...binding,run_id:randomUUID()},{...binding,instruction_revision_id:randomUUID()},
   {...binding,task_contract:'f'.repeat(64)},{...binding,run_contract:'e'.repeat(64)},
   {...binding,artifact_binding:[[randomUUID(),'verification.json','c'.repeat(64),20]]}])
  expect(selectContextDeferrals([entry],[changed],ledger,'v',now+1).eligible).toEqual([entry]);
 expect(contextCaseBindingsMatch([binding],[{...binding,run_contract:'f'.repeat(64)}])).toBe(false);
 expect(contextCaseBindingsMatch([binding],[structuredClone(binding)])).toBe(true);
});
it('malformed, duplicate, cross-task or unbounded receipts never suppress a case',()=>{
 const {entry,binding}=pureCase(),now=Date.now();
 const ledger=addContextDeferral({version:1,entries:[]},entry,binding,'v','d'.repeat(64),151000,150000,now);
 for(const bad of [{},{version:2,entries:ledger.entries},{version:1,entries:[null]},
   {...ledger,entries:[...ledger.entries,...ledger.entries]},
   ...[{taskId:randomUUID()},{sourceFingerprint:'bad'},{nextCheckAt:new Date(now+86400000).toISOString()},
    {checkedAt:'invalid'},{characters:10},{code:'IGNORED'}].map(delta=>({...ledger,entries:[{...ledger.entries[0],...delta}]}))])
  expect(selectContextDeferrals([entry],[binding],bad,'v',now+1).eligible).toEqual([entry]);
});

async function fixture(count=2,allLarge=false,glmWorkerIndex?:number){
 const goal=randomUUID(),coordinator=randomUUID(),now=Date.now(),tasks:string[]=[],cases:string[]=[],runs:string[]=[];
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Context deferral','Complete work','context-deferral',$1::text)",[goal]);
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id)
  VALUES('deferral-sol','Sol','codex','gpt-6.1-sol','["gpt-6.1-sol"]',true,true,12,'deferral-sol')`);
 if(glmWorkerIndex!==undefined)await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id)
  VALUES('deferral-glm','GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,true,12,'deferral-glm')`);
 const policy={enabled:true,glmAccountId:glmWorkerIndex===undefined?'deferral-sol':'deferral-glm',glmTarget:12,museTarget:0,workerAccounts:[],
  ...(glmWorkerIndex!==undefined?{glmHarnessEnabled:true}:{}),
  coordinatorAccountId:'deferral-sol',coordinatorModel:'gpt-6.1-sol'};
 await pool.query('INSERT INTO settings VALUES($1,$2),($3,$4),($5,$6)',[
  'workforce:'+goal,JSON.stringify(policy),'supervision:'+goal,'{"enabled":true}',
  'management:'+goal,JSON.stringify({enabled:true,intervalSeconds:120,enabledAt:new Date(now-10000).toISOString()})]);
 const criteria=[{id:'proof',label:'Executed verification',type:'artifact',name:'verification.json'}];
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution)
  VALUES($1,$2,'coordinate','Coordinate','Stopped coordinator','qa.backend','accepted','coordinate','deferral-sol','gpt-6.1-sol',$3,NULL)`,
  [coordinator,goal,JSON.stringify([{id:'coord',label:'Coordination',type:'artifact',name:'coordination.json'}])]);
 for(let i=0;i<count;i++){
  const task=randomUUID(),run=randomUUID(),caseId=randomUUID();tasks.push(task);runs.push(run);cases.push(caseId);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution,attempt_count)
   VALUES($1,$2,$3,$3,'Preserve the exact verification contract','qa.backend','failed','integrate',$6,$7,$4,$5,1)`,
   [task,goal,'delivery-'+i,JSON.stringify(criteria),JSON.stringify({kind:'harness',profile:'code',
    workspace:'context-deferral/task-'+i,baseWorkspace:'context-deferral/scope-'+i,
    ...(i===glmWorkerIndex?{workforceLane:'glm'}:{})}),
    i===glmWorkerIndex?'deferral-glm':'deferral-sol',i===glmWorkerIndex?'glm-5.3-flash':'gpt-6.1-sol']);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at,account_id,model)
   VALUES($1,$2,1,$3,'harness',1,'failed','FUNCTIONAL_TEST_FAILED',now(),$4,$5)`,
   [run,task,randomUUID(),i===glmWorkerIndex?'deferral-glm':null,i===glmWorkerIndex?'glm-5.3-flash':null]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence,created_at)
   VALUES($1,$2,$3,$4,'Required current evidence',$5,$6)`,[caseId,goal,task,run,
    JSON.stringify([{kind:'UNKNOWN_REQUIRED_EVIDENCE',details:i===0||allLarge?randomBytes(100000).toString('hex'):'Small exact evidence'}]),
    new Date(now-10000+i)]);
 }
 const state={requestedAt:new Date(now-240000).toISOString(),reviewedAt:new Date(now-230000).toISOString(),
  focus:{caseId:cases[0]},contextRequests:[{kind:'task_metadata',taskId:tasks[0],expectedAttempt:1}],
  contextRequestFocus:{caseId:cases[0],taskId:tasks[0]},managerFocusServiceHistory:{version:1,lastReviewed:{}}};
 await pool.query('INSERT INTO settings VALUES($1,$2)',['management-state:'+goal,JSON.stringify(state)]);
 return {goal,coordinator,tasks,cases,runs,now,state};
}
const state=async(goal:string)=>(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+goal])).rows[0].value;
async function workRows(f:Awaited<ReturnType<typeof fixture>>){return {
 tasks:(await pool.query('SELECT * FROM tasks WHERE id=ANY($1::uuid[]) ORDER BY id',[f.tasks])).rows,
 runs:(await pool.query('SELECT * FROM runs WHERE task_id=ANY($1::uuid[]) ORDER BY id',[f.tasks])).rows,
 cases:(await pool.query('SELECT * FROM supervision_cases WHERE goal_id=$1 ORDER BY id',[f.goal])).rows};}

it('blocks the oversized case honestly, serves a different case on the next passage and preserves all evidence and pending requests',async()=>{
 const f=await fixture(),before=await workRows(f),reader=vi.fn(managementSnapshot);
 await wakeSupervisors(f.now,reader);
 const blocked=await state(f.goal);
 expect(reader).toHaveBeenCalledTimes(1);
 expect(blocked.contextDeferrals.entries).toHaveLength(1);
 expect(blocked.contextProjection).toMatchObject({state:'blocked',caseId:f.cases[0],maximum:150000});
 expect(blocked.contextDeferrals.entries[0]).toMatchObject({caseId:f.cases[0],taskId:f.tasks[0],code:'CONTEXT_TOO_LARGE'});
 for(const key of ['requestedAt','reviewedAt','focus','contextRequests','contextRequestFocus','managerFocusServiceHistory'])
  expect(blocked[key]).toEqual((f.state as any)[key]);
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('accepted');
 await wakeSupervisors(f.now+5000,reader);
 const next=await state(f.goal);
 expect(reader).toHaveBeenCalledTimes(2);
 expect(next.focus.caseId).toBe(f.cases[1]);expect(next.contextProjection.state).toBe('ready');
 expect(next.contextProjection.deferredCases[0].caseId).toBe(f.cases[0]);
 expect(next.contextRequests).toEqual(f.state.contextRequests);expect(next.contextRequestFocus).toEqual(f.state.contextRequestFocus);
 expect(next.reviewedAt).toBe(f.state.reviewedAt);expect(next.managerFocusServiceHistory).toEqual(f.state.managerFocusServiceHistory);
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.coordinator])).rows[0].state).toBe('ready');
 expect(await workRows(f)).toEqual(before);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_REVIEWED' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
});
it('does not reconstruct any heavy context while every open case is deferred, then retries after the finite deadline',async()=>{
 const f=await fixture(1),reader=vi.fn(managementSnapshot);await wakeSupervisors(f.now,reader);
 const before=await state(f.goal);await wakeSupervisors(f.now+5000,reader);await wakeSupervisors(f.now+10000,reader);
 expect(reader).toHaveBeenCalledTimes(1);expect(await state(f.goal)).toEqual(before);
 await wakeSupervisors(f.now+managerContextDeferralMilliseconds,reader);
 expect(reader).toHaveBeenCalledTimes(2);
 expect((await state(f.goal)).contextDeferrals.entries[0].checkedAt).toBe(new Date(f.now+managerContextDeferralMilliseconds).toISOString());
});
it('new current evidence revokes a deferral immediately without waiting for the old deadline',async()=>{
 const f=await fixture(1),reader=vi.fn(managementSnapshot);await wakeSupervisors(f.now,reader);
 const before=(await state(f.goal)).contextDeferrals.entries[0];
 await pool.query("UPDATE supervision_cases SET evidence=evidence||'[ {\"kind\":\"NEW_REQUIRED_FACT\"} ]'::jsonb WHERE id=$1",[f.cases[0]]);
 await wakeSupervisors(f.now+5000,reader);
 expect(reader).toHaveBeenCalledTimes(2);
 expect((await state(f.goal)).contextDeferrals.entries[0].sourceFingerprint).not.toBe(before.sourceFingerprint);
});
it.each(['case','task','run','latest-run','ledger','pause','cancel'])(
 'rejects a prepared context after concurrent %s change without admitting or writing a false deferral',async(change)=>{
 const f=await fixture(1);let entered!:()=>void,release!:()=>void;
 const entry=new Promise<void>(r=>entered=r),held=new Promise<void>(r=>release=r);
 const work=wakeSupervisors(f.now,async(db,goal,policy,focus)=>{entered();await held;return managementSnapshot(db,goal,policy,focus);});
 try{
  await Promise.race([entry,work.then(()=>{throw Error('SNAPSHOT_NOT_REACHED');})]);
  if(change==='case')await pool.query("UPDATE supervision_cases SET evidence=evidence||'[ {\"kind\":\"CHANGED\"} ]'::jsonb WHERE id=$1",[f.cases[0]]);
  if(change==='task')await pool.query("UPDATE tasks SET description=description||' Material change' WHERE id=$1",[f.tasks[0]]);
  if(change==='run')await pool.query("UPDATE runs SET error_code='CHANGED_FAILURE' WHERE id=$1",[f.runs[0]]);
  if(change==='latest-run')await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code)
   VALUES($1,$2,2,$3,'harness',1,'failed','NEW_ATTEMPT')`,[randomUUID(),f.tasks[0],randomUUID()]);
  if(change==='ledger')await pool.query("UPDATE settings SET value=value||'{\"contextDeferrals\":{\"version\":1,\"entries\":[]}}'::jsonb WHERE id=$1",['management-state:'+f.goal]);
  if(change==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(change==='cancel')await pool.query("UPDATE tasks SET state='cancelled' WHERE id=$1",[f.coordinator]);
 }finally{release();await work;}
 const final=await state(f.goal);
 expect(final.contextProjection).toBeUndefined();
 expect(final.contextDeferrals).toEqual(change==='ledger'?{version:1,entries:[]}:undefined);
 expect((await pool.query("SELECT 1 FROM events WHERE kind IN('MANAGEMENT_WAKEUP','MANAGEMENT_CONTEXT_BLOCKED') AND entity_id=$1",[f.goal])).rowCount).toBe(0);
});
it('two concurrent builders persist one deferral and no fictitious review',async()=>{
 const f=await fixture(1);let release!:()=>void,entered!:()=>void,count=0;
 const both=new Promise<void>(r=>entered=r),held=new Promise<void>(r=>release=r);
 const reader:typeof managementSnapshot=async(db,goal,policy,focus)=>{if(++count===2)entered();await held;return managementSnapshot(db,goal,policy,focus);};
 const calls=[wakeSupervisors(f.now,reader),wakeSupervisors(f.now,reader)];
 try{await Promise.race([both,Promise.all(calls).then(()=>{throw Error('TWO_BUILDERS_NOT_REACHED');})]);}finally{release();await Promise.all(calls);}
 expect((await state(f.goal)).contextDeferrals.entries).toHaveLength(1);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_CONTEXT_BLOCKED' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
});
it('a real heartbeat and operational timestamp updates of an unrelated live case do not invalidate a feasible review',async()=>{
 const f=await fixture(2,false,1);
 await pool.query("UPDATE supervision_cases SET evidence='[{\"kind\":\"REQUIRED\",\"detail\":\"Small exact current evidence\"}]'::jsonb WHERE id=$1",[f.cases[0]]);
 await pool.query("UPDATE supervision_cases SET state='resolved' WHERE id=$1",[f.cases[1]]);
 await pool.query("UPDATE tasks SET state='running' WHERE id=$1",[f.tasks[1]]);
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.runs[1]]);
 expect((await pool.query('SELECT account_id,model,state FROM runs WHERE id=$1',[f.runs[1]])).rows[0])
  .toEqual({account_id:'deferral-glm',model:'glm-5.3-flash',state:'running'});
 await pool.query("UPDATE supervision_cases SET state='open' WHERE id=$1",[f.cases[1]]);
 const run=(await pool.query('SELECT fence,heartbeat_at FROM runs WHERE id=$1',[f.runs[1]])).rows[0];
 const before=await readContextCaseBindings(pool,f.goal,f.cases);
 let entered!:()=>void,release!:()=>void;
 const entry=new Promise<void>(r=>entered=r),held=new Promise<void>(r=>release=r);
 const work=wakeSupervisors(f.now,async(db,goal,policy,focus)=>{entered();await held;return managementSnapshot(db,goal,policy,focus);});
 try{
  await Promise.race([entry,work.then(()=>{throw Error('SNAPSHOT_NOT_REACHED');})]);
  await heartbeat(f.runs[1],run.fence);
  await pool.query("UPDATE tasks SET updated_at=clock_timestamp(),reason='Operational observation refreshed' WHERE id=$1",[f.tasks[1]]);
  await pool.query('UPDATE supervision_cases SET updated_at=clock_timestamp() WHERE id=$1',[f.cases[1]]);
 }finally{release();await work;}
 expect(contextCaseBindingsMatch(before,await readContextCaseBindings(pool,f.goal,f.cases))).toBe(true);
 expect((await pool.query('SELECT heartbeat_at FROM runs WHERE id=$1',[f.runs[1]])).rows[0].heartbeat_at).not.toEqual(run.heartbeat_at);
 const final=await state(f.goal);
 expect(final.contextProjection.state).toBe('ready');expect(final.focus.caseId).toBe(f.cases[0]);
 expect(final.reviewedAt).toBe(f.state.reviewedAt);expect(final.contextDeferrals.entries).toEqual([]);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
});
it('propagates an unexpected database error without inventing a context refusal or swallowing the failure',async()=>{
 const f=await fixture(1),before=await state(f.goal);
 const error=Object.assign(Error('fixture database error'),{code:'XX000'});
 await expect(wakeSupervisors(f.now,async()=>{throw error;})).rejects.toBe(error);
 expect(await state(f.goal)).toEqual(before);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_CONTEXT_BLOCKED' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
});
