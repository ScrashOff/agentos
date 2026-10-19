import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision,wakeSupervisors,closeSupervisorJobs} from '../server/supervision.js';
import {installManagement,type managementSnapshot} from '../server/management.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {managerContextSectionNames,isManagerContextSectionReference} from '../server/manager-context-sections.js';
import {readManagementRetrievalRequest} from '../server/management-retrieval.js';

beforeAll(async()=>{
 expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
 await migrate();await installBacklogPlanner();await installSupervision();await installManagement();
});
beforeEach(async()=>{config.bridgeHost='test-only';config.remoteExecution=true;
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterEach(()=>{vi.restoreAllMocks();config.bridgeHost='';config.remoteExecution=false;});
afterAll(async()=>{await closeSupervisorJobs();await pool.end();});
const sha=(text:string)=>createHash('sha256').update(text).digest('hex');
const noise=(seed:number,size:number)=>Array.from({length:Math.ceil(size/64)},(_,i)=>sha(`${seed}:${i}`)).join('').slice(0,size);
const noArtifacts=async(_id:string):Promise<{bytes:Buffer}>=>{throw Error('NO_ARTIFACT_IO_EXPECTED');};
const decoded=(description:string)=>expandSharedManagerContext(JSON.parse(description.slice(description.lastIndexOf('\n{"cases":')+1)));

async function fixture(){
 const goal=randomUUID(),coordinator=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID(),now=Date.now();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Section wake','Preserve all acceptance','section-wake',$1::text)",[goal]);
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id)
  VALUES('section-sol','Sol','codex','gpt-6.1-sol','["gpt-6.1-sol"]',true,true,12,'section-sol')`);
 const policy={enabled:true,glmAccountId:'section-sol',glmTarget:12,museTarget:0,workerAccounts:[],
  coordinatorAccountId:'section-sol',coordinatorModel:'gpt-6.1-sol'};
 await pool.query('INSERT INTO settings VALUES($1,$2),($3,$4),($5,$6)',[
  'workforce:'+goal,JSON.stringify(policy),'supervision:'+goal,'{"enabled":true}',
  'management:'+goal,JSON.stringify({enabled:true,intervalSeconds:120,enabledAt:new Date(now-10000).toISOString()})]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,account_id,model,criteria,execution)
  VALUES($1,$2,'coordinate','Coordinate','Stopped coordinator','qa.backend','accepted','coordinate','section-sol','gpt-6.1-sol',
  '[{"id":"coord","label":"Coordination","type":"artifact","name":"coordination.json"}]',NULL)`,[coordinator,goal]);
 const criteria=[{id:'proof',label:'Executed verification',type:'artifact',name:'verification.json'}];
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
  VALUES($1,$2,'consumer','Integrator','Do not change current criteria','qa.backend','failed','integrate',$3,$4,1)`,
  [task,goal,JSON.stringify(criteria),JSON.stringify({kind:'harness',profile:'code',workspace:'section/task',baseWorkspace:'section/scope'})]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at)
  VALUES($1,$2,1,$3,'harness',1,'failed','FUNCTIONAL_TEST_FAILED',now())`,[run,task,randomUUID()]);
 await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
  VALUES($1,$2,$3,$4,'Current evidence','[{"kind":"CURRENT_REQUIRED_EVIDENCE","instruction":"Preserve the task criteria and source guards."}]')`,
  [caseId,goal,task,run]);
 const state={requestedAt:new Date(now-240000).toISOString(),reviewedAt:new Date(now-230000).toISOString(),
  focus:{caseId,taskId:task,scope:'section/scope'},managerFocusServiceHistory:{version:1,lastReviewed:{}}};
 await pool.query('INSERT INTO settings VALUES($1,$2)',['management-state:'+goal,JSON.stringify(state)]);
 const operation:any={goalId:goal,focus:{caseId,taskId:task,scope:'section/scope'},
  tasks:[{id:task,title:'Integrator',state:'recovery_blocked',stage:'integrate',expectedAttempt:1,criteria,
   dependencies:[],workspace:'section/task',baseWorkspace:'section/scope',instructionRevisionId:null}],
  previous:{receipts:[{code:'READ_REQUIRED'}],followThrough:null},decisionFeedback:{version:1,entries:[]},
  backlog:{status:'awaiting_supervision',receipts:[]},recoveredContext:{requests:[],results:[]}};
 operation.workOpportunities={selection:{cursor:0},consumers:[],acceptedSources:[],existingCoverage:[],liveActors:[],detail:noise(1,40000)};
 operation.deferredWorkOpportunities={selection:{cursor:0},consumers:[],acceptedSources:[],existingCoverage:[],liveActors:[],detail:noise(2,40000)};
 operation.progress={version:1,delivery:{integrated:0,verified:0,total:1},milestones:[{detail:noise(3,40000)}]};
 operation.frontier={version:1,counts:{pending:1},scopeSummary:[{scope:'section/scope',pending:1}],requirements:[{description:noise(4,40000)}]};
 operation.sourceCatalog={columns:['id','title'],rows:[[randomUUID(),noise(5,40000)]],bases:[]};
 operation.deferredDeliveries=[{id:randomUUID(),state:'staged',detail:noise(6,40000)}];
 operation.deliveryConsumptions=[{id:randomUUID(),scope:'section/scope',state:'pending',receipt:{detail:noise(7,40000)}}];
 const reader:typeof managementSnapshot=async(db,_goal,_policy,focus)=>{
  expect((await db.query('SHOW transaction_read_only')).rows[0].transaction_read_only).toBe('on');
  return {...structuredClone(operation),focus} as any;
 };
 const coordinatorBefore=await coordinatorState(coordinator);
 expect(coordinatorBefore.state).toBe('accepted');
 // The installed BEFORE INSERT trigger has already attached the canonical backlog contract.
 expect(coordinatorBefore.description).not.toBe('Stopped coordinator');
 return {goal,coordinator,task,run,caseId,criteria,now,state,operation,reader,coordinatorBefore};
}
const coordinatorState=async(id:string)=>(await pool.query(
 'SELECT state,attempt_count,description,execution,criteria,account_id,model FROM tasks WHERE id=$1',[id])).rows[0];
const stored=async(goal:string)=>(await pool.query('SELECT section,digest,text,characters FROM management_context_sections WHERE goal_id=$1 ORDER BY section',[goal])).rows;
async function consumer(f:Awaited<ReturnType<typeof fixture>>){return {
 task:(await pool.query('SELECT state,attempt_count,criteria,description FROM tasks WHERE id=$1',[f.task])).rows[0],
 run:(await pool.query('SELECT state,attempt,fence FROM runs WHERE id=$1',[f.run])).rows[0],
 case:(await pool.query('SELECT state,evidence,decision,decision_run_id FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0]};}

it('commits referenced sections with a legitimate wake and every advertised source is immediately readable',async()=>{
 const f=await fixture(),before=await consumer(f);
 await wakeSupervisors(f.now,f.reader);
 const manager=(await pool.query('SELECT state,description FROM tasks WHERE id=$1',[f.coordinator])).rows[0];
 expect(manager.state).toBe('ready');
 const snapshot=decoded(manager.description),rows=await stored(f.goal);
 const refs=managerContextSectionNames.filter(section=>isManagerContextSectionReference(snapshot.operation[section],section,f.goal))
  .map(section=>snapshot.operation[section]);
 expect(refs.length).toBeGreaterThan(0);expect(rows.length).toBe(refs.length);
 for(const ref of refs){
  const row=rows.find((entry:any)=>entry.digest===ref.digest&&entry.section===ref.section);
  expect(row).toBeDefined();expect(row.text).toBe(JSON.stringify(f.operation[ref.section]));
  let text='',offset:number|null=0;
  do{
   const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,{...ref.recovery.request,offset});
   expect(page.status).toBe('OK');text+=page.text;offset=page.nextOffset;
  }while(offset!==null);
  expect(text).toBe(row.text);
 }
 expect(snapshot.operation.tasks).toEqual(f.operation.tasks);
 expect(snapshot.operation.previous).toEqual(f.operation.previous);expect(await consumer(f)).toEqual(before);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_REVIEWED' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
});

it.each(['pause','case-source','route'])('does not persist blobs when %s changes after the read-only proposal is built',async change=>{
 const f=await fixture();let entered!:()=>void,release!:()=>void;
 const entry=new Promise<void>(resolve=>entered=resolve),held=new Promise<void>(resolve=>release=resolve);
 const work=wakeSupervisors(f.now,async(db,goal,policy,focus)=>{const snapshot=await f.reader(db,goal,policy,focus);entered();await held;return snapshot;});
 try{
  await Promise.race([entry,work.then(()=>{throw Error('SNAPSHOT_NOT_REACHED');})]);
  if(change==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(change==='case-source')await pool.query("UPDATE supervision_cases SET evidence=evidence||'[{\"kind\":\"NEW_CURRENT_CONSTRAINT\"}]'::jsonb WHERE id=$1",[f.caseId]);
  if(change==='route')await pool.query("UPDATE accounts SET enabled=false WHERE id='section-sol'");
 }finally{release();await work;}
 expect(await stored(f.goal)).toEqual([]);
 expect(await coordinatorState(f.coordinator)).toEqual(f.coordinatorBefore);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
});

it('keeps an oversized unknown authority blocked without persisting otherwise valid section candidates',async()=>{
 const f=await fixture();f.operation.futureMandatoryConstraint={text:noise(200,180000),taskId:f.task};
 await wakeSupervisors(f.now,f.reader);
 expect(await stored(f.goal)).toEqual([]);
 expect(await coordinatorState(f.coordinator)).toEqual(f.coordinatorBefore);
 expect((await pool.query("SELECT value->'contextProjection' projection FROM settings WHERE id=$1",['management-state:'+f.goal])).rows[0].projection.state).toBe('blocked');
});

it('allows only one of two concurrent builders to publish a wake and the loser creates no orphan section versions',async()=>{
 const f=await fixture();let entered!:()=>void,release!:()=>void,calls=0;
 const both=new Promise<void>(resolve=>entered=resolve),held=new Promise<void>(resolve=>release=resolve);
 const reader:typeof managementSnapshot=async(db,goal,policy,focus)=>{
  const operation=await f.reader(db,goal,policy,focus);const index=++calls;
  (operation as any).progress={...(operation as any).progress,observation:index};
  if(calls===2)entered();await held;return operation;
 };
 const jobs=[wakeSupervisors(f.now,reader),wakeSupervisors(f.now,reader)];
 try{await Promise.race([both,Promise.all(jobs).then(()=>{throw Error('TWO_BUILDERS_NOT_REACHED');})]);}
 finally{release();await Promise.all(jobs);}
 expect((await pool.query("SELECT 1 FROM events WHERE kind='MANAGEMENT_WAKEUP' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
 const snapshot=decoded((await pool.query('SELECT description FROM tasks WHERE id=$1',[f.coordinator])).rows[0].description);
 const refs=managerContextSectionNames.filter(section=>isManagerContextSectionReference(snapshot.operation[section],section,f.goal))
  .map(section=>snapshot.operation[section]);
 const rows=await stored(f.goal);expect(rows.length).toBe(refs.length);
 expect(new Set(rows.map((row:any)=>row.digest))).toEqual(new Set(refs.map(ref=>ref.digest)));
});
