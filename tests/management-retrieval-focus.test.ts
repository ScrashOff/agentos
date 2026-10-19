import {afterAll,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installDeliveryConsumptions} from '../server/delivery-consumption.js';
import {completeRun,validateTask,writeArtifact,readArtifact} from '../server/service.js';
import {readManagementRetrieval,managementRetrievalCharacterLimit} from '../server/management-retrieval.js';
import {compactSharedManagerContext} from '../server/manager-context-sharing.js';

beforeAll(async()=>{await migrate();await installBacklogPlanner();await installManagement();await installDeliveryConsumptions();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});

async function fixture(){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Research','Read specific evidence','retrieval-focus',$1::text)",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
  VALUES($1,$2,'source','Research source','Exact evidence for a bounded follow-up.','research.web','running','work',
  '[{"id":"report","type":"artifact","name":"findings.json"}]',
  '{"baseWorkspace":"research/source","workKind":"diagnostic"}',1)`,[task,goal]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'running','harness')",[run,task,fence]);
 const text=JSON.stringify({finding:'A verified source, not a delivery acceptance.'});
 const artifact=await writeArtifact(run,fence,'findings.json',text,'application/json');
 await completeRun(run,fence,'Evidence persisted',{});await validateTask(task);
 const a={caseId:randomUUID(),taskId:task,scope:'research/source',sourceFingerprint:'a'.repeat(64)};
 const b={caseId:randomUUID(),taskId:randomUUID(),scope:'research/other',sourceFingerprint:'b'.repeat(64)};
 return {goal,task,run,artifact,text,a,b};
}
async function review(goal:string,focus:any,requests:any[],options:{frozenFocus?:any;passed?:boolean;invalid?:boolean;shared?:boolean}={}){
 const existing=(await pool.query("SELECT id,attempt_count FROM tasks WHERE goal_id=$1 AND stage='coordinate'",[goal])).rows[0];
 const manager=existing?.id??randomUUID(),attempt=(existing?.attempt_count??0)+1,run=randomUUID();
 const snapshot={cases:[],operation:{focus:options.frozenFocus??focus,
  repeated:Array.from({length:100},()=>({id:focus.caseId,scope:focus.scope,text:'Long unchanged evidence for a shared context snapshot.'}))}};
 const frozen=options.shared?compactSharedManagerContext(snapshot):snapshot;
 expect(frozen.operation.focus).toEqual(options.frozenFocus??focus);
 const description='MANAGEMENT_V1\n'+JSON.stringify(frozen);
 if(existing)await pool.query("UPDATE tasks SET description=$2,attempt_count=$3,state='accepted' WHERE id=$1",[manager,description,attempt]);
 else await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
  VALUES($1,$2,'manager','Manager',$3,'engineering.supervisor','accepted','coordinate','[]',$4)`,[manager,goal,description,attempt]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,created_at)
  VALUES($1,$2,$3,$4,1,'submitted','harness',$5)`,[run,manager,attempt,randomUUID(),new Date(Date.parse('2026-10-11T20:00:00Z')+attempt*1000)]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,$4,'[]','fixture')",[randomUUID(),manager,run,options.passed??true]);
 await pool.query("INSERT INTO management_reviews(run_id,goal_id,summary,capacity_reason,receipts) VALUES($1,$2,'Read evidence','Bounded', $3)",[run,goal,JSON.stringify(options.invalid?[{code:'MANAGEMENT_FORMAT'}]:[])]);
 const state={runId:run,contextRequests:requests,contextRequestFocus:focus};
 await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('MANAGEMENT_REVIEWED',$1,$2)",[goal,JSON.stringify(state)]);
 await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',['management-state:'+goal,JSON.stringify(state)]);
 return {run,manager,attempt};
}
const packet=(f:Awaited<ReturnType<typeof fixture>>)=>[
 {kind:'task_metadata',taskId:f.task,expectedAttempt:1,offset:0},
 {kind:'artifact',taskId:f.task,expectedAttempt:1,name:'findings.json',offset:0},
 {kind:'coverage',baseWorkspace:f.a.scope,query:'Research',offset:0},
 {kind:'delivery_consumptions',scope:f.a.scope,offset:0},
];

it('returns all requested pages after Lucky -> Mahjong overwrite -> Lucky without borrowing the other decision or rewriting state',async()=>{
 const f=await fixture(),original=await review(f.goal,f.a,packet(f),{shared:true});
 const other=await review(f.goal,f.b,[{kind:'coverage',baseWorkspace:f.b.scope,query:'other only',offset:0}]);
 const before=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
 const resumed=await readManagementRetrieval(pool,f.goal,readArtifact,f.a);
 expect(resumed).toMatchObject({selection:{status:'FOCUS_RESUMED',reviewRunId:original.run}});
 expect(resumed.results.map(r=>r.request.kind)).toEqual(packet(f).map(r=>r.kind));
 expect(resumed.results.map(r=>r.status)).toEqual(['OK','OK','OK','OK']);
 expect(resumed.results[1]).toMatchObject({digest:f.artifact.digest,text:f.text,runId:f.run});
 expect(resumed.results[2].rows).toHaveLength(1);expect(JSON.stringify(resumed).length).toBeLessThanOrEqual(managementRetrievalCharacterLimit);
 const current=await readManagementRetrieval(pool,f.goal,readArtifact,f.b);
 expect(current.results).toHaveLength(1);expect(current.results[0].request.query).toBe('other only');
 expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value).toEqual(before);
 expect((await pool.query('SELECT attempt_count FROM tasks WHERE id=$1',[other.manager])).rows[0].attempt_count).toBe(2);
 expect(original.attempt).toBe(1);expect((await readArtifact(f.artifact.id)).bytes.toString()).toBe(f.text);
});

it('a newer empty packet for the same case clears the old requests even after another focus overwrites global state',async()=>{
 const f=await fixture();await review(f.goal,f.a,packet(f));const cleared=await review(f.goal,f.a,[]);
 await review(f.goal,f.b,[{kind:'coverage',baseWorkspace:f.b.scope,offset:0}]);
 let reads=0;const result=await readManagementRetrieval(pool,f.goal,async()=>{reads++;throw Error('UNEXPECTED_READ');},f.a);
 expect(result).toMatchObject({results:[],selection:{status:'FOCUS_RESUMED',reviewRunId:cleared.run}});expect(reads).toBe(0);
});

it('a newer malformed packet does not resurrect an older valid request for that case',async()=>{
 const f=await fixture();await review(f.goal,f.a,packet(f));await review(f.goal,f.a,[{kind:'unknown'}]);await review(f.goal,f.b,[]);
 expect(await readManagementRetrieval(pool,f.goal,readArtifact,f.a)).toMatchObject({results:[],invalidRequests:true});
});

it.each(['frozen-case','frozen-scope','frozen-task','unaccepted','invalid-review'] as const)('refuses %s history and does not fall through to its older valid packet',async(kind)=>{
 const f=await fixture();await review(f.goal,f.a,packet(f));
 await review(f.goal,f.a,packet(f),{
  ...(kind==='frozen-case'?{frozenFocus:{...f.a,caseId:randomUUID()}}:{}),
  ...(kind==='frozen-scope'?{frozenFocus:{...f.a,scope:'another/scope'}}:{}),
  ...(kind==='frozen-task'?{frozenFocus:{...f.a,taskId:randomUUID()}}:{}),
  ...(kind==='unaccepted'?{passed:false}:{}),...(kind==='invalid-review'?{invalid:true}:{}),
 });
 await review(f.goal,f.b,[]);
 expect(await readManagementRetrieval(pool,f.goal,readArtifact,f.a)).toMatchObject({results:[],selection:{status:'FOCUS_CHANGED'}});
});

it('does not recover another goal or case even when its event points at a real accepted review',async()=>{
 const f=await fixture();const original=await review(f.goal,f.a,packet(f));await review(f.goal,f.b,[]);
 expect((await readManagementRetrieval(pool,f.goal,readArtifact,{...f.a,caseId:randomUUID()})).results).toEqual([]);
 const other=await fixture();await review(other.goal,other.b,[]);
 await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('MANAGEMENT_REVIEWED',$1,$2)",[other.goal,JSON.stringify({runId:original.run,contextRequestFocus:f.a,contextRequests:packet(f)})]);
 expect((await readManagementRetrieval(pool,other.goal,readArtifact,f.a)).results).toEqual([]);
});

it('revalidates the source attempt, live execution and artifact bytes when replaying a previous case request',async()=>{
 const f=await fixture();await review(f.goal,f.a,packet(f).slice(0,2));await review(f.goal,f.b,[]);
 const integrity=await readManagementRetrieval(pool,f.goal,async()=>({bytes:Buffer.from('changed')}),f.a);
 expect(integrity.results[1].status).toBe('INTEGRITY_FAILED');
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
 expect((await readManagementRetrieval(pool,f.goal,readArtifact,f.a)).results.map(r=>r.status)).toEqual(['LIVE_SOURCE','LIVE_SOURCE']);
 await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[f.run]);
 const next=randomUUID();await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,2,$3,1,'submitted','harness')",[next,f.task,randomUUID()]);
 const stale=await readManagementRetrieval(pool,f.goal,readArtifact,f.a);
 expect(stale.results.map(r=>r.status)).toEqual(['STALE_ATTEMPT','STALE_ATTEMPT']);
 expect(stale.results.every(r=>r.text===undefined)).toBe(true);
});

it('keeps catalog digest and pagination guards after focus rotation; a page request is never a cached catalog result',async()=>{
 const f=await fixture();await review(f.goal,f.a,[{kind:'delivery_consumptions',scope:f.a.scope,offset:10,snapshotDigest:'0'.repeat(64)}]);await review(f.goal,f.b,[]);
 const result=await readManagementRetrieval(pool,f.goal,readArtifact,f.a);
 expect(result.results[0]).toMatchObject({status:'STALE_CATALOG',nextOffset:0});expect(result.results[0].rows??[]).toEqual([]);
});

it('orders by the accepted run chronology so late recording of an older case review cannot overwrite its latest empty packet',async()=>{
 const f=await fixture(),old=await review(f.goal,f.a,packet(f));const empty=await review(f.goal,f.a,[]);await review(f.goal,f.b,[]);
 await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('MANAGEMENT_REVIEWED',$1,$2)",[f.goal,JSON.stringify({runId:old.run,contextRequestFocus:f.a,contextRequests:packet(f)})]);
 expect(await readManagementRetrieval(pool,f.goal,readArtifact,f.a)).toMatchObject({results:[],selection:{reviewRunId:empty.run}});
});
