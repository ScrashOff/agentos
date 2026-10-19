import {afterAll,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {readManagementRetrieval,readManagementRetrievalRequest} from '../server/management-retrieval.js';
import {retainManagementEvidence,readManagementEvidenceLedger} from '../server/management-evidence-ledger.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installBacklogPlanner();await installManagement();
});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
const noArtifacts=async(_id:string):Promise<{bytes:Buffer}>=>{throw Error('UNEXPECTED_ARTIFACT_OR_WORKSPACE_READ');};
async function fixture(){
  const goal=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key,state) VALUES($1::uuid,'Live instruction memory','Keep actor identity','live-instruction-memory',$1::text,'active')",[goal]);
  const actor=async(key:string)=>{
    const taskId=randomUUID(),runId=randomUUID(),runFence=randomUUID(),contextVersion=randomUUID();
    const text=`ACTOR_${key}_EXACT_FROZEN_INSTRUCTION. `+'Preserve its own criteria, source and authorized scope. '.repeat(80);
    const scope='instruction-memory/'+key.toLowerCase();
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
      VALUES($1,$2,$3,$3,$4,'qa.backend','running','work','[]',$5,1)`,
      [taskId,goal,key,text,JSON.stringify({kind:'harness',workspace:scope,baseWorkspace:scope})]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,started_at) VALUES($1,$2,1,$3,1,'running','harness',now())",[runId,taskId,runFence]);
    await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','{}')",[contextVersion,runId]);
    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[runId,contextVersion]);
    const identity={goalId:goal,taskId,runId,runFence,contextVersion,expectedAttempt:1};
    return {...identity,text,scope,
      focus:{kind:'live_failure',...identity,scope,eventId:key==='A'?'101':'102',failureKey:(key==='A'?'a':'b').repeat(64)},
      request:{kind:'live_task_instruction' as const,...identity,offset:0}};
  };
  return {goal,a:await actor('A'),b:await actor('B')};
}
type Fixture=Awaited<ReturnType<typeof fixture>>;
async function review(f:Fixture,focus:any,requests:any[],options:{frozenFocus?:any;passed?:boolean;invalid?:boolean}={}){
  const existing=(await pool.query("SELECT id,attempt_count FROM tasks WHERE goal_id=$1 AND stage='coordinate'",[f.goal])).rows[0];
  const manager=existing?.id??randomUUID(),attempt=(existing?.attempt_count??0)+1,run=randomUUID();
  const description='MANAGEMENT_V1\n'+JSON.stringify({cases:[],operation:{focus:options.frozenFocus??focus}});
  if(existing)await pool.query("UPDATE tasks SET description=$2,attempt_count=$3,state='accepted' WHERE id=$1",[manager,description,attempt]);
  else await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,'manager','Manager',$3,'engineering.supervisor','accepted','coordinate','[]',$4)`,[manager,f.goal,description,attempt]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,created_at)
    VALUES($1,$2,$3,$4,1,'submitted','analytical',$5)`,[run,manager,attempt,randomUUID(),new Date(Date.parse('2026-10-16T20:00:00Z')+attempt*1000)]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,$4,'[]','fixture')",[randomUUID(),manager,run,options.passed??true]);
  await pool.query("INSERT INTO management_reviews(run_id,goal_id,summary,capacity_reason,receipts) VALUES($1,$2,'Bounded instruction request','Preserve actor',$3)",
    [run,f.goal,JSON.stringify(options.invalid?[{code:'MANAGEMENT_FORMAT'}]:[])]);
  const state={runId:run,contextRequestFocus:focus,contextRequests:requests};
  await event(pool,'MANAGEMENT_REVIEWED',f.goal,state);
  await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',['management-state:'+f.goal,JSON.stringify(state)]);
  return {run,attempt};
}
const page=(f:Fixture,actor=f.a)=>readManagementRetrievalRequest(pool,f.goal,noArtifacts,actor.request);
const ledger=(f:Fixture,focus:any=f.a.focus)=>readManagementEvidenceLedger(pool,f.goal,focus.scope,focus,noArtifacts);
const savedLedger=async(f:Fixture)=>(await pool.query('SELECT value FROM settings WHERE id=$1',['management-evidence:'+f.goal])).rows[0]?.value;
const actorState=async(f:Fixture)=>(await pool.query(`SELECT t.id,t.state,t.description,t.criteria,t.execution,t.attempt_count,
  r.id run_id,r.state run_state,r.fence,r.context_id,r.instruction_snapshot FROM tasks t JOIN runs r ON r.task_id=t.id
  WHERE t.id=ANY($1::uuid[]) ORDER BY t.id,r.attempt`,[[f.a.taskId,f.b.taskId]])).rows;

it('defers A explicitly when B is selected, then recovers A from its own accepted review without borrowing B pages',async()=>{
  const f=await fixture(),before=await actorState(f),first=await review(f,f.a.focus,[f.a.request]);
  const bBeforeReview=await readManagementRetrieval(pool,f.goal,noArtifacts,f.b.focus);
  expect(bBeforeReview).toMatchObject({results:[],selection:{status:'FOCUS_CHANGED',omittedRequests:1}});
  const second=await review(f,f.b.focus,[f.b.request]);
  const stateBefore=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value;
  const a=await readManagementRetrieval(pool,f.goal,noArtifacts,f.a.focus);
  expect(a).toMatchObject({selection:{status:'FOCUS_RESUMED',reviewRunId:first.run}});
  expect(a.results).toHaveLength(1);expect(a.results[0]).toMatchObject({status:'OK',runId:f.a.runId,text:f.a.text});
  expect(JSON.stringify(a)).not.toContain('ACTOR_B_EXACT');
  const b=await readManagementRetrieval(pool,f.goal,noArtifacts,f.b.focus);
  expect(b.results[0]).toMatchObject({status:'OK',runId:f.b.runId,text:f.b.text});
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0].value).toEqual(stateBefore);
  expect(second.attempt).toBe(2);expect(await actorState(f)).toEqual(before);
});

it('reuses an exact cached instruction for a new cause of the same actor after an empty review packet',async()=>{
  const f=await fixture(),before=await actorState(f),read=await page(f);
  expect(read).toMatchObject({status:'OK',runId:f.a.runId,text:f.a.text});
  await retainManagementEvidence(pool,f.goal,{results:[read]},noArtifacts,f.a.focus);
  await review(f,f.a.focus,[]);
  const nextCause={...f.a.focus,eventId:'103',failureKey:'c'.repeat(64)};
  expect((await readManagementRetrieval(pool,f.goal,noArtifacts,nextCause)).results).toEqual([]);
  const cached=await ledger(f,nextCause);
  expect(cached.entries).toHaveLength(1);
  expect(cached.entries[0]).toMatchObject({runId:f.a.runId,digest:read.digest,complete:true,pages:[{offset:0,text:f.a.text}]});
  expect((await ledger(f,f.b.focus)).entries).toEqual([]);
  expect((await readManagementEvidenceLedger(pool,f.goal,undefined,undefined,noArtifacts)).entries).toEqual([]);
  await pool.query("UPDATE tasks SET description='MUTABLE_FUTURE_DESCRIPTION' WHERE id=$1",[f.a.taskId]);
  expect((await ledger(f,nextCause)).entries[0].pages[0].text).toBe(f.a.text);
  const after=await actorState(f);expect(after.find(row=>row.id===f.a.taskId)?.instruction_snapshot).toEqual(before.find(row=>row.id===f.a.taskId)?.instruction_snapshot);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=ANY($1::uuid[])',[[f.a.runId,f.b.runId]])).rows[0].n).toBe(0);
});

it('a newer empty packet clears only its actor and survives rotation and late recording of an older review',async()=>{
  const f=await fixture(),old=await review(f,f.a.focus,[f.a.request]);
  const cleared=await review(f,{...f.a.focus,eventId:'103',failureKey:'c'.repeat(64)},[]);
  await review(f,f.b.focus,[f.b.request]);
  await event(pool,'MANAGEMENT_REVIEWED',f.goal,{runId:old.run,contextRequestFocus:f.a.focus,contextRequests:[f.a.request]});
  expect(await readManagementRetrieval(pool,f.goal,noArtifacts,f.a.focus)).toMatchObject({results:[],selection:{status:'FOCUS_RESUMED',reviewRunId:cleared.run}});
  expect((await readManagementRetrieval(pool,f.goal,noArtifacts,f.b.focus)).results[0].text).toBe(f.b.text);
});

it('ignores malformed manager run identities without throwing or hiding the actor review',async()=>{
  const f=await fixture(),first=await review(f,f.a.focus,[f.a.request]);
  await review(f,f.b.focus,[f.b.request]);
  for(const runId of [undefined,null,'not-a-uuid','00000000-0000-0000-0000-00000000000z',{},randomUUID()]){
    await event(pool,'MANAGEMENT_REVIEWED',f.goal,{runId,contextRequestFocus:f.a.focus,contextRequests:[]});
  }
  const before=await actorState(f);
  const recovered=await readManagementRetrieval(pool,f.goal,noArtifacts,f.a.focus);
  expect(recovered).toMatchObject({selection:{status:'FOCUS_RESUMED',reviewRunId:first.run}});
  expect(recovered.results[0]).toMatchObject({status:'OK',runId:f.a.runId,text:f.a.text});
  expect(await actorState(f)).toEqual(before);
});

it('excludes pre-actor events while preserving the inclusive actor creation boundary',async()=>{
  const f=await fixture(),first=await review(f,f.a.focus,[f.a.request]);
  await pool.query(`UPDATE events SET created_at=(SELECT created_at FROM runs WHERE id=$1)
    WHERE kind='MANAGEMENT_REVIEWED' AND payload->>'runId'=$2`,[f.a.runId,first.run]);
  const impossible=await review(f,f.a.focus,[]);
  await pool.query(`UPDATE events SET created_at=(SELECT created_at FROM runs WHERE id=$1)-interval '1 second'
    WHERE kind='MANAGEMENT_REVIEWED' AND payload->>'runId'=$2`,[f.a.runId,impossible.run]);
  await review(f,f.b.focus,[f.b.request]);
  const before=await actorState(f);
  const recovered=await readManagementRetrieval(pool,f.goal,noArtifacts,f.a.focus);
  expect(recovered).toMatchObject({selection:{status:'FOCUS_RESUMED',reviewRunId:first.run}});
  expect(recovered.results[0]).toMatchObject({status:'OK',runId:f.a.runId,text:f.a.text});
  expect(await actorState(f)).toEqual(before);
});

it.each(['unaccepted','invalid-review','frozen-identity','malformed-packet'] as const)('a newer %s review never resurrects an older actor packet',async(kind)=>{
  const f=await fixture();await review(f,f.a.focus,[f.a.request]);
  await review(f,f.a.focus,kind==='malformed-packet'?[{kind:'unknown'}]:[f.a.request],
    {...(kind==='unaccepted'?{passed:false}:{}),...(kind==='invalid-review'?{invalid:true}:{}),
      ...(kind==='frozen-identity'?{frozenFocus:f.b.focus}:{})});
  await review(f,f.b.focus,[f.b.request]);
  const result=await readManagementRetrieval(pool,f.goal,noArtifacts,f.a.focus);
  expect(result.results).toEqual([]);
  expect(kind==='malformed-packet'?'invalidRequests' in result:result.selection?.status==='FOCUS_CHANGED').toBe(true);
});

it('refuses another actor request inside the current focus and does not lend a live packet to a stopped case',async()=>{
  const f=await fixture();await review(f,f.a.focus,[f.b.request]);
  const result=await readManagementRetrieval(pool,f.goal,noArtifacts,f.a.focus);
  expect(result.results).toHaveLength(1);expect(result.results[0]).toMatchObject({status:'FOCUS_BINDING_MISMATCH'});
  expect(result.results[0].text).toBeUndefined();
  const stoppedFocus={caseId:randomUUID(),taskId:f.b.taskId,scope:f.b.scope};
  expect(await readManagementRetrieval(pool,f.goal,noArtifacts,stoppedFocus)).toMatchObject({results:[],selection:{status:'FOCUS_CHANGED'}});
  expect(await readManagementRetrieval(pool,f.goal,noArtifacts)).toMatchObject({results:[],selection:{status:'FOCUS_CHANGED'}});
  expect(await readManagementRetrieval(pool,f.goal,noArtifacts,{taskId:f.b.taskId,scope:f.b.scope}))
    .toMatchObject({results:[],selection:{status:'FOCUS_CHANGED'}});
});

it.each(['fence','context','attempt','cancel','terminal','runtime','digest'] as const)('invalidates cached frozen-live pages after %s changes without changing the actor',async(kind)=>{
  const f=await fixture(),read=await page(f);
  await retainManagementEvidence(pool,f.goal,{results:[read]},noArtifacts,f.a.focus);
  if(kind==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.a.runId,randomUUID()]);
  if(kind==='context'){
    const context=randomUUID();await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','{}')",[context,f.a.runId]);
    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.a.runId,context]);
  }
  if(kind==='attempt')await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.a.taskId]);
  if(kind==='cancel')await event(pool,'CANCEL_REQUESTED',f.a.taskId,{runId:f.a.runId});
  if(kind==='terminal')await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[f.a.runId]);
  if(kind==='runtime')await pool.query("UPDATE runs SET runtime='opencode' WHERE id=$1",[f.a.runId]);
  if(kind==='digest')await pool.query("UPDATE settings SET value=jsonb_set(value,'{entries,0,digest}',to_jsonb($2::text)) WHERE id=$1",['management-evidence:'+f.goal,'f'.repeat(64)]);
  const before=await actorState(f);expect((await ledger(f)).entries).toEqual([]);expect(await actorState(f)).toEqual(before);
});

it('refuses fabricated page bytes and a cache request bound to another focus before persistence',async()=>{
  const f=await fixture(),read=await page(f);await retainManagementEvidence(pool,f.goal,{results:[read]},noArtifacts,f.a.focus);
  const before=await savedLedger(f);
  await expect(retainManagementEvidence(pool,f.goal,{results:[{...read,text:'FORGED_INSTRUCTION_BYTES'}]},noArtifacts,f.a.focus)).rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
  expect(await savedLedger(f)).toEqual(before);
  await retainManagementEvidence(pool,f.goal,{results:[await page(f,f.b)]},noArtifacts,f.a.focus);
  expect((await savedLedger(f)).entries).toHaveLength(1);expect((await ledger(f,f.b.focus)).entries).toEqual([]);
});

it('never relaxes the generic stopped-task and artifact readers when a live instruction is cached',async()=>{
  const f=await fixture();await retainManagementEvidence(pool,f.goal,{results:[await page(f)]},noArtifacts,f.a.focus);
  for(const kind of ['task','task_metadata','artifacts','artifact'] as const){
    const result=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,{kind,taskId:f.a.taskId,expectedAttempt:1,offset:0,
      ...(kind==='artifact'?{name:'result.json'}:{})} as any);
    expect(result.status).toBe('LIVE_SOURCE');expect(result.text).toBeUndefined();
  }
});
