import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installPlanningDemand,createPlanningDemands,assignPlanningRequest,recoverScopedPlannerContexts,scopeSnapshot} from '../server/planning-demand.js';
import {completeRun,validateTask,writeArtifact} from '../server/service.js';
import * as supervisionContext from '../server/supervision-context.js';
import {selectPlannerPreparationSource,plannerPreparationBindingVersion,plannerWorkspace} from '../server/planner-preparation-binding.js';

const scope='test/binding';
const sha=(value:Buffer)=>createHash('sha256').update(value).digest('hex');
beforeAll(async()=>{await migrate();await installBacklogPlanner();await installManagement();await installPlanningDemand();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterEach(()=>vi.restoreAllMocks());afterAll(async()=>{await pool.end();});

async function fixture(large=false){
 const goal=randomUUID(),consumer=randomUUID();
 await pool.query("DELETE FROM settings WHERE id='autonomy:test'");
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Source binding','Preserve actual acceptance','test',$2)",[goal,'goal-'+goal]);
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
   VALUES('glm','GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'glm')`);
 await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+goal,JSON.stringify({enabled:true,glmAccountId:'glm',glmTarget:12,museTarget:0})]);
 const sources=[];
 for(let i=0;i<2;i++){
  const id=randomUUID(),run=randomUUID(),fence=randomUUID(),workspace='test/source-'+i;
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
    VALUES($1,$2,$3,'Accepted source','Immutable diagnostics','qa.backend','running','work','[]',1,$4)`,
    [id,goal,'source-'+i,JSON.stringify({kind:'opencode',baseWorkspace:scope,workspace,workKind:'diagnostic'})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,id,fence]);
  await writeArtifact(run,fence,'protocol-report.json','{"suiteExecuted":true,"exitCode":0,"cases":["source"],"limitations":[]}');
  await completeRun(run,fence,'Source accepted',{});await validateTask(id);
  sources.push({id,run,workspace});
 }
 const intent=large?'Exact source requirement '.repeat(375000):'Keep the actual final criterion';
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,'consumer','Candidate',''||$3,'engineering.integration','recovery_blocked','integrate',$4,$5)`,[
    consumer,goal,intent,JSON.stringify([{id:'final',label:'Final proof',type:'artifact',name:'final-proof.json'}]),
    JSON.stringify({kind:'harness',baseWorkspace:scope,workspace:'test/candidate'})]);
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
 await createPlanningDemands(goal);
 const request=(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1',[goal])).rows[0];
 sources.sort((a,b)=>a.id.localeCompare(b.id));
 const bound=sources[1],preparation=randomUUID();
 await pool.query(`INSERT INTO workspace_preparations(id,goal_id,workspace,source_workspace,state,receipt)
   VALUES($1,$2,$3,$4,'prepared','{"confirmed":true}')`,[preparation,goal,plannerWorkspace(request),bound.workspace]);
 // Simulate recovery rotating the discovery window away from the existing folder.
 const visible=request.snapshot.opportunities.acceptedSources.filter((s:any)=>s.taskId!==bound.id);
 await pool.query("UPDATE planning_requests SET snapshot=jsonb_set(snapshot,'{opportunities,acceptedSources}',$2) WHERE id=$1",[request.id,JSON.stringify(visible)]);
 vi.spyOn(supervisionContext,'supervisionInputs').mockResolvedValue({inputs:[],provenance:[],omitted:[]} as any);
 return {goal,consumer,request,bound,other:sources[0],preparation,intent};
}

const pureSource=(id:string,patch:any={})=>({id,last_run_id:'run-'+id,state:'accepted',has_live_run:false,last_run_state:'submitted',
 last_run_attempt:1,attempt_count:1,acceptance_passed:true,execution:{workKind:'diagnostic',workspace:'test/'+id,baseWorkspace:scope},...patch});
it('chooses the same new source independent of snapshot order and discovery window order',()=>{
 const a=pureSource('a'),b=pureSource('b');
 const request={id:randomUUID(),goal_id:randomUUID(),scope,snapshot:{tasks:[b,a],opportunities:{acceptedSources:[{taskId:'b'}]}}};
 expect(selectPlannerPreparationSource(request)?.taskId).toBe('a');
 request.snapshot.tasks.reverse();request.snapshot.opportunities.acceptedSources=[];
 expect(selectPlannerPreparationSource(request)?.taskId).toBe('a');
});
it.each(['removed','live','not-accepted','not-current','planning','ambiguous','other-scope'])('refuses a bound %s source instead of retargeting its folder',mode=>{
 const a=pureSource('a'),b=pureSource('b'),request={id:randomUUID(),goal_id:randomUUID(),scope,snapshot:{tasks:[a,b]}};
 if(mode==='removed')request.snapshot.tasks=[b];
 if(mode==='live')a.has_live_run=true;
 if(mode==='not-accepted')a.acceptance_passed=false;
 if(mode==='not-current')a.last_run_attempt=2;
 if(mode==='planning')a.execution.workKind='planning';
 if(mode==='ambiguous')b.execution.workspace=a.execution.workspace;
 if(mode==='other-scope')a.execution.baseWorkspace='test/other';
 expect(()=>selectPlannerPreparationSource(request,{goal_id:request.goal_id,workspace:plannerWorkspace(request),source_workspace:'test/a'}))
  .toThrow('PLANNER_PREPARATION_SOURCE_INELIGIBLE');
});
it('rejects a preparation owned by another goal even when its source path matches',()=>{
 const request={id:randomUUID(),goal_id:randomUUID(),scope,snapshot:{tasks:[pureSource('a')]}};
 expect(()=>selectPlannerPreparationSource(request,{goal_id:randomUUID(),workspace:plannerWorkspace(request),source_workspace:'test/a'}))
  .toThrow('PREPARATION_SOURCE_CHANGED');
});
it('keeps the exact existing source beyond the discovery window and mounts a large intact catalog',async()=>{
 const f=await fixture(true);
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt='{\"code\":\"PREPARATION_SOURCE_CHANGED\"}' WHERE id=$1",[f.request.id]);
 expect(await recoverScopedPlannerContexts()).toEqual([f.request.id]);
 const taskId=await assignPlanningRequest(f.request.id);
 const task=(await pool.query('SELECT execution FROM tasks WHERE id=$1',[taskId])).rows[0];
 expect(task.execution.sourceWorkspace).toBe(f.bound.workspace);
 const prep=(await pool.query('SELECT id,source_workspace,attempt,state FROM workspace_preparations WHERE id=$1',[f.preparation])).rows[0];
 expect(prep).toEqual({id:f.preparation,source_workspace:f.bound.workspace,attempt:0,state:'prepared'});
 const index=JSON.parse(Buffer.from(task.execution.inputs.find((i:any)=>i.name==='planning-catalog.json').base64,'base64').toString());
 expect(index.format).toBe('PLANNER_CATALOG_PARTS_V1');
 const bytes=Buffer.concat(index.parts.map((part:any)=>{
  const input=task.execution.inputs.find((i:any)=>i.id===part.id),data=Buffer.from(input.base64,'base64');
  expect(sha(data)).toBe(part.digest);return data;
 }));
 expect(sha(bytes)).toBe(index.catalogDigest);
 const catalog=JSON.parse(bytes.toString());
 expect(catalog.tasks.find((t:any)=>t.id===f.consumer).intent===f.intent).toBe(true);
 expect((await pool.query('SELECT criteria FROM tasks WHERE id=$1',[f.consumer])).rows[0].criteria)
   .toEqual([{id:'final',label:'Final proof',type:'artifact',name:'final-proof.json'}]);
 expect((await pool.query('SELECT count(*)::int n FROM workspace_preparations WHERE goal_id=$1',[f.goal])).rows[0].n).toBe(1);
});
it('coalesces concurrent admissions without creating a second planner or preparation',async()=>{
 const f=await fixture(),ids=await Promise.all([assignPlanningRequest(f.request.id),assignPlanningRequest(f.request.id)]);
 expect(ids.filter(Boolean)).toHaveLength(1);
 expect((await pool.query('SELECT count(*)::int n FROM tasks WHERE goal_id=$1 AND execution->>\'workKind\'=\'planning\'',[f.goal])).rows[0].n).toBe(1);
});
it.each(['request','binding','source-run','pause'])('rechecks %s after asynchronous input collection',async change=>{
 const f=await fixture();let enter!:()=>void,release!:()=>void;
 const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
 vi.mocked(supervisionContext.supervisionInputs).mockImplementation(async()=>{enter();await held;return {inputs:[],provenance:[],omitted:[]} as any;});
 const assigning=assignPlanningRequest(f.request.id).then(id=>({id}),error=>({error:error.message}));
 await Promise.race([entered,assigning.then(()=>{throw Error('INPUT_COLLECTION_NOT_REACHED');})]);
 try{
  if(change==='request')await pool.query("UPDATE planning_requests SET receipt=receipt||'{\"reviewed\":true}'::jsonb WHERE id=$1",[f.request.id]);
  if(change==='binding')await pool.query('UPDATE workspace_preparations SET source_workspace=$2 WHERE id=$1',[f.preparation,f.other.workspace]);
  // Run state is not in the semantic source revision: a precise eligibility
  // recheck must catch this even with unchanged attempt, acceptance and artifacts.
  if(change==='source-run')await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[f.bound.run]);
  if(change==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 }finally{release();}
 const result=await assigning;
 if(change==='binding')expect(result).toEqual({error:'PREPARATION_SOURCE_CHANGED'});
 else if(change==='source-run')expect(result).toEqual({error:'PLANNER_PREPARATION_SOURCE_INELIGIBLE'});
 else expect(result).toEqual({id:null});
 expect((await pool.query('SELECT task_id FROM planning_requests WHERE id=$1',[f.request.id])).rows[0].task_id).toBe(null);
});
it('recovers binding once per version, keeps actor-owned requests untouched and supersedes obsolete sources',async()=>{
 const f=await fixture();
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt='{\"code\":\"PREPARATION_SOURCE_CHANGED\"}' WHERE id=$1",[f.request.id]);
 expect(await recoverScopedPlannerContexts()).toEqual([f.request.id]);
 const receipt=(await pool.query('SELECT receipt FROM planning_requests WHERE id=$1',[f.request.id])).rows[0].receipt;
 expect(receipt).toMatchObject({preparationBindingVersion:plannerPreparationBindingVersion,recoveryCode:'PREPARATION_BINDING_UPDATED'});
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision' WHERE id=$1",[f.request.id]);
 expect(await recoverScopedPlannerContexts()).toEqual([]);
 await pool.query("UPDATE planning_requests SET receipt='{\"code\":\"PREPARATION_SOURCE_CHANGED\"}',task_id=$2 WHERE id=$1",[f.request.id,f.consumer]);
 expect(await recoverScopedPlannerContexts()).toEqual([]);
 await pool.query('UPDATE planning_requests SET task_id=NULL WHERE id=$1',[f.request.id]);
 await pool.query("UPDATE artifacts SET digest=repeat('b',64) WHERE run_id=$1",[f.bound.run]);
 expect(await recoverScopedPlannerContexts()).toEqual([]);
 expect((await pool.query('SELECT state FROM planning_requests WHERE id=$1',[f.request.id])).rows[0].state).toBe('superseded');
 expect((await pool.query('SELECT source_workspace FROM workspace_preparations WHERE id=$1',[f.preparation])).rows[0].source_workspace).toBe(f.bound.workspace);
});
it('refuses an ineligible bound source once without retrying or overwriting the old preparation',async()=>{
 const f=await fixture();
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt='{\"code\":\"PREPARATION_SOURCE_CHANGED\"}' WHERE id=$1",[f.request.id]);
 await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[f.bound.run]);
 expect(await recoverScopedPlannerContexts()).toEqual([]);
 const state=(await pool.query('SELECT state,receipt FROM planning_requests WHERE id=$1',[f.request.id])).rows[0];
 expect(state).toMatchObject({state:'awaiting_supervision',receipt:{preparationBindingVersion:plannerPreparationBindingVersion,recoveryCode:'PLANNER_PREPARATION_SOURCE_INELIGIBLE'}});
 expect(await recoverScopedPlannerContexts()).toEqual([]);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_PLANNING_BINDING_RECOVERY_BLOCKED' AND entity_id=$1",[f.goal])).rows[0].n).toBe(1);
});
it('does not recover a request while another planner actor owns the same scope',async()=>{
 const f=await fixture(),task=randomUUID(),run=randomUUID();
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt='{\"code\":\"PREPARATION_SOURCE_CHANGED\"}' WHERE id=$1",[f.request.id]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
   VALUES($1,$2,$3,'Other planner','Planning task with live actor','research.requirements','running','work','[]',1,$4)`,
   [task,f.goal,'other-'+task,JSON.stringify({kind:'opencode',baseWorkspace:scope,workspace:'test/o-other',workKind:'planning'})]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'opencode',1,'running')",[run,task,randomUUID()]);
 expect(await recoverScopedPlannerContexts()).toEqual([]);
 expect((await pool.query('SELECT state FROM planning_requests WHERE id=$1',[f.request.id])).rows[0].state).toBe('awaiting_supervision');
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[run])).rows[0].state).toBe('running');
});
it.each(['request','source','binding'])('rejects recovery after a concurrent %s change without holding the admission lock during reads',async change=>{
 const f=await fixture();
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt='{\"code\":\"PREPARATION_SOURCE_CHANGED\"}' WHERE id=$1",[f.request.id]);
 let enter!:()=>void,release!:()=>void;const entered=new Promise<void>(r=>enter=r),held=new Promise<void>(r=>release=r);
 const recovering=recoverScopedPlannerContexts(async(db,...args)=>{
  expect((await db.query("SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND objid=4280001 AND granted")).rowCount).toBe(0);
  enter();await held;return scopeSnapshot(db,...args);
 });
 await Promise.race([entered,recovering.then(()=>{throw Error('CATALOG_NOT_REACHED');})]);
 try{
  if(change==='request')await pool.query("UPDATE planning_requests SET receipt=receipt||'{\"reviewed\":true}'::jsonb WHERE id=$1",[f.request.id]);
  if(change==='source')await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[f.bound.run]);
  if(change==='binding')await pool.query('UPDATE workspace_preparations SET source_workspace=$2 WHERE id=$1',[f.preparation,f.other.workspace]);
 }finally{release();}
 expect(await recovering).toEqual([]);
 expect((await pool.query('SELECT state,task_id FROM planning_requests WHERE id=$1',[f.request.id])).rows[0]).toEqual({state:'awaiting_supervision',task_id:null});
});
