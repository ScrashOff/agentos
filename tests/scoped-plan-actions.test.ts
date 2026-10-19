import { beforeAll,beforeEach,afterAll,it,expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool,migrate,tx,event } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installBacklogPlanner } from '../server/backlog.js';
import { installPlanningDemand,scopeSnapshot } from '../server/planning-demand.js';
import { installSupervision } from '../server/supervision.js';
import { scopedSourceRevision,type ScopedPlanAction } from '../server/scoped-plan-contract.js';
import { applyScopedPlanAction } from '../server/scoped-plan-actions.js';
import { stageDeferredHandoff } from '../server/deferred-handoffs.js';
import { completeRun,validateTask,writeArtifact } from '../server/service.js';

beforeAll(async()=>{await migrate();await installBacklogPlanner();await installPlanningDemand();await installSupervision();});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
 await pool.query("DELETE FROM settings WHERE id='autonomy:scope-actions-test'");
});
afterAll(async()=>{await pool.end();});

async function fixture(options:{verify?:boolean;live?:boolean;failed?:boolean;baseline?:boolean}={}){
 const goal=randomUUID(),source=randomUUID(),target=randomUUID(),requestId=randomUUID(),planner=randomUUID();
 const scope='test/action-scope';
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'An actual delivery','Keep all original acceptance criteria','scope-actions-test',$2)",[goal,'goal-'+goal]);
 for(const [id,value] of [['workforce:'+goal,{enabled:true,glmTarget:12,glmAccountId:'glm'}],['supervision:'+goal,{enabled:true}]])
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',[id,JSON.stringify(value)]);
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
  VALUES('glm','GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12,true,'glm')`);
 const insertTask=async(id:string,key:string,stage:string,execution:any,criteria:any[],state='ready')=>
  pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
   VALUES($1,$2,$3,'Concrete scoped delivery',$4,'qa.backend',$5,$6,$7,$8)`,
   [id,goal,key,'Exact immutable instructions for '+key,state,stage,JSON.stringify(criteria),JSON.stringify({kind:'opencode',workforceLane:'glm',baseWorkspace:scope,workspace:'test/'+key,...execution})]);
 const createRun=async(taskId:string)=>{
  const run=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[taskId]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id) VALUES($1,$2,1,$3,'opencode',1,'running','glm')",[run,taskId,fence]);
  return {run,fence};
 };
 const mandatory=[{id:'passed',label:'Original real acceptance',type:'json_field',name:'verification.json',field:'passed',equals:true}];
 await insertTask(target,'consumer',options.verify?'verify':'integrate',{readOnlySource:options.verify===true},mandatory,
  options.verify?'blocked_dependencies':'ready');
 await insertTask(source,'accepted-source',options.verify?'integrate':'work',options.verify?{}:{workKind:'repair'},
  [{id:'source_bytes',label:'Real source artifact',type:'artifact',name:options.verify?'candidate-report.json':'implementation.patch'}]);
 const sourceRun=await createRun(source);
 const sourceArtifact=await writeArtifact(sourceRun.run,sourceRun.fence,options.verify?'candidate-report.json':'implementation.patch',
  options.verify?JSON.stringify({candidate:'immutable',tested:true}):'--- a/module.js\n+++ b/module.js\n@@ -1 +1 @@\n-old\n+fixed\n');
 await completeRun(sourceRun.run,sourceRun.fence,'Accepted source bytes',{});expect((await validateTask(source)).passed).toBe(true);
 if(options.verify)await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[target,source]);
 let targetRun:{run:string;fence:string}|undefined;
 if(options.live || options.failed){
  targetRun=await createRun(target);
  if(options.failed){await completeRun(targetRun.run,targetRun.fence,'Verification has a real unmet criterion',{});
   expect((await validateTask(target)).passed).toBe(false);}
 }
 let baseline:string|undefined,priorHandoff:any;
 if(options.baseline){
  baseline=randomUUID();await insertTask(baseline,'original-baseline','work',{workKind:'diagnostic'},[]);
  const baselineRun=await createRun(baseline);await writeArtifact(baselineRun.run,baselineRun.fence,'source-report.json','{"source":"unchanged"}');
  await completeRun(baselineRun.run,baselineRun.fence,'Earlier accepted baseline',{});await validateTask(baseline);
  await tx(async db=>{
   const original=(await db.query('SELECT * FROM tasks WHERE id=$1',[baseline])).rows[0];
   const consumer=(await db.query('SELECT * FROM tasks WHERE id=$1',[target])).rows[0];
   await stageDeferredHandoff(db,goal,source,original,consumer,{taskId:source,plannerRunId:sourceRun.run,
    coverageKey:'accepted-existing-repair',lane:'glm',workKind:'repair'});
  });
  priorHandoff=(await pool.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1',[source])).rows[0];
 }
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
 const snapshot=await scopeSnapshot(pool,goal,scope),revision=scopedSourceRevision(snapshot.tasks,snapshot.artifacts,snapshot.deferredDeliveries);
 await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,reason,snapshot)
  VALUES($1,$2,$3,'glm',$4,'DELIVERY_EVIDENCE_CHANGED',$5)`,[requestId,goal,scope,revision,JSON.stringify(snapshot)]);
 await insertTask(planner,'scope-planner','work',{workKind:'planning',planningRequestId:requestId},
  [{id:'plan',label:'Published structured plan',type:'artifact',name:'scope-plan.json'}]);
 await pool.query("UPDATE planning_requests SET state='assigned',task_id=$2 WHERE id=$1",[requestId,planner]);
 const action:ScopedPlanAction={kind:options.verify?'request_verification':'prepare_integration',taskId:target,
  expectedAttempt:targetRun?1:0,sourceTaskId:source,expectedSourceRunId:sourceRun.run,artifactIds:[sourceArtifact.id],
  reason:'Consume the accepted evidence and progress the original delivery with its immutable acceptance criteria.'};
 const plan={version:1,requestId,sourceRevision:revision,reason:'Plan a concrete transition using the accepted bytes already available.',tasks:[],blockers:[],actions:[action]};
 const planRun=await createRun(planner),planArtifact=await writeArtifact(planRun.run,planRun.fence,'scope-plan.json',JSON.stringify(plan));
 await completeRun(planRun.run,planRun.fence,'A proposed delivery transition',{});expect((await validateTask(planner)).passed).toBe(true);
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
 const request={id:requestId,goal_id:goal,scope,run_id:planRun.run};
 return {goal,scope,source,target,planner,request,action,sourceRun,targetRun,sourceArtifact,planRun,planArtifact,baseline,priorHandoff};
}

async function consumerEvidence(id:string){
 return {task:(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0],
  runs:(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[id])).rows,
  dependencies:(await pool.query('SELECT * FROM dependencies WHERE task_id=$1 ORDER BY depends_on',[id])).rows};
}

it('stages one accepted repair concurrently without altering a live consumer or its final acceptance',async()=>{
 const f=await fixture({live:true}),before=await consumerEvidence(f.target);
 const results=await Promise.all([applyScopedPlanAction(f.request,f.action),applyScopedPlanAction(f.request,f.action),applyScopedPlanAction(f.request,f.action)]);
 expect(results.every(receipt=>receipt.applied && receipt.code==='HANDOFF_STAGED')).toBe(true);
 expect(results.filter(receipt=>receipt.existing)).toHaveLength(2);
 expect((await pool.query('SELECT state FROM deferred_handoffs WHERE producer_task_id=$1',[f.source])).rows).toEqual([{state:'staged'}]);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_DELIVERY_ACTION_RECORDED'")).rows[0].n).toBe(1);
 expect(await consumerEvidence(f.target)).toEqual(before);
 expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.target])).rowCount).toBe(0);
});

it('reuses an existing handoff bound to an earlier baseline without rewriting its reservation',async()=>{
 const f=await fixture({live:true,baseline:true});
 expect(f.priorHandoff.source_task_id).toBe(f.baseline);expect(f.baseline).not.toBe(f.source);
 expect(await applyScopedPlanAction(f.request,f.action)).toMatchObject({applied:true,handoffId:f.priorHandoff.id});
 expect((await pool.query('SELECT * FROM deferred_handoffs WHERE id=$1',[f.priorHandoff.id])).rows[0]).toEqual(f.priorHandoff);
 expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.target])).rowCount).toBe(0);
});

it('requires the exact action published by the current accepted planner',async()=>{
 const f=await fixture();
 expect(await applyScopedPlanAction(f.request,{...f.action,reason:'An invented alternative action was never published by this planner.'})).toMatchObject({applied:false,code:'PLANNING_ACTION_NOT_PUBLISHED'});
 expect(await applyScopedPlanAction({...f.request,run_id:randomUUID()},f.action)).toMatchObject({applied:false,code:'PLANNING_ACTION_AUTHORITY'});
 await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[f.planRun.run]);
 expect(await applyScopedPlanAction(f.request,f.action)).toMatchObject({applied:false,code:'PLANNING_ACTION_AUTHORITY'});
 expect((await pool.query('SELECT 1 FROM deferred_handoffs')).rowCount).toBe(0);
});

it.each(['pause','autonomy','manual'] as const)('honors %s before any task or handoff mutation',async(mode)=>{
 const f=await fixture();
 if(mode==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 if(mode==='autonomy')await pool.query("INSERT INTO settings VALUES('autonomy:scope-actions-test','{\"enabled\":false}')");
 if(mode==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.target]);
 const before=await consumerEvidence(f.target);
 expect((await applyScopedPlanAction(f.request,f.action)).applied).toBe(false);
 expect(await consumerEvidence(f.target)).toEqual(before);
 expect((await pool.query('SELECT 1 FROM deferred_handoffs')).rowCount).toBe(0);
});

it('defers a paused action without consuming its idempotency key, then applies once after resume',async()=>{
 const f=await fixture();
 await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 expect(await applyScopedPlanAction(f.request,f.action)).toMatchObject({applied:false,preparing:true,code:'PLANNING_ACTION_DISABLED'});
 expect((await pool.query("SELECT 1 FROM events WHERE kind='SCOPED_DELIVERY_ACTION_RECORDED'")).rowCount).toBe(0);
 await pool.query("UPDATE planning_requests SET state='preparing',receipt=$2 WHERE id=$1",
  [f.request.id,JSON.stringify({evaluatedRunId:f.planRun.run})]);
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
 expect(await applyScopedPlanAction(f.request,f.action)).toMatchObject({applied:true});
 expect(await applyScopedPlanAction(f.request,f.action)).toMatchObject({applied:true,existing:true});
 expect((await pool.query('SELECT 1 FROM deferred_handoffs')).rowCount).toBe(1);
});

it.each(['description','criterion','workspace','scope','dependency','cancel'] as const)('rejects a target changed after planning: %s',async(change)=>{
 const f=await fixture({live:true});
 if(change==='description')await pool.query("UPDATE tasks SET description=description||' New required constraint.' WHERE id=$1",[f.target]);
 if(change==='criterion')await pool.query("UPDATE tasks SET criteria=criteria||'[{\"id\":\"additional\",\"type\":\"artifact\",\"name\":\"other.json\",\"label\":\"Must be retained\"}]'::jsonb WHERE id=$1",[f.target]);
 if(change==='workspace'||change==='scope')await pool.query('UPDATE tasks SET execution=jsonb_set(execution,$2::text[],to_jsonb($3::text)) WHERE id=$1',
  [f.target,[change==='scope'?'baseWorkspace':'workspace'],'test/changed']);
 if(change==='dependency')await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.target,f.source]);
 if(change==='cancel')await event(pool,'CANCEL_REQUESTED',f.target,{runId:f.targetRun!.run});
 const before=await consumerEvidence(f.target);
 expect((await applyScopedPlanAction(f.request,f.action)).applied).toBe(false);
 expect(await consumerEvidence(f.target)).toEqual(before);
 expect((await pool.query('SELECT 1 FROM deferred_handoffs')).rowCount).toBe(0);
});

it.each(['acceptance','new_attempt','manifest','description'] as const)('rejects changed accepted source evidence: %s',async(change)=>{
 const f=await fixture();
 if(change==='acceptance')await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[f.sourceRun.run]);
 if(change==='new_attempt')await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.source]);
 if(change==='manifest')await pool.query("UPDATE artifacts SET digest=repeat('f',64) WHERE id=$1",[f.sourceArtifact.id]);
 if(change==='description')await pool.query("UPDATE tasks SET description=description||' New source requirement.' WHERE id=$1",[f.source]);
 expect((await applyScopedPlanAction(f.request,f.action)).applied).toBe(false);
 expect((await pool.query('SELECT 1 FROM deferred_handoffs')).rowCount).toBe(0);
});

it('requests supervision for failed readonly verification without restarting or accepting it',async()=>{
 const f=await fixture({verify:true,failed:true}),before=await consumerEvidence(f.target);
 expect(await applyScopedPlanAction(f.request,f.action)).toMatchObject({applied:true,code:'SUPERVISION_REQUESTED'});
 const item=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.target])).rows[0];
 expect(item).toMatchObject({state:'open',source_run_id:f.targetRun!.run,decision:null,decision_run_id:null});
 expect(item.evidence).toContainEqual(expect.objectContaining({kind:'SCOPED_DELIVERY_ACTION',sourcePlanRunId:f.planRun.run}));
 expect(await consumerEvidence(f.target)).toEqual(before);
 expect((await pool.query('SELECT passed FROM acceptances WHERE task_id=$1',[f.target])).rows).toEqual([{passed:false}]);
 expect((await pool.query('SELECT 1 FROM deferred_handoffs')).rowCount).toBe(0);
});

it('never turns a verification request into write access or a fabricated supervisor decision',async()=>{
 const f=await fixture({verify:true,failed:true});
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{readOnlySource}','false') WHERE id=$1",[f.target]);
 expect(await applyScopedPlanAction(f.request,f.action)).toMatchObject({applied:false,code:'VERIFICATION_CONTRACT_CHANGED'});
 expect((await pool.query('SELECT 1 FROM supervision_cases WHERE task_id=$1',[f.target])).rowCount).toBe(0);
});

it('preserves an existing Sol decision while recording new planner evidence for review',async()=>{
 const f=await fixture({verify:true,failed:true}),caseId=randomUUID(),decision={kind:'wait_for_evidence',reason:'Existing reviewed decision'};
 await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,decision_run_id)
  VALUES($1,$2,$3,$4,'decided','Original case','[]',$5,$6)`,[caseId,f.goal,f.target,f.targetRun!.run,JSON.stringify(decision),f.planRun.run]);
 const before=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
 expect(await applyScopedPlanAction(f.request,f.action)).toMatchObject({applied:false,code:'SUPERVISION_DECISION_PENDING',caseId});
 expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0]).toEqual(before);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SCOPED_ACTION_SUPERVISION_CONFLICT'")).rows[0].n).toBe(1);
});

it('accepts a newly completed planner after an old rejected run was recorded for supervision',async()=>{
 const f=await fixture();
 await pool.query("UPDATE planning_requests SET state='awaiting_supervision',receipt=$2 WHERE id=$1",
  [f.request.id,JSON.stringify({evaluatedRunId:randomUUID(),code:'PLANNING_CONTRACT_INVALID'})]);
 expect((await applyScopedPlanAction(f.request,f.action)).applied).toBe(true);
});
