import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx,lock} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner,applyBacklog} from '../server/backlog.js';
import {installPlanningDemand} from '../server/planning-demand.js';
import {installManagement} from '../server/management.js';
import {installSupervision} from '../server/supervision.js';
import {accountBridge} from '../server/bridge.js';
import {writeArtifact,completeRun,validateTask,readArtifact} from '../server/service.js';
import {recordScopedPlanningEffects,recoverPartialPlanningRemainders,preparePlanningRemainderReplacement,
  checkPlanningRemainderReplacement,linkPlanningRemainderReplacement,readPlanningDemands} from '../server/planning-remainders.js';
import {planningProposalDigest} from '../server/planning-remainder-contract.js';
import {hasActionableManagementConsumption,canWaitForManagementFacts} from '../server/management-materiality.js';
import {readManagementRetrieval} from '../server/management-retrieval.js';
import {scopedPlanPreflight,scopedPlanningLaneHints} from '../server/scoped-plan-contract.js';
import {projectScopedPlannerContext} from '../server/scoped-planner-context.js';

beforeAll(async()=>{await migrate();await installBacklogPlanner();await installPlanningDemand();await installManagement();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='autonomy:test'");vi.spyOn(accountBridge,'request').mockResolvedValue({});});
afterEach(()=>vi.restoreAllMocks());afterAll(async()=>{await pool.end();});

async function accepted(goal:string,stage:string,artifactName?:string,body?:unknown){
  const task=randomUUID(),run=randomUUID(),fence=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count,execution)
    VALUES($1,$2,$3,'Accepted fixture','MANAGEMENT_V1','qa.backend','running',$4,'[]',1,$5)`,
    [task,goal,'fixture-'+task,stage,JSON.stringify({kind:'harness',baseWorkspace:'test/source',workspace:'test/'+task,workKind:'diagnostic'})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[run,task,fence]);
  if(artifactName)await writeArtifact(run,fence,artifactName,JSON.stringify(body));
  await completeRun(run,fence,'Published immutable fixture',{});await validateTask(task);
  return {task,run,fence};
}
async function fixture(count=6){
  const goal=randomUUID(),consumer=randomUUID(),requestId=randomUUID(),sourceRevision='a'.repeat(64);
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Delivery','Original criteria','test',$2)",[goal,'goal-'+goal]);
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown)
    VALUES('glm','GLM','openai-compatible','glm-5.3-flash','["glm-5.3-flash"]',true,12,true)`);
  for(const [kind,value] of Object.entries({workforce:{enabled:true,glmTarget:12,glmAccountId:'glm',museTarget:0,workerAccounts:[]},management:{enabled:true}}))
    await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',[kind+':'+goal,JSON.stringify(value)]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,'consumer','Final consumer','Complete the unchanged canonical delivery','engineering.integration','recovery_blocked','integrate',
      '[{"id":"verified","type":"artifact","name":"verification.json","label":"Final verification"}]',$3)`,
    [consumer,goal,JSON.stringify({kind:'harness',baseWorkspace:'test/source',workspace:'test/current'})]);
  const source=await accepted(goal,'work','protocol-report.json',{suiteExecuted:true,exitCode:1,command:'node real.mjs',cases:['real gap'],limitations:[]});
  const tasks=Array.from({length:count},(_,index)=>({coverageKey:'independent-case-'+index,title:'Independent boundary '+index,
    description:'Exercise the distinct functional boundary number '+index+' and publish the actual regression evidence.',
    baseWorkspace:'test/source',sourceTaskId:source.task,deliverToTaskId:consumer,workKind:'diagnostic' as const,
    lane:index===count-1?'muse' as const:'glm' as const,delivery:'deferred' as const}));
  const plan={version:1,requestId,sourceRevision,reason:'Cover every distinct boundary in the immutable source',tasks,actions:[],blockers:[],dependencyChanges:[]};
  const planner=await accepted(goal,'work','scope-plan.json',plan);
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,task_id)
    VALUES($1,$2,'test/source','glm',$3,'assigned','Missing coverage','{}',$4)`,[requestId,goal,sourceRevision,planner.task]);
  const request=(await pool.query('SELECT * FROM planning_requests WHERE id=$1',[requestId])).rows[0];
  Object.assign(request,{run_id:planner.run,fence:planner.fence});
  const artifact=(await pool.query("SELECT id,digest FROM artifacts WHERE run_id=$1 AND name='scope-plan.json'",[planner.run])).rows[0];
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  return {goal,consumer,source,planner,plan,request,artifact};
}
async function applyOriginal(f:Awaited<ReturnType<typeof fixture>>,mixed=false){
  const created=await applyBacklog(f.goal,f.planner.run,{reason:f.plan.reason,tasks:f.plan.tasks});
  const backlog=(await pool.query('SELECT state,receipts,task_ids FROM workforce_backlog_plans WHERE run_id=$1',[f.planner.run])).rows[0];
  const receipt={code:mixed?'DELIVERY_ACTION_REVIEW_REQUIRED':backlog.state,evaluatedRunId:f.planner.run,backlog,
    blockers:[],dependencyReceipts:[],deliveryActionReceipts:mixed?[{applied:false,preparing:false,code:'SUPERVISION_UNAVAILABLE'}]:[]};
  return {created,backlog,receipt};
}
const current=async(f:any)=>(await pool.query('SELECT * FROM planning_requests WHERE id=$1',[f.request.id])).rows[0];
async function staged(f:any,mixed=false){
  const applied=await applyOriginal(f,mixed);
  expect(await recordScopedPlanningEffects(f.request,f.artifact,f.plan,applied.receipt,mixed?'awaiting_supervision':'applied')).toBe(true);
  return {...applied,row:await current(f)};
}
async function correction(f:any,row:any){
  const item=row.receipt.remaining.items.find((v:any)=>v.status==='pending');
  const proposal={...f.plan.tasks[item.reference.index],lane:'glm' as const,remainingPlanItem:item.reference};
  const review=await accepted(f.goal,'coordinate','coordination.json',{backlog:{reason:'Explicitly move only the refused item to the enabled lane',tasks:[proposal]}});
  return {proposal,review};
}

it('keeps five created tasks and exposes only the refused sixth without rerunning the accepted planner',async()=>{
  const f=await fixture(),a=await staged(f);expect(a.created).toHaveLength(5);
  expect(a.row).toMatchObject({state:'awaiting_supervision',receipt:{code:'PLANNING_REMAINDER_REQUIRED',evaluatedRunId:f.planner.run,
    remaining:{items:[{status:'pending',code:'MUSE_LANE_DISABLED',reference:{index:5}}]}}});
  expect(a.row.receipt.backlog.task_ids).toEqual(a.backlog.task_ids);
  const shown=await readPlanningDemands(pool,f.goal);expect(shown.rows[0].remainingItemCount).toBe(1);
  expect(shown.rows[0].createdTaskIds).toEqual(a.backlog.task_ids);
  expect(shown.rows[0].originalPlanRequest).toEqual({kind:'artifact',taskId:f.planner.task,expectedAttempt:1,name:'scope-plan.json',offset:0});
  expect(await hasActionableManagementConsumption(pool,f.goal)).toBe(true);
  expect(canWaitForManagementFacts({runId:f.planner.run,wakeMateriality:'same',reviewedAt:'now',contextRequests:[]},'same',
    {code:'MANAGEMENT_WAIT_ACTOR',satisfied:true},{runId:f.planner.run,runState:'submitted',openCases:0,contextRepair:false,refill:false,actionableObligations:true})).toBe(false);
  expect(await recordScopedPlanningEffects(f.request,f.artifact,f.plan,a.receipt,'applied')).toBe(false);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='BACKLOG_TASK_CREATED' AND entity_id=$1",[f.goal])).rowCount).toBe(5);
});

it.each(['applied','awaiting_supervision'])('reprojects legacy partial %s by exact artifact/CAS and never reapplies its five successes',async(state)=>{
  const f=await fixture(),a=await applyOriginal(f,state==='awaiting_supervision');
  await pool.query('UPDATE planning_requests SET state=$2,receipt=$3 WHERE id=$1',[f.request.id,state,JSON.stringify(a.receipt)]);
  vi.mocked(accountBridge.request).mockClear();
  expect(await recoverPartialPlanningRemainders(readArtifact)).toEqual([f.request.id]);
  expect(await recoverPartialPlanningRemainders(readArtifact)).toEqual([]);
  expect(accountBridge.request).not.toHaveBeenCalled();
  expect((await current(f)).receipt.backlog).toEqual(a.backlog);
  expect((await pool.query('SELECT task_ids FROM workforce_backlog_plans WHERE run_id=$1',[f.planner.run])).rows[0].task_ids).toHaveLength(5);
});

it.each([false,true])('links an explicitly corrected item atomically and preserves independent action refusal=%s',async(mixed)=>{
  const f=await fixture(),a=await staged(f,mixed),c=await correction(f,a.row);
  const [replacement]=await applyBacklog(f.goal,c.review.run,{reason:'Explicit correction',tasks:[c.proposal]});expect(replacement).toBeTruthy();
  const after=await current(f);expect(after.state).toBe(mixed?'awaiting_supervision':'applied');
  expect(after.receipt.code).toBe(mixed?'DELIVERY_ACTION_REVIEW_REQUIRED':'PLANNING_REMAINDER_LINKED');
  expect(after.receipt.remaining.items[0]).toMatchObject({status:'linked',replacement:{taskId:replacement,reviewRunId:c.review.run}});
  expect(after.receipt.backlog).toEqual(a.backlog);expect(after.receipt.deliveryActionReceipts).toEqual(a.receipt.deliveryActionReceipts);
  expect(await applyBacklog(f.goal,c.review.run,{reason:'Idempotent repeated callback',tasks:[c.proposal]})).toEqual([]);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='SCOPED_PLANNING_REMAINDER_LINKED' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
  expect((await pool.query('SELECT count(*)::int n FROM workforce_backlog_items WHERE goal_id=$1',[f.goal])).rows[0].n).toBe(6);
});

it('serializes two accepted corrections of the same item into exactly one task and one link',async()=>{
  const f=await fixture(1),a=await staged(f),one=await correction(f,a.row),two=await correction(f,a.row);
  const results=await Promise.all([one,two].map(c=>applyBacklog(f.goal,c.review.run,{reason:'Exact same explicit item',tasks:[c.proposal]})));
  expect(results.flat()).toHaveLength(1);
  expect((await pool.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='SCOPED_PLANNING_REMAINDER_LINKED'",[f.goal])).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM workforce_backlog_items WHERE goal_id=$1',[f.goal])).rowCount).toBe(1);
  expect((await current(f)).receipt.remaining.items[0].replacement.taskId).toBe(results.flat()[0]);
});

it('does not count another strategy or an unrelated backlog task as resolution',async()=>{
  const f=await fixture(1),a=await staged(f),c=await correction(f,a.row);
  const {remainingPlanItem,...unbound}=c.proposal;
  expect(await applyBacklog(f.goal,c.review.run,{reason:'Unrelated coverage only',tasks:[{...unbound,coverageKey:'another-coverage',description:'A completely different boundary hypothesis requiring actual independent evidence.'}]})).toHaveLength(1);
  expect((await current(f)).receipt.remaining.items[0].status).toBe('pending');
});

it.each(['artifact','review_acceptance','review_fence','original_fence','goal_paused','autonomy_off'])('refuses a changed %s after replacement proof and leaves receipt/task history intact',async(change)=>{
  const f=await fixture(1),a=await staged(f),c=await correction(f,a.row);
  const proof=await preparePlanningRemainderReplacement(f.goal,c.review.run,c.proposal,readArtifact);expect(proof).toBeTruthy();
  if(change==='artifact')await pool.query('UPDATE artifacts SET digest=$2 WHERE id=$1',[f.artifact.id,'b'.repeat(64)]);
  if(change==='review_acceptance')await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[c.review.run]);
  if(change==='review_fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[c.review.run,randomUUID()]);
  if(change==='original_fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.planner.run,randomUUID()]);
  if(change==='goal_paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(change==='autonomy_off')await pool.query("INSERT INTO settings VALUES('autonomy:test','{\"enabled\":false}')");
  await expect(tx(async db=>{await lock(db);return checkPlanningRemainderReplacement(db,f.goal,proof!);})).rejects.toThrow(/PLANNING_REMAINDER_/);
  expect((await current(f)).receipt).toEqual(a.row.receipt);
});

it('does not erase a concurrent replacement when delayed original application returns from IO',async()=>{
  const f=await fixture(1),a=await staged(f,true),observed=await current(f),c=await correction(f,a.row);
  Object.assign(observed,{run_id:f.planner.run,fence:f.planner.fence});
  await applyBacklog(f.goal,c.review.run,{reason:'Link before delayed result',tasks:[c.proposal]});
  const linked=(await current(f)).receipt;
  expect(await recordScopedPlanningEffects(observed,f.artifact,f.plan,a.receipt,'applied')).toBe(false);
  expect((await current(f)).receipt).toEqual(linked);
});

it('rolls back both the replacement task and remainder receipt if linking fails before commit',async()=>{
  const f=await fixture(1),a=await staged(f),c=await correction(f,a.row),proof=await preparePlanningRemainderReplacement(f.goal,c.review.run,c.proposal,readArtifact);
  const marker=randomUUID();
  await expect(tx(async db=>{await lock(db);
    await db.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,$3,'Replacement','Explicit','qa.backend','ready','[]')`,[marker,f.goal,'new-'+marker]);
    await linkPlanningRemainderReplacement(db,f.goal,proof!,marker);throw Error('ROLLBACK_PROBE');
  })).rejects.toThrow('ROLLBACK_PROBE');
  expect((await pool.query('SELECT 1 FROM tasks WHERE id=$1',[marker])).rowCount).toBe(0);
  expect((await current(f)).receipt).toEqual(a.row.receipt);
});

it('does not poison a new generation when old historical bytes fail during IO',async()=>{
  const f=await fixture(2),a=await applyOriginal(f);
  await pool.query('UPDATE planning_requests SET state=$2,receipt=$3 WHERE id=$1',[f.request.id,'applied',JSON.stringify(a.receipt)]);
  const newer={...a.receipt,code:'NEW_DECISION_OWNER'};
  const read=vi.fn(async()=>{await pool.query('UPDATE planning_requests SET receipt=$2 WHERE id=$1',[f.request.id,JSON.stringify(newer)]);throw Error('OLD_BYTES_UNAVAILABLE');});
  await recoverPartialPlanningRemainders(read);expect(read).toHaveBeenCalledTimes(1);
  expect((await current(f)).receipt).toEqual(newer);
});

it('allows an accepted retry to replace the previous evaluated run receipt without changing that run history',async()=>{
  const f=await fixture(1),oldRun=f.planner.run;
  await pool.query("UPDATE runs SET state='failed',error_code='PLANNING_SCOPE_INVALID' WHERE id=$1",[oldRun]);
  await pool.query("UPDATE tasks SET state='running',attempt_count=2 WHERE id=$1",[f.planner.task]);
  const run=randomUUID(),fence=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'running')",[run,f.planner.task,fence]);
  await writeArtifact(run,fence,'scope-plan.json',JSON.stringify(f.plan));await completeRun(run,fence,'Corrected plan',{});await validateTask(f.planner.task);
  await pool.query('UPDATE planning_requests SET receipt=$2 WHERE id=$1',[f.request.id,JSON.stringify({evaluatedRunId:oldRun,code:'PLANNING_SCOPE_INVALID'})]);
  f.planner={...f.planner,run,fence};f.artifact=(await pool.query("SELECT id,digest FROM artifacts WHERE run_id=$1 AND name='scope-plan.json'",[run])).rows[0];
  const a=await applyOriginal(f),request={...(await current(f)),run_id:run,fence};
  expect(await recordScopedPlanningEffects(request,f.artifact,f.plan,a.receipt,'applied')).toBe(true);
  expect((await current(f)).receipt.evaluatedRunId).toBe(run);
  expect((await pool.query('SELECT state,error_code FROM runs WHERE id=$1',[oldRun])).rows[0]).toEqual({state:'failed',error_code:'PLANNING_SCOPE_INVALID'});
});

it('does not let eight ineligible historical planners hide the ninth eligible partial',async()=>{
  const f=await fixture(2),a=await applyOriginal(f);
  await pool.query('UPDATE planning_requests SET state=$2,receipt=$3 WHERE id=$1',[f.request.id,'applied',JSON.stringify(a.receipt)]);
  for(let i=0;i<8;i++)await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,receipt,updated_at)
    VALUES($1,$2,$3,'glm',$4,'applied','No current actor binding','{}',$5,'2020-01-01')`,
    [randomUUID(),f.goal,'test/stale-'+i,'b'.repeat(64),JSON.stringify(a.receipt)]);
  expect(await recoverPartialPlanningRemainders(readArtifact)).toEqual([f.request.id]);
});

it('supports discovery beyond fifteen requests and exact remaining-item pages with stale and cross-goal guards',async()=>{
  const f=await fixture(1),a=await staged(f);
  for(let i=0;i<16;i++)await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot,receipt)
    VALUES($1,$2,$3,'glm',$4,'awaiting_supervision','Additional pending scope','{}','{}')`,[randomUUID(),f.goal,'test/extra-'+i,'b'.repeat(64)]);
  const initial=await readPlanningDemands(pool,f.goal);expect(initial.rows).toHaveLength(15);expect(initial.selection.total).toBe(17);
  const requests=[initial.selection.recovery,{kind:'planning_request',requestId:f.request.id,offset:0}];
  await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',['management-state:'+f.goal,JSON.stringify({contextRequests:requests})]);
  const recovered=await readManagementRetrieval(pool,f.goal,readArtifact);
  expect(recovered.results[0]).toMatchObject({status:'OK',totalRows:17,nextOffset:null});expect(recovered.results[0].rows).toHaveLength(2);
  expect(JSON.parse(recovered.results[1].text).remaining.items[0].reference).toEqual(a.row.receipt.remaining.items[0].reference);
  await pool.query("UPDATE planning_requests SET updated_at=clock_timestamp() WHERE id=$1",[f.request.id]);
  expect((await readManagementRetrieval(pool,f.goal,readArtifact)).results[0].status).toBe('STALE_CATALOG');
  const foreignGoal=randomUUID(),foreignRequest=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Foreign','Private','other',$2)",[foreignGoal,'foreign-'+foreignGoal]);
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot)
    VALUES($1,$2,'other/scope','glm',$3,'awaiting_supervision','Foreign request','{}')`,[foreignRequest,foreignGoal,'c'.repeat(64)]);
  await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',['management-state:'+f.goal,JSON.stringify({contextRequests:[{kind:'planning_request',requestId:foreignRequest,offset:0}]})]);
  expect((await readManagementRetrieval(pool,f.goal,readArtifact)).results[0].status).toBe('NOT_FOUND');
  await pool.query('UPDATE settings SET value=$2 WHERE id=$1',['management-state:'+f.goal,JSON.stringify({contextRequests:[{kind:'planning_request',requestId:f.request.id,itemIndex:0,offset:0}]})]);
  const exact=(await readManagementRetrieval(pool,f.goal,readArtifact)).results[0];
  expect(exact.status).toBe('OK');expect(JSON.parse(exact.text).originalProposal).toEqual(f.plan.tasks[0]);
  expect(exact.text).not.toContain('another-coverage');expect(exact.nextOffset).toBeNull();
});

it('warns on a disabled snapshot lane without changing legacy validation, request identity or current authority',async()=>{
  const f=await fixture(1),snapshot={tasks:[{id:f.source.task,state:'accepted',stage:'work',attempt_count:1,last_run_attempt:1,last_run_state:'submitted',last_run_id:f.source.run,acceptance_passed:true,execution:{baseWorkspace:'test/source'}},
    {id:f.consumer,state:'recovery_blocked',stage:'integrate',execution:{baseWorkspace:'test/source'}}],artifacts:[],opportunities:{},coverage:[],repairConsumption:[],deferredDeliveries:[]};
  const request={id:f.request.id,scope:'test/source',source_revision:f.plan.sourceRevision,snapshot};
  expect(scopedPlanPreflight(f.plan,request)).toMatchObject({validAgainstFrozenCatalog:true,liveAuthorization:false});
  expect(scopedPlanPreflight(f.plan,request)).not.toHaveProperty('warnings');
  const hinted={...snapshot,laneAvailability:scopedPlanningLaneHints({enabled:true,glmAccountId:'glm',glmTarget:12,museTarget:0},'2026-10-11T00:00:00Z')};
  expect(scopedPlanPreflight(f.plan,{...request,snapshot:hinted})).toMatchObject({validAgainstFrozenCatalog:true,
    warnings:[{path:['tasks',0,'lane'],code:'PLANNING_LANE_DISABLED_AT_SNAPSHOT',lane:'muse'}]});
  const projected=projectScopedPlannerContext(hinted,{readerPath:'/reader',readerDigest:'a'.repeat(64),path:'/catalog',digest:'b'.repeat(64)},[]);
  expect(projected.laneAvailability).toEqual(hinted.laneAvailability);
  expect(planningProposalDigest({...f.plan.tasks[0],delivery:undefined})).not.toBe(planningProposalDigest(f.plan.tasks[0]));
});
