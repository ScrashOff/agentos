import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx,type DB} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner,applyBacklog} from '../server/backlog.js';
import {installPlanningDemand,scopeSnapshot,scopeSourceRevision} from '../server/planning-demand.js';
import {installSupervision} from '../server/supervision.js';
import {installManagement} from '../server/management.js';
import {scopedSourceRevision,type ScopedPlanAction} from '../server/scoped-plan-contract.js';
import {applyScopedPlanAction} from '../server/scoped-plan-actions.js';
import {stageDeferredHandoff} from '../server/deferred-handoffs.js';
import {writeArtifact,completeRun,validateTask,readArtifact} from '../server/service.js';
import {recordScopedPlanningEffects,readPlanningDemandPage} from '../server/planning-remainders.js';
import {readPlanningActionReviewDetail} from '../server/planning-action-review.js';
import {readManagementRequestPages,readManagementRetrieval} from '../server/management-retrieval.js';
import {projectManagerReview} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';

const secret='PRIVATE_ACTION_REASON_NOT_FOR_MANAGER_CONTEXT';
beforeAll(async()=>{await migrate();await installBacklogPlanner();await installPlanningDemand();await installSupervision();await installManagement();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});

/** Use the real accepted artifact, action applier, handoff and receipt writer.
 * The reservation is made AFTER the planner snapshot to exercise the live
 * conflict path without forging acceptance under the new frozen validator. */
async function fixture(count=1,mixed=false){
  const goal=randomUUID(),requestId=randomUUID(),scope='test/planning-action-review';
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Delivery review','Preserve delivery criteria','action-review-test',$2)",[goal,'goal-'+goal]);
  for(const [key,value] of [['workforce:'+goal,{enabled:true,glmTarget:12,glmAccountId:'glm'}],['supervision:'+goal,{enabled:false}],['management:'+goal,{enabled:true}]])
    await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',[key,JSON.stringify(value)]);
  const task=async(stage:string,workKind?:string)=>{
    const id=randomUUID();
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
      VALUES($1,$2,$3,'Concrete delivery','The original delivery and acceptance criteria remain intact','qa.backend','ready',$4,$5,$6)`,
      [id,goal,'task-'+id,stage,JSON.stringify(workKind==='planning'?[{id:'plan',label:'Published plan',type:'artifact',name:'scope-plan.json'}]:[]),
        JSON.stringify({kind:'harness',baseWorkspace:scope,workspace:'test/'+id,workforceLane:'glm',workKind,
          ...(workKind==='planning'?{planningRequestId:requestId}:{})})]);
    return id;
  };
  const running=async(taskId:string)=>{
    const run=randomUUID(),fence=randomUUID();
    await pool.query("UPDATE tasks SET attempt_count=1,state='running' WHERE id=$1",[taskId]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[run,taskId,fence]);
    return {run,fence};
  };
  const entries:any[]=[],actions:ScopedPlanAction[]=[];
  for(let index=0;index<count;index++){
    const producer=await task('work','repair'),consumer=await task('integrate'),reserved=await task('integrate');
    const source=await running(producer);
    const artifact=await writeArtifact(source.run,source.fence,'implementation.patch','--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+fixed\n');
    await completeRun(source.run,source.fence,'A real accepted producer',{});expect((await validateTask(producer)).passed).toBe(true);
    entries.push({producer,consumer,reserved,source,artifact});
    actions.push({kind:'prepare_integration',taskId:consumer,expectedAttempt:0,sourceTaskId:producer,
      expectedSourceRunId:source.run,artifactIds:[artifact.id],reason:secret+' '+index+' consume the unchanged accepted source correctly.'});
  }
  const snapshot=await scopeSnapshot(pool,goal,scope),revision=scopedSourceRevision(snapshot.tasks,snapshot.artifacts,snapshot.deferredDeliveries);
  await pool.query(`INSERT INTO planning_requests(id,goal_id,scope,lane,source_revision,state,reason,snapshot)
    VALUES($1,$2,$3,'glm',$4,'pending','Review concrete deliveries',$5)`,[requestId,goal,scope,revision,JSON.stringify(snapshot)]);
  const planner=await task('work','planning'),planRun=await running(planner);
  await pool.query("UPDATE planning_requests SET state='assigned',task_id=$2 WHERE id=$1",[requestId,planner]);
  const plan={version:1,requestId,sourceRevision:revision,reason:'Review producer deliveries with real accepted artifacts',tasks:[],actions,blockers:[],dependencyChanges:[]};
  const planArtifact=await writeArtifact(planRun.run,planRun.fence,'scope-plan.json',JSON.stringify(plan));
  await completeRun(planRun.run,planRun.fence,'Published structured plan',{});expect((await validateTask(planner)).passed).toBe(true);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  for(let index=0;index<entries.length;index++){
    const entry=entries[index];
    await tx(async db=>{
      const source=(await db.query('SELECT * FROM tasks WHERE id=$1',[entry.producer])).rows[0];
      const target=(await db.query('SELECT * FROM tasks WHERE id=$1',[mixed&&index===1?entry.consumer:entry.reserved])).rows[0];
      await stageDeferredHandoff(db,goal,entry.producer,source,target,{taskId:entry.producer,plannerRunId:planRun.run,
        coverageKey:'concrete-delivery-'+index,lane:'glm',workKind:'repair'});
    });
    entry.handoff=(await pool.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1',[entry.producer])).rows[0];
  }
  const request={...(await pool.query('SELECT * FROM planning_requests WHERE id=$1',[requestId])).rows[0],run_id:planRun.run,fence:planRun.fence};
  await applyBacklog(goal,planRun.run,{reason:plan.reason,tasks:[]});
  const backlog=(await pool.query('SELECT state,receipts,task_ids FROM workforce_backlog_plans WHERE run_id=$1',[planRun.run])).rows[0];
  const receipts=[];
  for(const action of actions)receipts.push(await applyScopedPlanAction(request,action));
  expect(receipts[0]).toMatchObject({applied:false,code:'HANDOFF_CONFLICT'});
  if(mixed)expect(receipts[1]).toMatchObject({applied:true,code:'HANDOFF_STAGED'});
  const receipt={code:'DELIVERY_ACTION_REVIEW_REQUIRED',evaluatedRunId:planRun.run,backlog,blockers:[],dependencyReceipts:[],deliveryActionReceipts:receipts};
  expect(await recordScopedPlanningEffects(request,planArtifact,plan,receipt,'awaiting_supervision')).toBe(true);
  return {goal,scope,requestId,request,planner,planRun,planArtifact,plan,entries,receipts};
}
async function pages(f:any,requests:any[],db:DB=pool,reader=readArtifact){
  return (await readManagementRequestPages(db,f.goal,reader,requests)).results;
}
const detailRequest=(f:any,extra:any={})=>({kind:'planning_request',requestId:f.requestId,offset:0,...extra});
async function state(f:any){
  return {requests:(await pool.query('SELECT * FROM planning_requests WHERE goal_id=$1 ORDER BY id',[f.goal])).rows,
    tasks:(await pool.query('SELECT * FROM tasks WHERE goal_id=$1 ORDER BY id',[f.goal])).rows,
    runs:(await pool.query('SELECT r.* FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 ORDER BY r.id',[f.goal])).rows,
    events:(await pool.query('SELECT * FROM events ORDER BY id')).rows,
    handoffs:(await pool.query('SELECT * FROM deferred_handoffs WHERE goal_id=$1 ORDER BY id',[f.goal])).rows,
    acceptances:(await pool.query('SELECT a.* FROM acceptances a JOIN tasks t ON t.id=a.task_id WHERE t.goal_id=$1 ORDER BY a.id',[f.goal])).rows};
}

it('roundtrips the actual pending request through manager projection and the existing reader without granting actions or changing state',async()=>{
  const f=await fixture(2,true),before=await state(f),directoryQueries:string[]=[];
  const directoryDb={query:async(sql:any,values:any)=>{directoryQueries.push(String(sql));return pool.query(sql,values);}} as DB;
  const directory=await readPlanningDemandPage(directoryDb,f.goal);
  expect(directoryQueries).toHaveLength(3);
  expect(directoryQueries.join('\n')).not.toMatch(/FROM events|FROM deferred_handoffs|task_execution_metadata/);
  const row:any=directory.rows.find(r=>r.id===f.requestId)!;
  expect(row.deliveryActionReview).toMatchObject({status:'REPORTED_REQUIRES_DETAIL',reported:{receiptApplied:1,refused:1}});
  expect(JSON.stringify(row.deliveryActionReview).length).toBeLessThan(1000);
  const projected=projectManagerReview({cases:[],operation:{planningDemands:directory.rows}},'',0,30000);
  const delivered=expandSharedManagerContext(JSON.parse(projected.description));
  expect(delivered.operation.planningDemands[0].deliveryActionReview).toEqual(row.deliveryActionReview);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',[
    'management-state:'+f.goal,JSON.stringify({contextRequests:[row.deliveryActionReview.detailRequest]})]);
  const readonlyDb={query:async(sql:any,values:any)=>{
    expect(String(sql)).toMatch(/^\s*(?:SELECT|WITH)\b/);
    return pool.query(sql,values);
  }} as DB;
  const read=(await readManagementRetrieval(readonlyDb,f.goal,readArtifact)).results[0],value=JSON.parse(read.text);
  expect(value.deliveryActionReview).toMatchObject({authority:'review_only',status:'VERIFIED_ACCEPTED_ARTIFACT',
    planner:{runId:f.planRun.run,artifactId:f.planArtifact.id,artifactDigest:f.planArtifact.digest},counts:{receiptApplied:1,refused:1}});
  expect(value.deliveryActionReview.actions[0]).toMatchObject({receiptApplied:false,code:'HANDOFF_CONFLICT',
    producerTaskId:f.entries[0].producer,requestedConsumerTaskId:f.entries[0].consumer,
    reservation:{handoffId:f.entries[0].handoff.id,reservedConsumerTaskId:f.entries[0].reserved,conflictsWithRequestedConsumer:true}});
  expect(value.deliveryActionReview.actions[1]).toMatchObject({receiptApplied:true,reservation:{conflictsWithRequestedConsumer:false}});
  expect(value.currentScopeRevision).toMatchObject({status:'OBSERVED',value:await scopeSourceRevision(pool,f.goal,f.scope),authority:'read_only'});
  expect(value.currentScopeRevision.value).not.toBe(f.request.source_revision);
  expect(read.observedAt).toEqual(expect.any(String));expect(read.text).not.toContain(read.observedAt);
  expect(read.text).not.toContain(secret);expect(read.text).not.toContain('source_proof');
  expect(read.text).not.toContain('implementation.patch');
  expect(await state(f)).toEqual(before);
});

it('treats existing:true as replay metadata while preserving the exact original event receipt',async()=>{
  const f=await fixture();
  await pool.query(`UPDATE planning_requests SET receipt=jsonb_set(receipt,'{deliveryActionReceipts,0,existing}','true') WHERE id=$1`,[f.requestId]);
  const review=await readPlanningActionReviewDetail(pool,f.goal,f.requestId,readArtifact);
  expect(review?.value.status).toBe('VERIFIED_ACCEPTED_ARTIFACT');
});

it('shows the permanent reservation of an applied historical handoff without calling it consumption by the newly requested consumer',async()=>{
  const f=await fixture();
  // Seed an already-applied historical reservation; the reader must not erase
  // it as the old inline deferredDeliveries (state<>applied) catalog did.
  await pool.query("UPDATE deferred_handoffs SET state='applied',applied_at=clock_timestamp() WHERE id=$1",[f.entries[0].handoff.id]);
  const before=await state(f),value=JSON.parse((await pages(f,[detailRequest(f)]))[0].text);
  expect(value.deliveryActionReview.actions[0]).toMatchObject({receiptApplied:false,
    reservation:{state:'APPLIED',reservedConsumerTaskId:f.entries[0].reserved,conflictsWithRequestedConsumer:true,appliedAt:expect.any(String)}});
  expect(value.deliveryActionReview.semantics).toContain('does not prove material consumption');
  expect(await state(f)).toEqual(before);
});

it.each(['missing_event','foreign_goal','wrong_run','wrong_artifact','wrong_digest','changed_receipt','changed_action','extra_receipt_field','extra_event_field'] as const)(
  'keeps a visible unknown pending issue when the exact event binding fails: %s',async(mode)=>{
    const f=await fixture(),key=f.receipts[0].actionKey;
    if(mode==='missing_event')await pool.query("DELETE FROM events WHERE kind='SCOPED_DELIVERY_ACTION_RECORDED' AND payload->>'actionKey'=$1",[key]);
    else if(mode==='foreign_goal')await pool.query("UPDATE events SET entity_id=$2 WHERE kind='SCOPED_DELIVERY_ACTION_RECORDED' AND payload->>'actionKey'=$1",[key,randomUUID()]);
    else if(mode==='changed_receipt')await pool.query(`UPDATE planning_requests SET receipt=jsonb_set(receipt,'{deliveryActionReceipts,0,applied}','true') WHERE id=$1`,[f.requestId]);
    else if(mode==='extra_receipt_field')await pool.query(`UPDATE planning_requests SET receipt=jsonb_set(receipt,'{deliveryActionReceipts,0,unexpected}','true') WHERE id=$1`,[f.requestId]);
    else if(mode==='extra_event_field')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{receipt,unexpected}','true') WHERE kind='SCOPED_DELIVERY_ACTION_RECORDED' AND payload->>'actionKey'=$1",[key]);
    else {
      const path=mode==='wrong_run'?['planRunId']:mode==='wrong_artifact'?['planArtifactId']:mode==='wrong_digest'?['planDigest']:['action','taskId'];
      await pool.query("UPDATE events SET payload=jsonb_set(payload,$2::text[],to_jsonb($3::text)) WHERE kind='SCOPED_DELIVERY_ACTION_RECORDED' AND payload->>'actionKey'=$1",
        [key,path,mode==='wrong_digest'?'f'.repeat(64):randomUUID()]);
    }
    const directory=await readPlanningDemandPage(pool,f.goal);
    expect(directory.rows[0].deliveryActionReview).toMatchObject({status:'REPORTED_REQUIRES_DETAIL',totalReceipts:1});
    const read=(await pages(f,[detailRequest(f)]))[0],value=JSON.parse(read.text);
    expect(value.deliveryActionReview.status).toBe('ARTIFACT_BINDING_UNVERIFIED');
    expect(value.currentScopeRevision.status).toBe('UNKNOWN');
    expect(value.deliveryActionReview.totalReceipts).toBe(1);
    expect(value.deliveryActionReview.actions).toBeUndefined();
  });

it.each(['unaccepted','new_attempt','different_request','different_scope'] as const)('does not borrow current planner authority after %s',async(mode)=>{
  const f=await fixture();
  if(mode==='unaccepted')await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[f.planRun.run]);
  if(mode==='new_attempt')await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.planner]);
  if(mode==='different_request')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{planningRequestId}',to_jsonb($2::text)) WHERE id=$1",[f.planner,randomUUID()]);
  if(mode==='different_scope')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{baseWorkspace}','\"test/elsewhere\"') WHERE id=$1",[f.planner]);
  const read=(await pages(f,[detailRequest(f)]))[0],value=JSON.parse(read.text);
  expect(value.deliveryActionReview.status).toBe('SOURCE_UNVERIFIED');expect(value.currentScopeRevision.status).toBe('UNKNOWN');
});

it.each(['fence','context','request','acceptance'] as const)('revalidates actual identity after artifact IO: %s',async(mode)=>{
  const f=await fixture();let changed=false;
  const reader=async(artifactId:string)=>{
    const artifact=await readArtifact(artifactId);
    if(!changed){changed=true;
      if(mode==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.planRun.run,randomUUID()]);
      if(mode==='context'){
        const ctx=randomUUID();await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','A new current context')",[ctx,f.planRun.run]);
        await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.planRun.run,ctx]);
      }
      if(mode==='request')await pool.query("UPDATE planning_requests SET state='superseded' WHERE id=$1",[f.requestId]);
      if(mode==='acceptance')await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[f.planRun.run]);
    }
    return artifact;
  };
  const value=JSON.parse((await pages(f,[detailRequest(f)],pool,reader))[0].text);
  expect(value.deliveryActionReview.status).toBe('STALE_SOURCE');expect(value.currentScopeRevision.status).toBe('UNKNOWN');
});

it('keeps corrupt accepted bytes and missing reservations explicitly unverified, never replacing their bindings',async()=>{
  const f=await fixture();
  const bad=await readPlanningActionReviewDetail(pool,f.goal,f.requestId,async()=>({bytes:Buffer.from('{}')}));
  expect(bad?.value.status).toBe('ARTIFACT_BINDING_UNVERIFIED');
  await pool.query('DELETE FROM deferred_handoffs WHERE id=$1',[f.entries[0].handoff.id]);
  const observed=await readPlanningActionReviewDetail(pool,f.goal,f.requestId,readArtifact);
  expect(observed?.value.actions[0].reservation).toEqual({status:'BINDING_UNVERIFIED'});
});

it('keeps repeated detail pages on a stable digest and refuses a page after the underlying reservation changes',async()=>{
  const f=await fixture(12),request=detailRequest(f),first=(await pages(f,[request]))[0];
  // A nonzero text page is valid even if the document happens to fit one window.
  const offset=first.nextOffset??Math.min(4000,first.totalCharacters-1),second=(await pages(f,[{...request,offset,snapshotDigest:first.snapshotDigest}]))[0];
  expect(second.status).toBe('OK');expect(second.snapshotDigest).toBe(first.snapshotDigest);
  const same=(await pages(f,[request]))[0];expect(same.text).toBe(first.text);expect(same.snapshotDigest).toBe(first.snapshotDigest);
  await pool.query("UPDATE deferred_handoffs SET state='staged' WHERE id=$1",[f.entries[0].handoff.id]);
  const stale=(await pages(f,[{...request,offset,snapshotDigest:first.snapshotDigest}]))[0];
  expect(stale).toMatchObject({status:'STALE_CATALOG',nextOffset:0});expect(stale.text).toBeUndefined();
});

it('invalidates discovery on receipt changes without depending on updated_at, and does not hide an oversized receipt',async()=>{
  const f=await fixture(),first=await readPlanningDemandPage(pool,f.goal);
  await pool.query(`UPDATE planning_requests SET receipt=jsonb_set(receipt,'{deliveryActionReceipts}', $2::jsonb) WHERE id=$1`,
    [f.requestId,JSON.stringify(Array.from({length:13},()=>f.receipts[0]))]);
  expect((await readPlanningDemandPage(pool,f.goal,{offset:0,expectedDigest:first.snapshotDigest})).status).toBe('STALE_CATALOG');
  const current=await readPlanningDemandPage(pool,f.goal);expect(current.rows[0].deliveryActionReview).toMatchObject({totalReceipts:13,truncated:true});
  expect((await readPlanningActionReviewDetail(pool,f.goal,f.requestId,readArtifact))?.value.status).toBe('RECEIPT_LIMIT_EXCEEDED');
});

it('does not read another goal artifact or compute its scope revision',async()=>{
  const f=await fixture(),foreign=randomUUID();
  const requests=[detailRequest(f)];let artifactRead=false;
  const result=(await readManagementRequestPages(pool,foreign,async()=>{artifactRead=true;throw Error('NOT_ALLOWED');},requests)).results[0];
  expect(result.status).toBe('NOT_FOUND');expect(artifactRead).toBe(false);
});

it('keeps the review-required request discoverable when its action receipts are absent',async()=>{
  const f=await fixture();
  await pool.query("UPDATE planning_requests SET receipt=receipt-'deliveryActionReceipts' WHERE id=$1",[f.requestId]);
  const directory=await readPlanningDemandPage(pool,f.goal);
  expect(directory.rows[0].deliveryActionReview).toMatchObject({status:'REPORTED_REQUIRES_DETAIL',totalReceipts:0});
  const value=JSON.parse((await pages(f,[detailRequest(f)]))[0].text);
  expect(value.deliveryActionReview.status).toBe('RECEIPT_UNAVAILABLE');expect(value.currentScopeRevision.status).toBe('UNKNOWN');
});

it('labels unavailable canonical revision unknown while keeping the verified historical action evidence',async()=>{
  const f=await fixture();
  const db={query:async(sql:any,values:any)=>{
    if(typeof sql==='string'&&sql.includes('metadata.base_workspace=$2'))throw Error('NO_PRIVATE_SQL_DETAILS');
    return pool.query(sql,values);
  }} as DB;
  const result=(await pages(f,[detailRequest(f)],db))[0],value=JSON.parse(result.text);
  expect(value.deliveryActionReview.status).toBe('VERIFIED_ACCEPTED_ARTIFACT');expect(value.currentScopeRevision.status).toBe('UNKNOWN');
  expect(result.text).not.toContain('NO_PRIVATE_SQL_DETAILS');
});
