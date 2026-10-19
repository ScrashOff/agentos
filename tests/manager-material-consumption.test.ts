import {beforeAll,beforeEach,afterAll,afterEach,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,migrate,tx,event} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installSupervision} from '../server/supervision.js';
import {installDeferredHandoffs,stageDeferredHandoff,applyDeferredHandoff} from '../server/deferred-handoffs.js';
import {reconcileDeliveryConsumptions} from '../server/delivery-consumption.js';
import {glmRepairCriteria} from '../server/glm-repair.js';
import * as artifactService from '../server/service.js';
import {readManagerCases,projectManagerCases} from '../server/manager-material-consumption.js';
import {managerCasePriority,managerFocusSourceFingerprint,selectManagerFocus,selectManagerCaseWindow,
 focusManagerTasks,managerFocusServiceHistory} from '../server/manager-focus.js';
import {projectManagerReview} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {deliveryDigest} from '../server/delivery-supervision.js';

const sha=(s:string)=>createHash('sha256').update(s).digest('hex');
beforeAll(async()=>{await migrate();await installTaskExecutionMetadata();await installDeferredHandoffs();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
 await pool.query("DELETE FROM settings WHERE id='autonomy:material-focus-test'");});
afterEach(()=>vi.restoreAllMocks());
afterAll(async()=>pool.end());
const task=async(id:string)=>(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
async function run(id:string){
 const t=await task(id),rid=randomUUID(),fence=randomUUID();
 await pool.query("UPDATE tasks SET state='running',attempt_count=attempt_count+1 WHERE id=$1",[id]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
  VALUES($1,$2,$3,$4,'harness',1,'running')`,[rid,id,t.attempt_count+1,fence]);
 return {id:rid,fence,task_id:id,attempt:t.attempt_count+1};
}
async function stop(id:string){
 const r=(await pool.query('SELECT task_id FROM runs WHERE id=$1',[id])).rows[0],t=await task(r.task_id);
 return {runId:id,workspace:t.execution.workspace,checkedAt:new Date().toISOString(),
  process:{unit:'agentos-run-'+id,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
}
async function fixture(existingGoal?:string){
 const goal=existingGoal??randomUUID(),producer=randomUUID(),consumer=randomUUID(),scope='test/material-consumption';
 if(!existingGoal){
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
   VALUES($1,'Consume material','Original final acceptance','material-focus-test',$2)`,[goal,goal]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)', ['supervision:'+goal,JSON.stringify({enabled:true})]);
 }
 for(const [id,criteria] of [[producer,glmRepairCriteria],[consumer,[{id:'final',type:'artifact',name:'verification.json',label:'Original final verification'}]]] as const)
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
   VALUES($1,$2,$3,'Preserved task','Original instruction and criteria','engineering.integration','ready','work',$4,$5)`,
   [id,goal,id,JSON.stringify(criteria),JSON.stringify({kind:'harness',workKind:'repair',workspace:'test/o-'+id,sourceWorkspace:'test/source',baseWorkspace:scope})]);
 const pr=await run(producer);
 for(const [name,bytes] of Object.entries({'implementation.patch':'--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-original\n+fixed\n',
  'repair-report.json':JSON.stringify({fixVerified:true,sourceHashes:[{path:'src/app.js',before:sha('original\n'),after:sha('fixed\n')}]}),
  'repair-tests.log':'before exit 1; after exit 0'}))await artifactService.writeArtifact(pr.id,pr.fence,name,bytes);
 await artifactService.completeRun(pr.id,pr.fence,'Actual fixture evidence',{});
 expect((await artifactService.validateTask(producer)).passed).toBe(true);
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
 const cr=await run(consumer);await artifactService.failRun(cr.id,cr.fence,'FUNCTIONAL_TEST_FAILED','Older failed consumer');
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
 await tx(async db=>stageDeferredHandoff(db,goal,producer,await task(producer),await task(consumer),
  {taskId:producer,plannerRunId:pr.id,coverageKey:'material-'+producer,lane:'glm',workKind:'repair'}));
 const handoff=(await pool.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1',[producer])).rows[0];
 expect(await applyDeferredHandoff(handoff.id,await stop(cr.id),await stop(pr.id))).toBe(true);
 await reconcileDeliveryConsumptions(stop);
 const obligation=(await pool.query('SELECT * FROM delivery_consumptions WHERE handoff_id=$1',[handoff.id])).rows[0];
 expect(obligation).toMatchObject({state:'waiting_supervision',receipt:{code:'SUPERVISION_REQUESTED',owner:'supervisor'}});
 const c=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[obligation.receipt.caseId])).rows[0];
 expect(c.evidence).toContainEqual(expect.objectContaining({kind:'DELIVERY_CONSUMPTION_REQUIRED',code:'CONSUMER_INPUTS_NOT_BOUND'}));
 return {goal,scope,producer,consumer,pr,cr,handoff,obligation,c};
}
type Fixture=Awaited<ReturnType<typeof fixture>>;
async function entry(f:Fixture){return (await readManagerCases(pool,f.goal)).find(c=>c.id===f.c.id);}
async function persisted(goal:string){
 return {
  tasks:(await pool.query('SELECT id,state,attempt_count,description,criteria,execution FROM tasks WHERE goal_id=$1 ORDER BY id',[goal])).rows,
  runs:(await pool.query('SELECT r.* FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 ORDER BY r.id',[goal])).rows,
  cases:(await pool.query('SELECT * FROM supervision_cases WHERE goal_id=$1 ORDER BY id',[goal])).rows,
  acceptances:(await pool.query('SELECT a.* FROM acceptances a JOIN tasks t ON t.id=a.task_id WHERE t.goal_id=$1 ORDER BY a.id',[goal])).rows,
  dependencies:(await pool.query('SELECT d.* FROM dependencies d JOIN tasks t ON t.id=d.task_id WHERE t.goal_id=$1 ORDER BY d.task_id,d.depends_on',[goal])).rows,
  obligations:(await pool.query('SELECT * FROM delivery_consumptions WHERE goal_id=$1 ORDER BY id',[goal])).rows,
 };
}

async function historicalReviewer(f:Fixture,options:{state?:string;foreignGoal?:boolean;stage?:string;assignCase?:boolean}={}){
 const goal=options.foreignGoal?randomUUID():f.goal,id=randomUUID();
 if(options.foreignGoal)await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
  VALUES($1,'Unrelated review','Different authority','material-review-other',$2)`,[goal,goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
  VALUES($1,$2,$3,'Historical coordinator','MANAGEMENT_V1 Historical decision retained for audit',
   'research.requirements','ready',$4,'[]','{}')`,[id,goal,id,options.stage??'coordinate']);
 const review=await run(id);
 await artifactService.writeArtifact(review.id,review.fence,'coordination.json',JSON.stringify({management:{
  summary:'Historical review only',capacityReason:'Keep original final delivery criteria',actions:[]}}));
 await artifactService.completeRun(review.id,review.fence,'Historical review returned',{});
 if(options.state&&options.state!=='submitted'){
  // Explicit test setup of other persisted lifecycle states; it does not claim
  // that a cancellation or physical stop was performed by the manager reader.
  await pool.query('UPDATE runs SET state=$2 WHERE id=$1',[review.id,options.state]);
  await pool.query('UPDATE tasks SET state=$2 WHERE id=$1',[id,options.state]);
 }
 const decision={kind:'revise',diagnosis:'Prior review is historical',instruction:'Preserve this audit record.'};
 if(options.assignCase!==false)await pool.query('UPDATE supervision_cases SET decision_run_id=$2,decision=$3 WHERE id=$1',
  [f.c.id,review.id,JSON.stringify(decision)]);
 return {...review,goal,decision};
}

it.each(['submitted','failed','cancelled'] as const)
('keeps accepted repair material reviewable after a historical %s coordinator without applying its old decision',async state=>{
 const f=await fixture(),original=await entry(f),review=await historicalReviewer(f,{state});
 const before=await persisted(f.goal);
 const artifacts=(await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id',[review.id])).rows;
 const artifactReads=vi.spyOn(artifactService,'readArtifact').mockImplementation(async()=>{throw Error('priority must not read artifact bytes');});
 const rows=await readManagerCases(pool,f.goal),current=rows.find(c=>c.id===f.c.id)!;
 expect(current).toMatchObject({state:'open',decision_run_id:review.id,decision_review_id:review.id,
  decision_review_goal_id:f.goal,decision_review_stage:'coordinate',decision_review_state:state});
 expect(current.materialConsumptionReview).toEqual(original.materialConsumptionReview);
 expect(managerCasePriority(current)).toBe(4);
 const focus=selectManagerFocus(rows,null)!;
 expect(focus.caseId).toBe(f.c.id);
 const selected=selectManagerCaseWindow(rows,focus);
 const projection=projectManagerReview({cases:projectManagerCases(selected),operation:{focus,tasks:[]}},'',0,150000);
 expect(projection.withinBudget).toBe(true);
 const projected=expandSharedManagerContext(JSON.parse(projection.description));
 expect(projected.cases[0].materialConsumptionReview).toEqual(original.materialConsumptionReview);
 expect(projected.cases[0].evidence).toEqual(current.evidence);
 expect(artifactReads).not.toHaveBeenCalled();expect(await persisted(f.goal)).toEqual(before);
 expect((await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id',[review.id])).rows).toEqual(artifacts);
});

it.each(['queued','running','cancelling','reconciling','unknown','foreign-goal','non-coordinate',
 'invalid-receipt','source-acceptance-revoked'] as const)
('does not promote material through an unqualified historical review: %s',async mode=>{
 const f=await fixture();
 const state=['queued','running','cancelling','reconciling','unknown'].includes(mode)?mode:'submitted';
 const review=await historicalReviewer(f,{state,foreignGoal:mode==='foreign-goal',stage:mode==='non-coordinate'?'work':'coordinate'});
 if(mode==='invalid-receipt')await pool.query("UPDATE delivery_consumptions SET receipt=jsonb_set(receipt,'{generation}',to_jsonb($2::text)) WHERE id=$1",
  [f.obligation.id,sha('A different material receipt')]);
 if(mode==='source-acceptance-revoked')await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
  VALUES($1,$2,$3,false,'[]','fixture-revocation',clock_timestamp()+interval '1 second')`,[randomUUID(),f.producer,f.pr.id]);
 const before=await persisted(f.goal),current=await entry(f);
 expect(current.materialConsumptionReview).toBeUndefined();expect(managerCasePriority(current)).toBe(2);
 expect(current.decision_run_id).toBe(review.id);
 expect(await persisted(f.goal)).toEqual(before);
});

it('keeps the canonical foreign key when a missing reviewer is proposed instead of fabricating release evidence',async()=>{
 const f=await fixture(),review=await historicalReviewer(f),before=await persisted(f.goal);
 await expect(pool.query('UPDATE supervision_cases SET decision_run_id=$2 WHERE id=$1',[f.c.id,randomUUID()]))
  .rejects.toMatchObject({code:'23503'});
 const current=await entry(f);
 expect(current.decision_run_id).toBe(review.id);expect(current.materialConsumptionReview).toBeDefined();
 expect(await persisted(f.goal)).toEqual(before);
});

it.each(['fence','context','case-owner','case-state','reviewer-live','source-acceptance'] as const)
('discards material enrichment when %s changes after the first real case projection',async mode=>{
 const f=await fixture(),review=await historicalReviewer(f),replacement=randomUUID();
 const replacementReview=mode==='case-owner'?await historicalReviewer(f,{assignCase:false}):null;
 expect((await entry(f)).materialConsumptionReview).toBeDefined();
 let crossed=false,afterRace:Awaited<ReturnType<typeof persisted>>|undefined;
 // Every query still executes against PostgreSQL. The wrapper inserts a real
 // committed change at the READ COMMITTED boundary between the two readers.
 const db={query:async(sql:string,values?:any[])=>{
  const result=await pool.query(sql,values);
  if(!crossed&&sql.includes('SELECT c.*,r.id current_run_id')){
   crossed=true;
   if(mode==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.cr.id,replacement]);
   if(mode==='context'){
    await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','A new current context')",[replacement,f.cr.id]);
    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.cr.id,replacement]);
   }
   if(mode==='case-owner')await pool.query('UPDATE supervision_cases SET decision_run_id=$2 WHERE id=$1',[f.c.id,replacementReview!.id]);
   if(mode==='case-state')await pool.query("UPDATE supervision_cases SET state='resolved' WHERE id=$1",[f.c.id]);
   if(mode==='reviewer-live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[review.id]);
   if(mode==='source-acceptance')await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,false,'[]','fixture-race',clock_timestamp()+interval '1 second')`,[randomUUID(),f.producer,f.pr.id]);
   afterRace=await persisted(f.goal);
  }
  return result;
 }};
 const current=(await readManagerCases(db as any,f.goal)).find(c=>c.id===f.c.id)!;
 expect(crossed).toBe(true);expect(current.materialConsumptionReview).toBeUndefined();
 expect(managerCasePriority(current)).toBe(2);expect(await persisted(f.goal)).toEqual(afterRace);
 if(mode==='fence'||mode==='context'){
  // A new coherent observation can offer a review hint. It still cannot grant
  // stop authority, consume the material or accept the delivery by itself.
  const refreshed=await entry(f);
  expect(refreshed.materialConsumptionReview).toBeDefined();expect(managerCasePriority(refreshed)).toBe(4);
  expect(refreshed.materialConsumptionReview.authority).toBe('review_only');
  expect(await persisted(f.goal)).toEqual(afterRace);
 }
});

it('uses the real manager query and context boundary for two stopped repair consumers, without authorizing or changing any execution',async()=>{
 const a=await fixture(),b=await fixture(a.goal),before=await persisted(a.goal);
 const artifactReads=vi.spyOn(artifactService,'readArtifact').mockImplementation(async()=>{throw Error('priority must not read artifact bytes');});
 const rows=await readManagerCases(pool,a.goal);
 expect(rows).toHaveLength(2);
 for(const f of [a,b]){
  const c=rows.find(row=>row.id===f.c.id)!;
  expect(c.materialConsumptionReview).toMatchObject({version:1,authority:'review_only',actionValidationRequired:true,
   caseId:f.c.id,goalId:f.goal,consumerTaskId:f.consumer,consumerRunId:f.cr.id,consumerAttempt:1,
   obligations:[{obligationId:f.obligation.id,generation:f.obligation.receipt.generation,handoffId:f.handoff.id,
    producerTaskId:f.producer,producerRunId:f.pr.id,producerManifestDigest:f.obligation.producer_manifest_digest,
    repairArtifacts:f.obligation.binding.repairArtifacts}]});
  expect(managerCasePriority(c)).toBe(4);
 }
 const canonical={id:randomUUID(),task_id:randomUUID(),state:'open',pending_dependencies:0,fresh_handoffs:1,
  current_run_state:'submitted',base_workspace:a.scope};
 const prior=selectManagerFocus([canonical],null)!;
 const reviews=Array.from({length:3},(_,i)=>({eventId:String(100-i),caseId:canonical.id,sourceFingerprint:prior.sourceFingerprint}));
 const focus=selectManagerFocus([canonical,...rows],prior,undefined,reviews)!;
 expect(rows.map(c=>c.id)).toContain(focus.caseId);
 const selected=selectManagerCaseWindow([canonical,...rows],focus);
 expect(selected).toHaveLength(1);expect(selected[0].operationalRecovery).toBeUndefined();
 const tasks=await Promise.all(before.tasks.map(async t=>({id:t.id,baseWorkspace:t.execution.baseWorkspace,stage:'work',
  state:t.state,workKind:t.execution.workKind,expectedAttempt:t.attempt_count,criteria:t.criteria,intent:{text:t.description},
  dependencies:before.dependencies.filter(d=>d.task_id===t.id).map(d=>d.depends_on)})));
 const snapshot={cases:projectManagerCases(selected),operation:{focus,tasks:focusManagerTasks(tasks,focus)}};
 const review=projectManagerReview(snapshot,'',0,150000);
 expect(review.withinBudget).toBe(true);
 const parsed=JSON.parse(review.description),context=parsed.sharedContext?expandSharedManagerContext(parsed):parsed;
 const directory=context.operation.focusedTaskDirectory;
 const focused=directory?.lossless?directory.rows.map((r:any[])=>Object.fromEntries(directory.schemas[r[0]].map((key:string,i:number)=>[key,r[i+1]]))):context.operation.tasks;
 expect(context.cases[0].materialConsumptionReview).toEqual(selected[0].materialConsumptionReview);
 expect(context.cases[0].evidence).toEqual(selected[0].evidence);
 expect(focused.find((t:any)=>t.id===focus.taskId).criteria).toEqual(tasks.find(t=>t.id===focus.taskId)!.criteria);
 const source=selected[0].materialConsumptionReview.obligations[0].producerTaskId;
 expect(focused.find((t:any)=>t.id===source).criteria).toEqual(tasks.find(t=>t.id===source)!.criteria);
 expect(artifactReads).not.toHaveBeenCalled();expect(await persisted(a.goal)).toEqual(before);
});

it.each(['source-attempt','source-run','manifest','latest-acceptance','consumer-attempt','consumer-run',
 'receipt-generation','evidence-generation','evidence-source','nonmaterial','decision-owner','live-run','lease','cancel',
 'source-cancel','ready','retry_scheduled','waiting_quota','SUPERVISION_ADMISSION_PENDING','SUPERVISION_RESUME_OWNED',
 'source-conflict-receipt'] as const)
('does not promote stale or already owned material: %s',async mode=>{
 const f=await fixture();expect((await entry(f)).materialConsumptionReview).toBeDefined();
 if(mode==='source-attempt')await pool.query('UPDATE tasks SET attempt_count=attempt_count+1 WHERE id=$1',[f.producer]);
 if(mode==='source-run'){const r=await run(f.producer);await artifactService.failRun(r.id,r.fence,'RESULT_FORMAT','Later source');}
 if(mode==='manifest')await pool.query("UPDATE artifacts SET digest=$2 WHERE run_id=$1 AND name='implementation.patch'",[f.pr.id,sha('changed')]);
 if(mode==='latest-acceptance')await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
  VALUES($1,$2,$3,false,'[]','fixture-revocation',clock_timestamp()+interval '1 second')`,[randomUUID(),f.producer,f.pr.id]);
 if(mode==='consumer-attempt')await pool.query('UPDATE tasks SET attempt_count=attempt_count+1 WHERE id=$1',[f.consumer]);
 if(mode==='consumer-run'){const r=await run(f.consumer);await artifactService.failRun(r.id,r.fence,'RESULT_FORMAT','Later consumer');}
 if(mode==='receipt-generation')await pool.query("UPDATE delivery_consumptions SET receipt=jsonb_set(receipt,'{generation}',to_jsonb($2::text)) WHERE id=$1",[f.obligation.id,sha('changed')]);
 if(mode==='evidence-generation'||mode==='evidence-source'){
  const evidence=structuredClone(f.c.evidence),e=evidence.find((x:any)=>x.kind==='DELIVERY_CONSUMPTION_REQUIRED');
  if(mode==='evidence-generation')e.generation=sha('changed');else e.producerRunId=randomUUID();
  await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[f.c.id,JSON.stringify(evidence)]);
 }
 if(mode==='nonmaterial')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workKind}','\"diagnostic\"') WHERE id=$1",[f.producer]);
 if(mode==='decision-owner')await pool.query('UPDATE supervision_cases SET decision_run_id=$2 WHERE id=$1',[f.c.id,f.pr.id]);
 if(mode==='live-run')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.cr.id]);
 if(mode==='lease')await pool.query('INSERT INTO leases VALUES($1,$2)',['fixture-material',f.cr.id]);
 if(mode==='cancel'||mode==='source-cancel')await event(pool,'CANCEL_REQUESTED',f.consumer,{runId:mode==='cancel'?f.cr.id:f.pr.id});
 if(['ready','retry_scheduled','waiting_quota'].includes(mode)){
  // Model a stale open case after admission. The real supervision hold would
  // otherwise legitimately convert the fixture's UPDATE to recovery_blocked.
  await pool.query("UPDATE supervision_cases SET state='resolved' WHERE id=$1",[f.c.id]);
  await pool.query('UPDATE tasks SET state=$2 WHERE id=$1',[f.consumer,mode]);
  await pool.query("UPDATE supervision_cases SET state='open' WHERE id=$1",[f.c.id]);
  expect((await task(f.consumer)).state).toBe(mode);
 }
 if(mode==='SUPERVISION_ADMISSION_PENDING'||mode==='SUPERVISION_RESUME_OWNED')
  await pool.query("UPDATE delivery_consumptions SET receipt=jsonb_set(receipt,'{code}',to_jsonb($2::text)) WHERE id=$1",[f.obligation.id,mode]);
 if(mode==='source-conflict-receipt')await pool.query("UPDATE delivery_consumptions SET receipt=receipt||$2::jsonb WHERE id=$1",
  [f.obligation.id,JSON.stringify({sourceConflict:{code:'SOURCE_CONTRACT_CHANGED',generation:sha('source contract changed without run or artifact changes')}})]);
 const result=await entry(f);
 if(['ready','retry_scheduled','waiting_quota'].includes(mode))expect(result.current_task_state).toBe(mode);
 expect(result.materialConsumptionReview).toBeUndefined();expect(managerCasePriority(result)).toBe(2);
});

it('restores a qualified consumption hint when the authoritative reconciler clears a source conflict without reappending its old requestKey',async()=>{
 const f=await fixture(),original=await task(f.producer),before=await entry(f);
 await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[f.producer,'Changed source contract awaiting review']);
 await reconcileDeliveryConsumptions(stop);
 const conflicted=(await pool.query('SELECT receipt FROM delivery_consumptions WHERE id=$1',[f.obligation.id])).rows[0].receipt;
 expect(conflicted.sourceConflict).toBeDefined();expect((await entry(f)).materialConsumptionReview).toBeUndefined();
 await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[f.producer,original.description]);
 await reconcileDeliveryConsumptions(stop);
 const restored=(await pool.query('SELECT receipt FROM delivery_consumptions WHERE id=$1',[f.obligation.id])).rows[0].receipt;
 expect(restored.sourceConflict).toBeUndefined();expect(restored.generation).toBe(f.obligation.receipt.generation);
 const current=await entry(f),typed=current.evidence.filter((e:any)=>['DELIVERY_CONSUMPTION_REQUIRED','DELIVERY_SOURCE_REVIEW_REQUIRED'].includes(e.kind));
 expect(typed.filter((e:any)=>e.kind==='DELIVERY_CONSUMPTION_REQUIRED')).toHaveLength(1);
 expect(typed.at(-1).kind).toBe('DELIVERY_SOURCE_REVIEW_REQUIRED');
 expect(current.materialConsumptionReview).toEqual(before.materialConsumptionReview);expect(managerCasePriority(current)).toBe(4);
});

it('keeps the real input-binding review eligible with a pending dependency and leaves all action guards intact',async()=>{
 const f=await fixture(),dependency=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria)
  VALUES($1,$2,$3,'Missing material','A different real prerequisite','qa.backend','recovery_blocked','[]')`,[dependency,f.goal,dependency]);
 await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.consumer,dependency]);
 await reconcileDeliveryConsumptions(stop);
 const before=await persisted(f.goal),c=await entry(f);
 expect(c.pending_dependencies).toBe(1);expect(managerCasePriority(c)).toBe(4);
 expect(c.materialConsumptionReview.authority).toBe('review_only');
 // Input binding is the earlier failure in the authoritative reconciler. Do
 // not invent a dependency-specific episode from the separate current count.
 expect(c.evidence).toEqual(f.c.evidence);
 expect(c.evidence).toContainEqual(expect.objectContaining({kind:'DELIVERY_CONSUMPTION_REQUIRED',code:'CONSUMER_INPUTS_NOT_BOUND',blockedTaskIds:[]}));
 expect(await persisted(f.goal)).toEqual(before);
});

it('distinguishes a new accepted material source at the same handoff count, preserving frozen old evidence',async()=>{
 const f=await fixture(),old=await entry(f),oldFingerprint=managerFocusSourceFingerprint(old);
 const other=await fixture(f.goal);
 // Fixture a canonical append-only resolution receipt. Source acceptance and
 // artifacts are genuine; priority does not apply this receipt or authorize it.
 const refs=(await pool.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[other.pr.id])).rows;
 const material=refs.filter(a=>['implementation.patch','repair-report.json'].includes(a.name));
 const producerManifestDigest=deliveryDigest(refs),generation=sha('new exact material generation');
 await event(pool,'DELIVERY_SOURCE_RESOLVED',f.obligation.id,{version:1,obligationId:f.obligation.id,consumerTaskId:f.consumer,
  original:{producerTaskId:f.producer,producerRunId:f.pr.id,producerManifestDigest:f.obligation.producer_manifest_digest},
  replacement:{producerTaskId:other.producer,producerRunId:other.pr.id,producerManifestDigest,producerArtifacts:refs,repairArtifacts:material}});
 const evidence={...f.c.evidence.find((e:any)=>e.kind==='DELIVERY_CONSUMPTION_REQUIRED'),generation,
  producerTaskId:other.producer,producerRunId:other.pr.id,producerManifestDigest,repairArtifacts:material};
 await pool.query('UPDATE supervision_cases SET evidence=evidence||$2::jsonb WHERE id=$1',[f.c.id,JSON.stringify([evidence])]);
 await pool.query("UPDATE delivery_consumptions SET receipt=jsonb_set(receipt,'{generation}',to_jsonb($2::text)) WHERE id=$1",[f.obligation.id,generation]);
 const current=await entry(f);
 expect(current.fresh_handoffs).toBe(old.fresh_handoffs);expect(managerCasePriority(current)).toBe(4);
 expect(current.materialConsumptionReview.obligations[0]).toMatchObject({producerTaskId:other.producer,producerRunId:other.pr.id,producerManifestDigest,generation});
 expect(managerFocusSourceFingerprint(current)).not.toBe(oldFingerprint);
 expect(current.evidence).toEqual([...f.c.evidence,evidence]);
});

it('preserves service fairness and final evidence under receipt polls and auxiliary chatter across restarts',async()=>{
 const a=await fixture(),b=await fixture(a.goal),rows=await readManagerCases(pool,a.goal);
 const first=rows.find(c=>c.id===a.c.id)!,second=rows.find(c=>c.id===b.c.id)!;
 const fingerprint=managerFocusSourceFingerprint(first);
 await pool.query("UPDATE delivery_consumptions SET checked_at=now(),updated_at=now(),receipt=receipt||'{\"observation\":\"later\"}' WHERE id=$1",[a.obligation.id]);
 await pool.query("UPDATE supervision_cases SET updated_at=now(),evidence=evidence||'[{\"kind\":\"AUXILIARY_OBSERVATION\"}]' WHERE id=$1",[a.c.id]);
 const polled=await entry(a);expect(managerFocusSourceFingerprint(polled)).toBe(fingerprint);
 const focus=selectManagerFocus([polled,second],null)!;
 const reviews=Array.from({length:3},(_,i)=>({eventId:String(300-i),caseId:focus.caseId,
  sourceFingerprint:managerFocusSourceFingerprint({...polled,fresh_handoffs:i+1,current_artifacts:[{id:'chatter',digest:sha(String(i))}]})}));
 const ledger=managerFocusServiceHistory([polled,second],null,reviews);
 const next=selectManagerFocus([polled,second],JSON.parse(JSON.stringify(focus)),undefined,reviews,Date.now(),JSON.parse(JSON.stringify(ledger)))!;
 expect(next).toMatchObject({caseId:b.c.id,selectionReason:'review_quantum'});
 expect(selectManagerFocus([polled,second],focus,undefined,reviews.slice(0,2))?.caseId).toBe(a.c.id);
 expect((await task(a.consumer)).attempt_count).toBe(1);expect((await task(b.consumer)).attempt_count).toBe(1);
});

it('does not import material repair consumers into the transport-only operational batch',async()=>{
 const a=await fixture(),b=await fixture(a.goal),rows=await readManagerCases(pool,a.goal),focus=selectManagerFocus(rows,null)!;
 const other=rows.find(c=>c.id!==focus.caseId)!;
 const window=selectManagerCaseWindow(rows,focus);
 expect(window.map(c=>c.id)).toEqual([focus.caseId]);expect(other.materialConsumptionReview).toBeDefined();
 expect(projectManagerCases(window)[0]).not.toHaveProperty('operationalRecovery');
});
