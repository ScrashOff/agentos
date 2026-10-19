import {beforeAll,beforeEach,afterAll,expect,it} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,migrate,lock} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
import {installSupervision,recordSupervisionDecisions,applySupervisionDecisions} from '../server/supervision.js';
import {requestSemanticReplan,reconcileCanonicalRepairRejections,semanticRepairRejection,
  readCanonicalRepairRejection,readCanonicalRepairRejections,semanticRetryCode} from '../server/semantic-retry.js';
import {recoverOpenCodeRun} from '../server/opencode-recovery.js';
import {resumeFailedHarness} from '../server/continuation.js';
import {glmRepairCriteria} from '../server/glm-repair.js';
import {canonicalRepairReviewIsFresh,recordCanonicalRepairRejection} from '../server/canonical-repair-rejection.js';
import {admissionReviewIsFresh} from '../server/admission-supervision.js';
import {readRecertificationPhase,recertificationDecisionCode} from '../server/supervision-recertification.js';
import {installManagement} from '../server/management.js';
import {installDeferredHandoffs} from '../server/deferred-handoffs.js';
import {readManagerCases,projectManagerCases} from '../server/manager-material-consumption.js';
import {selectManagerFocus,selectManagerCaseWindow,managerCasePriority} from '../server/manager-focus.js';
import {projectManagerReview} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installSupervision();await installManagement();await installDeferredHandoffs();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});

async function fixture(kind:'proof'|'semantic'='proof',runtime='opencode',ownerState:string|null='open'){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),context=randomUUID(),owner=randomUUID();
  const workspace='canonical-test/'+task;
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown)
    VALUES('canonical-test','test','openai','test','["test"]',true,2,true)`);
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Canonical rejection','Preserve final criteria','canonical-test',$1::text)`,[goal]);
  await pool.query(`INSERT INTO settings(id,value) VALUES($1,'{"enabled":true}')`,['supervision:'+goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,stage,
    criteria,execution,attempt_count,max_attempts) VALUES($1,$2,'repair','Repair','Exact authorized scope',
    'qa.backend','canonical-test','test','running','work',$3,$4,1,1)`,[task,goal,JSON.stringify(glmRepairCriteria),
      JSON.stringify({kind:runtime,workKind:'repair',workspace,baseWorkspace:'canonical-test/game'})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'canonical-test','test',$4,1,'running')`,[run,task,fence,runtime]);
  await pool.query(`INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Original immutable context')`,[context,run]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,context]);
  const patch=await writeArtifact(run,fence,'implementation.patch',
    (kind==='proof'?'Untrusted explanatory preamble\n':'')+'--- a/server.mjs\n+++ b/server.mjs\n@@ -1 +1 @@\n-old\n+new\n');
  const report=await writeArtifact(run,fence,'repair-report.json',JSON.stringify({fixVerified:kind==='proof',
    sourceHashes:[{path:'server.mjs',before:'a'.repeat(64),after:'b'.repeat(64)}],
    detail:'private detail must not enter the projected canonical fact'}));
  await writeArtifact(run,fence,'repair-tests.log','A real negative or unverified test report remains negative.');
  await completeRun(run,fence,'Repair delivered, acceptance is independent');await validateTask(task);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  if(ownerState)await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,request_key)
    VALUES($1,$2,$3,$4,$5,'Existing consumption owner','[{"kind":"DELIVERY_CONSUMPTION_REQUIRED","code":"CONSUMER_INPUTS_NOT_BOUND"}]','delivery:existing')`,
    [owner,goal,task,run,ownerState]);
  return {goal,task,run,fence,context,owner,patch,report,workspace,runtime};
}
async function immutable(f:Awaited<ReturnType<typeof fixture>>){
  return {runs:(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt',[f.task])).rows,
    artifacts:(await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows,
    acceptances:(await pool.query('SELECT * FROM acceptances WHERE run_id=$1 ORDER BY id',[f.run])).rows,
    contexts:(await pool.query('SELECT * FROM contexts WHERE run_id=$1',[f.run])).rows,
    task:(await pool.query('SELECT criteria,execution,attempt_count,max_attempts FROM tasks WHERE id=$1',[f.task])).rows[0],
    leases:(await pool.query('SELECT * FROM leases WHERE run_id=$1',[f.run])).rows};
}
const ownerRow=async(id:string)=>(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[id])).rows[0];
async function acceptedReview(f:Awaited<ReturnType<typeof fixture>>){
  const task=randomUUID(),run=randomUUID(),fence=randomUUID();
  const action={caseId:f.owner,kind:'wait_for_evidence',diagnosis:'A precise input is still absent from the preserved delivery.',
    instruction:'Preserve the original criteria and wait only for the explicit missing input.',
    waitingFor:['The exact missing input recorded in the source report'],evidenceArtifactIds:[f.report.id]};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,stage,criteria,state,attempt_count)
    VALUES($1,$2,$3,'Review','Frozen review before or after canonical evidence','qa.backend','coordinate',
      '[{"id":"coord","type":"artifact","name":"coordination.json","label":"Decision"}]','running',1)`,
    [task,f.goal,'review-'+task]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'harness',1,'running')`,[run,task,fence]);
  await writeArtifact(run,fence,'coordination.json',JSON.stringify({supervision:{actions:[action]}}));
  await completeRun(run,fence,'Accepted review is not acceptance of the repair');
  expect((await validateTask(task)).passed).toBe(true);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
  return {run,task,action,createdAt:(await pool.query('SELECT created_at FROM runs WHERE id=$1',[run])).rows[0].created_at};
}
const stoppedProof=(f:Awaited<ReturnType<typeof fixture>>)=>({runId:f.run,runtime:f.runtime,workspace:f.workspace,
  checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+f.run,activeState:'inactive',mainPid:0,populated:false,stopped:true}});

it.each(['proof','semantic'] as const)('attaches %s rejection exactly once to the existing consumption owner',async(kind)=>{
  const f=await fixture(kind),before=await immutable(f),original=await ownerRow(f.owner);
  await Promise.all([requestSemanticReplan(f.task,f.run),requestSemanticReplan(f.task,f.run)]);
  const owner=await ownerRow(f.owner),fact=await readCanonicalRepairRejection(pool,f.run);
  expect(owner.evidence).toHaveLength(2);expect(owner.evidence[0]).toEqual(original.evidence[0]);
  expect(owner).toMatchObject({state:'open',request_key:'delivery:existing',decision:null,decision_run_id:null});
  expect(fact).toMatchObject({authority:'review_only',taskId:f.task,runId:f.run,runFence:f.fence,
    contextVersion:f.context,ownerCaseId:f.owner,disposition:'routed',routed:true,acceptance:false,
    rejection:{kind:kind==='proof'?'repair_source_proof_rejected':'semantic_repair_rejected'}});
  if(kind==='proof')expect(fact.rejection).toMatchObject({code:'REPAIR_SOURCE_PROOF_PATCH_FORMAT',
    patch:{id:f.patch.id,digest:f.patch.digest},report:{id:f.report.id,digest:f.report.digest}});
  expect(JSON.stringify(fact)).not.toContain('private detail');expect(await immutable(f)).toEqual(before);
  expect(await readCanonicalRepairRejections(pool,f.goal)).toEqual([{...fact,
    reason:kind==='proof'?'REPAIR_SOURCE_PROOF_PATCH_FORMAT':'semantic_repair_rejected'}]);
  expect(await readCanonicalRepairRejections(pool,randomUUID())).toEqual([]);
  expect((await pool.query("SELECT id FROM events WHERE kind='CANONICAL_REPAIR_REJECTION_OBSERVED' AND entity_id=$1",[f.task])).rowCount).toBe(1);
});

it.each(['decided','delegated','waiting_evidence'])('retains a visible deferred fact without changing the %s decision',async(state)=>{
  const f=await fixture('proof','opencode',state),before=await immutable(f),owner=await ownerRow(f.owner);
  await reconcileCanonicalRepairRejections();await reconcileCanonicalRepairRejections();
  expect(await ownerRow(f.owner)).toEqual(owner);expect(await immutable(f)).toEqual(before);
  expect(await readCanonicalRepairRejection(pool,f.run)).toMatchObject({ownerCaseId:f.owner,
    ownerState:state,disposition:'deferred_existing_decision',routed:false,rejection:{code:'REPAIR_SOURCE_PROOF_PATCH_FORMAT'}});
  expect((await pool.query("SELECT id FROM events WHERE kind='CANONICAL_REPAIR_REJECTION_OBSERVED' AND entity_id=$1",[f.task])).rowCount).toBe(1);
  await pool.query("UPDATE supervision_cases SET state='open' WHERE id=$1",[f.owner]);
  expect(await readCanonicalRepairRejection(pool,f.run)).toBeNull();
  await reconcileCanonicalRepairRejections();
  expect(await readCanonicalRepairRejection(pool,f.run)).toMatchObject({disposition:'routed',routed:true});
  expect((await ownerRow(f.owner)).evidence).toHaveLength(2);
});

it('keeps an applied unrelated decision immutable and opens a distinct follow-up for its deferred rejection',async()=>{
  const f=await fixture('semantic','opencode','decided');await reconcileCanonicalRepairRejections();
  await pool.query("UPDATE supervision_cases SET state='applied',applied_attempt=1 WHERE id=$1",[f.owner]);
  const before=await ownerRow(f.owner);
  await reconcileCanonicalRepairRejections();
  expect(await ownerRow(f.owner)).toEqual(before);
  const cases=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1 ORDER BY created_at,id',[f.task])).rows;
  expect(cases).toHaveLength(2);expect(cases.find(c=>c.id!==f.owner)).toMatchObject({state:'open',source_run_id:f.run,
    parent_case_id:f.owner,evidence:[expect.objectContaining({kind:'semantic_repair_rejected'})]});
  expect((await pool.query('SELECT id FROM runs WHERE task_id=$1',[f.task])).rowCount).toBe(1);
});

it.each(['opencode','harness'])('a canonical PATCH_FORMAT rejection blocks a generic %s retry without changing its original receipt',async(runtime)=>{
  const f=await fixture('proof',runtime,null),before=await immutable(f);
  const proof={runId:f.run,runtime,workspace:f.workspace,checkedAt:new Date().toISOString(),state:{},
    candidate:{patch:'',files:[]},process:{unit:'agentos-run-'+f.run,activeState:'inactive',mainPid:0,populated:false,stopped:true}};
  await expect((runtime==='opencode'?recoverOpenCodeRun:resumeFailedHarness)(f.run,f.fence,proof)).rejects.toThrow(semanticRetryCode);
  expect(await immutable(f)).toEqual(before);
});

it.each(['fence','context','acceptance','cancel','live'])('discards prepared evidence after a %s race',async(race)=>{
  const f=await fixture(),task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0],
    run=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
  const rejection=await semanticRepairRejection(pool,task,run);
  const prepared={runFence:f.fence,contextId:f.context,attempt:1,
    criteriaDigest:createHash('sha256').update(JSON.stringify(task.criteria)).digest('hex'),rejection};
  if(race==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
  if(race==='context')await pool.query('UPDATE runs SET context_id=NULL WHERE id=$1',[f.run]);
  if(race==='acceptance')await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,true,'[]','newer',now()+interval '1 second')`,[randomUUID(),f.task,f.run]);
  if(race==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.task,JSON.stringify({runId:f.run})]);
  if(race==='live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
  const before=await immutable(f),owner=await ownerRow(f.owner);
  expect(await requestSemanticReplan(f.task,f.run,prepared)).toBe(false);
  expect(await immutable(f)).toEqual(before);expect(await ownerRow(f.owner)).toEqual(owner);
  expect(await readCanonicalRepairRejection(pool,f.run)).toBeNull();
});

it('does not project a routed receipt for a replaced fence or rewrite an open case with a decision already recorded',async()=>{
  const f=await fixture();
  await pool.query('UPDATE supervision_cases SET decision=$2::jsonb WHERE id=$1',[f.owner,JSON.stringify({actions:[]})]);
  const owner=await ownerRow(f.owner);
  await reconcileCanonicalRepairRejections();
  expect(await ownerRow(f.owner)).toEqual(owner);
  expect(await readCanonicalRepairRejection(pool,f.run)).toMatchObject({routed:false,disposition:'deferred_existing_decision'});
  await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
  expect(await readCanonicalRepairRejection(pool,f.run)).toBeNull();
  expect(await readCanonicalRepairRejections(pool,f.goal)).toEqual([]);
});

it.each(['submitted','running'])('handles a reopened case whose historical reviewer is %s without rewriting the decision',async(state)=>{
  const f=await fixture(),coordinator=randomUUID(),review=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,stage,criteria,state,attempt_count)
    VALUES($1,$2,'old-review','Historical review','Preserved review','qa.backend','coordinate','[]',$3,1)`,
    [coordinator,f.goal,state]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'harness',1,$4)`,[review,coordinator,randomUUID(),state]);
  await pool.query('UPDATE supervision_cases SET decision_run_id=$2,decision=$3::jsonb WHERE id=$1',
    [f.owner,review,JSON.stringify({actions:[{caseId:f.owner,kind:'wait_for_evidence'}]})]);
  const before=await ownerRow(f.owner);await reconcileCanonicalRepairRejections();
  const after=await ownerRow(f.owner);
  expect(after).toMatchObject({state:'open',decision:before.decision,decision_run_id:review});
  expect(after.evidence).toHaveLength(state==='submitted'?2:1);
  expect(await readCanonicalRepairRejection(pool,f.run)).toMatchObject({routed:state==='submitted',
    disposition:state==='submitted'?'routed':'deferred_existing_decision'});
  if(state==='submitted'){
    const at=Date.parse(after.evidence[1].observedAt);
    expect(canonicalRepairReviewIsFresh(after.evidence,new Date(at-1))).toBe(false);
    expect(canonicalRepairReviewIsFresh(after.evidence,new Date(at))).toBe(false);
    expect(canonicalRepairReviewIsFresh(after.evidence,new Date(at+1))).toBe(true);
    expect(canonicalRepairReviewIsFresh([{...after.evidence[1],observedAt:'invalid'}],new Date())).toBe(false);
  }
});

it('rejects a stale decision recorded across the append race before any effect, then permits a genuinely fresh review',async()=>{
  const f=await fixture(),review=await acceptedReview(f),before=await immutable(f);
  const earlierCase=await ownerRow(f.owner);
  expect(admissionReviewIsFresh(earlierCase.evidence,review.createdAt)).toBe(true);
  await reconcileCanonicalRepairRejections();
  expect(admissionReviewIsFresh((await ownerRow(f.owner)).evidence,review.createdAt)).toBe(false);
  await recordSupervisionDecisions();expect((await ownerRow(f.owner)).state).toBe('open');
  // Simulate the old recorder's UPDATE after its initial read/check: the locked
  // applier must reject this interleaving even without modifying pinned SQL.
  await pool.query("UPDATE supervision_cases SET state='decided',decision_run_id=$2,decision=$3 WHERE id=$1",
    [f.owner,review.run,JSON.stringify({actions:[review.action]})]);
  const phase=await readRecertificationPhase(pool,await ownerRow(f.owner),review.run);
  expect(recertificationDecisionCode(phase,[review.action])).toBe('CANONICAL_REPAIR_REVIEW_STALE');
  await Promise.all([applySupervisionDecisions(async()=>stoppedProof(f)),applySupervisionDecisions(async()=>stoppedProof(f))]);
  expect(await ownerRow(f.owner)).toMatchObject({state:'open',decision_run_id:null,decision:null});
  expect(await immutable(f)).toEqual(before);
  expect((await pool.query("SELECT id FROM events WHERE kind='DELIVERY_SOURCE_DECISION_REJECTED' AND payload->>'caseId'=$1 AND payload->>'code'='CANONICAL_REPAIR_REVIEW_STALE'",[f.owner])).rowCount).toBe(1);
  const fresh=await acceptedReview(f);await recordSupervisionDecisions();
  expect((await ownerRow(f.owner)).decision_run_id).toBe(fresh.run);
  expect(await readRecertificationPhase(pool,await ownerRow(f.owner),fresh.run)).toBeNull();
  await applySupervisionDecisions(async()=>stoppedProof(f));
  expect((await ownerRow(f.owner)).state).toBe('waiting_evidence');
  expect((await pool.query('SELECT id FROM runs WHERE task_id=$1',[f.task])).rowCount).toBe(1);
  expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1 ORDER BY id',[f.run])).rows).toEqual(before.acceptances);
});

it.each(['stop','cancel','paused'])('a stale review preserves the existing %s guard before reopening',async(guard)=>{
  const f=await fixture(),review=await acceptedReview(f);await reconcileCanonicalRepairRejections();
  await pool.query("UPDATE supervision_cases SET state='decided',decision_run_id=$2,decision=$3 WHERE id=$1",
    [f.owner,review.run,JSON.stringify({actions:[review.action]})]);
  if(guard==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.task,JSON.stringify({runId:f.run})]);
  if(guard==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  const before=await ownerRow(f.owner);
  await applySupervisionDecisions(async()=>({...stoppedProof(f),...(guard==='stop'?{checkedAt:'2000-01-01T00:00:00Z'}:{})}));
  const after=await ownerRow(f.owner);
  if(guard==='stop'){
    const {reason:previousReason,updated_at:previousUpdated,...beforeIdentity}=before;
    const {reason,updated_at:updatedAt,...afterIdentity}=after;
    expect(afterIdentity).toEqual(beforeIdentity);
    expect(reason).toBe('Aguardando aplicação segura: STOP_UNCONFIRMED');
    expect(new Date(updatedAt).getTime()).toBeGreaterThanOrEqual(new Date(previousUpdated).getTime());
  }else expect(after).toEqual(before);
  expect((await pool.query("SELECT id FROM events WHERE kind='DELIVERY_SOURCE_DECISION_REJECTED' AND payload->>'caseId'=$1 AND payload->>'code'='CANONICAL_REPAIR_REVIEW_STALE'",[f.owner])).rowCount).toBe(0);
  expect((await pool.query('SELECT id FROM runs WHERE task_id=$1',[f.task])).rowCount).toBe(1);
});

it('fences actual evidence attachment even when its transaction began before a concurrent review',async()=>{
  const f=await fixture(),client=await pool.connect();
  try{
    await client.query('BEGIN');await client.query('SELECT now()');
    const review=await acceptedReview(f);
    const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0],
      run=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
    const rejection=await semanticRepairRejection(pool,task,run);
    await lock(client);
    const owner=(await client.query('SELECT * FROM supervision_cases WHERE id=$1 FOR UPDATE',[f.owner])).rows[0];
    await recordCanonicalRepairRejection(client,task,run,rejection,owner);
    await client.query('COMMIT');
    expect(recertificationDecisionCode(await readRecertificationPhase(pool,await ownerRow(f.owner),review.run),[review.action]))
      .toBe('CANONICAL_REPAIR_REVIEW_STALE');
  }finally{await client.query('ROLLBACK').catch(()=>{});client.release();}
});

it('keeps a current rejection visible beside an older pending owner without attaching to the wrong run',async()=>{
  const f=await fixture(),run=randomUUID(),fence=randomUUID(),context=randomUUID();
  // The current attempt must pass the normal submit/validate contract before
  // this older owner reopens; an already pending case correctly holds submit.
  await pool.query("UPDATE supervision_cases SET state='resolved' WHERE id=$1",[f.owner]);
  await pool.query("UPDATE tasks SET state='running',attempt_count=2,max_attempts=2 WHERE id=$1",[f.task]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,2,$3,'opencode',1,'running')`,[run,f.task,fence]);
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Current attempt context')",[context,run]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,context]);
  const patch=await writeArtifact(run,fence,'implementation.patch','Wrong preamble\n--- a/server.mjs\n+++ b/server.mjs\n@@ -1 +1 @@\n-old\n+new\n');
  const report=await writeArtifact(run,fence,'repair-report.json',JSON.stringify({fixVerified:true,
    sourceHashes:[{path:'server.mjs',before:'a'.repeat(64),after:'b'.repeat(64)}]}));
  await writeArtifact(run,fence,'repair-tests.log','Retained evidence.');
  await completeRun(run,fence,'New current delivery');await validateTask(f.task);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
  await pool.query("UPDATE supervision_cases SET state='open' WHERE id=$1",[f.owner]);
  const current={...f,run,fence,context,patch,report},before=await immutable(current),owner=await ownerRow(f.owner);
  await reconcileCanonicalRepairRejections();
  expect(await ownerRow(f.owner)).toEqual(owner);expect(await immutable(current)).toEqual(before);
  expect((await pool.query('SELECT id FROM supervision_cases WHERE task_id=$1',[f.task])).rowCount).toBe(1);
  expect(await readCanonicalRepairRejection(pool,run)).toMatchObject({runId:run,runFence:fence,contextVersion:context,
    expectedAttempt:2,ownerCaseId:f.owner,ownerSourceRunId:f.run,routed:false,disposition:'deferred_owner_run_mismatch',
    rejection:{code:'REPAIR_SOURCE_PROOF_PATCH_FORMAT'}});
  expect(await readCanonicalRepairRejections(pool,f.goal)).toHaveLength(1);
  await pool.query('UPDATE supervision_cases SET source_run_id=$2 WHERE id=$1',[f.owner,run]);
  expect(await readCanonicalRepairRejection(pool,run)).toBeNull();
  expect(await readCanonicalRepairRejections(pool,f.goal)).toEqual([]);
});

function historicalFocus(entry:any){
  const high={id:'canonical-delivery',task_id:'canonical-task',state:'open',pending_dependencies:0,
    fresh_handoffs:6,current_run_state:'submitted',base_workspace:'other/scope'};
  const cases=[high,entry],prior=selectManagerFocus(cases,null)!;
  const reviews=Array.from({length:3},(_,i)=>({eventId:String(100-i),caseId:high.id,
    sourceFingerprint:prior.sourceFingerprint,valid:true}));
  return {cases,high,focus:selectManagerFocus(cases,prior,undefined,reviews)!};
}

it('routes a new canonical cause to an aged open case with a terminal historical review while preserving freshness and history',async()=>{
  const f=await fixture(),old=await acceptedReview(f);
  await pool.query(`UPDATE supervision_cases SET decision_run_id=$2,decision=$3,
    created_at=clock_timestamp()-interval '1 hour' WHERE id=$1`,[f.owner,old.run,JSON.stringify({actions:[old.action]})]);
  await requestSemanticReplan(f.task,f.run);
  const original=await ownerRow(f.owner),before=await immutable(f);
  const entry=(await readManagerCases(pool,f.goal,false)).find(c=>c.id===f.owner)!;
  expect(entry).toMatchObject({state:'open',decision_run_id:old.run,decision_review_id:old.run,
    decision_review_state:'submitted',decision_review_goal_id:f.goal,decision_review_stage:'coordinate',
    current_run_state:'submitted',has_live_run:false});
  expect(managerCasePriority(entry)).toBe(2);
  const {cases,focus}=historicalFocus(entry);
  expect(focus).toMatchObject({caseId:f.owner,selectionReason:'aged_review_quantum'});
  const selected=selectManagerCaseWindow(cases,focus);
  expect(selected.map(c=>c.id)).toEqual([f.owner]);
  const review=projectManagerReview({cases:projectManagerCases(selected),operation:{focus,tasks:[]}},'',0,150000);
  expect(review.withinBudget).toBe(true);
  const parsed=JSON.parse(review.description),context=parsed.sharedContext?expandSharedManagerContext(parsed):parsed;
  const cause=original.evidence.find((e:any)=>e.kind==='repair_source_proof_rejected');
  expect(cause?.causeDigest).toMatch(/^[a-f0-9]{64}$/);
  expect(context.cases[0].evidence).toContainEqual(expect.objectContaining({causeDigest:cause.causeDigest,
    sourceRunId:f.run,sourceRunFence:f.fence,sourceContextVersion:f.context,authority:'review_only'}));
  expect(admissionReviewIsFresh(original.evidence,old.createdAt)).toBe(false);
  expect(recertificationDecisionCode(await readRecertificationPhase(pool,original,old.run),[old.action]))
    .toBe('CANONICAL_REPAIR_REVIEW_STALE');
  expect(await ownerRow(f.owner)).toEqual(original);expect(await immutable(f)).toEqual(before);
  const fresh=await acceptedReview(f);
  expect(admissionReviewIsFresh(original.evidence,fresh.createdAt)).toBe(true);
  expect(await readRecertificationPhase(pool,original,fresh.run)).toBeNull();
  expect(await ownerRow(f.owner)).toEqual(original);expect(await immutable(f)).toEqual(before);
});

it.each(['queued','running','cancelling','reconciling'])('keeps an open case out of borrowed reviews while its referenced review is %s',async state=>{
  const f=await fixture(),review=await acceptedReview(f);
  await pool.query('UPDATE runs SET state=$2 WHERE id=$1',[review.run,state]);
  await pool.query(`UPDATE supervision_cases SET decision_run_id=$2,
    created_at=clock_timestamp()-interval '1 hour' WHERE id=$1`,[f.owner,review.run]);
  const before=await ownerRow(f.owner),entry=(await readManagerCases(pool,f.goal,false)).find(c=>c.id===f.owner)!;
  expect(entry).toMatchObject({decision_review_id:review.run,decision_review_state:state,
    decision_review_goal_id:f.goal,decision_review_stage:'coordinate',has_live_run:false});
  const result=historicalFocus(entry);expect(result.focus.caseId).toBe(result.high.id);
  expect(await ownerRow(f.owner)).toEqual(before);
});

it.each(['foreign-goal','non-coordinator'])('does not infer historical review identity from a terminal run with %s binding',async mode=>{
  const f=await fixture(),reviewGoal=mode==='foreign-goal'?randomUUID():f.goal;
  if(reviewGoal!==f.goal)await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Other goal','Independent scope','canonical-test',$1::text)`,[reviewGoal]);
  const review=await acceptedReview({...f,goal:reviewGoal});
  if(mode==='non-coordinator')await pool.query("UPDATE tasks SET stage='work' WHERE id=$1",[review.task]);
  await pool.query(`UPDATE supervision_cases SET decision_run_id=$2,
    created_at=clock_timestamp()-interval '1 hour' WHERE id=$1`,[f.owner,review.run]);
  const before=await ownerRow(f.owner),entry=(await readManagerCases(pool,f.goal,false)).find(c=>c.id===f.owner)!;
  expect(entry.decision_review_id).toBe(review.run);expect(entry.decision_review_state).toBe('submitted');
  if(mode==='foreign-goal')expect(entry.decision_review_goal_id).not.toBe(entry.goal_id);
  else expect(entry.decision_review_stage).toBe('work');
  const result=historicalFocus(entry);expect(result.focus.caseId).toBe(result.high.id);
  expect(await ownerRow(f.owner)).toEqual(before);
});
