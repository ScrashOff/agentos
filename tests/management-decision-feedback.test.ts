import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {config} from '../server/config.js';
import {installSupervision,recordSupervisionDecisions} from '../server/supervision.js';
import {installDeliveryTransitions} from '../server/delivery-transitions.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
import {readManagementDecisionFeedback} from '../server/management-decision-feedback.js';
import {readManagementFollowThrough} from '../server/management-follow-through.js';
import {projectManagerReview,projectPersistedManagerDescription,managerContextVersion,managerContextGuide} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {installDeferredHandoffs} from '../server/deferred-handoffs.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installDeferredHandoffs();await installSupervision();await installDeliveryTransitions();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('feedback-test','Test','test','[]','test',1,'Test fixture')");});
afterAll(async()=>{await pool.end();});
async function goal(){const id=randomUUID();await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
 VALUES($1,'Exact delivery','Preserve source proof','feedback-test',$2)`,[id,id]);return id;}
async function target(goalId:string){
  const task=randomUUID(),source=randomUUID(),caseId=randomUUID(),obligation=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Blocked integration','Preserve actual verification','feedback-test','failed','integrate',
    '[{"id":"verified","type":"artifact","name":"verification.json"}]',1)`,[task,goalId,task]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code)
    VALUES($1,$2,1,$3,'test',1,'failed','RUNTIME_EXITED')`,[source,task,randomUUID()]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
    VALUES($1,$2,$3,$4,'open','Source proof must be resolved',$5)`,[caseId,goalId,task,source,
      JSON.stringify([{kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:obligation,generation:'a'.repeat(64)}])]);
  return {goal:goalId,task,source,caseId,obligation};
}
async function review(goalId:string,caseId?:string){
  const task=randomUUID(),run=randomUUID(),fence=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Source decision','Resolve the exact case','feedback-test','running','coordinate',
    '[{"id":"decision","type":"artifact","name":"coordination.json"}]',1)`,[task,goalId,task]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,started_at)
    VALUES($1,$2,1,$3,'test',1,'running',now())`,[run,task,fence]);
  await writeArtifact(run,fence,'coordination.json',JSON.stringify({supervision:{actions:caseId?[{
    caseId,kind:'revise',diagnosis:'The source requires a verified explicit binding, not a textual promise.',
    instruction:'Use the existing accepted repair after verifying the exact source, then preserve independent QA and all original criteria.',
  }]:[]}}),'application/json');
  await completeRun(run,fence,'Actual coordination artifact',{});
  expect((await validateTask(task)).passed).toBe(true);
  return {task,run};
}
async function fixture(){const f=await target(await goal()),r=await review(f.goal,f.caseId);
  await recordSupervisionDecisions();return {...f,review:r.run,manager:r.task};}
async function rejection(f:Awaited<ReturnType<typeof target>>,runId:string,code='DELIVERY_RESOLUTION_REQUIRED',extra:Record<string,unknown>={}){
  await event(pool,'SUPERVISION_PLAN_REJECTED',f.task,{caseId:f.caseId,runId,code,...extra});
}
const inspect=(f:Awaited<ReturnType<typeof target>>)=>readManagementDecisionFeedback(pool,f.goal,{caseId:f.caseId});

it('returns only current case-scoped conflict bindings, not the consumption hash or an invented replacement run',async()=>{
 const f=await fixture(),other=await target(f.goal),generation='a'.repeat(64),consumption='b'.repeat(64);
 const evidence=[{kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:f.obligation,generation:consumption,
  sourceConflict:{generation,observed:{runId:randomUUID()}},reason:'PRIVATE EVIDENCE'},
  {kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:randomUUID(),sourceConflict:{generation:'invalid'}},
  {kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:'not-uuid',sourceConflict:{generation}},
  {kind:'OTHER_EVENT',obligationId:randomUUID(),sourceConflict:{generation:'c'.repeat(64)}}];
 await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[f.caseId,JSON.stringify(evidence)]);
 await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[other.caseId,JSON.stringify([
  {kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:other.obligation,sourceConflict:{generation:'d'.repeat(64)}}])]);
 const handoff=randomUUID();
 await pool.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,
  source_proof,consumer_proof,handoff,state) VALUES($1,$2,$3,$4,$3,'{}','{}','{}','applied')`,[handoff,f.goal,other.task,f.task]);
 await pool.query(`INSERT INTO delivery_consumptions(id,handoff_id,goal_id,producer_task_id,producer_run_id,consumer_task_id,
  consumer_contract_digest,producer_manifest_digest,binding,state,receipt)
  VALUES($1,$2,$3,$4,$5,$6,$7,$7,'{}','waiting_supervision',$8)`,
  [f.obligation,handoff,f.goal,other.task,other.source,f.task,'a'.repeat(64),JSON.stringify({generation:consumption,sourceConflict:{generation}})]);
 const result=await inspect(f),entry=result.entries[0];
 expect(entry.currentResolutionBindings).toEqual([{obligationId:f.obligation,generation}]);
 expect(entry.currentResolutionBindingsTruncated).toBe(false);
 expect(entry.currentResolutionBindings?.[0]).not.toHaveProperty('expectedSourceRunId');
 expect(entry.nextAction).toContain('receipt.sourceConflict.generation');
 expect(entry.nextAction).toContain('never consumptionGeneration');
 expect(JSON.stringify(result)).not.toContain('PRIVATE EVIDENCE');
 expect(JSON.stringify(result)).not.toContain(other.obligation);
 expect((await pool.query('SELECT evidence FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0].evidence).toEqual(evidence);
 await pool.query("UPDATE delivery_consumptions SET receipt=jsonb_set(receipt,'{sourceConflict,generation}',to_jsonb($2::text)) WHERE id=$1",[f.obligation,'e'.repeat(64)]);
 expect((await inspect(f)).entries[0].currentResolutionBindings).toEqual([]);
 await pool.query('UPDATE delivery_consumptions SET receipt=$2,consumer_task_id=$3 WHERE id=$1',
  [f.obligation,JSON.stringify({generation:consumption,sourceConflict:{generation}}),other.task]);
 expect((await inspect(f)).entries[0].currentResolutionBindings).toEqual([]);
 await pool.query("UPDATE delivery_consumptions SET consumer_task_id=$2,state='completed' WHERE id=$1",[f.obligation,f.task]);
 expect((await inspect(f)).entries[0].currentResolutionBindings).toEqual([]);
});

it('carries the real recorder rejection through follow-through and a budgeted persisted manager projection',async()=>{
  const f=await fixture();
  const before=(await pool.query('SELECT state,decision,decision_run_id FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0];
  expect(before).toMatchObject({state:'open',decision:null,decision_run_id:null});
  const feedback=await inspect(f),entry=feedback.entries[0];
  expect(entry).toMatchObject({caseId:f.caseId,taskId:f.task,sourceRunId:f.source,sourceAttempt:1,
    decisionRunId:f.review,eventKind:'SUPERVISION_PLAN_REJECTED',code:'DELIVERY_RESOLUTION_REQUIRED'});
  expect(Number.isFinite(Date.parse(entry.rejectedAt))).toBe(true);
  expect(entry.nextAction).toContain('expectedSourceRunId');
  await pool.query(`INSERT INTO delivery_transitions(id,goal_id,review_run_id,task_id,expected_attempt,state,proposal,receipt)
    VALUES($1,$2,$3,$4,1,'tracking','{"expectedArtifact":"verification.json"}','{"code":"TRANSITION_BOUND"}')`,
    [randomUUID(),f.goal,f.review,f.task]);
  const followThrough=await readManagementFollowThrough(pool,f.goal,f.review);
  expect(followThrough).toMatchObject({code:'MANAGEMENT_CASE_ACTION_REQUIRED',satisfied:false,decisionFeedback:entry,nextAction:entry.nextAction});
  const source={cases:[{id:f.caseId,taskId:f.task,evidence:[{kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:f.obligation}]}],
    operation:{focus:{caseId:f.caseId,taskId:f.task},decisionFeedback:feedback,previous:{followThrough},
      tasks:[],sourceCatalog:{selection:{windowed:true},columns:['id'],rows:[]},
      coverageIndex:{scope:'test/scope',complete:true,rows:Array.from({length:600},(_,i)=>({id:'coverage-'+i,
        criteria:[{id:'criterion-'+i,label:'Original condition '+i}],title:'Distinct tracked task '.repeat(30)}))}}};
  const original=structuredClone(source),prefix=`MANAGEMENT_V1\n${managerContextVersion}\n${managerContextGuide}\n`;
  const projected=projectManagerReview(source,prefix,0,14000);
  expect(projected.withinBudget).toBe(true);
  const delivered=expandSharedManagerContext(JSON.parse(projected.description.slice(prefix.length)));
  expect(delivered.operation.decisionFeedback).toEqual(feedback);
  expect(delivered.operation.previous.followThrough.decisionFeedback).toEqual(entry);
  expect(delivered.caseWindow.selectedCaseIds).toContain(f.caseId);
  const persisted=projectPersistedManagerDescription(projected.description);
  const again=expandSharedManagerContext(JSON.parse(persisted.slice(persisted.indexOf('\n{"cases":')+1)));
  expect(again.operation.decisionFeedback).toEqual(feedback);
  expect(source).toEqual(original);
  expect((await pool.query('SELECT state,decision,decision_run_id FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0]).toEqual(before);
});

it('isolates exact goal, case, event entity and accepted reviewer authority',async()=>{
  const f=await fixture(),other=await target(await goal()),otherReview=await review(other.goal);
  await rejection(other,f.review); // cross-goal review cannot supply authority
  await event(pool,'SUPERVISION_PLAN_REJECTED',other.task,{caseId:f.caseId,runId:otherReview.run,code:'NEW_EVIDENCE_REQUIRED'});
  expect((await inspect(f)).entries[0].decisionRunId).toBe(f.review);
  expect((await inspect(other)).entries).toEqual([]);
  expect((await readManagementDecisionFeedback(pool,f.goal,{caseId:other.caseId})).entries).toEqual([]);
  const sameGoal=await target(f.goal);await rejection(sameGoal,f.review,'NEW_EVIDENCE_REQUIRED');
  expect((await inspect(f)).entries).toHaveLength(1);
  expect((await inspect(sameGoal)).entries[0]).toMatchObject({caseId:sameGoal.caseId,code:'NEW_EVIDENCE_REQUIRED'});
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,false,'[]','fixture',now()+interval '1 second')`,[randomUUID(),f.manager,f.review]);
  expect((await inspect(f)).entries).toEqual([]);
});

it.each(['DELIVERY_RECERTIFICATION_PHASE_CHANGED','DELIVERY_RECERTIFICATION_SOURCE_SELECTION_REQUIRED',
  'SUPERVISION_DELEGATION_IDENTITY_CONFLICT'])('delivers actionable recertification feedback for %s without exposing rejected model text',async(code)=>{
  const f=await fixture();
  await event(pool,'DELIVERY_RECERTIFICATION_READY',f.task,{caseId:f.caseId,decisionRunId:f.review});
  await event(pool,'DELIVERY_SOURCE_DECISION_REJECTED',f.task,{caseId:f.caseId,decisionRunId:f.review,
    code,decision:{instruction:'PRIVATE REJECTED DECISION'},reason:'PRIVATE DATABASE ERROR'});
  const feedback=await inspect(f);
  expect(feedback.entries).toHaveLength(1);
  expect(feedback.entries[0]).toMatchObject({caseId:f.caseId,decisionRunId:f.review,code});
  expect(feedback.entries[0].nextAction).toMatch(/source|provenance/);
  expect(JSON.stringify(feedback)).not.toContain('PRIVATE');
  await event(pool,'SUPERVISION_APPLIED',f.task,{caseId:f.caseId,decisionRunId:f.review});
  expect((await inspect(f)).entries).toEqual([]);
});

it.each(['decided','delegated','waiting_evidence','applied'])('omits a rejection superseded by current case state %s',async(state)=>{
  const f=await fixture();await pool.query('UPDATE supervision_cases SET state=$2,decision_run_id=$3 WHERE id=$1',[f.caseId,state,f.review]);
  expect((await inspect(f)).entries).toEqual([]);
});

it.each(['SUPERVISION_APPLIED','SUPERVISION_DELEGATED','SUPERVISION_WAITING_EVIDENCE','SUPERVISION_RESUMED','DELIVERY_RECERTIFICATION_READY'])
('does not revive an old rejection after %s even if that same case is open again',async(kind)=>{
  const f=await fixture(),newReview=await review(f.goal);
  await event(pool,kind,f.task,{caseId:f.caseId,decisionRunId:newReview.run,sourceRunId:f.source});
  expect((await inspect(f)).entries).toEqual([]);
  await rejection(f,newReview.run,'NEW_EVIDENCE_REQUIRED');
  expect((await inspect(f)).entries[0]).toMatchObject({code:'NEW_EVIDENCE_REQUIRED',decisionRunId:newReview.run});
});

it('does not attach a rejection to a newer worker attempt or a different historical episode',async()=>{
  const f=await fixture();await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,2,$3,'test',1,'failed')`,[randomUUID(),f.task,randomUUID()]);
  expect((await inspect(f)).entries).toEqual([]);
  const g=await fixture();await pool.query("UPDATE events SET created_at=(SELECT created_at-interval '1 second' FROM supervision_cases WHERE id=$1) WHERE entity_id=$2",[g.caseId,g.task]);
  expect((await inspect(g)).entries).toEqual([]);
});

it('returns only bounded deterministic feedback and labels the rejected binding as historical',async()=>{
  const f=await fixture(),obligation=randomUUID(),replacement=randomUUID(),secret='PRIVATE EVENT CONTENT '.repeat(10000);
  await event(pool,'DELIVERY_SOURCE_DECISION_REJECTED',f.task,{caseId:f.caseId,decisionRunId:f.review,
    code:'DELIVERY_RESOLUTION_GENERATION_CHANGED',obligationId:obligation,generation:'b'.repeat(64),
    expectedSourceRunId:replacement,reason:secret,decision:{instruction:secret},nextAction:secret});
  const feedback=await inspect(f);
  expect(feedback.entries[0]).toMatchObject({code:'DELIVERY_RESOLUTION_GENERATION_CHANGED',
    rejectedBinding:{obligationId:obligation,generation:'b'.repeat(64),expectedSourceRunId:replacement}});
  expect(JSON.stringify(feedback)).not.toContain('PRIVATE EVENT CONTENT');
  expect(JSON.stringify(feedback).length).toBeLessThan(2000);
  expect(feedback.guide).toContain('not a new permission');
  await rejection(f,f.review,'UNRECOGNIZED_PRIVATE_CODE');
  expect((await inspect(f)).entries).toEqual([]); // never fall back to an older, now misleading reason
  await rejection(f,'not-a-run-id');
  expect((await inspect(f)).entries).toEqual([]); // malformed event data cannot abort the manager snapshot
});

it('keeps the latest review feedback when a historical review is rejected later, without bypassing invalid latest authority',async()=>{
  const f=await fixture(),fresh=await review(f.goal);
  await rejection(f,fresh.run,'DELIVERY_RESOLUTION_GENERATION_CHANGED');
  await rejection(f,f.review,'DELIVERY_RECERTIFICATION_PHASE_CHANGED');
  const before=(await pool.query('SELECT id,payload FROM events WHERE entity_id=$1 ORDER BY id',[f.task])).rows;
  expect((await inspect(f)).entries[0]).toMatchObject({decisionRunId:fresh.run,code:'DELIVERY_RESOLUTION_GENERATION_CHANGED'});
  expect((await inspect(f)).entries[0]).toMatchObject({decisionRunId:fresh.run,code:'DELIVERY_RESOLUTION_GENERATION_CHANGED'});
  expect((await pool.query('SELECT id,payload FROM events WHERE entity_id=$1 ORDER BY id',[f.task])).rows).toEqual(before);
  await rejection(f,fresh.run,'UNRECOGNIZED_PRIVATE_CODE');
  expect((await inspect(f)).entries).toEqual([]);
  await rejection(f,fresh.run,'DELIVERY_RESOLUTION_REQUIRED');
  expect((await inspect(f)).entries[0]).toMatchObject({decisionRunId:fresh.run,code:'DELIVERY_RESOLUTION_REQUIRED'});
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,false,'[]','fixture',now()+interval '1 second')`,[randomUUID(),fresh.task,fresh.run]);
  expect((await inspect(f)).entries).toEqual([]); // no fallback to an older accepted review
});

it('fails closed on an unknown current reviewer but permits a genuinely newer accepted review to supersede it',async()=>{
  const f=await fixture();await rejection(f,'not-a-run-id');
  expect((await inspect(f)).entries).toEqual([]);
  const fresh=await review(f.goal);await rejection(f,fresh.run,'NEW_EVIDENCE_REQUIRED');
  expect((await inspect(f)).entries[0]).toMatchObject({decisionRunId:fresh.run,code:'NEW_EVIDENCE_REQUIRED'});
  await rejection(f,randomUUID()); // well-formed, but no corresponding run
  expect((await inspect(f)).entries).toEqual([]);
  const newer=await review(f.goal);await rejection(f,newer.run,'DELIVERY_RESOLUTION_REQUIRED');
  expect((await inspect(f)).entries[0]).toMatchObject({decisionRunId:newer.run,code:'DELIVERY_RESOLUTION_REQUIRED'});
});

it('keeps the focal rejection beyond the six-row window and declares other omissions',async()=>{
  const goalId=await goal(),r=await review(goalId),cases=[];
  for(let i=0;i<9;i++){const c=await target(goalId);cases.push(c);await rejection(c,r.run);}
  const result=await readManagementDecisionFeedback(pool,goalId,{focusCaseId:cases[0].caseId});
  expect(result).toMatchObject({total:9,truncated:true});expect(result.entries).toHaveLength(6);
  expect(result.entries[0].caseId).toBe(cases[0].caseId);
  expect(JSON.stringify(result).length).toBeLessThan(8500);
  expect((await inspect(cases[1])).entries[0].caseId).toBe(cases[1].caseId);
});


it.each(['DELIVERY_RECERTIFICATION_SOURCE_SELECTION_REQUIRED','DELIVERY_RESOLUTION_SOURCE_UNPROVEN'])
('routes diagnostic-only recertification feedback through the existing repair backlog without authorizing substitution: %s',async(code)=>{
  const f=await fixture(),diagnostic=randomUUID(),diagnosticRun=randomUUID();
  await event(pool,'DELIVERY_RECERTIFICATION_READY',f.task,{caseId:f.caseId,decisionRunId:f.review,
    sources:[{taskId:diagnostic,runId:diagnosticRun,currentAccepted:true,currentFrozen:true,workKind:'diagnostic'}]});
  await event(pool,'DELIVERY_SOURCE_DECISION_REJECTED',f.task,{caseId:f.caseId,decisionRunId:f.review,
    code,reason:'PRIVATE RAW DECISION',nextAction:'PRIVATE UNTRUSTED GUIDANCE'});
  const before=(await pool.query('SELECT state,decision,decision_run_id,evidence FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0];
  const feedback=await inspect(f),entry=feedback.entries[0];
  expect(entry).toMatchObject({caseId:f.caseId,taskId:f.task,decisionRunId:f.review,code});
  expect(entry.nextAction).toContain('If the current DELIVERY_RECERTIFICATION_READY sources are only accepted frozen diagnostics');
  for(const term of ['coordination.backlog.tasks','workKind:"repair"','lane:"glm"','sourceTaskId','deliverToTaskId','baseWorkspace','delivery:"deferred"','stable coverageKey','DAG','alreadyPlanned','Reuse equivalent existing work'])
    expect(entry.nextAction).toContain(term);
  expect(entry.nextAction).toContain('do not substitute a diagnostic or repeat supervision.delegate');
  expect(entry.nextAction).toContain('not source replacement or acceptance');
  expect(entry.nextAction).toContain('actually accepted with patch/report/source proof');
  expect(entry.nextAction).toContain('independent QA remain required');
  expect(JSON.stringify(feedback)).not.toContain('PRIVATE');
  expect((await pool.query('SELECT state,decision,decision_run_id,evidence FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0]).toEqual(before);
  expect((await pool.query("SELECT 1 FROM events WHERE kind IN('SUPERVISION_DELEGATED','DELIVERY_SOURCE_RESOLVED','BACKLOG_TASK_CREATED') AND entity_id=$1",[f.task])).rowCount).toBe(0);
});

it('carries the discriminating repair proof cause into bounded frozen manager context without rejected artifact prose',async()=>{
 const f=await fixture(),source=randomUUID(),generation='b'.repeat(64);
 await event(pool,'DELIVERY_SOURCE_DECISION_REJECTED',f.task,{caseId:f.caseId,decisionRunId:f.review,
   code:'DELIVERY_RESOLUTION_PROOF_COVERAGE',obligationId:f.obligation,generation,expectedSourceRunId:source,
   reason:'PRIVATE ARTIFACT',decision:{instruction:'PRIVATE SOURCE'}});
 const feedback=await inspect(f),entry=feedback.entries[0];
 expect(entry).toMatchObject({code:'DELIVERY_RESOLUTION_PROOF_COVERAGE',caseId:f.caseId,
   rejectedBinding:{obligationId:f.obligation,generation,expectedSourceRunId:source}});
 expect(entry.cause).toContain('canonical sourceHashes array');expect(entry.nextAction).toContain('workKind:"repair"');
 expect(entry.nextAction).toContain('independent QA');expect(JSON.stringify(feedback)).not.toContain('PRIVATE');
 const projection=projectManagerReview({cases:[],operation:{decisionFeedback:feedback}},managerContextGuide,0,14000);
 expect(JSON.stringify(projection)).toContain('DELIVERY_RESOLUTION_PROOF_COVERAGE');
 expect(JSON.stringify(projection)).not.toContain('PRIVATE');
});

it('gives historical generic proof rejection the canonical repair path without inventing a failure cause',async()=>{
 const f=await fixture();await event(pool,'DELIVERY_SOURCE_DECISION_REJECTED',f.task,{caseId:f.caseId,decisionRunId:f.review,code:'DELIVERY_RESOLUTION_ARTIFACTS_INVALID'});
 const entry=(await inspect(f)).entries[0];expect(entry.code).toBe('DELIVERY_RESOLUTION_ARTIFACTS_INVALID');
 expect(entry.nextAction).toContain('does not identify which proof check failed');expect(entry.nextAction).toContain('sourceHashes:[{path,before,after}]');
 expect(entry.nextAction).toContain('workKind:"repair"');
});
