import {test,after} from 'node:test';
import assert from 'node:assert/strict';
import {register} from 'tsx/esm/api';
// Resolve TypeScript's production .js specifiers within this test's namespace.
// Native node --test needs no loader flag and unrelated imports keep their loader.
const loader=register({namespace:'agentos-manager-live-failure-test'});
after(()=>loader.unregister());
const {currentLiveFailure,liveFailureKey,selectLiveFailureFocus,liveFailureReviewDisposition}=
  await loader.import('../server/manager-live-failure.ts',import.meta.url);
const {stageDigest,stageFactsDigest}=await loader.import('../server/runtime-stage-contract.ts',import.meta.url);
const goal='11111111-1111-4111-8111-111111111111',task='22222222-2222-4222-8222-222222222222',
 run='33333333-3333-4333-8333-333333333333',fence='44444444-4444-4444-8444-444444444444',context='55555555-5555-4555-8555-555555555555';
function row(){return {goal_id:goal,task_id:task,run_id:run,run_fence:fence,context_id:context,
 event_id:'42',semantic_cause_digest:'a'.repeat(64),task_state:'running',run_state:'running',runtime:'harness',execution_kind:'harness',
 stage:'work',attempt:2,attempt_count:2,cancel_requested:false,guidance_available:true,persisted_context_id:context,
 payload_goal_id:goal,payload_task_id:task,payload_run_id:run,payload_run_fence:fence,payload_context_id:context,
 category:'missing_resource',scope:'project/canary',tool_name:'bash',call_id:'call_1'};}
test('current live failure selects factual nonfatal guide review with exact identity and no fabricated case',()=>{
 const focus=currentLiveFailure(row());assert.equal(focus.authority,'review_only');assert.equal(focus.nonfatal,true);
 assert.equal(focus.runFence,fence);assert.equal(focus.contextVersion,context);assert.equal(focus.evidenceRef,'event:42');
 assert.equal(focus.kind,'live_failure');assert.equal('caseId' in focus,false);assert.match(focus.statement,/intentional negative test/);
});
test('each stale or closed control binding is refused before review',()=>{
 for(const invalid of [{payload_run_id:task},{payload_goal_id:task},{payload_task_id:run},{payload_run_fence:task},
  {payload_context_id:task},{persisted_context_id:task},{attempt_count:3},{run_state:'submitted'},
  {task_state:'cancelling'},{cancel_requested:true},{guidance_available:false},{stage:'coordinate'},
  {runtime:'opencode'},{execution_kind:'opencode'},{run_fence:4},{context_id:'legacy'},
  {category:'fatal'},{tool_name:'bash; rm data'},{call_id:'secret\nline'},{event_id:'0'}])assert.equal(currentLiveFailure({...row(),...invalid}),null,JSON.stringify(invalid));
});
test('repeat tool calls/timestamps do not create a second semantic review',()=>{
 const a=row(),b={...a,event_id:'43',call_id:'call_2',timestamp:'later'};
 assert.equal(liveFailureKey(a),liveFailureKey(b));
 assert.equal(selectLiveFailureFocus([b,a],new Set()).eventId,'42');
 assert.equal(selectLiveFailureFocus([a,b],new Set([liveFailureKey(a)])),null);
});
test('new current cause and new run binding each earn their own review',()=>{
 const a=row(),b={...a,event_id:'43',semantic_cause_digest:'b'.repeat(64)};
 assert.equal(selectLiveFailureFocus([a,b],new Set([liveFailureKey(a)])).eventId,'43');
 const c={...a,run_id:task,payload_run_id:task};assert.notEqual(liveFailureKey(a),liveFailureKey(c));
});
test('a pending live failure can own one typed focus while retaining the existing cases count',()=>{
 const focus=selectLiveFailureFocus([row()],new Set(),7);assert.equal(focus.remainingCases,7);
 assert.equal(focus.taskId,task);assert.equal(focus.scope,'project/canary');
});
test('only bounded allowlisted failure fields enter manager hint',()=>{
 const focus=currentLiveFailure({...row(),raw_error:'sk-secret',arguments:'private',stdout:'secret'});
 const text=JSON.stringify(focus);assert.equal(text.includes('secret'),false);assert.equal(text.includes('arguments'),false);
});
test('accepted but declined/requested/rejected reviews do not discharge the pending cause',()=>{
 const focus=currentLiveFailure(row());
 for(const code of ['MANAGEMENT_FORMAT','CONTEXT_REQUESTED','GUIDANCE_BINDING_STALE','CAPACITY_ASSESSMENT_REQUIRED','DECLINED'])
   assert.equal(liveFailureReviewDisposition(focus,{kind:'guide',code}),null);
});
test('an exactly bound owned guidance debounces while keeping consumption separate from acceptance',()=>{
 const focus=currentLiveFailure(row()),actionId='66666666-6666-4666-8666-666666666666';
 const receipt={kind:'guide',code:'PROPOSED',actionId};
 const action={actionId,goalId:goal,taskId:task,runId:run,runFence:fence,contextVersion:context,expectedAttempt:2,state:'queued_next_step',facts:[{evidenceRef:'event:42'}]};
 assert.equal(liveFailureReviewDisposition(focus,receipt,action).consumed,false);
 assert.equal(liveFailureReviewDisposition(focus,receipt,{...action,state:'consumed'}).accepted,false);
 assert.equal(liveFailureReviewDisposition(focus,receipt,{...action,state:'uncertain'}).requiresReconciliation,true);
 for(const variant of [{state:'rejected'},{runFence:goal},{facts:[]},{contextVersion:goal}])
   assert.equal(liveFailureReviewDisposition(focus,receipt,{...action,...variant}),null);
});
test('bounded explicit no-action requires current frozen identity and the exact cause/event',()=>{
 const focus=currentLiveFailure(row());const receipt={code:'LIVE_FAILURE_NO_ACTION',failureKey:focus.failureKey,eventId:'42',
   goalId:goal,taskId:task,runId:run,runFence:fence,contextVersion:context,decision:'continue_observing',reason:'The current controlled negative test is intentional; preserve its real output and continue this existing worker.'};
 assert.equal(liveFailureReviewDisposition(focus,receipt).resolved,false);
 assert.equal(liveFailureReviewDisposition(focus,{...receipt,eventId:'43'}),null);
 assert.equal(liveFailureReviewDisposition(focus,{...receipt,reason:'wait'}),null);
});
test('stage observations require a current controller ledger and retain unverified provenance',()=>{
 const sample={...row(),event_kind:'RUNTIME_STAGE_OBSERVATION',observation_authority:'review_only',
   observation_current:true,observation_generation:'0',category:'stage_failure',tool_name:'stage_report',
   stage_observation:{category:'stage_failure',evidenceDigest:'b'.repeat(64),toolName:'stage_report',callId:'report_1',
     statement:'UNTRUSTED_CONTENT_MUST_NOT_ENTER_FOCUS',
     reports:[{path:'evidence/protocol.json',digest:'c'.repeat(64),size:42,mtimeMs:100,passed:false,exitCode:1}]}};
 const focus=currentLiveFailure(sample);assert.equal(focus.category,'stage_failure');assert.match(focus.statement,/unverified/);
 assert.deepEqual(focus.stageEvidence,{verification:'unverified_observation',category:'stage_failure',
   evidenceDigest:'b'.repeat(64),reports:sample.stage_observation.reports});
 assert.equal(JSON.stringify(focus).includes('UNTRUSTED_CONTENT'),false);
 for(const variant of [{observation_current:false},{observation_authority:'stop'},{event_kind:'HARNESS_TOOL_FAILURE'},
   {category:'unproductive_wait'},{stage_observation:{...sample.stage_observation,reports:[{path:'../private'}]}}])
   assert.equal(currentLiveFailure({...sample,...variant}),null);
});

test('case facts retain exact event/file binding and stay observations rather than acceptance or raw output',()=>{
 const facts={version:1,suiteExecuted:true,observedCounts:{total:11,examined:11,passed:10,failed:1,unknown:0},
   reportedCounts:{total:11,passed:10,failed:1},inconsistency:false,
   failedCases:[{index:6,name:'recovery keeps exactly-once effects',ok:false,
     detailClass:'assertion_mismatch',detailDigest:'d'.repeat(64)}],truncated:false};
 const report={path:'evidence/protocol.json',digest:'c'.repeat(64),size:900,mtimeMs:100,passed:null,exitCode:1,facts};
 const evidenceDigest=stageDigest([report.path,report.digest,stageFactsDigest(facts)]);
 const sample={...row(),event_kind:'RUNTIME_STAGE_OBSERVATION',observation_authority:'review_only',
   observation_current:true,observation_generation:'0',category:'stage_failure',tool_name:'stage_report',
   raw_error:'PRIVATE_UNTRUSTED_OUTPUT',stage_observation:{category:'stage_failure',evidenceDigest,
     toolName:'stage_report',callId:'report_1',statement:'PRIVATE_UNTRUSTED_OUTPUT',reports:[report]}};
 const focus=currentLiveFailure(sample);
 assert.deepEqual(focus.stageEvidence.reports[0].facts,facts);
 assert.equal(focus.stageEvidence.verification,'unverified_observation');assert.equal(focus.authority,'review_only');
 assert.equal(focus.eventId,'42');assert.equal(focus.runId,run);assert.equal('acceptance' in focus,false);
 assert.equal(JSON.stringify(focus).includes('PRIVATE_'),false);
 for(const changed of [{...report,digest:'e'.repeat(64)},
   {...report,facts:{...facts,failedCases:[{...facts.failedCases[0],detail:'PRIVATE_RAW_DETAIL'}]}},
   {...report,facts:{...facts,observedCounts:{...facts.observedCounts,failed:2}}},
   {...report,facts:{...facts,failedCases:[{...facts.failedCases[0],name:'different case'}]}}])
   assert.equal(currentLiveFailure({...sample,stage_observation:{...sample.stage_observation,reports:[changed]}}),null);
 const reordered={truncated:facts.truncated,failedCases:facts.failedCases,inconsistency:facts.inconsistency,
   reportedCounts:facts.reportedCounts,observedCounts:facts.observedCounts,suiteExecuted:facts.suiteExecuted,version:1};
 assert.equal(stageFactsDigest(reordered),stageFactsDigest(facts));
});
