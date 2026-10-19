import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {installSupervision} from '../server/supervision.js';
import {installManagement,recordManagementReviews} from '../server/management.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
import {deliveryDigest} from '../server/delivery-supervision.js';
import {materialContextGapSchema,readMaterialDecisionOutcome} from '../server/management-material-decision.js';
import {managementContextRequests} from '../server/management-retrieval.js';
import {managementFormatFeedback} from '../server/management-format-feedback.js';

beforeAll(async()=>{
 expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
 await migrate();await installSupervision();await installBacklogPlanner();await installManagement();
});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
 await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('gap-feedback','Test','test','[]','test',1,'Fixture')");
});
afterAll(()=>pool.end());

async function fixture(obligationCount=17) {
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID();
 const manager=randomUUID(),review=randomUUID(),fence=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Delivery','Exact criteria','gap-feedback',$2)",[goal,goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
  VALUES($1,$2,$3,'Integrator','Keep original acceptance','gap-feedback','failed','integrate',
  '[{"id":"candidate","type":"artifact","name":"candidate.json"}]',1)`,[task,goal,task]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'test',1,'failed')",[run,task,randomUUID()]);
 await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
  VALUES($1,$2,$3,$4,'open','Review current material','[]')`,[caseId,goal,task,run]);
 // A real current consumer episode, plus a frozen review-only control binding.
 // These identities do not claim that any source passed its action applier.
 const binding={goalId:goal,caseId,consumerTaskId:task,consumerRunId:run,consumerAttempt:1,
  obligations:Array.from({length:obligationCount},(_,i)=>({obligationId:randomUUID(),generation:deliveryDigest(['generation',i]),
   handoffId:randomUUID(),producerTaskId:randomUUID(),producerRunId:randomUUID(),
   producerManifestDigest:deliveryDigest(['manifest',i]),repairArtifacts:[
    {id:randomUUID(),name:'implementation.patch',digest:deliveryDigest(['patch',i]),size:12},
    {id:randomUUID(),name:'repair-report.json',digest:deliveryDigest(['report',i]),size:13}]}))};
 const hint={version:1,authority:'review_only',...binding,identityDigest:deliveryDigest(binding),actionValidationRequired:true};
 const snapshot:any={cases:[{id:caseId,taskId:task,evidence:[],materialConsumptionReview:hint}],operation:{focus:{caseId,taskId:task}}};
 const description=()=>`MANAGEMENT_V1\n${JSON.stringify(snapshot)}`;
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
  VALUES($1,$2,$3,'Review',$4,'gap-feedback','running','coordinate',
  '[{"id":"review","type":"artifact","name":"coordination.json"}]',1)`,[manager,goal,manager,description()]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'test',1,'running')",[review,manager,fence]);
 const coordination:any={management:{summary:'Resolve current obligation',capacityReason:'Use exact evidence',actions:[]},backlog:{tasks:[],reason:'No new work'}};
 const inspect=(goalId=goal)=>readMaterialDecisionOutcome(pool,goalId,review,description(),coordination);
 const gap=(requests:any[],indices=requests.map((_,i)=>i))=>{
  coordination.management.contextRequests=requests;
  coordination.management.materialContextGap={caseId,consumerRunId:run,identityDigest:hint.identityDigest,
   reason:'The exact source fields for the current decision are not delivered yet.',requestIndices:indices};
 };
 return {goal,task,run,caseId,manager,review,fence,binding,hint,snapshot,description,coordination,inspect,gap};
}
const validTaskRead=(f:Awaited<ReturnType<typeof fixture>>)=>({kind:'task_metadata',taskId:f.task,expectedAttempt:1,part:'decision',offset:0});
const refusal=(f:Awaited<ReturnType<typeof fixture>>)=>({caseId:f.caseId,kind:'revise',
 diagnosis:'A proposed revision has no canonical action owner.',
 instruction:'Preserve all criteria and consume the exact accepted source through the existing guards.'});
async function actorState(f:Awaited<ReturnType<typeof fixture>>) {
 return {task:(await pool.query('SELECT state,attempt_count,criteria FROM tasks WHERE id=$1',[f.task])).rows[0],
  run:(await pool.query('SELECT state,attempt,fence FROM runs WHERE id=$1',[f.run])).rows[0],
  case:(await pool.query('SELECT state,decision,decision_run_id FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0],
  runs:(await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n,
  acceptances:(await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n};
}

it('reproduces2401: rejects two foreign obligation indices among four reads and explains exactly which ones',async()=>{
 const f=await fixture(),foreign=[randomUUID(),randomUUID()],source=f.binding.obligations[0];
 f.gap([validTaskRead(f),{kind:'delivery_consumption',obligationId:foreign[0],offset:0},
  {kind:'delivery_consumption',obligationId:source.obligationId,offset:256,snapshotDigest:deliveryDigest(['catalog',source.obligationId])},
  {kind:'delivery_consumption',obligationId:foreign[1],offset:0}]);
 expect(managementContextRequests.safeParse(f.coordination.management.contextRequests).success).toBe(true);
 // Merely appearing in a discovery catalog does not authorize the gap.
 f.snapshot.operation.deliveryConsumptions=foreign.map(id=>({id,consumer_task_id:f.task}));
 const before=await actorState(f),input=structuredClone(f.snapshot),result:any=await f.inspect();
 expect(result).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false,
  contextGapFeedback:{bindingMatches:true,relatedRequestIndices:[0,2],unrelatedRequestIndices:[1,3]}});
 expect(result.contextGapFeedback.expectedSource).toMatchObject({caseId:f.caseId,consumerRunId:f.run,
  identityDigest:f.hint.identityDigest,consumerTaskId:f.task,
  obligationIdsPath:'operation.requiredDecision.materialReview.obligations[].obligationId',
  producerTaskIdsPath:'operation.requiredDecision.materialReview.obligations[].producerTaskId'});
 expect(result.requiredDecision.materialReview.obligations).toEqual(f.binding.obligations);
 expect(result.contextGapFeedback.expectedSource).not.toHaveProperty('obligationIds');
 expect(result.contextGapFeedback.expectedSource).not.toHaveProperty('producerTaskIds');
 expect(JSON.stringify(result.contextGapFeedback.expectedSource)).not.toContain(foreign[0]);
 expect(JSON.stringify(result.contextGapFeedback.expectedSource)).not.toContain(foreign[1]);
 expect(JSON.stringify(result.contextGapFeedback).length).toBeLessThan(2500);
 expect(result).not.toHaveProperty('contextGap');expect(f.snapshot).toEqual(input);expect(await actorState(f)).toEqual(before);
});

it('delivers fixed identity fields separately from model-authored reason and indices, with bounded extra metadata',async()=>{
 const f=await fixture(),result:any=await f.inspect(),contract=result.requiredDecision.contextGapContract;
 expect(contract).toMatchObject({address:'management.materialContextGap',
  template:{caseId:f.caseId,consumerRunId:f.run,identityDigest:f.hint.identityDigest,reason:'',requestIndices:[]},
  modelFields:['reason','requestIndices'],reasonCharacters:{minimum:20,maximum:1200},
  requestIndices:{minimum:1,maximum:4,range:[0,3]}});
 expect(Object.keys(contract.template).sort()).toEqual(['caseId','consumerRunId','identityDigest','reason','requestIndices'].sort());
 expect(materialContextGapSchema.safeParse(contract.template).success).toBe(false);
 const filled={...contract.template,reason:'The current exact repair report has not yet been delivered.',requestIndices:[0]};
 expect(materialContextGapSchema.safeParse(filled).success).toBe(true);
 f.coordination.management.contextRequests=[validTaskRead(f)];f.coordination.management.materialContextGap=filled;
 expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_GAP',satisfied:false});
 expect(JSON.stringify(contract).length).toBeLessThan(1200);
 expect(contract).not.toHaveProperty('materialReview');expect(contract).not.toHaveProperty('obligations');
});

it('bounds feedback at128 obligations without truncating eligibility or the complete material binding',async()=>{
 const f=await fixture(128),planning=Array.from({length:130},()=>randomUUID()),last=f.binding.obligations[127];
 f.snapshot.cases[0].evidence=planning.map(requestId=>({kind:'SCOPED_DELIVERY_ACTION',requestId}));
 f.gap([{kind:'delivery_consumption',obligationId:last.obligationId,offset:0},
  {kind:'task_metadata',taskId:last.producerTaskId,expectedAttempt:1,part:'decision',offset:0},
  {kind:'planning_request',requestId:planning[129],offset:0},
  {kind:'delivery_consumption',obligationId:randomUUID(),offset:0}]);
 expect(managementContextRequests.safeParse(f.coordination.management.contextRequests).success).toBe(true);
 const before=await actorState(f),input=structuredClone(f.snapshot),result:any=await f.inspect();
 expect(result).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false,
  contextGapFeedback:{bindingMatches:true,relatedRequestIndices:[0,1,2],unrelatedRequestIndices:[3]}});
 const source=result.contextGapFeedback.expectedSource;
 expect(source).toMatchObject({planningRequestIds:planning.slice(0,16),planningRequestIdsTruncated:true,
  planningRequestSource:{caseId:f.caseId,path:'cases[].evidence',fields:['requestId','planningRequestId']}});
 expect(source).not.toHaveProperty('obligationIds');expect(source).not.toHaveProperty('producerTaskIds');
 expect(result.requiredDecision.materialReview).toEqual(f.hint);
 expect(result.requiredDecision.materialReview.obligations).toHaveLength(128);
 // This bound applies to the NEW explanatory metadata, not the mandatory
 // material authority, which remains complete and may still overflow a prompt.
 expect(JSON.stringify(result.contextGapFeedback).length+JSON.stringify(result.requiredDecision.contextGapContract).length).toBeLessThan(3200);
 f.coordination.management.materialContextGap.requestIndices=[0,1,2];
 expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_GAP',satisfied:false,
  contextGap:{requestIndices:[0,1,2]}});
 expect(f.snapshot).toEqual(input);expect(await actorState(f)).toEqual(before);
});

it.each(['caseId','consumerRunId','identityDigest'])('keeps a syntactically valid but wrong %s bound to a refusal',async field=>{
 const f=await fixture();f.gap([validTaskRead(f)]);
 f.coordination.management.materialContextGap[field]=field==='identityDigest'?'e'.repeat(64):randomUUID();
 const before=await actorState(f),result:any=await f.inspect();
 expect(result).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false,contextGapFeedback:{bindingMatches:false}});
 expect(result.contextGapFeedback.expectedSource.identityDigest).toBe(f.hint.identityDigest);
 expect(result).not.toHaveProperty('contextGap');expect(await actorState(f)).toEqual(before);
});

it('retains cross-goal and corrupted frozen-binding guards before explaining a gap',async()=>{
 const f=await fixture();f.gap([validTaskRead(f)]);const before=await actorState(f);
 expect(await f.inspect(randomUUID())).toMatchObject({code:'MANAGEMENT_MATERIAL_BINDING_INVALID',satisfied:false});
 f.snapshot.cases[0].materialConsumptionReview.identityDigest='f'.repeat(64);
 expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_BINDING_INVALID',satisfied:false});
 expect(await actorState(f)).toEqual(before);
});

it('rejects a foreign case and planning catalog reference, but keeps an explicitly cited focal planning request eligible',async()=>{
 const f=await fixture(),foreignCase=randomUUID(),foreignPlan=randomUUID(),focalPlan=randomUUID();
 f.snapshot.cases.push({id:foreignCase,taskId:randomUUID(),evidence:[{kind:'SCOPED_DELIVERY_ACTION',requestId:foreignPlan}]});
 f.snapshot.operation.planningDemands=[{id:foreignPlan,scope:'same-discovery-scope'}];
 f.snapshot.cases[0].evidence=[{kind:'SCOPED_DELIVERY_ACTION',requestId:focalPlan}];
 f.gap([{kind:'case_evidence',caseId:foreignCase,evidenceIndex:0,digest:'a'.repeat(64),offset:0},
  {kind:'planning_request',requestId:foreignPlan,offset:0},
  {kind:'planning_request',requestId:focalPlan,offset:0}]);
 expect(managementContextRequests.safeParse(f.coordination.management.contextRequests).success).toBe(true);
 const denied:any=await f.inspect();
 expect(denied).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false,
  contextGapFeedback:{relatedRequestIndices:[2],unrelatedRequestIndices:[0,1]}});
 expect(denied.contextGapFeedback.expectedSource.planningRequestIds).toEqual([focalPlan]);
 f.coordination.management.materialContextGap.requestIndices=[2];
 expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_GAP',satisfied:false,contextGap:{requestIndices:[2]}});
});

it('reports an in-range index missing from this review rather than selecting another request',async()=>{
 const f=await fixture();f.gap([validTaskRead(f)],[0,3]);
 expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false,
  contextGapFeedback:{relatedRequestIndices:[0],unrelatedRequestIndices:[3]}});
});

it('does not replace an unapplied action refusal or mark a gap as action ownership',async()=>{
 const f=await fixture();f.gap([{kind:'delivery_consumption',obligationId:randomUUID(),offset:0}]);
 f.coordination.supervision={actions:[refusal(f)]};const before=await actorState(f);
 expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_ACTION_UNAPPLIED',satisfied:false,
  contextGapFeedback:{unrelatedRequestIndices:[0]}});
 expect(await actorState(f)).toEqual(before);
});

it('preserves delivered-interval feedback and allows only the next missing interval as a gap',async()=>{
 const f=await fixture(),request=validTaskRead(f);f.gap([request]);
 f.snapshot.operation.recoveredContext={results:[{status:'OK',request,text:'Delivered interval',digest:'a'.repeat(64),
  totalCharacters:40,truncated:true,nextOffset:18}]};
 expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_ALREADY_DELIVERED',satisfied:false,
  readFeedback:[{requestIndex:0,code:'READ_ALREADY_DELIVERED',nextOffset:18}]});
 f.coordination.management.contextRequests[0]={...request,offset:18};
 expect(await f.inspect()).toMatchObject({code:'MANAGEMENT_MATERIAL_CONTEXT_GAP',satisfied:false});
});

it('reproduces2402: UUID identity stays invalid and reports only the static SHA contract and canonical source',async()=>{
 const f=await fixture();f.gap([validTaskRead(f)]);const received=randomUUID();
 f.coordination.management.materialContextGap.identityDigest=received;
 const parsed=materialContextGapSchema.safeParse(f.coordination.management.materialContextGap);
 expect(parsed.success).toBe(false);if(parsed.success)throw Error('EXPECTED_INVALID');
 const issues=parsed.error.issues.map(issue=>({...issue,path:['materialContextGap',...issue.path]}));
 const feedback=managementFormatFeedback(issues);
 expect(feedback).toMatchObject({issueCount:1,issuesTruncated:false,issues:[{code:'invalid_format',
  path:['materialContextGap','identityDigest'],expectedPattern:'^[a-f0-9]{64}$',
  expectedSource:'operation.requiredDecision.contextGapContract.template.identityDigest'}]});
 expect(JSON.stringify(feedback)).not.toContain(received);
 // The genuine accepted-artifact recorder must reject the whole management
 // proposal, not execute its syntactically valid requests despite this error.
 await writeArtifact(f.review,f.fence,'coordination.json',JSON.stringify(f.coordination),'application/json');
 await completeRun(f.review,f.fence,'Invalid management body fixture',{});
 expect((await validateTask(f.manager)).passed).toBe(true);
 await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
  ['management:'+f.goal,JSON.stringify({enabled:true,enabledAt:'2026-01-01T00:00:00Z'})]);
 const before=await actorState(f);await recordManagementReviews();
 const row=(await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1',[f.review])).rows[0];
 expect(row.receipts).toEqual(expect.arrayContaining([expect.objectContaining({code:'MANAGEMENT_FORMAT',...feedback})]));
 expect(row.receipts.some((r:any)=>r.code==='CONTEXT_REQUESTED')).toBe(false);
 const state=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-state:'+f.goal])).rows[0]?.value;
 expect(state?.contextRequests??[]).toEqual([]);expect(await actorState(f)).toEqual(before);
});

it.each([
 {identityDigest:'A'.repeat(64)}, {identityDigest:'a'.repeat(63)}, {identityDigest:'g'.repeat(64)},
 {reason:'too short'}, {reason:'x'.repeat(1201)}, {requestIndices:[]}, {requestIndices:[4]},
 {requestIndices:[0.5]}, {requestIndices:[0,1,2,3,0]}, {unexpectedAuthority:true},
])('keeps the material gap schema strict for invalid input %#',change=>{
 const valid={caseId:randomUUID(),consumerRunId:randomUUID(),identityDigest:'a'.repeat(64),
  reason:'Exact current required information is still missing.',requestIndices:[0]};
 expect(materialContextGapSchema.safeParse({...valid,...change}).success).toBe(false);
});

it('never reflects received values or attaches the SHA hint to other issue paths',()=>{
 const issues=[{code:'invalid_format',path:['materialContextGap','identityDigest'],message:'SECRET BODY',received:'SECRET TOKEN'},
  {code:'invalid_format',path:['materialContextGap','identityDigest','SECRET/FIELD']},
  {code:'invalid_type',path:['materialContextGap','identityDigest']},
  {code:'unknown-code',path:['materialContextGap','SECRET/FIELD']}];
 const feedback=managementFormatFeedback(issues);
 expect(feedback.issues[0]).toMatchObject({expectedPattern:'^[a-f0-9]{64}$'});
 for(const entry of feedback.issues.slice(1))expect(entry).not.toHaveProperty('expectedSource');
 expect(JSON.stringify(feedback)).not.toMatch(/SECRET|BODY|TOKEN/);
 expect(feedback.issues[3].code).toBe('SCHEMA_INVALID');
});
