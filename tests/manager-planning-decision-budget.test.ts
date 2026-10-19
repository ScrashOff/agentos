import {createHash} from 'node:crypto';
import {expect,it} from 'vitest';
import {managerTextHash,projectManagerReview} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {frozenMaterialDecision} from '../server/management-material-decision.js';

const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const sha=(text:string)=>createHash('sha256').update(text).digest('hex');
const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?
 Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const digest=(value:any)=>sha(JSON.stringify(canonical(value)));
const noise=(seed:number,characters:number)=>Array.from({length:Math.ceil(characters/64)},(_,i)=>sha(`${seed}:${i}`)).join('').slice(0,characters);
const maximum=24000;

function fixture():any {
 const identity={goalId:id(1),caseId:id(2),consumerTaskId:id(3),consumerRunId:id(4),consumerAttempt:7,
  obligations:[{obligationId:id(5),generation:sha('generation'),handoffId:id(6),producerTaskId:id(7),
   producerRunId:id(8),producerManifestDigest:sha('manifest'),repairArtifacts:[
    {id:id(9),name:'implementation.patch',digest:sha('patch'),size:1234},
    {id:id(10),name:'repair-report.json',digest:sha('report'),size:345}]}]};
 const material={version:1,authority:'review_only',...identity,identityDigest:digest(identity),actionValidationRequired:true};
 const required={version:1,caseId:identity.caseId,taskId:identity.consumerTaskId,consumerRunId:identity.consumerRunId,
  consumerAttempt:identity.consumerAttempt,identityDigest:material.identityDigest,materialReview:structuredClone(material),
  actionAddress:'supervision.actions'};
 const demands=Array.from({length:12},(_,i)=>({id:id(100+i),scope:'project/focus',lane:'GLM',state:'awaiting_supervision',
  source_revision:sha('revision'+i),task_id:i===0?identity.consumerTaskId:id(200+i),code:'PENDING_LIMIT',
  updated_at:'2026-10-12T16:00:00.000Z',createdTaskIds:[id(300+i)],
  remainingItems:Array.from({length:4},(_,j)=>({status:'pending',code:'PENDING_LIMIT',coverageKey:noise(10000+i*4+j,640),
   lane:'GLM',sourceTaskId:id(400+i*4+j),consumerTaskId:id(500+i*4+j),baseWorkspace:'project/focus',workKind:'repair',
   reference:{requestId:id(100+i),runId:id(600+i),artifactId:id(700+i),artifactDigest:sha('artifact'+i),
    index:j,proposalDigest:sha(`proposal:${i}:${j}`)},
   replacement:{taskId:id(800+i*4+j),reviewRunId:id(900+i),reviewArtifactId:id(1000+i),
    reviewArtifactDigest:sha('replacement-artifact'+i),proposalDigest:sha(`replacement:${i}:${j}`)}})),
  remainingItemCount:4,remainingItemsTruncated:false,
  remainingItemsRequest:{kind:'planning_request',requestId:id(100+i),offset:0},
  originalPlanRequest:{kind:'artifact',taskId:id(200+i),expectedAttempt:1,name:'scope-plan.json',offset:0}}));
 const text=noise(20000,2631),request={kind:'task_metadata',taskId:identity.consumerTaskId,expectedAttempt:7,part:'decision',offset:0};
 return {cases:[{id:identity.caseId,taskId:identity.consumerTaskId,currentRun:{id:identity.consumerRunId,attempt:7,state:'failed'},
   evidence:[{kind:'CURRENT_CAUSAL_EVIDENCE',instruction:'Read the exact current repair and keep acceptance criteria unchanged.'}],
   materialConsumptionReview:material}],operation:{goalId:identity.goalId,
   focus:{caseId:identity.caseId,taskId:identity.consumerTaskId,scope:'project/focus'},
   tasks:[{id:identity.consumerTaskId,expectedAttempt:7,state:'recovery_blocked',workKind:'integrate',
    criteria:[{id:'candidate',type:'artifact',name:'verification.json'},{id:'qa',type:'artifact',name:'independent-qa.json'}],
    dependencies:[id(7)],executionInputs:structuredClone(identity.obligations[0].repairArtifacts),
    intent:{text:'Integrate the exact accepted repair; verify the same candidate independently.'}},
    {id:id(50),expectedAttempt:3,state:'submitted',workKind:'repair',criteria:[{id:'repair',type:'artifact',name:'implementation.patch'}],
     dependencies:[],intent:{text:'An auxiliary task in the same operation is not a planning request identity.'}}],
   requiredDecision:required,previous:{receipts:[{code:'MATERIAL_DECISION_REQUIRED',requiredDecision:structuredClone(required)}],
    followThrough:{status:'decision_pending',requiredDecision:structuredClone(required)}},
   decisionFeedback:{code:'MATERIAL_DECISION_REQUIRED',caseId:identity.caseId,consumerRunId:identity.consumerRunId},
   planningDemands:demands,planningDemandWindow:{total:90,included:12,truncated:true,nextOffset:12,snapshotDigest:sha('catalog')},
   deliveryConsumptions:[{id:identity.obligations[0].obligationId,scope:'project/focus',state:'waiting_supervision',
    consumer_task_id:identity.consumerTaskId,producer_task_id:identity.obligations[0].producerTaskId,
    producer_run_id:identity.obligations[0].producerRunId,receipt:{code:'SUPERVISION_REQUESTED',caseId:identity.caseId}}],
   recoveredContext:{requests:[request],results:[{status:'OK',request,text,digest:sha(text),totalCharacters:text.length,
    truncated:false,nextOffset:null}],selection:{code:'FOCUS_RESUMED',reviewRunId:id(55),binding:{caseId:identity.caseId,taskId:identity.consumerTaskId}}}}};
}

function project(source:any,limit=maximum) {
 const review=projectManagerReview(source,'',0,limit);
 const out=expandSharedManagerContext(JSON.parse(review.description));
 const texts=new Map((out.evidenceTexts??[]).filter((row:any)=>typeof row.text==='string'&&sha(row.text)===row.hash)
  .map((row:any)=>[row.hash,row.text]));
 const restore=(value:any):any=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===1&&texts.has(value.$text)?texts.get(value.$text):
  Array.isArray(value)?value.map(restore):value&&typeof value==='object'?Object.fromEntries(Object.entries(value).map(([key,item])=>[key,restore(item)])):value;
 return {review,out:restore(out)};
}
function assertAuthority(source:any,out:any) {
 expect(out.cases).toEqual(source.cases);
 for(const key of ['focus','tasks','requiredDecision','previous','decisionFeedback','deliveryConsumptions','planningDemandWindow'])
  expect(out.operation[key]).toEqual(source.operation[key]);
 const frozen=frozenMaterialDecision('MANAGEMENT_V1\n'+JSON.stringify(out));
 expect(frozen?.invalid).toBe(false);
 if(!frozen||frozen.invalid)throw Error('MATERIAL_BINDING_FIXTURE_INVALID');
 expect(frozen.identityDigest).toBe(source.operation.requiredDecision.identityDigest);
 expect(frozen.binding.obligations).toEqual(source.cases[0].materialConsumptionReview.obligations);
}
function incidental(source:any,field:string) {
 for(const row of source.operation.planningDemands){
  if(field==='createdTaskIds')row.createdTaskIds=[id(50)];
  else for(const item of row.remainingItems)item[field]=id(50);
 }
}
function projectedOrAbsent(out:any,row:any) {
 const found=out.operation.planningDemands.find((entry:any)=>entry.id===row.id);
 expect(found===undefined||found.detailsProjected===true).toBe(true);
 if(found){expect(found.sourceRowSha256).toBe(managerTextHash(JSON.stringify(row)));expect(found).not.toHaveProperty('remainingItems');}
 const projection=out.operation.catalogDetailProjection.catalogs.planningDemands;
 expect(projection.fullSourcesPreserved).toBe(true);
 expect(projection.recovery).toMatchObject({kind:'planning_request',idField:'requestId',idColumn:'id',offset:0});
}
function assertUsefulRead(source:any,out:any) {
 expect(out.operation.recoveredContext.requests).toEqual(source.operation.recoveredContext.requests);
 expect(out.operation.recoveredContext.selection).toMatchObject(source.operation.recoveredContext.selection);
 expect(out.operation.recoveredContext.results).toHaveLength(source.operation.recoveredContext.results.length);
 for(let i=0;i<source.operation.recoveredContext.results.length;i++){
  const original=source.operation.recoveredContext.results[i],actual=out.operation.recoveredContext.results[i];
  const {text:expectedText,nextOffset:_expectedNext,truncated:_expectedTruncated,...expectedIdentity}=original;
  const {text,nextOffset,truncated,...identity}=actual;
  expect(identity).toEqual(expectedIdentity);expect(text.length).toBeGreaterThanOrEqual(Math.min(2048,expectedText.length));
  expect(text).toBe(expectedText.slice(0,text.length));
  const end=(original.request.offset??0)+text.length;
  expect(nextOffset).toBe(end<original.totalCharacters?end:null);expect(truncated).toBe(end<original.totalCharacters);
 }
}

function diagnosticInputsFixture():any {
 const source=fixture();source.operation.planningDemands=[];
 const acceptedSources=Array.from({length:16},(_,i)=>({taskId:id(3000+i),runId:id(3100+i),expectedAttempt:i+1,
  acceptancePassed:true,baseWorkspace:'project/focus',workspace:'isolated/diagnostic-'+i,workKind:'diagnostic',covers:['criterion-'+i],
  inputs:Array.from({length:8},(_,j)=>({artifactId:id(3200+i*8+j),
   name:`diagnostic-${i}-${j}-${noise(50000+i*8+j,128)}.json`,digest:sha(`diagnostic-body:${i}:${j}`),size:2000+i*8+j})),omittedInputs:0}));
 source.operation.tasks[0].dependencies=[id(7),...acceptedSources.map(row=>row.taskId)];
 // This detailed auxiliary task shares the first source identity. The graph
 // and its contract remain delivered; only unreferenced input metadata may
 // use the existing exact task/attempt artifacts reader.
 source.operation.tasks[1]={id:acceptedSources[0].taskId,expectedAttempt:acceptedSources[0].expectedAttempt,
  state:'submitted',workKind:'diagnostic',criteria:[{id:'report',type:'artifact',name:'diagnostic-report.json'}],dependencies:[],
  intent:{text:'Observe the source; do not alter the implementation.'}};
 source.operation.workOpportunities={selection:{cursor:0},acceptedSources,consumers:[{taskId:source.operation.focus.taskId,
  workKinds:['diagnostic','repair'],sourceTaskIds:acceptedSources.map(row=>row.taskId),sourceCount:acceptedSources.length}],
  existingCoverage:[],liveActors:[]};
 return source;
}
function assertProjectedInputs(source:any,out:any,index:number) {
 const rows=out.operation.workOpportunities.acceptedSources,original=source.operation.workOpportunities.acceptedSources[index];
 const actual=rows.find((row:any)=>row.taskId===original.taskId);
 expect(actual).toBeDefined();
 const {inputs,...metadata}=actual,{inputs:originalInputs,...originalMetadata}=original;
 expect(metadata).toEqual(originalMetadata);expect(inputs.detailsProjected).toBe(true);
 expect(inputs.inputCount).toBe(originalInputs.length);expect(inputs.sourceSha256).toBe(managerTextHash(JSON.stringify(originalInputs)));
 expect(inputs.recovery).toEqual({kind:'artifacts',taskId:original.taskId,expectedAttempt:original.expectedAttempt,offset:0});
}

it('projects only diagnostic input details whose references are graph or inventory membership, while keeping that graph exact',()=>{
 const source=diagnosticInputsFixture(),before=structuredClone(source),{review,out}=project(source);
 expect(review.withinBudget).toBe(true);
 expect(out.operation.workOpportunities.acceptedSources.map((row:any)=>row.taskId)).toEqual(source.operation.workOpportunities.acceptedSources.map((row:any)=>row.taskId));
 for(let i=0;i<source.operation.workOpportunities.acceptedSources.length;i++)assertProjectedInputs(source,out,i);
 expect(out.operation.workOpportunities.consumers).toEqual(source.operation.workOpportunities.consumers);
 assertUsefulRead(source,out);
 assertAuthority(source,out);expect(source).toEqual(before);
});

it.each(['criteria','intent','case','requiredDecision','previous','feedback','request','unknown','digest'])(
 'retains diagnostic inputs when %s refers to the source beyond graph membership',where=>{
  const source=diagnosticInputsFixture(),row=source.operation.workOpportunities.acceptedSources[0];
  if(where==='criteria')source.operation.tasks[0].criteria.push({id:'binding',label:'Inspect exact source '+row.taskId});
  if(where==='intent')source.operation.tasks[0].intent.text='Inspect source '+row.runId+' before deciding.';
  if(where==='case')source.cases[0].evidence.push({kind:'FUTURE_SOURCE_REQUIREMENT',sourceTaskId:row.taskId});
  if(where==='requiredDecision')source.operation.requiredDecision.sourceRequirement={sourceTaskId:row.taskId};
  if(where==='previous')source.operation.previous.receipts.push({code:'FUTURE_SOURCE_REQUIREMENT',sourceTaskId:row.taskId});
  if(where==='feedback')source.operation.decisionFeedback.sourceRequirement={sourceTaskId:row.taskId};
  if(where==='request')source.operation.recoveredContext.requests.push({kind:'artifacts',taskId:row.taskId,expectedAttempt:row.expectedAttempt,offset:0});
  if(where==='unknown')source.operation.tasks[0].futureConstraint={sourceTaskId:row.taskId};
  if(where==='digest')source.operation.tasks[0].criteria.push({id:'bytes',label:'Require exact artifact '+row.inputs[0].digest});
  const before=structuredClone(source),{review,out}=project(source);
  expect(review.withinBudget).toBe(true);
  expect(out.operation.workOpportunities.acceptedSources.find((entry:any)=>entry.taskId===row.taskId)).toEqual(row);
  assertProjectedInputs(source,out,15);assertAuthority(source,out);expect(source).toEqual(before);
 });

it('does not extend the graph-only diagnostic exception to a repair source',()=>{
 const source=diagnosticInputsFixture(),row=source.operation.workOpportunities.acceptedSources[0];row.workKind='repair';
 source.operation.tasks[1].workKind='repair';const before=structuredClone(source),{out}=project(source);
 expect(out.operation.workOpportunities.acceptedSources.find((entry:any)=>entry.taskId===row.taskId)).toEqual(row);
 assertProjectedInputs(source,out,15);assertAuthority(source,out);expect(source).toEqual(before);
});

it.each(['source-field','input-field','artifact-id','digest','size','acceptance','attempt','dependency-object'])(
 'keeps an unknown or invalid diagnostic contract complete at %s',where=>{
  const source=diagnosticInputsFixture(),row=source.operation.workOpportunities.acceptedSources[0];
  if(where==='source-field')row.futureConstraint={sourceMustRemain:true};
  if(where==='input-field')row.inputs[0].futureConstraint={sourceMustRemain:true};
  if(where==='artifact-id')row.inputs[0].artifactId='not-a-persisted-id';
  if(where==='digest')row.inputs[0].digest='not-a-digest';
  if(where==='size')row.inputs[0].size=-1;
  if(where==='acceptance')row.acceptancePassed=false;
  if(where==='attempt')row.expectedAttempt=-1;
  if(where==='dependency-object')source.operation.tasks[0].dependencies=[{taskId:row.taskId,futureConstraint:true}];
  const before=structuredClone(source),{out}=project(source);
  expect(out.operation.workOpportunities.acceptedSources.find((entry:any)=>entry.taskId===row.taskId)).toEqual(row);
  assertProjectedInputs(source,out,15);assertAuthority(source,out);expect(source).toEqual(before);
 });

it.each(['createdTaskIds','sourceTaskId','consumerTaskId'])(
 'does not make all planning bodies mandatory through incidental %s task membership',field=>{
  const source=fixture();incidental(source,field);const before=structuredClone(source),{review,out}=project(source);
  expect(review.withinBudget).toBe(true);expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(maximum);
  projectedOrAbsent(out,source.operation.planningDemands.at(-1));
  expect(out.operation.planningDemands.find((row:any)=>row.id===source.operation.planningDemands[0].id)).toEqual(source.operation.planningDemands[0]);
  expect(out.operation.catalogDetailProjection.catalogs.planningDemands.sourceRowsSha256).toBe(managerTextHash(JSON.stringify(source.operation.planningDemands)));
  assertUsefulRead(source,out);assertAuthority(source,out);expect(source).toEqual(before);
 });

it.each(['decisionFeedback','backlog','requiredDecision'])(
 'does not discard future nested decision semantics within the known %s envelope',root=>{
  const source=fixture();incidental(source,'consumerTaskId');
  source.operation[root]={...source.operation[root],futureConstraint:{taskReference:id(50),instruction:'Do not infer the authority of a future field.'}};
  const before=structuredClone(source),{out}=project(source);
  expect(out.operation.planningDemands).toEqual(source.operation.planningDemands);
  expect(out.operation[root]).toEqual(source.operation[root]);assertAuthority(source,out);expect(source).toEqual(before);
 });

it.each(['requestId','runId','artifactId','artifactDigest','proposalDigest','replacementTask','replacementReview','replacementArtifact','replacementDigest','replacementProposal'])(
 'keeps the complete planning body for explicit immutable %s references',kind=>{
  const source=fixture();incidental(source,'createdTaskIds');const row=source.operation.planningDemands.at(-1),item=row.remainingItems[0];
  const references:any={requestId:row.id,runId:item.reference.runId,artifactId:item.reference.artifactId,
   artifactDigest:item.reference.artifactDigest,proposalDigest:item.reference.proposalDigest,replacementTask:item.replacement.taskId,
   replacementReview:item.replacement.reviewRunId,replacementArtifact:item.replacement.reviewArtifactId,
   replacementDigest:item.replacement.reviewArtifactDigest,replacementProposal:item.replacement.proposalDigest};
  source.operation.previous.receipts.push({code:'UNKNOWN_FUTURE_RECEIPT',reference:references[kind]});
  const before=structuredClone(source),{review,out}=project(source);
  expect(review.withinBudget).toBe(true);
  expect(out.operation.planningDemands.find((entry:any)=>entry.id===row.id)).toEqual(row);
  projectedOrAbsent(out,source.operation.planningDemands.at(-2));
  assertAuthority(source,out);expect(source).toEqual(before);
 });

it.each(['case','feedback','request','taskOwner'])(
 'retains a planning demand explicitly addressed by the current %s',kind=>{
  const source=fixture();incidental(source,'consumerTaskId');const row=source.operation.planningDemands.at(-1);
  if(kind==='case')source.cases[0].evidence.push({kind:'CURRENT_PLAN_DECISION',requestId:row.id});
  if(kind==='feedback')source.operation.decisionFeedback.planningRequestId=row.id;
  if(kind==='request')source.operation.recoveredContext.requests.push({kind:'planning_request',requestId:row.id,offset:0});
  if(kind==='taskOwner')row.task_id=source.operation.tasks[1].id;
  const before=structuredClone(source),{review,out}=project(source);
  expect(review.withinBudget).toBe(true);expect(out.operation.planningDemands.find((entry:any)=>entry.id===row.id)).toEqual(row);
  assertAuthority(source,out);expect(source).toEqual(before);
 });

it('preserves unknown case evidence even when its nested value resembles a repair action',()=>{
 const source=fixture();incidental(source,'sourceTaskId');
 source.cases[0].evidence.push({kind:'CURRENT_ACTION',action:{kind:'revise',caseId:source.cases[0].id,sourceTaskId:id(50)}});
 const before=structuredClone(source),{out}=project(source);
 expect(out.operation.planningDemands).toEqual(source.operation.planningDemands);
 assertAuthority(source,out);expect(source).toEqual(before);
});

it.each(['top','item','reference','replacement','request','item-value','reference-index'])(
 'never treats an unknown planning shape at %s as optional discovery',where=>{
  const source=fixture();incidental(source,'createdTaskIds');const row=source.operation.planningDemands.at(-1),item=row.remainingItems[0];
  if(where==='top')row.futureConstraint={instruction:'A new constraint must remain complete.'};
  if(where==='item')item.futureConstraint={instruction:'A new item constraint must remain complete.'};
  if(where==='reference')item.reference.futureBinding={permission:false};
  if(where==='replacement')item.replacement.futureBinding={permission:false};
  if(where==='request')row.remainingItemsRequest.futureCursor={offset:17};
  if(where==='item-value')item.consumerTaskId={futureIdentity:id(50)};
  if(where==='reference-index')item.reference.index={future:2};
  const before=structuredClone(source),{out}=project(source);
  expect(out.operation.planningDemands.find((entry:any)=>entry.id===row.id)).toEqual(row);
  projectedOrAbsent(out,source.operation.planningDemands.at(-2));assertAuthority(source,out);expect(source).toEqual(before);
 });

it.each(['createdTaskIds','sourceTaskId','consumerTaskId'])(
 'preserves conservative matching of %s from an unknown operation decision root',field=>{
  const source=fixture();incidental(source,field);source.operation.futureDecisionEnvelope={taskReference:id(50),requireFullBinding:true};
  const before=structuredClone(source),{out}=project(source);
  expect(out.operation.planningDemands).toEqual(source.operation.planningDemands);
  expect(out.operation.futureDecisionEnvelope).toEqual(source.operation.futureDecisionEnvelope);
  assertAuthority(source,out);expect(source).toEqual(before);
 });

it('does not change any planning body when the complete review fits',()=>{
 const source=fixture();incidental(source,'createdTaskIds');const before=structuredClone(source),{review,out}=project(source,1000000);
 expect(review.withinBudget).toBe(true);expect(out.operation.planningDemands).toEqual(source.operation.planningDemands);
 expect(out.operation.catalogDetailProjection).toBeUndefined();expect(out.operation.recoveredContext).toEqual(source.operation.recoveredContext);
 assertAuthority(source,out);expect(source).toEqual(before);
});

it('keeps a useful exact interval but stays blocked when mandatory evidence plus the minimum cannot fit',()=>{
 const source=fixture();incidental(source,'createdTaskIds');source.cases[0].evidence[0].instruction=noise(30000,31000);
 const first=source.operation.recoveredContext.results[0];
 source.operation.recoveredContext.results.push(...[6823,9691].map((size,i)=>{
  const text=noise(31000+i,size),request={kind:'artifact',taskId:id(7),expectedAttempt:1,name:i?'repair-report.json':'implementation.patch',offset:0};
  source.operation.recoveredContext.requests.push(request);
  return {status:'OK',request,text,digest:sha(text),totalCharacters:text.length,truncated:false,nextOffset:null};
 }));
 const before=structuredClone(source),{review,out}=project(source);
 expect(review.withinBudget).toBe(false);
 const originals=[first,...source.operation.recoveredContext.results.slice(1)];
 expect(originals.some(original=>out.operation.recoveredContext.results.some((entry:any)=>entry.digest===original.digest &&
  entry.text.length>=Math.min(2048,original.text.length) && original.text.startsWith(entry.text)))).toBe(true);
 for(const entry of out.operation.recoveredContext.results){
  const original=originals.find(value=>value.digest===entry.digest)!;
  expect(original).toBeDefined();expect(entry.request).toEqual(original.request);expect(entry.totalCharacters).toBe(original.totalCharacters);
  expect(original.text.startsWith(entry.text)).toBe(true);
  if(entry.text!==original.text){expect(entry.truncated).toBe(true);expect(entry.nextOffset).toBe(entry.text.length);}
 }
 assertAuthority(source,out);expect(source).toEqual(before);
});

it.each([0,1])('rotates a useful current source at real evidence cursor %i without treating stale responses as candidates',cursor=>{
 const source=fixture();source.cases[0].evidence[0].instruction=noise(40000,31000);
 source.operation.workOpportunities={selection:{cursor}};
 const complete=Array.from({length:2},(_,i)=>{
  const text=noise(41000+i,2631+i*100),request={kind:'artifact',taskId:id(7),expectedAttempt:1,name:i?'repair-report.json':'implementation.patch',offset:0};
  return {status:'OK',request,text,digest:sha(text),totalCharacters:text.length,truncated:false,nextOffset:null};
 });
 const stale={status:'STALE_CONTEXT',request:{kind:'task',taskId:id(50),expectedAttempt:2,offset:0},text:'',digest:sha('stale'),totalCharacters:0};
 source.operation.recoveredContext.results=[stale,...complete];
 source.operation.recoveredContext.requests=source.operation.recoveredContext.results.map((entry:any)=>entry.request);
 const before=structuredClone(source),{review,out}=project(source);
 expect(review.withinBudget).toBe(false);expect(out.operation.recoveredContext.results[0]).toEqual(stale);
 const selected=out.operation.recoveredContext.results[cursor+1];
 expect(selected.text).toBe(complete[cursor].text.slice(0,2048));
 expect(selected.request).toEqual(complete[cursor].request);expect(selected.digest).toBe(complete[cursor].digest);
 expect(selected.totalCharacters).toBe(complete[cursor].totalCharacters);expect(selected.truncated).toBe(true);expect(selected.nextOffset).toBe(2048);
 expect(out.operation.recoveredContext.results[2-cursor].text.length).toBeLessThan(complete[1-cursor].text.length);
 expect(out.operation.recoveredContext.selection).toMatchObject({code:'FOCUS_RESUMED',reviewRunId:id(55),
  pageBudgetAdjusted:true,fullSourcesPreserved:true,selectedReadPage:{request:complete[cursor].request,
   digest:complete[cursor].digest,offset:0,characters:2048,originalCharacters:complete[cursor].text.length,
   minimumCharacters:2048,nextOffset:2048,sourceComplete:false,pageShortened:true,cursor,totalUnits:2,nextCursor:(cursor+1)%2}});
 assertAuthority(source,out);expect(source).toEqual(before);
});

it.each([0,1])('retains a useful partial interval and exact continuation at cursor %i',cursor=>{
 const source=fixture();source.cases[0].evidence[0].instruction=noise(42000,31000);
 source.operation.workOpportunities={selection:{cursor}};
 const partial=Array.from({length:2},(_,i)=>{
  const offset=1000+i*500,whole=noise(43000+i,10000),text=whole.slice(offset,offset+3000);
  const request={kind:'task',taskId:i?id(50):id(3),expectedAttempt:i?3:7,offset};
  return {status:'OK',request,text,digest:sha(whole),totalCharacters:whole.length,truncated:true,nextOffset:offset+text.length};
 });
 source.operation.recoveredContext.results=partial;
 source.operation.recoveredContext.requests=partial.map(entry=>entry.request);
 const before=structuredClone(source),{review,out}=project(source);
 expect(review.withinBudget).toBe(false);
 const selected=out.operation.recoveredContext.results[cursor];
 expect(selected.text).toBe(partial[cursor].text.slice(0,2048));expect(selected.request).toEqual(partial[cursor].request);
 expect(selected.digest).toBe(partial[cursor].digest);expect(selected.totalCharacters).toBe(partial[cursor].totalCharacters);
 expect(out.operation.recoveredContext.results[1-cursor].text.length).toBeLessThan(partial[1-cursor].text.length);
 expect(out.operation.recoveredContext.selection.selectedReadPage).toEqual({request:partial[cursor].request,
  digest:partial[cursor].digest,offset:partial[cursor].request.offset,characters:2048,originalCharacters:3000,
  minimumCharacters:2048,nextOffset:partial[cursor].request.offset+2048,pageShortened:true,
  sourceComplete:false,cursor,totalUnits:2,nextCursor:(cursor+1)%2});
 expect(out.operation.recoveredContext.results[cursor].truncated).toBe(true);
 expect(out.operation.recoveredContext.results[cursor].nextOffset).toBe(partial[cursor].request.offset+2048);
 assertAuthority(source,out);expect(source).toEqual(before);
});

it.each([0,1])('gives a partial current read its turn beside a complete source at cursor %i',cursor=>{
 const source=fixture();source.cases[0].evidence[0].instruction=noise(44000,31000);
 source.operation.workOpportunities={selection:{cursor}};
 const complete=source.operation.recoveredContext.results[0],whole=noise(45000,10000),offset=3000,text=whole.slice(offset,offset+3000);
 const partial={status:'OK',request:{kind:'task',taskId:id(50),expectedAttempt:3,offset},text,
  digest:sha(whole),totalCharacters:whole.length,truncated:true,nextOffset:offset+text.length};
 source.operation.recoveredContext.results=[complete,partial];
 source.operation.recoveredContext.requests=[complete.request,partial.request];
 const before=structuredClone(source),{review,out}=project(source),selected=[complete,partial][cursor];
 expect(review.withinBudget).toBe(false);
 const delivered=out.operation.recoveredContext.results[cursor];
 expect(delivered.text).toBe(selected.text.slice(0,2048));expect(delivered.request).toEqual(selected.request);
 expect(delivered.digest).toBe(selected.digest);expect(delivered.totalCharacters).toBe(selected.totalCharacters);
 expect(out.operation.recoveredContext.selection.selectedReadPage).toMatchObject({request:selected.request,
  digest:selected.digest,characters:2048,originalCharacters:selected.text.length,minimumCharacters:2048,
  sourceComplete:false,pageShortened:true,cursor,totalUnits:2,nextCursor:(cursor+1)%2});
 if(cursor===1)expect(out.operation.recoveredContext.results[1].nextOffset).toBe(5048);
 assertAuthority(source,out);expect(source).toEqual(before);
});

it('reprojects a frozen encoded review with the same decision, required planning body and exact delivered page',()=>{
 const source=fixture();incidental(source,'sourceTaskId');const before=structuredClone(source),first=project(source);
 expect(first.review.withinBudget).toBe(true);
 const second=project(JSON.parse(first.review.description));
 expect(second.review.withinBudget).toBe(true);assertAuthority(source,second.out);
 expect(second.out.operation.planningDemands.find((row:any)=>row.id===source.operation.planningDemands[0].id)).toEqual(source.operation.planningDemands[0]);
 assertUsefulRead(source,first.out);expect(second.out.operation.recoveredContext).toEqual(first.out.operation.recoveredContext);
 expect(source).toEqual(before);
});

it.each(['ascii','escaped-utf16'])('reconstructs the exact %s source through successive delivered UTF-16 intervals',kind=>{
 const source=fixture();source.operation.planningDemands=[];
 const whole=kind==='ascii'?noise(61000,19001):'a'.repeat(2047)+'🧭"\\\n\t\r'.repeat(2401)+'END';
 const sourceDigest=sha(whole),initial=structuredClone(source);
 source.operation.recoveredContext.requests=[];source.operation.recoveredContext.results=[];
 const limit=JSON.stringify(project(source,1000000).review.description).length+9000;
 const parts:string[]=[];let offset=0,shortened=false;
 for(let step=0;offset<whole.length&&step<32;step++){
  const request={kind:'artifact',taskId:id(7),expectedAttempt:1,name:'repair-report.json',offset,snapshotDigest:sourceDigest};
  const original={status:'OK',request,text:whole.slice(offset),digest:sourceDigest,totalCharacters:whole.length,truncated:false,nextOffset:null};
  source.operation.recoveredContext.requests=[request];source.operation.recoveredContext.results=[original];
  const before=structuredClone(source),{review,out}=project(source,limit),entry=out.operation.recoveredContext.results[0];
  expect(review.withinBudget).toBe(true);expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(limit);
  expect(entry.request).toEqual(request);expect(entry.digest).toBe(sourceDigest);expect(entry.totalCharacters).toBe(whole.length);
  expect(entry.text.length).toBeGreaterThanOrEqual(Math.min(2048,original.text.length));
  expect(entry.text).toBe(whole.slice(offset,offset+entry.text.length));
  const end=offset+entry.text.length;
  expect(entry.nextOffset).toBe(end<whole.length?end:null);expect(entry.truncated).toBe(end<whole.length);
  if(entry.text.length<original.text.length){
   shortened=true;
   expect(out.operation.recoveredContext.selection.selectedReadPage).toMatchObject({request,digest:sourceDigest,
    offset,characters:entry.text.length,originalCharacters:original.text.length,minimumCharacters:Math.min(2048,original.text.length),
    nextOffset:end<whole.length?end:null,sourceComplete:false,pageShortened:true,cursor:0,totalUnits:1,nextCursor:0});
  }
  assertAuthority(source,out);expect(source).toEqual(before);parts.push(entry.text);expect(end).toBeGreaterThan(offset);offset=end;
 }
 expect(shortened).toBe(true);expect(parts.length).toBeGreaterThan(1);expect(offset).toBe(whole.length);
 expect(parts.join('')).toBe(whole);expect(parts.join('')).not.toContain('\uFFFD');expect(sha(parts.join(''))).toBe(sourceDigest);
 expect(source.cases).toEqual(initial.cases);expect(source.operation.tasks).toEqual(initial.operation.tasks);
});
