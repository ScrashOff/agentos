import {createHash} from 'node:crypto';
import {expect,it} from 'vitest';
import {projectManagerReview,managerTextHash} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {managementContextRequests} from '../server/management-retrieval.js';
const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const body=(n:number)=>Array.from({length:64},(_,i)=>createHash('sha256').update(`${n}:${i}`).digest('hex')).join('');
function fixture():any{
 const focus={scope:'project/focus',taskId:id(999)};
 return {cases:[{id:id(998),taskId:focus.taskId,currentRun:{id:id(997),attempt:4},evidence:[{instruction:'Preserve exact source, candidate, independent QA and acceptance criteria.',generation:'a'.repeat(64)}]}],
 operation:{focus,tasks:[{id:focus.taskId,criteria:[{id:'required',label:'Exact acceptance'}],dependencies:[id(995)],intent:{text:'Original user instruction'}}],
 planningDemands:Array.from({length:15},(_,i)=>({id:id(i+1),scope:i===14?focus.scope:'project/other',state:'awaiting_supervision',task_id:i===14?focus.taskId:id(i+100),
  remainingItems:[{status:'pending',code:body(i),coverageKey:`coverage-${i}`,reference:{requestId:id(i+1),runId:id(1500+i),artifactId:id(1600+i),artifactDigest:body(i).slice(0,64),index:0,proposalDigest:body(i+300).slice(0,64)}}],
  remainingItemCount:1,remainingItemsTruncated:false,remainingItemsRequest:{kind:'planning_request',requestId:id(i+1),offset:0}})),
 deliveryConsumptions:Array.from({length:24},(_,i)=>({id:id(i+200),scope:i===23?focus.scope:'project/other',state:'consumption_required',consumer_task_id:i===23?focus.taskId:id(i+400),producer_run_id:id(i+500),receipt:{generation:body(i+700).slice(0,64),nextAction:body(i+100)}})),
 planningDemandWindow:{total:103,included:15,truncated:true,recovery:{kind:'planning_demands',offset:0}},
 deliveryConsumptionWindow:{totalRows:170,includedRows:24,nextCursor:{scope:focus.scope,focusOffset:32,otherOffset:48},focal:{snapshotDigest:'c'.repeat(64),offset:16,nextOffset:32},other:{snapshotDigest:'d'.repeat(64),offset:40,nextOffset:48}}}};
}
it('budgets optional catalog details with explicit recovery while preserving cursors, focal full records and case authority',()=>{
 const source=fixture(),before=structuredClone(source),review=projectManagerReview(source,'Original authority.\n',0,30000);
 const result=expandSharedManagerContext(JSON.parse(review.description.slice('Original authority.\n'.length)));
 expect(review.withinBudget).toBe(true);expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(30000);
 expect(result.cases).toEqual(source.cases);expect(result.operation.tasks).toEqual(source.operation.tasks);
 expect(result.operation.planningDemandWindow).toEqual(source.operation.planningDemandWindow);
 expect(result.operation.deliveryConsumptionWindow).toEqual(source.operation.deliveryConsumptionWindow);
 for(const key of ['planningDemands','deliveryConsumptions'] as const){
  const rows=result.operation[key],original=source.operation[key],projection=result.operation.catalogDetailProjection.catalogs[key];
  expect(rows.at(-1)).toEqual(original.at(-1));
  expect(projection.sourceRowsSha256).toBe(managerTextHash(JSON.stringify(original)));
  if(projection.discoveryRowsOmitted){
   expect(projection.sourceWindowRows).toBe(original.length);
   expect(projection.omittedRows).toBe(original.length-rows.length);
   expect(projection.discoveryRequest).toEqual({kind:key==='planningDemands'?'planning_demands':'delivery_consumptions',offset:0});
   expect(managementContextRequests.safeParse([projection.discoveryRequest]).success).toBe(true);
  }else expect(rows.map((r:any)=>[r.id,r.scope,r.state])).toEqual(original.map((r:any)=>[r.id,r.scope,r.state]));
  expect(rows.length<original.length||rows.some((r:any)=>r.detailsProjected)).toBe(true);
  rows.forEach((row:any)=>{if(!row.detailsProjected)return;
   expect(row.sourceRowSha256).toBe(managerTextHash(JSON.stringify(original.find((r:any)=>r.id===row.id))));
   const recovery=projection.recovery;
   const request={kind:recovery.kind,[recovery.idField]:row[recovery.idColumn],offset:recovery.offset};
   expect(managementContextRequests.safeParse([request]).success).toBe(true);
   expect(request).not.toHaveProperty('snapshotDigest');expect(row).not.toHaveProperty('receipt');
  });
 }
 expect(source).toEqual(before);
});
it('leaves every catalog body unchanged when the complete review fits',()=>{
 const source=fixture(),review=projectManagerReview(source,'',0,1000000),result=JSON.parse(review.description);
 expect(review.withinBudget).toBe(true);expect(result.operation.planningDemands).toEqual(source.operation.planningDemands);
 expect(result.operation.deliveryConsumptions).toEqual(source.operation.deliveryConsumptions);
 expect(result.operation.catalogDetailProjection).toBeUndefined();
});

it('keeps an executable focal decision and its source references ahead of optional discovery under budget pressure',()=>{
 const source=fixture();
 source.operation.requiredDecision={caseId:source.cases[0].id,consumerTaskId:source.operation.focus.taskId,
  actionAddress:'supervision.actions',identityDigest:'f'.repeat(64),
  nextAction:'Review the accepted repair and submit the existing revise action with its exact source.',
  sourceTaskId:id(7001),sourceRunId:id(7002),expectedAttempt:4};
 source.operation.decisionFeedback={backlogRejections:[{code:'CONSUMER_CLOSED',consumerTaskId:id(7003),sourceTaskId:id(7001)}]};
 const before=structuredClone(source),review=projectManagerReview(source,'Authority remains unchanged.\n',0,30000);
 const result=expandSharedManagerContext(JSON.parse(review.description.slice('Authority remains unchanged.\n'.length)));
 expect(review.withinBudget).toBe(true);
 expect(result.operation.requiredDecision).toEqual(before.operation.requiredDecision);
 expect(result.operation.decisionFeedback).toEqual(before.operation.decisionFeedback);
 expect(source).toEqual(before);
});
it('fails closed when mandatory case evidence exceeds the budget despite catalog projection',()=>{
 const source=fixture();source.cases[0].evidence[0].instruction=body(999)+body(1000)+body(1001);
 const review=projectManagerReview(source,'',0,9000),result=expandSharedManagerContext(JSON.parse(review.description));
 expect(review.withinBudget).toBe(false);
 const instruction=result.cases[0].evidence[0].instruction;
 expect(typeof instruction==='string'?instruction:result.evidenceTexts.find((row:any)=>row.hash===instruction.$text)?.text).toBe(source.cases[0].evidence[0].instruction);
 expect(result.operation.planningDemands.at(-1)).toEqual(source.operation.planningDemands.at(-1));
 expect(result.operation.deliveryConsumptions.at(-1)).toEqual(source.operation.deliveryConsumptions.at(-1));
});
it('does not manufacture recovery for a row without a valid persisted identity',()=>{
 const source=fixture();source.operation.planningDemands[0].id='invalid';
 const review=projectManagerReview(source,'',0,30000),result=expandSharedManagerContext(JSON.parse(review.description));
 expect(result.operation.planningDemands[0]).toEqual(source.operation.planningDemands[0]);
});
it('adapts existing directory windows while retaining every live actor, focal record and preferred source',async()=>{
 const {projectManagerContext,compactManagementSources}=await import('../server/manager-context.js');
 const open=Array.from({length:8},(_,i)=>({id:id(600+i),title:'Task '+i,state:'blocked_dependencies',expectedAttempt:1,
  live:i===7,baseWorkspace:'project/other',workKind:'repair',dependencySummary:{count:1,sha256:'e'.repeat(64)},selection:{detailOmitted:true}}));
 const focal={id:id(699),title:'Focal',state:'waiting_resource',criteria:[{id:'must',label:'Keep exact contract'}]};
 const closed=Array.from({length:10},(_,i)=>({id:id(700+i),title:'Source '+i,state:'accepted',expectedAttempt:i+1,baseWorkspace:'project/focus'}));
 const source={cases:[],operation:{tasks:[focal,...open],sourceCatalog:compactManagementSources(closed).sourceCatalog,
  workOpportunities:{selection:{cursor:0},acceptedSources:closed.slice(0,4).map(t=>({taskId:t.id}))}}};
 const original=structuredClone(source),first=projectManagerContext(source,{sourceCatalogLimit:1,idleTaskLimit:2});
 const other=first.operation.otherTaskDirectory;
 expect(other.rows.map((row:any)=>row[0])).toEqual([open[7].id,open[0].id,open[1].id]);
 expect(other.selection).toMatchObject({totalRows:8,includedRows:3,omittedRows:5,allLiveActorsIncluded:true,nextCursor:2});
 expect(other.recovery).toMatchObject({tool:'management.contextRequests',kind:'task_metadata',taskIdColumn:'id',expectedAttemptColumn:'expectedAttempt'});
 expect(first.operation.sourceCatalog.rows.map((row:any)=>row[0])).toEqual(closed.slice(0,4).map(t=>t.id));
 expect(first.operation.sourceCatalog.selection).toMatchObject({totalRows:10,includedRows:4,omittedRows:6,truncated:true});
 expect(first.operation.sourceCatalog.recovery.completeDirectoryPreserved).toBe(true);
 const secondSource=structuredClone(source);secondSource.operation.workOpportunities.selection.cursor=2;
 const second=projectManagerContext(secondSource,{sourceCatalogLimit:1,idleTaskLimit:2});
 expect(second.operation.otherTaskDirectory.rows.map((row:any)=>row[0])).toEqual([open[7].id,open[2].id,open[3].id]);
 const ref=first.operation.tasks[0],row=first.operation.focusedTaskDirectory.rows[ref.index];
 expect(Object.fromEntries(first.operation.focusedTaskDirectory.schemas[row[0]].map((key:string,i:number)=>[key,row[i+1]]))).toEqual(focal);
 expect(source).toEqual(original);
});
it('defers only unreferenced source input details with exact attempt-bound artifact recovery',()=>{
 const source:any=fixture();
 const sources=Array.from({length:12},(_,i)=>({taskId:id(1000+i),runId:id(1100+i),expectedAttempt:i+1,acceptancePassed:true,
  baseWorkspace:'project/focus',workspace:'isolated/'+i,workKind:'repair',covers:['criterion-'+i],
  inputs:Array.from({length:6},(_,j)=>({artifactId:id(1200+i*10+j),name:`${body(i*10+j).slice(0,300)}.json`,digest:body(i*10+j).slice(0,64),size:300+j}))}));
 source.operation.workOpportunities={acceptedSources:sources,consumers:[],existingCoverage:[],liveActors:[]};
 source.operation.deferredWorkOpportunities={acceptedSources:structuredClone(sources),consumers:[],existingCoverage:[],liveActors:[]};
 source.cases[0].evidence.push({sourceRunId:sources[0].runId});
 source.operation.decisionFeedback={sourceTaskId:sources[1].taskId};
 source.operation.recoveredContext={results:[{status:'OK',artifactId:sources[2].inputs[0].artifactId}]};
 const before=structuredClone(source),r=projectManagerReview(source,'',0,35000),x=expandSharedManagerContext(JSON.parse(r.description));
 expect(r.withinBudget).toBe(true);
 for(const key of ['workOpportunities','deferredWorkOpportunities']){
  const rows=x.operation[key].acceptedSources;
  // Identical deferred inputs may use the existing lossless array reference.
  const actual=Array.isArray(rows)?rows:x.operation.workOpportunities.acceptedSources;
  expect(actual.slice(0,3)).toEqual(sources.slice(0,3));expect(actual.some((row:any)=>row.inputs?.detailsProjected)).toBe(true);
  for(const row of actual){const original=sources.find(v=>v.taskId===row.taskId)!;
   const {inputs,...metadata}=row;const {inputs:originalInputs,...originalMetadata}=original;
   expect(metadata).toEqual(originalMetadata);
   if(!inputs.detailsProjected)continue;
   expect(inputs.inputCount).toBe(originalInputs.length);expect(inputs.sourceSha256).toBe(managerTextHash(JSON.stringify(originalInputs)));
   expect(inputs.recovery).toEqual({kind:'artifacts',taskId:row.taskId,expectedAttempt:row.expectedAttempt,offset:0});
   expect(managementContextRequests.safeParse([inputs.recovery]).success).toBe(true);
  }
 }
 expect(source).toEqual(before);
});

it('omits only uncited discovery identities and restarts each real catalog at zero without rewriting its cursor or digest',()=>{
 const source=fixture(),before=structuredClone(source);
 // Same scope is not a causal reference. Every row remains recoverable in the
 // authoritative catalog even after its local identity directory is omitted.
 for(const row of [...source.operation.planningDemands,...source.operation.deliveryConsumptions])row.scope=source.operation.focus.scope;
 const review=projectManagerReview(source,'',0,18000),result=expandSharedManagerContext(JSON.parse(review.description));
 expect(review.withinBudget).toBe(true);
 for(const key of ['planningDemands','deliveryConsumptions']){
  const projection=result.operation.catalogDetailProjection.catalogs[key],original=source.operation[key],rows=result.operation[key];
  expect(projection.discoveryRowsOmitted).toBe(true);
  expect(projection.sourceWindowRows).toBe(original.length);
  expect(projection.omittedRows).toBe(original.length-rows.length);
  expect(rows).toEqual([original.at(-1)]);
  expect(projection.sourceRowsSha256).toBe(managerTextHash(JSON.stringify(original)));
  expect(projection.discoveryRequest).toEqual({kind:key==='planningDemands'?'planning_demands':'delivery_consumptions',offset:0});
  expect(projection.discoveryRequest).not.toHaveProperty('snapshotDigest');
  expect(managementContextRequests.safeParse([projection.discoveryRequest]).success).toBe(true);
 }
 expect(result.operation.deliveryConsumptionWindow).toEqual(source.operation.deliveryConsumptionWindow);
 expect(result.operation.planningDemandWindow).toEqual(source.operation.planningDemandWindow);
 expect(result.operation.tasks).toEqual(source.operation.tasks);expect(result.cases).toEqual(source.cases);
 // Only the deliberate same-scope fixture edit changed the caller's snapshot.
 expect(source.operation.planningDemands[0].remainingItems).toEqual(before.operation.planningDemands[0].remainingItems);
});

it('keeps nested pending, previous, transition and requested-page references complete before spending optional discovery',()=>{
 const source=fixture(),planning=source.operation.planningDemands,delivery=source.operation.deliveryConsumptions;
 source.cases[0].evidence.push({unrecognizedFutureEnvelope:{exactDigest:planning[0].remainingItems[0].reference.artifactDigest}});
 source.operation.decisionFeedback={source:{obligationId:delivery[0].id}};
 source.operation.backlog={receipts:[{capacityIntent:{state:'waiting_capacity',binding:{nested:{requestId:planning[1].id}}}}]};
 source.operation.previous={summary:'Optional old prose.',receipts:[{obligationId:delivery[1].id}],followThrough:{nested:{requestId:planning[2].id}}};
 source.operation.deferredDeliveries=[{pending:true,obligation:{id:delivery[2].id}}];
 source.operation.scopeRevisions=[{scope:'project/other',pendingReference:planning[3].id}];
 source.operation.deliveryTransitions=[{id:id(7000),scope:source.operation.focus.scope,state:'tracking',receipt:{obligationId:delivery[3].id}}];
 source.operation.recoveredContext={results:Array.from({length:4},(_,i)=>({status:'OK',request:{kind:'planning_request',requestId:planning[4+i].id,offset:0},
  text:body(8000+i).slice(0,700),digest:body(9000+i).slice(0,64),totalCharacters:700,nextOffset:null}))};
 const before=structuredClone(source),review=projectManagerReview(source,'',0,80000),result=expandSharedManagerContext(JSON.parse(review.description));
 expect(review.withinBudget).toBe(true);
 for(const row of planning.slice(0,8))expect(result.operation.planningDemands.find((r:any)=>r.id===row.id)).toEqual(row);
 for(const row of delivery.slice(0,4))expect(result.operation.deliveryConsumptions.find((r:any)=>r.id===row.id)).toEqual(row);
 for(const key of ['backlog','previous','deferredDeliveries','scopeRevisions','deliveryTransitions','recoveredContext','decisionFeedback'])expect(result.operation[key]).toEqual(source.operation[key]);
 expect(result.cases).toEqual(source.cases);expect(result.operation.tasks).toEqual(source.operation.tasks);
 expect(result.operation.catalogDetailProjection).toBeDefined();expect(source).toEqual(before);
});

it.each(['top','receipt-owner','receipt-blocked','reference-index','request-offset','item-code','resolution-generation'])(
 'preserves unknown row values at %s even beneath a recognized field',mode=>{
  const source=fixture();let key='deliveryConsumptions',row=source.operation.deliveryConsumptions[0];
  if(mode==='top')row.unknownFutureDecision={constraint:body(5000)};
  if(mode==='receipt-owner')row.receipt.owner={future:{required:body(5001)}};
  if(mode==='receipt-blocked')row.receipt.blockedTaskIds=[{future:body(5002)}];
  if(mode==='resolution-generation')row.deliveryResolutionBinding={obligationId:row.id,generation:{future:body(5003)}};
  if(['reference-index','request-offset','item-code'].includes(mode)){
   key='planningDemands';row=source.operation.planningDemands[0];
   if(mode==='reference-index')row.remainingItems[0].reference.index={future:body(5004)};
   if(mode==='request-offset')row.remainingItemsRequest.offset={future:body(5005)};
   if(mode==='item-code')row.remainingItems[0].code={future:body(5006)};
  }
  const before=structuredClone(row),review=projectManagerReview(source,'',0,28000),result=expandSharedManagerContext(JSON.parse(review.description));
  expect(result.operation[key].find((r:any)=>r.id===row.id)).toEqual(before);
  expect(source.operation[key][0]).toEqual(before);
 });

it('accepts actual timestamp Date values as known leaves but never omits an unknown timestamp object',()=>{
 const source=fixture();source.operation.planningDemands[0].updated_at=new Date('2026-10-12T04:40:00.000Z');
 source.operation.planningDemands[1].updated_at={future:body(111)};
 const review=projectManagerReview(source,'',0,24000),result=expandSharedManagerContext(JSON.parse(review.description));
 expect(result.operation.planningDemands.find((r:any)=>r.id===source.operation.planningDemands[1].id)).toEqual(source.operation.planningDemands[1]);
 const known=result.operation.planningDemands.find((r:any)=>r.id===source.operation.planningDemands[0].id);
 expect(known===undefined||known.detailsProjected===true).toBe(true);
});

it('does not retain catalog authority solely through an omitted historical transition, while preserving required and unknown roots',()=>{
 const source=fixture(),rows=source.operation.deliveryConsumptions;
 source.operation.deliveryTransitions=[
  {id:id(8100),scope:'project/other',state:'completed',receipt:{obligationId:rows[0].id}},
  {id:id(8101),scope:source.operation.focus.scope,state:'tracking',receipt:{obligationId:rows[1].id}},
  {id:id(8102),scope:'project/other',state:'completed',receipt:{obligationId:rows[2].id}},
 ];
 source.operation.futureDecisionField={nested:{obligationId:rows[3].id}};
 const before=structuredClone(source),review=projectManagerReview(source,'',0,30000),result=expandSharedManagerContext(JSON.parse(review.description));
 expect(review.withinBudget).toBe(true);
 for(const row of [rows[0],rows[1],rows[3],rows.at(-1)])expect(result.operation.deliveryConsumptions.find((r:any)=>r.id===row.id)).toEqual(row);
 const unrelated=result.operation.deliveryConsumptions.find((r:any)=>r.id===rows[2].id);
 expect(unrelated===undefined||unrelated.detailsProjected===true).toBe(true);
 expect(result.operation.deliveryTransitions).toEqual(source.operation.deliveryTransitions.slice(0,2));
 expect(result.operation.futureDecisionField).toEqual(source.operation.futureDecisionField);
 expect(source).toEqual(before);
});
