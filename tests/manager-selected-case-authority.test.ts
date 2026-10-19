import {createHash} from 'node:crypto';
import {expect,it} from 'vitest';
import {projectManagerReview,managerTextHash} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';

const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const noise=(n:number,count=64)=>Array.from({length:count},(_,i)=>createHash('sha256').update(`${n}:${i}`).digest('hex')).join('');
function fixture():any {
 const rows=Array.from({length:8},(_,i)=>({id:id(100+i),scope:'project/other',state:'waiting_supervision',
  consumer_task_id:id(200+i),producer_run_id:id(300+i),receipt:{generation:noise(400+i,1),nextAction:noise(500+i)}}));
 // Six sources with eight canonical refs keep the original input pressure
 // without an invented inputs[].description field. Unknown input fields are
 // independently required to remain complete by the diagnostic budget suite.
 const sources=Array.from({length:6},(_,i)=>({taskId:id(1000+i),runId:id(1100+i),expectedAttempt:2,
  baseWorkspace:'project/other',workspace:`isolated/${i}`,workKind:'repair',acceptancePassed:true,
  covers:[],omittedInputs:0,inputs:Array.from({length:8},(_,j)=>({artifactId:id(1200+i*8+j),
   name:`report-${noise(1400+i*8+j).slice(0,180)}.json`,digest:noise(1300+i*8+j,1),size:123+j}))}));
 return {cases:[
  {id:id(1),taskId:id(2),currentRun:{id:id(3),attempt:5},evidence:[{instruction:'Keep this decision and its acceptance contract.'}]},
  {id:id(4),taskId:id(5),currentRun:{id:id(6),attempt:1},evidence:[{instruction:noise(7,400),
    obligations:rows.map(r=>r.id),sources:sources.map(s=>s.runId)}]},
 ],operation:{focus:{caseId:id(1),taskId:id(2),scope:'project/focal'},
  tasks:[{id:id(2),criteria:[{id:'final',label:'Independent same-candidate verification'}],dependencies:[]}],
  deliveryConsumptions:rows,workOpportunities:{acceptedSources:sources,consumers:[],existingCoverage:[],liveActors:[]},
  deliveryConsumptionWindow:{totalRows:8,nextCursor:{scope:'project/other',focusOffset:0,otherOffset:8}}}};
}
const decode=(review:any)=>expandSharedManagerContext(JSON.parse(review.description));

it('releases references owned solely by an omitted case without dropping the focal contract or recovery identities',()=>{
 const source=fixture(),before=structuredClone(source),review=projectManagerReview(source,'',0,16000),out=decode(review);
 expect(review.withinBudget).toBe(true);
 expect(review.caseWindow.selectedCaseIds).toEqual([id(1)]);
 expect(out.cases).toEqual([source.cases[0]]);
 expect(out.operation.tasks).toEqual(source.operation.tasks);
 expect(out.operation.deliveryConsumptionWindow).toEqual(source.operation.deliveryConsumptionWindow);
 const projection=out.operation.catalogDetailProjection.catalogs.deliveryConsumptions;
 expect(projection.fullSourcesPreserved).toBe(true);
 expect(projection.sourceRowsSha256).toBe(managerTextHash(JSON.stringify(source.operation.deliveryConsumptions)));
 expect(projection.recovery).toMatchObject({kind:'delivery_consumption',idField:'obligationId',offset:0});
 expect(out.operation.deliveryConsumptions.length<8 || out.operation.deliveryConsumptions.some((r:any)=>r.detailsProjected)).toBe(true);
 expect(out.operation.workOpportunities.acceptedSources.some((s:any)=>s.inputs.detailsProjected)).toBe(true);
 expect(source).toEqual(before);
});

it('retains an omitted-case reference if the same evidence is required by the delivered decision or an unknown operation field',()=>{
 const source=fixture(),rows=source.operation.deliveryConsumptions,sources=source.operation.workOpportunities.acceptedSources;
 source.cases[0].evidence.push({obligationId:rows[0].id,sourceRunId:sources[0].runId});
 source.operation.futureDecisionContract={obligationId:rows[1].id};
 const review=projectManagerReview(source,'',0,24000),out=decode(review);
 expect(review.withinBudget).toBe(true);expect(review.caseWindow.selectedCaseIds).toEqual([id(1)]);
 for(const row of rows.slice(0,2))expect(out.operation.deliveryConsumptions.find((r:any)=>r.id===row.id)).toEqual(row);
 expect(out.operation.futureDecisionContract).toEqual(source.operation.futureDecisionContract);
 expect(out.operation.workOpportunities.acceptedSources[0]).toEqual(sources[0]);
 expect(out.operation.tasks).toEqual(source.operation.tasks);
});

it('never downgrades an unknown catalog contract merely because its case leaves the current window',()=>{
 const source=fixture(),row=source.operation.deliveryConsumptions[0];
 row.futureAuthority={unrecognizedConstraint:noise(1700)};
 const review=projectManagerReview(source,'',0,20000),out=decode(review);
 expect(review.withinBudget).toBe(true);expect(review.caseWindow.selectedCaseIds).toEqual([id(1)]);
 expect(out.operation.deliveryConsumptions.find((r:any)=>r.id===row.id)).toEqual(row);
});

it('restores full references when the previously omitted case becomes the focal window',()=>{
 const source=fixture();
 source.operation.focus={caseId:id(4),taskId:id(5),scope:'project/other'};
 const review=projectManagerReview(source,'',1,120000),out=decode(review);
 expect(review.withinBudget).toBe(true);expect(review.caseWindow.selectedCaseIds[0]).toBe(id(4));
 expect(out.operation.deliveryConsumptions).toEqual(source.operation.deliveryConsumptions);
 expect(out.operation.workOpportunities.acceptedSources).toEqual(source.operation.workOpportunities.acceptedSources);
 expect(source.cases[1].evidence[0].instruction).toBe(noise(7,400));
});

it('spends recoverable discovery before evicting the exact earlier pages of the same pending decision',()=>{
 const source=fixture();source.cases=source.cases.slice(0,1);
 source.operation.deliveryConsumptions=Array.from({length:24},(_,i)=>({id:id(2000+i),scope:'project/other',
  state:'waiting_supervision',consumer_task_id:id(2100+i),producer_run_id:id(2200+i),
  receipt:{generation:noise(2300+i,1),nextAction:noise(2400+i)}}));
 const prior=noise(3000,110),next=noise(3001,25),digest=noise(3002,1);
 const request={kind:'task',taskId:id(2),expectedAttempt:5,offset:0};
 source.operation.retainedEvidence={version:2,entries:[{request,runId:id(3),digest,totalCharacters:100000,
  focus:{caseId:id(1),taskId:id(2),scope:'project/focal'},pages:[{offset:0,text:prior}],
  availableRanges:[{offset:0,characters:prior.length}],complete:false}],fullSourcesPreserved:true};
 source.operation.recoveredContext={requests:[{...request,offset:prior.length}],results:[{status:'OK',
  request:{...request,offset:prior.length},runId:id(3),digest,totalCharacters:100000,text:next,
  truncated:true,nextOffset:prior.length+next.length}]};
 const before=structuredClone(source),review=projectManagerReview(source,'',0,24000),out=decode(review);
 expect(review.withinBudget).toBe(true);
 expect(out.operation.retainedEvidence.entries).toEqual(source.operation.retainedEvidence.entries);
 expect(out.operation.recoveredContext).toEqual(source.operation.recoveredContext);
 expect(out.cases).toEqual(source.cases);expect(out.operation.tasks).toEqual(source.operation.tasks);
 expect(out.operation.catalogDetailProjection.catalogs.deliveryConsumptions.fullSourcesPreserved).toBe(true);
 expect(source).toEqual(before);
});
