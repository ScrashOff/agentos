import {createHash} from 'node:crypto';
import {expect,it} from 'vitest';
import {compactSharedManagerContext,expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {projectManagerReview,projectPersistedManagerDescription,managerContextVersion,managerContextGuide,
 managerTextHash} from '../server/manager-context.js';

const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const digest=(s:string)=>createHash('sha256').update(s).digest('hex');
const characters=(value:any)=>JSON.stringify(JSON.stringify(value)).length;
function fixture(){
 const obligations=Array.from({length:28},(_,index)=>({obligationId:id(100+index),generation:digest('generation'+index),
  handoffId:id(200+index),producerTaskId:id(300+index),producerRunId:id(400+index),
  producerManifestDigest:digest('manifest'+index),repairArtifacts:[
   {id:id(500+index),name:'implementation.patch',digest:digest('patch'+index),size:1400+index},
   {id:id(600+index),name:'repair-report.json',digest:digest('report'+index),size:2200+index}]}));
 const hint={version:1,authority:'review_only',goalId:id(1),caseId:id(2),consumerTaskId:id(3),consumerRunId:id(4),
  consumerAttempt:17,obligations,identityDigest:digest(JSON.stringify(obligations)),actionValidationRequired:true,
  literalValue:{$v:999},literalRecord:{$r:[0,'literal']}};
 return {cases:[{id:id(2),taskId:id(3),currentRun:{id:id(4),attempt:17,marker:{$v:999}},
  deliveryDiagnostic:{marker:{$r:[999]}},unknownFutureField:{marker:{$v:999}},materialConsumptionReview:hint,
  evidence:obligations.map(row=>({kind:'DELIVERY_CONSUMPTION_REQUIRED',code:'CONSUMER_INPUTS_NOT_BOUND',
   obligationKey:'delivery-consumption:'+row.obligationId,generation:row.generation,handoffId:row.handoffId,
   consumerTaskId:id(3),consumerRunId:id(4),producerTaskId:row.producerTaskId,producerRunId:row.producerRunId,
   producerManifestDigest:row.producerManifestDigest,repairArtifacts:row.repairArtifacts,
   instruction:'Review the exact accepted repair and preserve every consumer criterion, dependency and physical-stop guard.'}))}],
  caseWindow:{selectedCaseIds:[id(2)],included:1},operation:{focus:{caseId:id(2),taskId:id(3),scope:'project/current',literal:{$v:999}},
   tasks:[{id:id(3),state:'recovery_blocked',stage:'integrate',expectedAttempt:17,dependencies:obligations.map(o=>o.producerTaskId),
    criteria:[{id:'same-candidate',type:'artifact',name:'verification.json'},{id:'independent-qa',type:'artifact',name:'qa.json'}],
    intent:{text:'Consume only verified inputs, then run independent QA of the same candidate.'}}],
   sourceCatalog:{columns:['id'],bases:[],rows:[],selection:{windowed:true,totalRows:0,includedRows:0,omittedRows:0}},
   workOpportunities:{acceptedSources:obligations.map(row=>({taskId:row.producerTaskId,runId:row.producerRunId,expectedAttempt:1,
    inputs:row.repairArtifacts.map(a=>({artifactId:a.id,...a}))}))},
   recoveredContext:{results:Array.from({length:4},(_,index)=>({status:'OK',request:{kind:'artifact',taskId:id(300+index),
    expectedAttempt:1,name:'repair-report.json',offset:0},runId:id(400+index),artifactId:id(600+index),digest:digest('page'+index),
    text:('Exact requested page '+index+'; ').repeat(8),totalCharacters:1900,nextOffset:192,truncated:true}))}},
  evidenceTexts:[{hash:managerTextHash('Literal protected text'),text:'Literal protected text',originals:[{marker:{$v:999}}]}],
  inlineMemoryHashes:[managerTextHash('Literal protected text')]};
}
function packed(){const source=fixture(),value=compactSharedManagerContext(source);
 expect(value.sharedContext.caseMaterialConsumptionShared).toBe(true);return {source,value};}

it('shares material identities losslessly while preserving the ordinary case envelope and action guards',()=>{
 const source=fixture(),before=JSON.stringify(source),value=compactSharedManagerContext(source);
 expect(value.sharedContext.caseMaterialConsumptionShared).toBe(true);
 expect(value.cases[0].materialConsumptionReview).not.toEqual(source.cases[0].materialConsumptionReview);
 expect(value.sharedContext.guide).toContain('cases[i].materialConsumptionReview');
 expect(value.sharedContext.guide).toContain('SOMENTE quando sharedContext.caseMaterialConsumptionShared=true');
 const restored=expandSharedManagerContext(value);
 expect(JSON.stringify(restored)).toBe(before);expect(JSON.stringify(source)).toBe(before);
 expect(restored.cases[0].materialConsumptionReview.authority).toBe('review_only');
 expect(restored.cases[0].materialConsumptionReview.actionValidationRequired).toBe(true);
 for(const key of ['id','taskId','currentRun','deliveryDiagnostic','unknownFutureField'] as const)
  expect(value.cases[0][key]).toEqual(source.cases[0][key]);
 expect(value.operation.focus).toEqual(source.operation.focus);
 expect(value.evidenceTexts).toEqual(source.evidenceTexts);expect(value.caseWindow).toEqual(source.caseWindow);
 expect(value.inlineMemoryHashes).toEqual(source.inlineMemoryHashes);
 expect(expandSharedManagerContext(restored)).toEqual(source);
 expect(compactSharedManagerContext(value)).toEqual(value);
});

it('reads legacy V1 evidence sharing with an ordinary material hint and legacy literal cases',()=>{
 const {source,value}=packed();delete value.sharedContext.caseMaterialConsumptionShared;
 value.cases[0].materialConsumptionReview=structuredClone(source.cases[0].materialConsumptionReview);
 expect(JSON.stringify(expandSharedManagerContext(value))).toBe(JSON.stringify(source));
 delete value.sharedContext.caseEvidenceShared;value.cases=structuredClone(source.cases);
 expect(JSON.stringify(expandSharedManagerContext(value))).toBe(JSON.stringify(source));
});

it.each([false,'true',1,null])('rejects an invalid material sharing flag: %s',flag=>{
 const {value}=packed();value.sharedContext.caseMaterialConsumptionShared=flag;
 expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_INVALID');
});
it('requires explicit evidence-sharing compatibility for the additional material subtree',()=>{
 const {value}=packed();delete value.sharedContext.caseEvidenceShared;
 expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_INVALID');
});
it.each(['missing-flag','reference','schema','source-hash'])('fails closed for changed material encoding: %s',mode=>{
 const {value}=packed();
 if(mode==='missing-flag')delete value.sharedContext.caseMaterialConsumptionShared;
 if(mode==='reference')value.cases[0].materialConsumptionReview={$v:value.sharedContext.values.length};
 if(mode==='schema')value.cases[0].materialConsumptionReview={$r:[value.sharedContext.schemas.length]};
 if(mode==='source-hash')value.sharedContext.sourceSha256='0'.repeat(64);
 expect(()=>expandSharedManagerContext(value)).toThrow(/MANAGER_SHARED_CONTEXT_/);
});

it('keeps four requested pages, all case evidence and consumer criteria when the material sharing closes the budget gap',()=>{
 const source=fixture(),before=JSON.stringify(source);
 // An unknown envelope field is intentionally protected, reproducing the
 // previous codec boundary without depending on a second implementation.
 const protectedHint=structuredClone(source) as any;
 protectedHint.cases[0].unknownMaterialHint=protectedHint.cases[0].materialConsumptionReview;
 delete protectedHint.cases[0].materialConsumptionReview;
 const compact=compactSharedManagerContext(source),legacy=compactSharedManagerContext(protectedHint);
 expect(characters(legacy)-characters(compact)).toBeGreaterThan(5000);
 const maximum=Math.floor((characters(legacy)+characters(compact))/2);
 const review=projectManagerReview(source,'',0,maximum);
 expect(review.withinBudget).toBe(true);expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(maximum);
 const result=expandSharedManagerContext(JSON.parse(review.description));
 expect(result.cases).toEqual(source.cases);
 expect(result.operation.tasks).toEqual(source.operation.tasks);
 expect(result.operation.recoveredContext).toEqual(source.operation.recoveredContext);
 expect(result.operation.workOpportunities).toEqual(source.operation.workOpportunities);
 expect(JSON.stringify(source)).toBe(before);
});

it('reports the smaller exact representation but still blocks irreducible evidence beyond the hard limit',()=>{
 const source=fixture();
 const unique=Array.from({length:2800},(_,n)=>digest('uncompressible-evidence-'+n)).join('');
 source.cases[0].evidence.push({instruction:unique} as any);
 const review=projectManagerReview(source,'',0,150000);
 expect(review.withinBudget).toBe(false);expect(JSON.stringify(review.description).length).toBeGreaterThan(150000);
 const serialized=JSON.parse(review.description),result=expandSharedManagerContext(serialized);
 expect(serialized.sharedContext.caseMaterialConsumptionShared).toBe(true);
 expect(JSON.stringify(review.description).length).toBeLessThan(characters(result));
 expect(result.cases[0].materialConsumptionReview).toEqual(source.cases[0].materialConsumptionReview);
 expect(result.operation.tasks).toEqual(source.operation.tasks);
 const reference=result.cases[0].evidence.at(-1).instruction;
 expect(result.evidenceTexts.find((text:any)=>text.hash===reference.$text).text).toBe(unique);
});

it('reprojects a frozen material review without reinterpreting literal markers or changing its source identity',()=>{
 const source=fixture(),prefix='MANAGEMENT_V1\n'+managerContextVersion+'\n'+managerContextGuide+'\n';
 const serialized=compactSharedManagerContext(source),description=prefix+JSON.stringify(serialized);
 const result=projectPersistedManagerDescription(description),parsed=expandSharedManagerContext(JSON.parse(result.slice(prefix.length)));
 expect(parsed.cases).toEqual(source.cases);
 expect(parsed.cases[0].materialConsumptionReview.identityDigest).toBe(source.cases[0].materialConsumptionReview.identityDigest);
 expect(parsed.operation.focus).toEqual(source.operation.focus);
 expect(parsed.operation.tasks).toEqual(source.operation.tasks);
});
