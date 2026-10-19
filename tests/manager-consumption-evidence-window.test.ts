import {createHash,randomUUID} from 'node:crypto';
import {afterAll,expect,it} from 'vitest';
import {projectManagerCaseProposals} from '../server/manager-case-proposals.js';
import {managerCaseEvidenceSource,readManagerCaseEvidence} from '../server/manager-case-evidence.js';
import {projectManagerCases} from '../server/manager-material-consumption.js';
import {packManagerRecords,projectManagerReview} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {frozenMaterialDecision} from '../server/management-material-decision.js';
import {migrate,pool} from '../server/db.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installSupervision} from '../server/supervision.js';
import {seedRegistry} from '../server/registry.js';

afterAll(()=>pool.end());
const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const sha=(s:string)=>createHash('sha256').update(s).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?
 Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const digest=(v:any)=>sha(JSON.stringify(canonical(v)));
const noise=(n:number)=>Array.from({length:24},(_,i)=>sha(`${n}:${i}`)).join('');

function fixture(attempt=7):any{
 const consumption=Array.from({length:9},(_,i)=>({kind:'DELIVERY_CONSUMPTION_REQUIRED',
  obligationKey:'delivery-consumption:'+id(100+i),generation:sha('generation'+i),
  code:'CONSUMER_INPUTS_NOT_BOUND',handoffId:id(200+i),producerTaskId:id(300+i),producerRunId:id(400+i),
  producerManifestDigest:sha('manifest'+i),repairArtifacts:[
   {id:id(500+i),run_id:id(400+i),name:'implementation.patch',digest:sha('patch'+i),size:1000+i},
   {id:id(600+i),run_id:id(400+i),name:'repair-report.json',digest:sha('report'+i),size:2000+i}],
  consumerTaskId:id(2),consumerRunId:id(3),blockedTaskIds:[],criteriaUnchanged:true,
  instruction:'Inspect this exact current obligation before deciding. '+noise(i),requestKey:'delivery:'+sha('request'+i)}));
 const proposals=Array.from({length:4},(_,i)=>({kind:'SCOPED_DELIVERY_ACTION',
  action:{kind:'prepare_integration',taskId:id(2),expectedAttempt:attempt,sourceTaskId:id(700+i),
   expectedSourceRunId:id(800+i),artifactIds:[id(900+i)],reason:'Read the complete current proposal '+noise(30+i)},
  sourceProof:{fence:id(1000+i),runId:id(800+i),taskId:id(700+i),attempt:1,workspace:'scope/source'+i,manifestDigest:sha('source'+i)},
  consumerRunId:id(3),consumerAttempt:attempt,sourcePlanRunId:id(1100+i),requestId:id(1200+i),
  actionKey:'scoped-action:'+sha('action'+i),requestKey:'delivery:'+sha('proposal'+i)}));
 const other=[{kind:'DELIVERY_SOURCE_REVIEW_REQUIRED',obligationId:id(1400),generation:sha('conflict'),
   instruction:'Keep source conflict and every compatibility requirement complete.',sourceConflict:{expected:sha('old'),observed:sha('new')}},
  {kind:'DELIVERY_RECERTIFICATION_READY',sourceRunId:id(1401),proof:{mustRemain:true}},
  {kind:'FUTURE_CAUSAL_KIND',nested:{permission:false,instruction:'Unknown current evidence remains complete.'}}];
 const row={id:id(1),goal_id:id(10),task_id:id(2),source_run_id:id(3),current_run_id:id(3),
  current_attempt:attempt,current_run_state:'failed',base_workspace:'project/consumer',work_kind:'integrate',
  reason:'A material decision is still required',evidence:[other[0],...consumption,...proposals,...other.slice(1)]};
 const identity={goalId:row.goal_id,caseId:row.id,consumerTaskId:row.task_id,
  consumerRunId:row.current_run_id,consumerAttempt:attempt,
  obligations:consumption.map(e=>({obligationId:e.obligationKey.slice('delivery-consumption:'.length),generation:e.generation,
   handoffId:e.handoffId,producerTaskId:e.producerTaskId,producerRunId:e.producerRunId,
   producerManifestDigest:e.producerManifestDigest,repairArtifacts:structuredClone(e.repairArtifacts)}))};
 const material={version:1,authority:'review_only',...identity,identityDigest:digest(identity),actionValidationRequired:true};
 const required={version:1,caseId:row.id,taskId:row.task_id,consumerRunId:row.current_run_id,consumerAttempt:attempt,
  materialReview:structuredClone(material),identityDigest:material.identityDigest,actionAddress:'supervision.actions'};
 const snapshot={cases:projectManagerCases([{...row,materialConsumptionReview:material}]),operation:{
  goalId:row.goal_id,focus:{caseId:row.id,taskId:row.task_id,scope:row.base_workspace},requiredDecision:required,
  previous:{receipts:[{code:'MATERIAL_DECISION_REQUIRED',requiredDecision:structuredClone(required)}],
   followThrough:{status:'decision_pending',requiredDecision:structuredClone(required)}},
  tasks:[{id:row.task_id,expectedAttempt:attempt,state:'recovery_blocked',stage:'integrate',
   criteria:[{id:'candidate',type:'artifact',name:'verification.json'},{id:'qa',type:'artifact',name:'independent-qa.json'}],
   dependencies:[id(300)],executionInputs:consumption[0].repairArtifacts,
   intent:{description:'Preserve complete criteria, exact inputs and independent QA of the same candidate.'}}],
  recoveredContext:{requests:[] as any[],results:[] as any[]}}};
 return {row,snapshot,consumption,proposals,other};
}
function decode(description:string){
 const d=expandSharedManagerContext(JSON.parse(description));
 const texts=new Map((d.evidenceTexts??[]).filter((r:any)=>typeof r.text==='string'&&sha(r.text)===r.hash).map((r:any)=>[r.hash,r.text]));
 const restore=(v:any):any=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).length===1&&texts.has(v.$text)?texts.get(v.$text):
  Array.isArray(v)?v.map(restore):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).map(([k,x])=>[k,restore(x)])):v;
 return restore(d);
}

// The observed case has 28 current-run requests for 17 still-pending
// obligations: eleven generations have both composition and contract codes.
// Those are separate recorded reasons, never eleven completed/old obligations.
function twentyEightPendingFixture():any{
 const f=fixture(),unique=Array.from({length:17},(_,i)=>({
  ...structuredClone(f.consumption[0]),obligationKey:'delivery-consumption:'+id(2100+i),
  generation:sha('current-generation'+i),handoffId:id(2200+i),producerTaskId:id(2300+i),producerRunId:id(2400+i),
  producerManifestDigest:sha('current-manifest'+i),repairArtifacts:[
   {id:id(2500+i),run_id:id(2400+i),name:'implementation.patch',digest:sha('current-patch'+i),size:1000+i},
   {id:id(2600+i),run_id:id(2400+i),name:'repair-report.json',digest:sha('current-report'+i),size:2000+i}]}));
 const consumption=unique.flatMap((e:any,i:number)=>(i<11?
  ['COMPOSITION_PROOF_REQUIRED','CONSUMER_CONTRACT_CHANGED']:['CONSUMER_INPUTS_NOT_BOUND']).map(code=>({
   ...structuredClone(e),code,instruction:`The exact ${code} reason for obligation ${i} must be read before deciding. `+noise(100+i),
   requestKey:'delivery:'+digest({obligationKey:e.obligationKey,generation:e.generation,code})})));
 f.row.evidence=[f.other[0],...consumption,...f.proposals,...f.other.slice(1)];
 const identity={goalId:f.row.goal_id,caseId:f.row.id,consumerTaskId:f.row.task_id,
  consumerRunId:f.row.current_run_id,consumerAttempt:f.row.current_attempt,
  obligations:unique.map((e:any)=>({obligationId:e.obligationKey.slice('delivery-consumption:'.length),generation:e.generation,
   handoffId:e.handoffId,producerTaskId:e.producerTaskId,producerRunId:e.producerRunId,
   producerManifestDigest:e.producerManifestDigest,repairArtifacts:structuredClone(e.repairArtifacts)}))};
 const material={version:1,authority:'review_only',...identity,identityDigest:digest(identity),actionValidationRequired:true};
 const required={...f.snapshot.operation.requiredDecision,materialReview:structuredClone(material),identityDigest:material.identityDigest};
 f.snapshot.cases=projectManagerCases([{...f.row,materialConsumptionReview:material}]);
 f.snapshot.operation.requiredDecision=required;
 f.snapshot.operation.previous={receipts:[{code:'MATERIAL_DECISION_REQUIRED',requiredDecision:structuredClone(required)}],
  followThrough:{status:'decision_pending',requiredDecision:structuredClone(required)}};
 f.snapshot.operation.deliveryConsumptions=unique.map((e:any)=>({id:e.obligationKey.slice('delivery-consumption:'.length),
  state:'waiting_supervision',receipt:{code:'SUPERVISION_REQUESTED',caseId:f.row.id,generation:e.generation}}));
 return {...f,consumption,unique};
}

it('windows all 28 known requests without deduplicating codes or resolving any of the 17 current obligations',()=>{
 const f=twentyEightPendingFixture(),before=structuredClone(f.snapshot);
 const selected=projectManagerCaseProposals(f.snapshot,0,1),directory=selected.snapshot.operation.caseEvidenceDirectory.cases[0];
 const rows=directory.rows.filter((r:any)=>r.kind==='DELIVERY_CONSUMPTION_REQUIRED');
 expect(rows).toHaveLength(28);expect(new Set(rows.map((r:any)=>r.obligationId)).size).toBe(17);
 expect(new Set(rows.map((r:any)=>r.generation)).size).toBe(17);
 expect(rows.map((r:any)=>r.index)).toEqual(Array.from({length:28},(_,i)=>i+1));
 expect(rows.filter((r:any)=>r.code==='COMPOSITION_PROOF_REQUIRED')).toHaveLength(11);
 expect(rows.filter((r:any)=>r.code==='CONSUMER_CONTRACT_CHANGED')).toHaveLength(11);
 expect(rows.slice(0,2).map((r:any)=>[r.obligationId,r.generation])).toEqual([
  [id(2100),sha('current-generation0')],[id(2100),sha('current-generation0')]]);
 expect(directory.selectedIndices).toEqual([1,29]);
 const seen=new Set<number>();
 for(let cursor=0;cursor<28;cursor++){
  const r=projectManagerCaseProposals(f.snapshot,cursor,1),c=r.snapshot.cases[0];
  const d=r.snapshot.operation.caseEvidenceDirectory.cases[0];
  d.selectedIndices.filter((n:number)=>n<=28).forEach((n:number)=>seen.add(n));
  for(const index of d.selectedIndices)expect(c.evidence).toContainEqual(before.cases[0].evidence[index]);
  for(const e of f.other)expect(c.evidence).toContainEqual(e);
  expect(c.materialConsumptionReview).toEqual(before.cases[0].materialConsumptionReview);
  expect(c.materialConsumptionReview.obligations).toHaveLength(17);
  expect(r.snapshot.operation.requiredDecision).toEqual(before.operation.requiredDecision);
  expect(r.snapshot.operation.previous).toEqual(before.operation.previous);
  expect(r.snapshot.operation.deliveryConsumptions).toEqual(before.operation.deliveryConsumptions);
 }
 expect([...seen].sort((a,b)=>a-b)).toEqual(Array.from({length:28},(_,i)=>i+1));
 const review=projectManagerReview(f.snapshot,'',0,150000),out=decode(review.description);
 expect(review.withinBudget).toBe(true);
 expect(out.cases[0].materialConsumptionReview).toEqual(before.cases[0].materialConsumptionReview);
 expect(out.operation.requiredDecision).toEqual(before.operation.requiredDecision);
 expect(out.operation.previous).toEqual(before.operation.previous);
 expect(out.operation.tasks).toEqual(before.operation.tasks);
 expect(out.operation.deliveryConsumptions).toEqual(before.operation.deliveryConsumptions);
 const decision=frozenMaterialDecision('MANAGEMENT_V1\n'+review.description);
 expect(decision?.invalid).toBe(false);
 if(!decision||decision.invalid)throw Error('MATERIAL_17_OBLIGATIONS_FIXTURE_INVALID');
 expect(decision.binding.obligations).toEqual(before.cases[0].materialConsumptionReview.obligations);
 expect(decision.identityDigest).toBe(before.operation.requiredDecision.identityDigest);
 expect(f.snapshot).toEqual(before);
});

it.each(['COMPOSITION_PROOF_REQUIRED','CONSUMER_CONTRACT_CHANGED'])('keeps unknown nested constraints and requested bodies whole for %s',code=>{
 const f=twentyEightPendingFixture(),index=f.row.evidence.findIndex((e:any)=>e.code===code);
 const unknown=structuredClone(f.row.evidence[index]);
 unknown.code='FUTURE_CURRENT_CONSTRAINT';unknown.instruction='A new reason must stay complete.';
 const nested=structuredClone(f.row.evidence[index]);
 nested.repairArtifacts[0].futureConstraint={reason:'Unrecognized compatibility requirement',diffs:['current','frozen']};
 f.row.evidence.push(unknown,nested);
 f.snapshot.cases=projectManagerCases([{...f.row,materialConsumptionReview:f.snapshot.cases[0].materialConsumptionReview}]);
 const source=f.snapshot.cases[0].evidenceSource;
 f.snapshot.operation.recoveredContext={requests:[{kind:'case_evidence',caseId:f.row.id,digest:source.digest,evidenceIndex:index,offset:0}],results:[]};
 const before=structuredClone(f.snapshot),r=projectManagerCaseProposals(f.snapshot,9,1),c=r.snapshot.cases[0];
 expect(c.evidence).toContainEqual(before.cases[0].evidence[index]);
 expect(c.evidence).toContainEqual(unknown);expect(c.evidence).toContainEqual(nested);
 expect(c.evidence).toContainEqual(f.other[0]);
 expect(r.snapshot.operation.recoveredContext).toEqual(before.operation.recoveredContext);
 expect(r.snapshot.operation.caseEvidenceDirectory.cases[0].rows).toHaveLength(32);
 expect(f.snapshot).toEqual(before);
});

it('preserves separate recorded generations and never infers completion from repeated obligation IDs',()=>{
 const f=twentyEightPendingFixture(),older=structuredClone(f.consumption[1]);
 older.generation=sha('another-recorded-generation');
 older.requestKey='delivery:'+digest({obligationKey:older.obligationKey,generation:older.generation,code:older.code});
 f.row.evidence.push(older);
 f.snapshot.cases=projectManagerCases([{...f.row,materialConsumptionReview:f.snapshot.cases[0].materialConsumptionReview}]);
 const before=structuredClone(f.snapshot),r=projectManagerCaseProposals(f.snapshot,0,1);
 const rows=r.snapshot.operation.caseEvidenceDirectory.cases[0].rows.filter((x:any)=>x.obligationId===id(2100));
 expect(rows).toHaveLength(3);expect(rows.map((x:any)=>x.generation)).toEqual([
  sha('current-generation0'),sha('current-generation0'),older.generation]);
 expect(r.snapshot.cases[0].materialConsumptionReview).toEqual(before.cases[0].materialConsumptionReview);
 expect(r.snapshot.operation.deliveryConsumptions).toEqual(before.operation.deliveryConsumptions);
 expect(r.snapshot.operation.requiredDecision).toEqual(before.operation.requiredDecision);
});

it('accepts already persisted V2 directories without optional code, but rejects changed or unknown code',()=>{
 const f=twentyEightPendingFixture(),current=projectManagerCaseProposals(f.snapshot,0,1).snapshot;
 const resign=(snapshot:any)=>{
  const d=snapshot.operation.caseEvidenceDirectory,{digest:_,...body}=d;d.digest=digest(body);
  snapshot.cases[0].proposalWindow.directoryDigest=d.digest;
 };
 const legacy=structuredClone(current);
 for(const row of legacy.operation.caseEvidenceDirectory.cases[0].rows)delete row.code;
 resign(legacy);const before=structuredClone(legacy);
 expect(projectManagerCaseProposals(legacy,23,1)).toEqual({snapshot:before,discoveryDirectoryValidated:true});
 expect(legacy).toEqual(before);
 const changed=structuredClone(current);
 changed.operation.caseEvidenceDirectory.cases[0].rows[0].code='CONSUMER_CONTRACT_CHANGED';
 expect(()=>projectManagerCaseProposals(changed)).toThrow('MANAGER_CASE_PROPOSAL_DIRECTORY_CHANGED');
 for(const invalid of ['FUTURE_CURRENT_CONSTRAINT',{code:'COMPOSITION_PROOF_REQUIRED'},null]){
  const bad=structuredClone(current);bad.operation.caseEvidenceDirectory.cases[0].rows[0].code=invalid;resign(bad);
  expect(()=>projectManagerCaseProposals(bad)).toThrow('MANAGER_CASE_PROPOSAL_WINDOW_CHANGED');
 }
});

it('delivers one complete unit per group plus a digest-bound directory of every pending obligation',()=>{
 const f=fixture(),before=structuredClone(f.snapshot),r=projectManagerCaseProposals(f.snapshot,0,1);
 expect(r.discoveryDirectoryValidated).toBe(true);
 const d=r.snapshot.operation.caseEvidenceDirectory,c=r.snapshot.cases[0],source=managerCaseEvidenceSource(f.row)!;
 expect(d).toMatchObject({version:2,kind:'CURRENT_CASE_EVIDENCE_V2'});
 expect(d.digest).toBe(digest({version:d.version,kind:d.kind,guide:d.guide,cases:d.cases}));
 expect(d.cases[0].source.digest).toBe(source.digest);expect(d.cases[0].source.arrayDigest).toBe(source.arrayDigest);
 expect(d.cases[0].source).not.toHaveProperty('entryDigests');
 expect(d.cases[0].rows).toHaveLength(13);
 expect(d.cases[0].rows[0]).toEqual({index:1,kind:'DELIVERY_CONSUMPTION_REQUIRED',code:'CONSUMER_INPUTS_NOT_BOUND',obligationId:id(100),
  generation:sha('generation0'),sourceTaskId:id(300),artifactCount:2});
 expect(d.cases[0].selectedIndices).toEqual([1,10]);
 expect(c.proposalWindow).toMatchObject({version:2,selectedEvidenceIndices:[0,1,10,14,15],
  selectedWindowIndices:[1,10],omittedWindowEntries:11,sourceEvidenceCount:16});
 expect(c.evidence).toEqual([before.cases[0].evidence[0],before.cases[0].evidence[1],before.cases[0].evidence[10],
  before.cases[0].evidence[14],before.cases[0].evidence[15]]);
 expect(c.materialConsumptionReview).toEqual(before.cases[0].materialConsumptionReview);
 expect(c.materialConsumptionReview.obligations).toHaveLength(9);
 expect(c.currentRun).toEqual(before.cases[0].currentRun);
 expect(r.snapshot.operation.requiredDecision).toEqual(before.operation.requiredDecision);
 expect(r.snapshot.operation.previous).toEqual(before.operation.previous);
 expect(r.snapshot.operation.tasks).toEqual(before.operation.tasks);
 expect(f.snapshot).toEqual(before);
});

it('retains all explicitly requested consumption/proposal entries beyond the one-per-group selection',()=>{
 const f=fixture(),source=f.snapshot.cases[0].evidenceSource;
 f.snapshot.operation.recoveredContext={requests:[{kind:'case_evidence',caseId:id(1),digest:source.digest,evidenceIndex:9,offset:0},
  {kind:'case_evidence',caseId:id(1),digest:source.digest,evidenceIndex:13,offset:0}],
  results:[{status:'OK',caseId:id(1),evidenceIndex:8,evidenceDigest:source.digest,text:'Exact original requested page',digest:sha('page')}]};
 const r=projectManagerCaseProposals(f.snapshot,0,1),c=r.snapshot.cases[0];
 expect(c.proposalWindow.selectedWindowIndices).toEqual([1,8,9,10,13]);
 for(const index of [1,8,9,10,13])expect(c.evidence).toContainEqual(f.snapshot.cases[0].evidence[index]);
 expect(r.snapshot.operation.recoveredContext).toEqual(f.snapshot.operation.recoveredContext);
});

it('rotates each complete group without resolving or deleting any other obligation',()=>{
 const f=fixture(),seenConsumption=new Set<number>(),seenProposals=new Set<number>();
 for(let cursor=0;cursor<13;cursor++){
  const r=projectManagerCaseProposals(f.snapshot,cursor,1),d=r.snapshot.operation.caseEvidenceDirectory.cases[0];
  d.selectedIndices.forEach((n:number)=>{if(n<10)seenConsumption.add(n);else seenProposals.add(n);});
  expect(d.rows).toHaveLength(13);expect(d.consumerAttempt).toBe(7);
  expect(r.snapshot.cases[0].materialConsumptionReview).toEqual(f.snapshot.cases[0].materialConsumptionReview);
  expect(r.snapshot.operation.requiredDecision).toEqual(f.snapshot.operation.requiredDecision);
 }
 expect([...seenConsumption].sort((a,b)=>a-b)).toEqual([1,2,3,4,5,6,7,8,9]);
 expect([...seenProposals].sort((a,b)=>a-b)).toEqual([10,11,12,13]);
});

it.each(['unknownField','unknownCode','sourceReview','recertification','wrongConsumer','wrongRun','wrongArtifactRun','wrongArtifactShape',
 'invalidGeneration','changedBody'] as const)('keeps unproven evidence complete: %s',mode=>{
 const f=fixture(),e=f.snapshot.cases[0].evidence[9];
 if(mode==='unknownField')e.futureField={mustRemain:true};
 if(mode==='unknownCode')e.code='NEW_CURRENT_CONSTRAINT';
 if(mode==='sourceReview')e.kind='DELIVERY_SOURCE_REVIEW_REQUIRED';
 if(mode==='recertification')e.kind='DELIVERY_RECERTIFICATION_READY';
 if(mode==='wrongConsumer')e.consumerTaskId=id(9999);
 if(mode==='wrongRun')e.consumerRunId=id(9999);
 if(mode==='wrongArtifactRun')e.repairArtifacts[0].run_id=id(9999);
 if(mode==='wrongArtifactShape')e.repairArtifacts[0].newAuthority='unknown';
 if(mode==='invalidGeneration')e.generation='invalid';
 if(mode==='changedBody')e.instruction+=' modified after source binding';
 // Except the deliberate binding-drift test, prove these are authentic raw
 // entries which must stay whole due to semantics, not only a stale hash.
 if(mode!=='changedBody'){
  f.row.evidence[9]=structuredClone(e);f.snapshot.cases[0].evidenceSource=managerCaseEvidenceSource(f.row);
 }
 const r=projectManagerCaseProposals(f.snapshot,0,1);
 expect(r.snapshot.cases[0].evidence).toContainEqual(e);
 expect(r.snapshot.operation.caseEvidenceDirectory.cases[0].rows.some((row:any)=>row.index===9)).toBe(false);
});

it.each(['attemptZero','changedRun','missingBinding','wrongCount'] as const)('does not project a case without current original authority: %s',mode=>{
 const f=fixture(mode==='attemptZero'?0:7),c=f.snapshot.cases[0];
 if(mode==='changedRun')c.currentRun.id=id(9999);
 if(mode==='missingBinding')delete c.evidenceSource;
 if(mode==='wrongCount')c.evidenceSource.entryDigests.pop();
 const r=projectManagerCaseProposals(f.snapshot,0,1);
 expect(r.discoveryDirectoryValidated).toBe(false);expect(r.snapshot.cases[0].evidence).toEqual(c.evidence);
 expect(r.snapshot.cases[0].materialConsumptionReview).toEqual(c.materialConsumptionReview);
});

it('retains the persisted V1 contract for scoped-only proposals',()=>{
 const f=fixture();f.row.evidence=[...f.proposals,...f.other];
 f.snapshot.cases=projectManagerCases([f.row]);
 const r=projectManagerCaseProposals(f.snapshot,0,1);
 expect(r.snapshot.operation.caseEvidenceDirectory).toMatchObject({version:1,kind:'CURRENT_SCOPED_PROPOSALS_V1'});
 expect(r.snapshot.cases[0].proposalWindow.version).toBe(1);
 expect(projectManagerCaseProposals(r.snapshot,8,1)).toEqual(r);
});

it('does not rewrite or certify an unrelated preexisting window when another case gains a V2 directory',()=>{
 const f=fixture(),unknown={id:id(9990),taskId:id(9991),currentRun:{id:id(9992),attempt:3},
  evidenceSource:{version:1,caseId:id(9990),taskId:id(9991),sourceRunId:id(9992),evidenceCount:9,arrayDigest:'invalid',digest:'invalid'},
  evidence:[{kind:'UNKNOWN_CURRENT',permission:false}],proposalWindow:{version:1,directoryDigest:'untrusted',unknown:'preserve'}};
 f.snapshot.cases.unshift(unknown);const original=structuredClone(f.snapshot);
 const r=projectManagerCaseProposals(f.snapshot,0,1);
 expect(r.discoveryDirectoryValidated).toBe(true);
 expect(r.snapshot.cases[0]).toEqual(original.cases[0]);
 expect(r.snapshot.operation.caseEvidenceDirectory.cases.map((row:any)=>row.caseId)).toEqual([id(1)]);
 expect(r.snapshot.cases[1].proposalWindow.version).toBe(2);
 expect(f.snapshot).toEqual(original);
});

it('round-trips and reprojects a frozen V2 review without rotating its selected bodies or changing any decision',()=>{
 const f=fixture(),before=structuredClone(f.snapshot),review=projectManagerReview(f.snapshot,'',0,150000);
 expect(review.withinBudget).toBe(true);
 const raw=expandSharedManagerContext(JSON.parse(review.description)),d=decode(review.description);
 expect(projectManagerCaseProposals(raw,8,1).discoveryDirectoryValidated).toBe(true);
 const second=projectManagerReview(raw,'',8,150000),again=decode(second.description);
 expect(again.cases[0].proposalWindow).toEqual(d.cases[0].proposalWindow);
 expect(again.cases[0].evidence).toEqual(d.cases[0].evidence);
 expect(d.cases[0].materialConsumptionReview).toEqual(before.cases[0].materialConsumptionReview);
 expect(d.operation.requiredDecision).toEqual(before.operation.requiredDecision);
 expect(d.operation.previous).toEqual(before.operation.previous);
 expect(d.operation.tasks).toEqual(before.operation.tasks);
 const originalDecision=frozenMaterialDecision('MANAGEMENT_V1\n'+JSON.stringify(before));
 const frozenDecision=frozenMaterialDecision('MANAGEMENT_V1\n'+review.description);
 expect(originalDecision?.invalid).toBe(false);expect(frozenDecision?.invalid).toBe(false);
 if(!originalDecision||originalDecision.invalid||!frozenDecision||frozenDecision.invalid)throw Error('MATERIAL_FIXTURE_INVALID');
 expect(frozenDecision.binding).toEqual(originalDecision.binding);
 expect(frozenDecision.identityDigest).toBe(originalDecision.identityDigest);
 expect(frozenDecision.binding.obligations).toHaveLength(9);
 const laterDecision=frozenMaterialDecision('MANAGEMENT_V1\n'+second.description);
 expect(laterDecision?.invalid).toBe(false);
 if(!laterDecision||laterDecision.invalid)throw Error('MATERIAL_REPLAY_INVALID');
 expect(laterDecision.binding).toEqual(originalDecision.binding);
 expect(laterDecision.identityDigest).toBe(originalDecision.identityDigest);
 expect(f.snapshot).toEqual(before);
});

it('reprojects a frozen packed source catalog with a later ledger under pressure without interpreting the directory as an array',()=>{
 const f=fixture();
 const sources=[{taskId:id(8000),runId:id(8001),expectedAttempt:2,baseWorkspace:'project/source',workKind:'repair',
  inputs:[{artifactId:id(8002),name:'repair-report.json',digest:sha('repair catalog'),size:3000,description:noise(8003)}]}];
 const packed=packManagerRecords(sources);
 f.snapshot.operation.workOpportunities={acceptedSources:packed,consumers:[],existingCoverage:[],liveActors:[]};
 const initial=projectManagerReview(f.snapshot,'',0,150000);expect(initial.withinBudget).toBe(true);
 const frozen=expandSharedManagerContext(JSON.parse(initial.description));
 expect(frozen.operation.workOpportunities.acceptedSources).toEqual(packed);
 const pages=[{offset:0,text:noise(8004).repeat(8)}];
 frozen.operation.retainedEvidence={version:2,entries:[{request:{kind:'task',taskId:id(2),expectedAttempt:7,offset:0},
  runId:id(3),digest:sha('ledger source'),totalCharacters:100000,complete:false,pages,
  availableRanges:[{offset:0,characters:pages[0].text.length}]}]};
 const before=structuredClone(frozen);
 // An intentionally irreducible tiny budget guarantees the source-input
 // reduction branch is reached. It must report the limit, never drop or
 // falsely unpack source authority merely to make this fixture fit.
 const review=projectManagerReview(frozen,'',0,1000),out=decode(review.description);
 expect(review.withinBudget).toBe(false);
 expect(out.operation.workOpportunities.acceptedSources).toEqual(packed);
 expect(out.operation.requiredDecision).toEqual(f.snapshot.operation.requiredDecision);
 expect(out.cases[0].materialConsumptionReview).toEqual(f.snapshot.cases[0].materialConsumptionReview);
 const decision=frozenMaterialDecision('MANAGEMENT_V1\n'+review.description);
 expect(decision?.invalid).toBe(false);
 if(!decision||decision.invalid)throw Error('MATERIAL_PACKED_REPLAY_INVALID');
 expect(decision.binding.obligations).toHaveLength(9);
 expect(frozen).toEqual(before);
});

it.each(['body','directory','index','window','sourceRun','attempt'] as const)('rejects tampering with persisted V2 origin/selection: %s',mode=>{
 const f=fixture(),r=projectManagerCaseProposals(f.snapshot,0,1),forged=structuredClone(r.snapshot);
 if(mode==='body')forged.cases[0].evidence[1].instruction+='tampered';
 if(mode==='directory')forged.operation.caseEvidenceDirectory.cases[0].rows[0].generation=sha('changed');
 if(mode==='index')forged.operation.caseEvidenceDirectory.cases[0].rows[0].index=9;
 if(mode==='window')forged.cases[0].proposalWindow.selectedEvidenceIndices[1]=9;
 if(mode==='sourceRun')forged.cases[0].currentRun.id=id(9999);
 if(mode==='attempt')forged.cases[0].currentRun.attempt++;
 expect(()=>projectManagerCaseProposals(forged)).toThrow(/MANAGER_CASE_PROPOSAL_(DIRECTORY|WINDOW)_CHANGED/);
});

it.each(['CONSUMER_INPUTS_NOT_BOUND','COMPOSITION_PROOF_REQUIRED','CONSUMER_CONTRACT_CHANGED'])(
 'recovers the exact omitted %s unit and refuses later pages after raw binding or reason drift',async code=>{
 await migrate();await installBacklogPlanner();await installManagement();await installSupervision();await seedRegistry();
 const f=twentyEightPendingFixture(),goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID();
 const evidence=structuredClone(f.consumption);
 for(const e of evidence){e.consumerTaskId=task;e.consumerRunId=run;}
 evidence[8].code=code;
 evidence[8].requestKey='delivery:'+digest({obligationKey:evidence[8].obligationKey,generation:evidence[8].generation,code});
 evidence[8].instruction+=' '+noise(70).repeat(10);
 const row={...f.row,id:caseId,goal_id:goal,task_id:task,source_run_id:run,current_run_id:run,evidence};
 const projected=projectManagerCaseProposals({cases:projectManagerCases([row]),operation:{}},0,1);
 const directory=projected.snapshot.operation.caseEvidenceDirectory.cases[0];
 expect(directory.rows).toHaveLength(28);
 expect(new Set(directory.rows.map((r:any)=>r.obligationId)).size).toBe(17);
 expect(directory.selectedIndices).not.toContain(8);
 const db=await pool.connect();
 try{
  await db.query('BEGIN');
  await db.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Consumption window','Original evidence','consumption-window',$1::text)",[goal]);
  await db.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
   VALUES($1,$2,'consumer','Consumer','Preserve all criteria.','qa.backend','failed','integrate','[]','{}',7)`,[task,goal]);
  await db.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,7,$3,1,'failed','harness')",[run,task,randomUUID()]);
  await db.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,'Current consumption',$5)",[caseId,goal,task,run,JSON.stringify(evidence)]);
  const state=async()=> (await db.query(`SELECT c.state case_state,c.evidence,t.state task_state,t.attempt_count,
    (SELECT count(*)::int FROM runs WHERE task_id=t.id) runs,
    (SELECT count(*)::int FROM acceptances a JOIN runs r ON r.id=a.run_id WHERE r.task_id=t.id) acceptances
    FROM supervision_cases c JOIN tasks t ON t.id=c.task_id WHERE c.id=$1`,[caseId])).rows[0];
  const before=await state();
  const request={caseId,evidenceIndex:8,digest:directory.source.digest,offset:0};
  let text='',offset=0,total=Infinity;
  while(offset<total){
   const page=await readManagerCaseEvidence(db,goal,{...request,offset});
   expect(page.status).toBe('OK');expect(page.text!.length).toBeGreaterThan(0);expect(page.text!.length).toBeLessThanOrEqual(12000);
   expect(page.arrayDigest).toBe(directory.source.arrayDigest);
   expect(page.entryDigest).toBe(digest(evidence[8]));text+=page.text!;offset=text.length;total=page.totalCharacters!;
  }
  expect(JSON.parse(text)).toEqual(evidence[8]);expect(sha(text)).toBe(digest(evidence[8]));
  expect((await readManagerCaseEvidence(db,randomUUID(),request)).status).toBe('NOT_FOUND');
  expect(await state()).toEqual(before);
  const originalRows=directory.rows;
  for(const field of ['producerRunId','producerManifestDigest','instruction','generation','code'] as const){
   const changed=structuredClone(evidence);
   if(field==='producerRunId'){
    changed[8].producerRunId=id(9999);changed[8].repairArtifacts.forEach((a:any)=>a.run_id=id(9999));
   }else if(field==='producerManifestDigest')changed[8].producerManifestDigest=sha('new manifest');
   else if(field==='generation')changed[8].generation=sha('new generation');
   else if(field==='code')changed[8].code=code==='COMPOSITION_PROOF_REQUIRED'?'CONSUMER_CONTRACT_CHANGED':'COMPOSITION_PROOF_REQUIRED';
   else changed[8].instruction+='changed';
   const fresh=projectManagerCaseProposals({cases:projectManagerCases([{...row,evidence:changed}]),operation:{}},0,1);
   if(!['generation','code'].includes(field))expect(fresh.snapshot.operation.caseEvidenceDirectory.cases[0].rows).toEqual(originalRows);
   expect(fresh.snapshot.operation.caseEvidenceDirectory.cases[0].source.digest).not.toBe(request.digest);
   await db.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[caseId,JSON.stringify(changed)]);
   expect((await readManagerCaseEvidence(db,goal,{...request,offset:12000})).status).toBe('STALE_CASE_EVIDENCE');
  }
 }finally{await db.query('ROLLBACK');db.release();}
});
