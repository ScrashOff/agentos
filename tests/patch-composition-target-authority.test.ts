import {expect,it} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {patchComposition} from '../server/patch-composition.js';
import {bindCompositionInputs} from '../bridge/composition-inputs.mjs';

const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const digest=(v:any)=>createHash('sha256').update(JSON.stringify(canonical(v))).digest('hex');
const copy=<T>(v:T):T=>JSON.parse(JSON.stringify(v));
function seal(selection:any){const {digest:_old,...body}=selection;selection.digest=digest(body);}
function fixture(){
  const taskId=randomUUID(),goalId=randomUUID(),runId=randomUUID(),runFence=randomUUID(),
    targetRunId=randomUUID(),origin=randomUUID(),workspace='target-authority/o-consumer';
  const target={consumerTaskId:taskId,consumerRunId:targetRunId,workspace,candidateArtifactId:randomUUID(),
    candidateArtifactDigest:'a'.repeat(64),candidateDigest:'b'.repeat(64)};
  const dependencies:any[]=[],history:any[]=[],bindings:any[]=[],active:any[]=[],superseded:any[]=[];
  for(let i=0;i<6;i++){
    const oldTask=randomUUID(),oldRun=randomUUID(),producer=randomUUID(),producerRun=randomUUID();
    const ref=(task:string,run:string,name:string,tag:string)=>({id:randomUUID(),task_id:task,run_id:run,
      name,digest:createHash('sha256').update(tag).digest('hex'),base64:Buffer.from(tag).toString('base64')});
    const patch=ref(producer,producerRun,'implementation.patch','patch-'+i),report=ref(producer,producerRun,'repair-report.json','report-'+i),
      oldPatch=ref(oldTask,oldRun,'implementation.patch','old-patch-'+i),oldReport=ref(oldTask,oldRun,'repair-report.json','old-report-'+i);
    const selected=(p:any,r:any)=>({producerTaskId:p.task_id,producerRunId:p.run_id,patchArtifactId:p.id,
      patchDigest:p.digest,reportArtifactId:r.id,reportDigest:r.digest});
    dependencies.push(patch,report);history.push(oldPatch,oldReport);
    active.push(selected(patch,report));superseded.push(selected(oldPatch,oldReport));
    const obligationId=randomUUID(),eventId=String(100+i);
    const authority={version:1,obligationId,eventId,caseId:randomUUID(),decisionRunId:randomUUID(),
      consumerRunId:i===5?targetRunId:randomUUID(),consumerAttempt:i===5?10:i+1,
      revisionId:randomUUID(),revisionOriginId:origin,revisionNumber:i===5?20:i+1};
    bindings.push({obligationId,eventIds:[eventId],resolutionEventId:eventId,resolutionAuthority:authority,
      resolutionDigest:digest(authority),targetCandidate:i===5?copy(target):null,
      original:{taskId:oldTask,runId:oldRun,artifacts:[oldPatch,oldReport]},
      effectiveSource:{taskId:producer,runId:producerRun,artifacts:[patch,report]}});
  }
  const byObligation=(a:any,b:any)=>a.obligationId.localeCompare(b.obligationId);
  const targetAuthority={version:1,authority:'applied_consumer_revision',consumerRunId:targetRunId,
    consumerAttempt:10,revisionOriginId:origin,
    targetResolutionWitnesses:bindings.filter(b=>b.targetCandidate).map(b=>copy(b.resolutionAuthority)).sort(byObligation),
    historicalResolutionWitnesses:bindings.filter(b=>!b.targetCandidate).map(b=>copy(b.resolutionAuthority)).sort(byObligation)};
  const selection:any={version:1,authority:'supervisor-effective-source-manifest',consumerTaskId:taskId,
    consumerRunId:runId,consumerRunFence:runFence,workspace,target:copy(target),targetAuthority,
    active,superseded,resolutionEventIds:bindings.flatMap(b=>b.eventIds)};
  seal(selection);
  return {task:{id:taskId,goal_id:goalId,stage:'integrate',execution:{sourceWorkspace:'target-authority/source',workspace}},
    manifest:{runIdentity:{taskId,goalId,runId,runFence},dependencyArtifacts:dependencies,
      effectiveSources:{bindings,historicalArtifacts:history,compositionSelection:selection}},inputs:dependencies};
}
type Fixture=ReturnType<typeof fixture>;
const compose=(f:Fixture)=>patchComposition(f.task,f.manifest,f.inputs);
function refuse(f:Fixture){seal(f.manifest.effectiveSources.compositionSelection);
  expect(()=>compose(f)).toThrow('COMPOSITION_SELECTION_AUTHORITY_MISMATCH');}

it('uses one later applied target for six sources while preserving five targetless historical bindings',()=>{
  const f=fixture(),before=copy(f),plan:any=compose(f);
  expect(plan.version).toBe(2);expect(plan.selection.version).toBe(1);
  expect(plan.selection.targetAuthority).toEqual(f.manifest.effectiveSources.compositionSelection.targetAuthority);
  expect(plan.patches).toHaveLength(6);
  expect(plan.expectedCandidateDigest).toBe('b'.repeat(64));
  expect(f.manifest.effectiveSources.bindings.filter(b=>b.targetCandidate===null)).toHaveLength(5);
  expect(f).toEqual(before);
  const bound:any=bindCompositionInputs(plan,f.inputs);
  expect(bound.selection).toEqual(plan.selection);
  expect(bound.patches.map((p:any)=>p.patch.id).sort()).toEqual(f.inputs.filter(i=>i.name==='implementation.patch').map(i=>i.id).sort());
  expect(JSON.stringify(plan)).not.toContain('base64');
});

it('does not select the first binding arbitrarily when the explicit witness moves in the array',()=>{
  const f=fixture(),expected:any=compose(f),bindings=f.manifest.effectiveSources.bindings;
  bindings.unshift(bindings.pop()!);
  f.manifest.effectiveSources.compositionSelection.resolutionEventIds=bindings.flatMap(b=>b.eventIds);
  seal(f.manifest.effectiveSources.compositionSelection);
  const actual:any=compose(f);
  expect(actual.selection.targetAuthority).toEqual(expected.selection.targetAuthority);
  expect(actual.expectedCandidateDigest).toBe(expected.expectedCandidateDigest);
});

it.each(['eventId','caseId','decisionRunId','consumerRunId','revisionId','revisionOriginId','obligationId'] as const)
('refuses a resealed global witness whose %s differs from its frozen binding',field=>{
  const f=fixture(),a=f.manifest.effectiveSources.compositionSelection.targetAuthority;
  a.targetResolutionWitnesses[0][field]=randomUUID();refuse(f);
});

it.each(['consumerAttempt','revisionNumber'] as const)('refuses a forged numeric target witness %s',field=>{
  const f=fixture();f.manifest.effectiveSources.compositionSelection.targetAuthority.targetResolutionWitnesses[0][field]++;
  refuse(f);
});

it.each(['missing-history','missing-target','extra-target','missing-authority'] as const)
('requires a complete global authority rather than %s',mode=>{
  const f=fixture(),s=f.manifest.effectiveSources.compositionSelection;
  if(mode==='missing-history')s.targetAuthority.historicalResolutionWitnesses.pop();
  if(mode==='missing-target')s.targetAuthority.targetResolutionWitnesses=[];
  if(mode==='extra-target')s.targetAuthority.targetResolutionWitnesses.push(copy(s.targetAuthority.targetResolutionWitnesses[0]));
  if(mode==='missing-authority')delete s.targetAuthority;
  refuse(f);
});

it.each(['attempt-equal','attempt-newer','revision-equal','revision-newer','foreign-origin'] as const)
('does not reinterpret a targetless %s resolution as older authority',mode=>{
  const f=fixture(),binding=f.manifest.effectiveSources.bindings[0],a=binding.resolutionAuthority,
    target=f.manifest.effectiveSources.compositionSelection.targetAuthority;
  if(mode==='attempt-equal')a.consumerAttempt=10;
  if(mode==='attempt-newer')a.consumerAttempt=11;
  if(mode==='revision-equal')a.revisionNumber=20;
  if(mode==='revision-newer')a.revisionNumber=21;
  if(mode==='foreign-origin')a.revisionOriginId=randomUUID();
  const index=target.historicalResolutionWitnesses.findIndex((x:any)=>x.obligationId===binding.obligationId);
  target.historicalResolutionWitnesses[index]=copy(a);refuse(f);
});

it.each(['resolution-event','event-list','obligation'] as const)
('requires the claimed authority to bind the actual %s identity',mode=>{
  const f=fixture(),binding=f.manifest.effectiveSources.bindings[5];
  if(mode==='resolution-event')binding.resolutionEventId='9999';
  if(mode==='event-list')binding.eventIds=['9999'];
  if(mode==='obligation')binding.obligationId=randomUUID();
  f.manifest.effectiveSources.compositionSelection.resolutionEventIds=f.manifest.effectiveSources.bindings.flatMap(b=>b.eventIds);
  refuse(f);
});

it('refuses conflicting explicit targets even if the caller provides a resealed authority',()=>{
  const f=fixture(),s=f.manifest.effectiveSources.compositionSelection,b=f.manifest.effectiveSources.bindings[0];
  b.targetCandidate={...copy(s.target),candidateDigest:'c'.repeat(64)};
  b.resolutionAuthority={...b.resolutionAuthority,consumerRunId:s.target.consumerRunId,consumerAttempt:10,revisionNumber:21};
  s.targetAuthority.historicalResolutionWitnesses=s.targetAuthority.historicalResolutionWitnesses.filter((a:any)=>a.obligationId!==b.obligationId);
  s.targetAuthority.targetResolutionWitnesses.push(copy(b.resolutionAuthority));
  s.targetAuthority.targetResolutionWitnesses.sort((a:any,b:any)=>a.obligationId.localeCompare(b.obligationId));
  refuse(f);
});

it('allows agreeing explicit witnesses but compares history against their earliest revision',()=>{
  const f=fixture(),s=f.manifest.effectiveSources.compositionSelection,b=f.manifest.effectiveSources.bindings[0];
  b.targetCandidate=copy(s.target);
  b.resolutionAuthority={...b.resolutionAuthority,consumerRunId:s.target.consumerRunId,consumerAttempt:10,revisionNumber:30};
  s.targetAuthority.historicalResolutionWitnesses=s.targetAuthority.historicalResolutionWitnesses.filter((a:any)=>a.obligationId!==b.obligationId);
  s.targetAuthority.targetResolutionWitnesses.push(copy(b.resolutionAuthority));
  s.targetAuthority.targetResolutionWitnesses.sort((a:any,b:any)=>a.obligationId.localeCompare(b.obligationId));
  seal(s);expect(compose(f)?.patches).toHaveLength(6);
  const h=f.manifest.effectiveSources.bindings[1];h.resolutionAuthority.revisionNumber=25;
  const index=s.targetAuthority.historicalResolutionWitnesses.findIndex((a:any)=>a.obligationId===h.obligationId);
  s.targetAuthority.historicalResolutionWitnesses[index]=copy(h.resolutionAuthority);
  refuse(f);
});

it('preserves strict pre-existing manifests when every binding already has the same target',()=>{
  const f=fixture(),s=f.manifest.effectiveSources.compositionSelection;
  for(const b of f.manifest.effectiveSources.bindings){b.targetCandidate=copy(s.target);delete b.resolutionAuthority;}
  delete s.targetAuthority;seal(s);
  const plan:any=compose(f);expect(plan.version).toBe(2);expect(plan.patches).toHaveLength(6);
  expect(plan.selection).not.toHaveProperty('targetAuthority');
  f.manifest.effectiveSources.bindings[0].targetCandidate={...copy(s.target),candidateDigest:'c'.repeat(64)};
  expect(()=>compose(f)).toThrow('COMPOSITION_SELECTION_AUTHORITY_MISMATCH');
});

it('retains the exact active source set and remote input hash checks after adding global authority',()=>{
  const f=fixture(),s=f.manifest.effectiveSources.compositionSelection;
  s.active.pop();seal(s);expect(()=>compose(f)).toThrow('COMPOSITION_SELECTION_ACTIVE_SET');
  const intact=fixture(),plan=compose(intact),badInputs=copy(intact.inputs);badInputs[0].digest='f'.repeat(64);
  expect(()=>bindCompositionInputs(plan,badInputs)).toThrow('COMPOSITION_INPUT_BINDING');
});
