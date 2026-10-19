import {createHash} from 'node:crypto';
import {expect,it} from 'vitest';
import {compactSharedManagerContext,expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {managerContextVersion,projectManagerReview} from '../server/manager-context.js';

const sha=(value:string)=>createHash('sha256').update(value).digest('hex');
const id=(value:number)=>`00000000-0000-4000-8000-${String(value).padStart(12,'0')}`;
const characters=(value:unknown)=>JSON.stringify(JSON.stringify(value)).length;
const copy=<T>(value:T):T=>structuredClone(value);

function fixture(){
  const obligations=Array.from({length:16},(_,i)=>({obligationId:id(100+i),generation:sha('generation-'+i),
    handoffId:id(200+i),producerTaskId:id(300+i),producerRunId:id(400+i),producerManifestDigest:sha('manifest-'+i),
    consumerTaskId:id(3),consumerRunId:id(4),consumerAttempt:17,
    repairArtifacts:[{id:id(500+i),name:'implementation.patch',digest:sha('patch-'+i),size:1400+i},
      {id:id(600+i),name:'repair-report.json',digest:sha('report-'+i),size:2200+i}]}));
  const material={version:1,authority:'review_only',code:'MATERIAL_DECISION_REQUIRED',goalId:id(1),
    caseId:id(2),consumerTaskId:id(3),consumerRunId:id(4),consumerAttempt:17,scope:'project/current',
    identityDigest:sha(JSON.stringify(obligations)),actionValidationRequired:true,obligations,
    allowedAction:{kind:'revise',sourceTaskId:id(300),diagnosis:'Review exact current evidence before proposing a material revision.'}};
  return {cases:[{id:id(2),taskId:id(3),state:'open',source_run_id:id(4),currentRun:{id:id(4),attempt:17,literal:{$n:0}},
    futureProtectedField:{literal:{$n:1},text:'^0',padding:''},materialConsumptionReview:copy(material),
    evidence:[{kind:'DELIVERY_CONSUMPTION_REQUIRED',consumerTaskId:id(3),consumerRunId:id(4),code:'CONSUMER_INPUTS_NOT_BOUND'}]}],
    evidenceTexts:[{hash:sha('literal'),text:'^0',originals:[{$n:0},{$v:0},{$r:[0]}]}],
    inlineMemoryHashes:['^0'],caseWindow:{selectedCaseIds:[id(2)],literal:{$n:0}},
    operation:{focus:{caseId:id(2),taskId:id(3),scope:'project/current',literal:{$n:0},text:'^0'},
      requiredDecision:copy(material),previous:{receipts:[{code:'MATERIAL_DECISION_REQUIRED',requiredDecision:copy(material)}],
        followThrough:{status:'decision_pending',requiredDecision:copy(material)}},
      tasks:[{id:id(3),expectedAttempt:17,state:'recovery_blocked',stage:'integrate',dependencies:obligations.map(x=>x.producerTaskId),
        criteria:[{id:'candidate',type:'artifact',name:'verification.json'},{id:'qa',type:'artifact',name:'independent-qa.json'}],
        executionInputs:obligations.flatMap(x=>x.repairArtifacts),intent:{text:'Preserve every criterion and independent QA of the same candidate.'}}],
      recoveredContext:{results:Array.from({length:4},(_,i)=>({status:'OK',request:{kind:'artifact',taskId:id(300+i),expectedAttempt:1,offset:0},
        runId:id(400+i),artifactId:id(600+i),digest:sha('page-'+i),text:('Exact requested page '+i+'; ').repeat(12),
        totalCharacters:1900,nextOffset:256,truncated:true}))},
      literalMarkers:[{$n:0},{$n:-1},{$n:'0'},{$n:0,ordinary:true},{$v:0},{$r:[0,'^1']},{nested:{$n:99}}],
      literalStrings:['^0','^N','^00','^-1','^','^^0']}};
}

function packed(){
  const source=fixture(),value=compactSharedManagerContext(source);
  expect(value.sharedContext?.nodeReferences).toBe(true);
  expect(Array.isArray(value.sharedContext.nodes)).toBe(true);
  expect(value.sharedContext.nodes.length).toBeGreaterThan(0);
  return {source,value};
}

function envelope(source:any,nodes:any[],encoded:any={payload:{$n:0}}){
  const original=JSON.stringify(source);
  return {cases:[],operation:encoded,sharedContext:{format:'MANAGER_SHARED_CONTEXT_V1',nodeReferences:true,
    nodes,schemas:[] as string[][],values:[] as any[],sourceCharacters:original.length,sourceSha256:sha(original)}};
}

it('shares a repeated material decision without changing any evidence, identity, criterion or recovered page',()=>{
  const {source,value}=packed(),before=JSON.stringify(source),restored=expandSharedManagerContext(value);
  expect(managerContextVersion).toBe('MANAGER_CONTEXT_V21');
  expect(JSON.stringify(restored)).toBe(before);
  expect(value.sharedContext.sourceCharacters).toBe(before.length);
  expect(value.sharedContext.sourceSha256).toBe(sha(before));
  expect(characters(value)).toBeLessThan(characters(source)-5000);
  expect(restored.operation.requiredDecision).toEqual(restored.cases[0].materialConsumptionReview);
  expect(restored.operation.previous.receipts[0].requiredDecision).toEqual(source.operation.requiredDecision);
  expect(restored.operation.previous.followThrough.requiredDecision).toEqual(source.operation.requiredDecision);
  expect(restored.operation.tasks).toEqual(source.operation.tasks);
  expect(restored.operation.recoveredContext.results).toEqual(source.operation.recoveredContext.results);
  expect(restored.operation.requiredDecision.authority).toBe('review_only');
  expect(restored.operation.requiredDecision.actionValidationRequired).toBe(true);
  expect(JSON.stringify(source)).toBe(before);
});

it('keeps protected root fields and the ordinary case/focus envelope literal',()=>{
  const {source,value}=packed();
  for(const key of ['evidenceTexts','inlineMemoryHashes','caseWindow'] as const)expect(value[key]).toEqual(source[key]);
  for(const key of ['id','taskId','state','source_run_id','currentRun','futureProtectedField'] as const)
    expect(value.cases[0][key]).toEqual(source.cases[0][key]);
  expect(value.operation.focus).toEqual(source.operation.focus);
  expect(Array.isArray(value.cases)).toBe(true);
  expect(Object.keys(value.cases[0])).toEqual(Object.keys(source.cases[0]));
});

it('escapes literal node/value/row markers and caret strings without interpreting the copied data twice',()=>{
  const {source,value}=packed(),before=JSON.stringify(value),restored=expandSharedManagerContext(value);
  expect(restored.operation.literalMarkers).toEqual(source.operation.literalMarkers);
  expect(restored.operation.literalStrings).toEqual(source.operation.literalStrings);
  expect(expandSharedManagerContext(restored)).toEqual(source);
  expect(compactSharedManagerContext(value)).toEqual(value);
  expect(compactSharedManagerContext(restored)).toEqual(value);
  restored.operation.requiredDecision.obligations[0].consumerAttempt=999;
  expect(restored.cases[0].materialConsumptionReview.obligations[0].consumerAttempt).toBe(17);
  expect(JSON.stringify(value)).toBe(before);
  expect(expandSharedManagerContext(value)).toEqual(source);
});

it('decodes a topological node through row/value references while values remain literal',()=>{
  const leaf={identity:id(3),literal:{$n:999}},parent={pair:[copy(leaf),copy(leaf)]};
  const source={cases:[],operation:{payload:parent}};
  const value=envelope(source,[{$r:[0,id(3),{$v:0}]},{pair:[{$n:0},{$n:0}]}],{payload:{$n:1}});
  value.sharedContext.schemas=[['identity','literal']];value.sharedContext.values=[{$n:999}];
  const restored=expandSharedManagerContext(value);
  expect(JSON.stringify(restored)).toBe(JSON.stringify(source));
  restored.operation.payload.pair[0].literal.$n=1;
  expect(restored.operation.payload.pair[1].literal.$n).toBe(999);
  expect(expandSharedManagerContext(value)).toEqual(source);
});

it('keeps node-shaped data literal in legacy V1 envelopes without the node flag',()=>{
  const source={cases:[],operation:{node:{$n:0},short:'^0',oldValue:{$n:123},oldRow:{identity:id(3)}}};
  const text=JSON.stringify(source);
  const value={cases:[],operation:{node:{$n:0},short:'^0',oldValue:{$v:0},oldRow:{$r:[0,id(3)]}},
    sharedContext:{format:'MANAGER_SHARED_CONTEXT_V1',schemas:[['identity']],values:[{$n:123}],
      sourceCharacters:text.length,sourceSha256:sha(text)}};
  expect(expandSharedManagerContext(value)).toEqual(source);
});

it('fits the same complete material review below 150k while its repeated literal form exceeds the budget',()=>{
  const source=fixture(),first=compactSharedManagerContext(source),gain=characters(source)-characters(first);
  expect(first.sharedContext?.nodeReferences).toBe(true);expect(gain).toBeGreaterThan(5000);
  const padding=150000-Math.ceil((characters(source)+characters(first))/2);
  expect(padding).toBeGreaterThan(0);source.cases[0].futureProtectedField.padding='x'.repeat(padding);
  const before=JSON.stringify(source),encoded=compactSharedManagerContext(source);
  expect(characters(source)).toBeGreaterThan(150000);
  expect(characters(encoded)).toBeLessThan(150000);
  expect(encoded.sharedContext?.nodeReferences).toBe(true);
  expect(JSON.stringify(expandSharedManagerContext(encoded))).toBe(before);
});

it('tries lossless sharing before evicting complete retained pages that still fit alongside the current decision',()=>{
  const source:any=fixture();
  source.operation.retainedEvidence={version:2,characterLimit:24000,omitted:0,fullSourcesPreserved:true,
    entries:source.operation.recoveredContext.results.map((row:any)=>({
      request:row.request,runId:row.runId,artifactId:row.artifactId,digest:sha(row.text),
      totalCharacters:row.text.length,pages:[{offset:0,text:row.text}],complete:true,
      sourceVerified:true,focus:{caseId:source.cases[0].id,taskId:source.operation.focus.taskId,sourceRunId:id(4)}}))};
  const before=copy(source),prefix='Keep exact source authority and independent QA.\n';
  const roomy=projectManagerReview(source,prefix,0,1_000_000);
  const expected=expandSharedManagerContext(JSON.parse(roomy.description.slice(prefix.length)));
  const maximum=30000;
  expect(JSON.stringify(roomy.description).length).toBeGreaterThan(maximum);
  const review=projectManagerReview(source,prefix,0,maximum);
  const encoded=JSON.parse(review.description.slice(prefix.length));
  const result=expandSharedManagerContext(encoded);
  expect(review.withinBudget).toBe(true);
  expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(maximum);
  expect(encoded.sharedContext?.nodeReferences).toBe(true);
  expect(result.operation.retainedEvidence).toEqual(before.operation.retainedEvidence);
  expect(result.operation.retainedEvidence.entries).toHaveLength(4);
  expect(result.operation.retainedEvidence.omittedEntries).toBeUndefined();
  expect(result.operation.recoveredContext).toEqual(before.operation.recoveredContext);
  expect(result.operation.focus).toEqual(before.operation.focus);
  expect(result.operation.requiredDecision).toEqual(before.operation.requiredDecision);
  expect(result.operation.previous.receipts).toEqual(before.operation.previous.receipts);
  expect(result.operation.previous.followThrough).toEqual(before.operation.previous.followThrough);
  expect(result.operation.tasks).toEqual(expected.operation.tasks);
  expect(result.operation.focusedTaskDirectory).toEqual(expected.operation.focusedTaskDirectory);
  expect(result.cases).toEqual(expected.cases);
  expect(source).toEqual(before);
});

it.each([false,'true',1,null,{}])('rejects a malformed node flag without accepting partial authority: %s',flag=>{
  const {value}=packed();value.sharedContext.nodeReferences=flag;
  expect(()=>expandSharedManagerContext(value)).toThrow(/MANAGER_SHARED_CONTEXT_/);
});

it.each(['removed-flag','non-array','negative','fraction','string','out-of-range','self','forward','cycle','node-tamper','hash-tamper'])
('rejects an invalid or tampered node graph: %s',kind=>{
  const source={cases:[],operation:{payload:{identity:id(3)}}};
  const value:any=envelope(source,[{identity:id(3)}]);
  if(kind==='removed-flag')delete value.sharedContext.nodeReferences;
  if(kind==='non-array')value.sharedContext.nodes={0:{identity:id(3)}};
  if(kind==='negative')value.operation.payload={$n:-1};
  if(kind==='fraction')value.operation.payload={$n:0.5};
  if(kind==='string')value.operation.payload={$n:'0'};
  if(kind==='out-of-range')value.operation.payload={$n:1};
  if(kind==='self')value.sharedContext.nodes=[{child:{$n:0}}];
  if(kind==='forward')value.sharedContext.nodes=[{child:{$n:1}},{identity:id(3)}];
  if(kind==='cycle')value.sharedContext.nodes=[{child:{$n:1}},{child:{$n:0}}];
  if(kind==='node-tamper')value.sharedContext.nodes[0].identity=id(9);
  if(kind==='hash-tamper')value.sharedContext.sourceSha256='0'.repeat(64);
  expect(()=>expandSharedManagerContext(value)).toThrow(/MANAGER_SHARED_CONTEXT_/);
});

it('charges repeated node expansion against the same 8MB output limit',()=>{
  const source={cases:[],operation:{payload:null}};
  const value=envelope(source,[{text:'x'.repeat(1_000_000)}],{payload:Array.from({length:9},()=>({$n:0}))});
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_EXPANSION');
});

it('charges repeated object keys against the 8MB limit even when all leaves are tiny',()=>{
  const node={['k'.repeat(1_000_000)]:0};
  const value=envelope({cases:[],operation:{payload:null}},[node],{payload:Array.from({length:9},()=>({$n:0}))});
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_EXPANSION');
});

it('rejects small topological tables whose expanded traversal exceeds 500k nodes',()=>{
  const nodes:any[]=[{value:0}];
  for(let i=1;i<20;i++)nodes.push([{$n:i-1},{$n:i-1}]);
  const value=envelope({cases:[],operation:{payload:null}},nodes,{payload:{$n:nodes.length-1}});
  expect(JSON.stringify(value).length).toBeLessThan(10000);
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_EXPANSION');
});

it('retains the existing 8MB source and dictionary boundaries',()=>{
  const source={cases:[],operation:{text:'x'.repeat(8_000_001)}};
  expect(compactSharedManagerContext(source)).toBe(source);
  const value=envelope({cases:[],operation:{payload:null}},[{value:0}]);
  value.sharedContext.sourceCharacters=8_000_001;
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_INVALID');
  value.sharedContext.sourceCharacters=1;value.sharedContext.values=['x'.repeat(8_000_001)];
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_INVALID');
});
