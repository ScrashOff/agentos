import {createHash} from 'node:crypto';
import {expect,it} from 'vitest';
import {compactSharedManagerContext,expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {projectManagerReview,projectPersistedManagerDescription,managerContextVersion,managerContextGuide} from '../server/manager-context.js';

const sha=(text:string)=>createHash('sha256').update(text).digest('hex');
const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const characters=(value:any)=>JSON.stringify(JSON.stringify(value)).length;
const collisionStrings=['^0','^1','^00','^01','^-1','^+1','^1.5','^1e2','^9007199254740992','^','^text','^^0','^0\n'];

function fixture(){
  const obligations=Array.from({length:64},(_,n)=>({obligationId:id(100+n),generation:sha('generation-'+n),
    handoffId:id(200+n),producerTaskId:id(300+n),producerRunId:id(400+n),producerManifestDigest:sha('manifest-'+n),
    repairArtifacts:[{id:id(500+n),name:'implementation.patch',digest:sha('patch-'+n),size:1400+n},
      {id:id(600+n),name:'repair-report.json',digest:sha('report-'+n),size:2200+n}]}));
  return {cases:[{id:id(2),taskId:id(3),state:'open',currentRun:{id:id(4),attempt:17,literal:'^0'},
    source_run_id:id(4),unknownFutureField:{literal:'^1',marker:{$v:0},protectedPadding:''},
    materialConsumptionReview:{version:1,authority:'review_only',goalId:id(1),caseId:id(2),consumerTaskId:id(3),
      consumerRunId:id(4),consumerAttempt:17,identityDigest:sha(JSON.stringify(obligations)),
      actionValidationRequired:true,obligations},
    evidence:obligations.map(row=>({kind:'DELIVERY_CONSUMPTION_REQUIRED',code:'CONSUMER_INPUTS_NOT_BOUND',
      ...row,consumerTaskId:id(3),consumerRunId:id(4),instruction:'Preserve every frozen criterion, current source and physical stop proof.'}))}],
    caseWindow:{selectedCaseIds:[id(2)],literal:'^0'},
    operation:{focus:{caseId:id(2),taskId:id(3),scope:'project/current',literal:'^0',marker:{$v:1}},
      sample:id(3),literalStrings:collisionStrings,literalMarkers:[{$v:0},{$r:[0,'^1']},{nested:{$v:999}}],
      tasks:[{id:id(3),state:'recovery_blocked',stage:'integrate',expectedAttempt:17,
        dependencies:obligations.map(row=>row.producerTaskId),criteria:[
          {id:'candidate',type:'artifact',name:'verification.json'},
          {id:'independent-qa',type:'artifact',name:'qa.json'}],
        intent:{text:'Consume only verified inputs and preserve independent QA of the same candidate.'}}],
      workOpportunities:{acceptedSources:obligations.map(row=>({taskId:row.producerTaskId,runId:row.producerRunId,
        expectedAttempt:1,generation:row.generation,manifestDigest:row.producerManifestDigest,inputs:row.repairArtifacts}))},
      recoveredContext:{results:Array.from({length:4},(_,n)=>({status:'OK',request:{kind:'artifact',
        taskId:id(300+n),expectedAttempt:1,name:'repair-report.json',offset:0},runId:id(400+n),artifactId:id(600+n),
        digest:sha('page-'+n),text:('Exact requested page '+n+'; ').repeat(9),totalCharacters:1900,nextOffset:256,truncated:true}))}},
    evidenceTexts:[{hash:sha('^0'),text:'^0',originals:[{$v:0},'^1']}],inlineMemoryHashes:['^0']};
}
function packed(){const source=fixture(),value=compactSharedManagerContext(source);
  expect(value.sharedContext.shortValueReferences).toBe(true);return {source,value};}

// A legacy representation of exactly the same snapshot, not a second codec:
// inline encoded nodes and replace short references only in encoded fields.
// Literal values and protected fields are copied unchanged, never re-decoded.
function legacy(value:any){
  const result=structuredClone(value);
  const nodes=result.sharedContext.nodeReferences===true?result.sharedContext.nodes:[];
  const visit=(item:any,limit=nodes.length):any=>{
    if(result.sharedContext.nodeReferences===true && item && !Array.isArray(item) && typeof item==='object' &&
      Object.keys(item).length===1 && Object.prototype.hasOwnProperty.call(item,'$n')){
      if(!Number.isSafeInteger(item.$n)||item.$n<0||item.$n>=limit)throw Error('INVALID_FIXTURE_NODE_REFERENCE');
      return visit(nodes[item.$n],item.$n);
    }
    if(typeof item==='string'&&/^\^(0|[1-9][0-9]*)$/.test(item))return {$v:Number(item.slice(1))};
    if(Array.isArray(item))return item.map(value=>visit(value,limit));
    return item&&typeof item==='object'?Object.fromEntries(Object.entries(item).map(([key,v])=>[key,visit(v,limit)])):item;
  };
  for(const [key,item]of Object.entries(result)){
    if(key==='sharedContext'||['evidenceTexts','inlineMemoryHashes','caseWindow'].includes(key))continue;
    if(key==='cases')result.cases=result.cases.map((entry:any)=>({...entry,evidence:visit(entry.evidence),
      materialConsumptionReview:visit(entry.materialConsumptionReview)}));
    else if(key==='operation')result.operation=Object.fromEntries(Object.entries(item as any).map(([name,v])=>[name,name==='focus'?v:visit(v)]));
    else result[key]=visit(item);
  }
  delete result.sharedContext.shortValueReferences;
  delete result.sharedContext.nodeReferences;
  delete result.sharedContext.nodes;
  return result;
}

it('reconstructs the material review and every required contract exactly, preserving all literal boundaries',()=>{
  const {source,value}=packed(),before=JSON.stringify(source),restored=expandSharedManagerContext(value);
  expect(value.operation.sample).toMatch(/^\^[0-9]+$/);
  expect(value.sharedContext.guide).toContain('shortValueReferences=true');
  expect(JSON.stringify(restored)).toBe(before);expect(value.sharedContext.sourceCharacters).toBe(before.length);
  expect(value.sharedContext.sourceSha256).toBe(sha(before));expect(JSON.stringify(source)).toBe(before);
  for(const key of ['id','taskId','state','currentRun','source_run_id','unknownFutureField'])
    expect(value.cases[0][key]).toEqual((source.cases[0] as any)[key]);
  expect(value.operation.focus).toEqual(source.operation.focus);expect(value.caseWindow).toEqual(source.caseWindow);
  expect(value.evidenceTexts).toEqual(source.evidenceTexts);expect(value.inlineMemoryHashes).toEqual(source.inlineMemoryHashes);
  expect(restored.cases[0].materialConsumptionReview.authority).toBe('review_only');
  expect(restored.cases[0].materialConsumptionReview.actionValidationRequired).toBe(true);
  expect(restored.operation.tasks).toEqual(source.operation.tasks);
  expect(restored.operation.recoveredContext.results).toHaveLength(4);
});

it('escapes canonical and malformed literal prefixes without double decoding or mutation',()=>{
  const {source,value}=packed(),original=JSON.stringify(value);
  expect(value.operation.literalStrings.every((entry:any)=>typeof entry==='string'&&/^\^[0-9]+$/.test(entry))).toBe(true);
  const restored=expandSharedManagerContext(value);
  expect(restored.operation.literalStrings).toEqual(collisionStrings);
  expect(restored.operation.literalMarkers).toEqual(source.operation.literalMarkers);
  expect(expandSharedManagerContext(restored)).toEqual(source);
  expect(compactSharedManagerContext(value)).toEqual(value);
  expect(compactSharedManagerContext(restored)).toEqual(value);
  restored.operation.literalMarkers[0].$v=37;
  expect(JSON.stringify(value)).toBe(original);
  expect(expandSharedManagerContext(value).operation.literalMarkers).toEqual(source.operation.literalMarkers);
});

it('retains legacy V1 object references and permits mixed representations with an explicit flag',()=>{
  const {source,value}=packed(),old=legacy(value);
  expect(old.sharedContext.shortValueReferences).toBeUndefined();
  expect(old.sharedContext.nodeReferences).toBeUndefined();expect(old.sharedContext.nodes).toBeUndefined();
  expect(expandSharedManagerContext(old)).toEqual(source);
  const index=Number(value.operation.sample.slice(1));value.operation.sample={$v:index};
  expect(expandSharedManagerContext(value)).toEqual(source);
  // Legacy strings were never references, including a canonical-looking one.
  const legacySource={cases:[],operation:{text:'^0',literal:'^bad',nested:{$v:5}}};
  const text=JSON.stringify(legacySource);
  const envelope={cases:[],operation:{text:'^0',literal:'^bad',nested:{$v:0}},sharedContext:{
    format:'MANAGER_SHARED_CONTEXT_V1',schemas:[],values:[{$v:5}],sourceCharacters:text.length,sourceSha256:sha(text)}};
  expect(expandSharedManagerContext(envelope)).toEqual(legacySource);
});

it('converts an encoded DAG to actual legacy fields without decoding literal table entries or protected markers',()=>{
  const source:any=fixture();
  source.operation.originalNodeMarkers=[{$n:0},{$n:999},{$n:1,ordinary:true}];
  source.operation.focus.originalNodeMarker={$n:999};
  const value=compactSharedManagerContext(source),before=JSON.stringify(value),old=legacy(value);
  expect(value.sharedContext.nodeReferences).toBe(true);expect(value.sharedContext.nodes.length).toBeGreaterThan(0);
  expect(old.sharedContext.nodes).toBeUndefined();expect(old.sharedContext.nodeReferences).toBeUndefined();
  expect(old.sharedContext.shortValueReferences).toBeUndefined();
  expect(old.sharedContext.values).toEqual(value.sharedContext.values);
  expect(old.operation.focus).toEqual(source.operation.focus);
  expect(JSON.stringify(expandSharedManagerContext(old))).toBe(JSON.stringify(source));
  expect(expandSharedManagerContext(old).operation.originalNodeMarkers).toEqual(source.operation.originalNodeMarkers);
  expect(JSON.stringify(value)).toBe(before);
});

it.each([false,'true',1,null,{}])('rejects a malformed short-reference flag before reading authority: %s',flag=>{
  const {value}=packed();value.sharedContext.shortValueReferences=flag;
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_INVALID');
});
it('cannot silently read a short-reference snapshot if the flag is removed',()=>{
  const {value}=packed();delete value.sharedContext.shortValueReferences;
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_DIGEST');
});
it.each(['^999999','^9007199254740992','^-1','^+1','^1.5','^1e2','^00','^01','^','^word','^0\n'])
('rejects a corrupted reference without interpreting it as literal evidence: %s',reference=>{
  const {value}=packed();value.operation.sample=reference;
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_REFERENCE');
});
it.each(['table','hash','characters'])('keeps whole-snapshot integrity mandatory: %s',kind=>{
  const {value}=packed();
  if(kind==='table')value.sharedContext.values[Number(value.operation.sample.slice(1))]='changed source';
  if(kind==='hash')value.sharedContext.sourceSha256='0'.repeat(64);
  if(kind==='characters')value.sharedContext.sourceCharacters++;
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_DIGEST');
});
it('charges repeated literal table expansion against the same hard limit',()=>{
  const {value}=packed();value.sharedContext.values=['x'.repeat(1_000_000)];
  value.operation.tasks=Array.from({length:9},()=>'^0');
  // Remove earlier encoded paths so this exercises expansion, not a stale index.
  value.cases=[];value.operation={tasks:value.operation.tasks};
  delete value.sharedContext.nodes;delete value.sharedContext.nodeReferences;
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_EXPANSION');
});

it('provides a material budget margin without dropping evidence, four recovered pages or consumer requirements',()=>{
  const source=fixture(),first=compactSharedManagerContext(source),old=legacy(first);
  expect(expandSharedManagerContext(old)).toEqual(source);
  expect(characters(old)-characters(first)).toBeGreaterThan(1500);
  // Only a protected, unique literal padding changes; it cannot be shared.
  // Put the exact same review on opposite sides of the fixed 150k limit.
  source.cases[0].unknownFutureField.protectedPadding='x'.repeat(150000-Math.ceil((characters(first)+characters(old))/2));
  const final=compactSharedManagerContext(source),legacyFinal=legacy(final),before=JSON.stringify(source);
  expect(characters(final)).toBeLessThan(150000);expect(characters(legacyFinal)).toBeGreaterThan(150000);
  expect(JSON.stringify(expandSharedManagerContext(final))).toBe(before);
  const review=projectManagerReview(source,'',0,150000);
  expect(review.withinBudget).toBe(true);expect(JSON.stringify(review.description).length).toBeLessThanOrEqual(150000);
  const restored=expandSharedManagerContext(JSON.parse(review.description));
  expect(restored.cases).toEqual(source.cases);expect(restored.operation.tasks).toEqual(source.operation.tasks);
  expect(restored.operation.recoveredContext).toEqual(source.operation.recoveredContext);
  expect(JSON.stringify(source)).toBe(before);
});
it('reprojects a persisted description while retaining short-reference literals and exact focus',()=>{
  const {source,value}=packed(),prefix='MANAGEMENT_V1\n'+managerContextVersion+'\n'+managerContextGuide+'\n';
  const description=projectPersistedManagerDescription(prefix+JSON.stringify(value));
  const restored=expandSharedManagerContext(JSON.parse(description.slice(prefix.length)));
  expect(restored.operation.focus).toEqual(source.operation.focus);
  expect(restored.operation.literalStrings).toEqual(collisionStrings);
  expect(restored.cases).toEqual(source.cases);
});
