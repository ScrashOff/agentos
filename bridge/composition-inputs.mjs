import {createHash} from 'node:crypto';

const canonical=value=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'
  ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const digest=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');

/** Keep the RPC frame bounded: composition carries references, never a second
 * copy of the accepted input bytes. No name-only or hash-only fallback. */
export function bindCompositionInputs(plan,inputs=[]){
  if(![1,2].includes(plan?.version) || !Array.isArray(plan.patches) || plan.patches.length>128)
    throw Object.assign(Error('COMPOSITION_FORMAT'),{code:'COMPOSITION_FORMAT'});
  if(plan.version===2){
    const selection=plan.selection,target=selection?.target;
    const fail=code=>{throw Object.assign(Error(code),{code});};
    const {digest:selectionDigest,...selectionBody}=selection??{};
    if(!selection||!target||!selection.consumerTaskId||!selection.consumerRunId||!selection.consumerRunFence||
      !/^[a-f0-9]{64}$/.test(selection.digest??'')||digest(selectionBody)!==selectionDigest||!target.consumerRunId||!target.workspace||
      !target.candidateArtifactId||!/^[a-f0-9]{64}$/.test(target.candidateArtifactDigest??'')||
      !/^[a-f0-9]{64}$/.test(target.candidateDigest??'')||plan.expectedCandidateDigest!==target.candidateDigest||
      !Array.isArray(selection.active)||!Array.isArray(selection.superseded))fail('COMPOSITION_SELECTION_INVALID');
    const active=new Set(selection.active.map(x=>x.producerTaskId));
    const superseded=new Set(selection.superseded.map(x=>x.producerTaskId));
    if(active.size!==selection.active.length||superseded.size!==selection.superseded.length||
      [...active].some(id=>superseded.has(id)))fail('COMPOSITION_SELECTION_AMBIGUOUS');
    const selected=new Set(selection.active.map(x=>[x.producerTaskId,x.patchArtifactId,x.patchDigest,x.reportArtifactId,x.reportDigest].join(':')));
    const actual=new Set(plan.patches.map(x=>[x.producerTaskId,x.patch?.id,x.patch?.digest,x.report?.id,x.report?.digest].join(':')));
    if(selected.size!==actual.size||[...selected].some(key=>!actual.has(key)))fail('COMPOSITION_SELECTION_ACTIVE_SET');
  }
  const bind=ref=>{
    const matches=inputs.filter(i=>i.id===ref?.id);
    if(matches.length!==1 || matches[0].digest!==ref.digest || matches[0].name!==ref.name)
      throw Object.assign(Error('COMPOSITION_INPUT_BINDING'),{code:'COMPOSITION_INPUT_BINDING'});
    return matches[0];
  };
  return {...plan,patches:plan.patches.map(p=>({...p,patch:bind(p.patch),report:bind(p.report)}))};
}
