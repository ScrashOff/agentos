import {createHash} from 'node:crypto';

const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'
  ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const digest=(value:any)=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const uuid=(value:unknown)=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const positive=(value:unknown)=>typeof value==='number'&&Number.isSafeInteger(value)&&value>0;
const fail=(code:string):never=>{throw Object.assign(Error(code),{code});};

/** Source substitution remains per obligation; the stopped workspace target is
 * global. This pure projection consumes authority already proved by the frozen
 * source validator. Missing historic targets never become invented assertions.
 * An explicit conflict is refused, never resolved by choosing a later record. */
export function compositionTargetAuthority(bindings:any[]){
  if(!Array.isArray(bindings)||!bindings.length)fail('COMPOSITION_TARGET_AUTHORITY_INVALID');
  const explicit=bindings.filter(binding=>binding.targetCandidate!==undefined&&binding.targetCandidate!==null);
  if(!explicit.length)return null;
  const target=explicit[0].targetCandidate;
  if(explicit.some(binding=>digest(binding.targetCandidate)!==digest(target)))fail('COMPOSITION_TARGET_CONFLICT');
  // Existing manifests with a target on every binding retain their exact rule.
  if(explicit.length===bindings.length)return {target,targetAuthority:undefined};
  const witness=(binding:any)=>{
    const a=binding.resolutionAuthority;
    if(!a||a.version!==1||!uuid(binding.obligationId)||a.obligationId!==binding.obligationId||
      typeof a.eventId!=='string'||!/^[1-9][0-9]{0,19}$/.test(a.eventId)||a.eventId!==binding.resolutionEventId||
      !Array.isArray(binding.eventIds)||binding.eventIds.at(-1)!==a.eventId||
      !uuid(a.caseId)||!uuid(a.decisionRunId)||!uuid(a.consumerRunId)||!positive(a.consumerAttempt)||
      !uuid(a.revisionId)||!uuid(a.revisionOriginId)||!positive(a.revisionNumber))fail('COMPOSITION_TARGET_AUTHORITY_INVALID');
    return {version:1,obligationId:a.obligationId,eventId:a.eventId,caseId:a.caseId,decisionRunId:a.decisionRunId,
      consumerRunId:a.consumerRunId,consumerAttempt:a.consumerAttempt,revisionId:a.revisionId,
      revisionOriginId:a.revisionOriginId,revisionNumber:a.revisionNumber};
  };
  const targetResolutionWitnesses=explicit.map(witness).sort((a,b)=>a.obligationId.localeCompare(b.obligationId));
  const historicalResolutionWitnesses=bindings.filter(binding=>binding.targetCandidate===undefined||binding.targetCandidate===null)
    .map(witness).sort((a,b)=>a.obligationId.localeCompare(b.obligationId));
  const owner=targetResolutionWitnesses[0],firstTargetRevision=Math.min(...targetResolutionWitnesses.map(a=>a.revisionNumber));
  if(!uuid(target?.consumerRunId)||owner.consumerRunId!==target.consumerRunId||
    new Set(bindings.map(binding=>binding.obligationId)).size!==bindings.length||
    targetResolutionWitnesses.some(a=>a.consumerRunId!==owner.consumerRunId||a.consumerAttempt!==owner.consumerAttempt||
      a.revisionOriginId!==owner.revisionOriginId)||
    historicalResolutionWitnesses.some(a=>a.revisionOriginId!==owner.revisionOriginId||
      a.consumerAttempt>=owner.consumerAttempt||a.revisionNumber>=firstTargetRevision))fail('COMPOSITION_TARGET_AUTHORITY_INVALID');
  return {target,targetAuthority:{version:1,authority:'applied_consumer_revision',consumerRunId:owner.consumerRunId,
    consumerAttempt:owner.consumerAttempt,revisionOriginId:owner.revisionOriginId,
    targetResolutionWitnesses,historicalResolutionWitnesses}};
}
