/** Recovery copies accumulate across retries. Exact same-name/hash copies need
 * one mounted representative, with every original identity recorded as an
 * alias. Different content, missing hashes and different names remain inputs. */
export function selectRecoveryEvidence(references:any[]) {
 const selected:any[]=[],aliases:any[]=[],known=new Map<string,any>();
 for(const ref of references) {
  const key=typeof ref.name==='string' && /^[0-9a-f]{64}$/.test(ref.digest ?? '')
   ? JSON.stringify([ref.name,ref.digest]) : null;
  const prior=key ? known.get(key) : null;
  if(prior && prior.id!==ref.id && (prior.size===undefined || ref.size===undefined || prior.size===ref.size)) {
   aliases.push({id:ref.id,name:ref.name,digest:ref.digest,canonicalId:prior.id,
    reason:'identical_recovery_content',originalPreserved:true});
  }else {
   selected.push(ref);
   if(key) known.set(key,ref);
  }
 }
 return {selected,aliases};
}
