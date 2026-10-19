import {it,expect} from 'vitest';
import {selectRecoveryEvidence} from '../server/recovery-evidence-selection.js';
import {hash} from '../server/memory.js';
it('retains one identical recovery copy while preserving each original identity and changing no accepted artifact',()=>{
 const digest=hash('Exact recovery patch'),refs=Array.from({length:8},(_,i)=>({id:'recovery-'+i,
  name:'continuation-changes.patch',digest,size:2642400})),before=structuredClone(refs);
 const result=selectRecoveryEvidence(refs);
 expect(result.selected).toEqual([refs[0]]);
 expect(result.aliases.map(a=>a.id)).toEqual(refs.slice(1).map(a=>a.id));
 for(const alias of result.aliases) expect(alias).toMatchObject({canonicalId:refs[0].id,digest,originalPreserved:true});
 expect(refs).toEqual(before);
});
it('never deduplicates different names, changed bytes, incompatible sizes or unverified hashes',()=>{
 const ref={id:'one',name:'patch.diff',digest:hash('bytes'),size:10},refs=[ref,
  {...ref,id:'two',name:'other.diff'}, {...ref,id:'three',digest:hash('changed')},
  {...ref,id:'four',size:11}, {...ref,id:'five',digest:undefined}];
 expect(selectRecoveryEvidence(refs)).toEqual({selected:refs,aliases:[]});
});
