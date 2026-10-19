import {expect,it} from 'vitest';
import {compactSharedManagerContext,expandSharedManagerContext} from '../server/manager-context-sharing.js';

function packed(){
  const instruction='Preserve source identities, current authority and independent acceptance. '.repeat(4);
  const value=compactSharedManagerContext({cases:[],operation:{tasks:Array.from({length:70},(_,i)=>({
    id:'task-'+i,sourceRunId:'11111111-1111-4111-8111-111111111111',state:'waiting_supervision',instruction,
    receipt:{code:'SUPERVISION_REQUESTED',explanation:instruction,sourceDigest:'a'.repeat(64)}}))}});
  expect(value.sharedContext).toBeDefined();return value;
}
function reference(value:any,key:'$v'|'$r'):any{
  if(!value || typeof value!=='object')return undefined;
  if(!Array.isArray(value)&&Object.keys(value).length===1&&Object.hasOwn(value,key))return value;
  for(const item of Object.values(value)){const found=reference(item,key);if(found)return found;}
  return undefined;
}

it.each(['value-type','schema-type','schemas-limit','values-limit'])
('rejects malformed shared dictionary %s before treating it as evidence',kind=>{
  const value=packed(),r=reference(value.operation.tasks,'$r');
  expect(r).toBeDefined();
  // Both legacy object references and new short references use the same index
  // validation. Inject the malformed legacy form without assuming encoder style.
  if(kind==='value-type')value.operation.tasks[0]={$v:'0'};
  if(kind==='schema-type')value.sharedContext.schemas[r.$r[0]][0]=1;
  if(kind==='schemas-limit')value.sharedContext.schemas=Array.from({length:100001},()=>[]);
  if(kind==='values-limit')value.sharedContext.values=Array.from({length:100001},()=>null);
  expect(()=>expandSharedManagerContext(value)).toThrow(/MANAGER_SHARED_CONTEXT_/);
});

it('rejects excessive recursive encoded depth before reconstructing unbounded data',()=>{
  const value=packed();let nested:any='leaf';for(let i=0;i<140;i++)nested={nested};
  value.operation.tasks=nested;
  expect(()=>expandSharedManagerContext(value)).toThrow('MANAGER_SHARED_CONTEXT_EXPANSION');
});
