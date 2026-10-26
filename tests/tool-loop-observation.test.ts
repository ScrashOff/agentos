import {expect,it} from 'vitest';
import {toolLoopObservation} from '../server/tool-loop-observation.js';
const action=(i:number,command='read missing-input.txt',output='not found')=>({id:'call-'+i,sequence:i,tool:'bash',command,output,
  state:'returned',exitCode:0,startedAt:new Date(1700000000000+i*1000).toISOString(),finishedAt:new Date(1700000000001+i*1000).toISOString()});
const observation=(n=6)=>({actions:Array.from({length:n},(_,i)=>action(i)),files:[],evidence:[]});
it('records a bounded hash-only suspicion for identical completed calls without stop authority',()=>{
  const value=toolLoopObservation(observation());
  expect(value).toMatchObject({pattern:'identical_returns',completedCalls:6,firstSequence:0,lastSequence:5});
  expect(value?.fingerprints).toHaveLength(1);
  expect(JSON.stringify(value)).not.toContain('missing-input');expect(value).not.toHaveProperty('stop');
});
it('detects alternating calls only with repeated matching inputs and results',()=>{
  const input={actions:Array.from({length:12},(_,i)=>action(i,i%2?'ls unavailable':'cat unavailable',i%2?'empty':'missing'))};
  expect(toolLoopObservation(input)).toMatchObject({pattern:'alternating_returns',completedCalls:12});
});
it('keeps changing CI status and distinct investigation reads inconclusive',()=>{
  expect(toolLoopObservation({actions:Array.from({length:12},(_,i)=>action(i,'ci status','step '+i))})).toBeNull();
  expect(toolLoopObservation({actions:Array.from({length:12},(_,i)=>action(i,'read file-'+i,'contents'))})).toBeNull();
});
it('never flags one long compilation, overlapping tools or an unfinished call',()=>{
  const input=observation();input.actions.at(-1)!.state='running';expect(toolLoopObservation(input)).toBeNull();
  expect(toolLoopObservation({actions:[{...action(0),finishedAt:new Date(1700007200000).toISOString()}]})).toBeNull();
  const overlapping=observation();overlapping.actions[1].startedAt=overlapping.actions[0].startedAt;
  expect(toolLoopObservation(overlapping)).toBeNull();
});
it.each(['unavailable','traceTruncated'])('ignores %s observations',key=>{
  expect(toolLoopObservation({...observation(),[key]:true})).toBeNull();
});
it('does not compare redacted, omitted, malformed or partial evidence as exact repeated bytes',()=>{
  for(const change of [{command:'[Comando envolvendo credenciais omitido]'},{output:'[... trecho omitido ...]'},
    {sequence:0},{partial:true},{finishedAt:'invalid'}]){
    const input=observation();Object.assign(input.actions[5],change);expect(toolLoopObservation(input)).toBeNull();
  }
});
it('keeps presence and time outside the material fingerprint, but notices new file evidence',()=>{
  const first=toolLoopObservation(observation())!;
  const alive=toolLoopObservation({...observation(),observedAt:new Date(1700009999999).toISOString(),process:{cpuNs:1000000}})!;
  expect(alive.materialDigest).toBe(first.materialDigest);
  const changed=toolLoopObservation({...observation(),evidence:[{name:'test-result.json',size:42,modifiedAt:'later'}]})!;
  expect(changed.materialDigest).not.toBe(first.materialDigest);
  expect(changed.evidenceDigest).toBe(first.evidenceDigest);
});
it('rejects malformed projections and future completions rather than throwing or inventing progress',()=>{
  for(const extra of [{files:'invalid'},{evidence:[null]},{observedAt:'invalid'},{observedAt:new Date(1600000000000).toISOString()}])
    expect(toolLoopObservation({...observation(),...extra})).toBeNull();
});
const runtimeObservation=(count=12,code='ERR_MODULE_NOT_FOUND')=>({actions:Array.from({length:count},(_,i)=>
  action(i,'node attempt-'+i+'.ts',`Error [${code}]: Cannot load /workspace/module-${i}.ts\nstack line ${i}`)),files:[],evidence:[]});
it('observes the same recurring runtime error across different commands without asserting failure',()=>{
 const value=toolLoopObservation(runtimeObservation());
 expect(value).toMatchObject({pattern:'recurring_runtime_error',completedCalls:12,runtimeErrors:[{code:'ERR_MODULE_NOT_FOUND',count:12}]});
 expect(value?.fingerprints).toHaveLength(12);expect(value).not.toHaveProperty('stop');
 expect(JSON.stringify(value)).not.toContain('/workspace');
});
it.each(['ERR_UNKNOWN_FILE_EXTENSION','ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX'])('recognizes bounded %s diagnostics',code=>{
 expect(toolLoopObservation(runtimeObservation(6,code))).toMatchObject({pattern:'recurring_runtime_error',runtimeErrors:[{code,count:6}]});
});
it('coalesces diagnostic identity despite changing paths, commands, stacks and occurrence counts',()=>{
 const first=toolLoopObservation(runtimeObservation())!;
 const second=runtimeObservation(8);second.actions=second.actions.map((a,i)=>({...a,sequence:i+20,command:'run alternate-'+i,output:a.output+'\nnew stack'}));
 const later=toolLoopObservation(second)!;
 expect(later.evidenceDigest).toBe(first.evidenceDigest);expect(later.materialDigest).toBe(first.materialDigest);
 const identical=runtimeObservation(6);identical.actions=identical.actions.map(a=>({...a,command:'node same.ts',output:'Error [ERR_MODULE_NOT_FOUND]: same missing module'}));
 expect(toolLoopObservation(identical)!.evidenceDigest).toBe(first.evidenceDigest);
 expect(toolLoopObservation(runtimeObservation(12,'ERR_UNKNOWN_FILE_EXTENSION'))!.evidenceDigest).not.toBe(first.evidenceDigest);
});
it('does not escalate fewer than six diagnostics, mere documentation mentions or resolved recent calls',()=>{
 const sparse=runtimeObservation();sparse.actions=sparse.actions.map((a,i)=>({...a,output:i<5?a.output:'bounded check '+i}));
 expect(toolLoopObservation(sparse)).toBeNull();
 const mentions=runtimeObservation();mentions.actions=mentions.actions.map((a,i)=>({...a,output:'documented ERR_MODULE_NOT_FOUND case '+i}));
 expect(toolLoopObservation(mentions)).toBeNull();
 const resolved=runtimeObservation();resolved.actions=resolved.actions.map((a,i)=>({...a,output:i<9?a.output:'new successful check '+i}));
 expect(toolLoopObservation(resolved)).toBeNull();
});
it('retains partial/redacted trace protections even when runtime diagnostics recur',()=>{
 expect(toolLoopObservation({...runtimeObservation(),traceTruncated:true})).toBeNull();
 const partial=runtimeObservation();Object.assign(partial.actions[4],{partial:true});expect(toolLoopObservation(partial)).toBeNull();
 const redacted=runtimeObservation();redacted.actions[4].output='[... trecho omitido ...]';expect(toolLoopObservation(redacted)).toBeNull();
});
