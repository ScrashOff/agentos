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
