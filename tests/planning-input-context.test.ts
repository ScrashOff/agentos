import {it,expect} from 'vitest';
import {createHash} from 'node:crypto';
import {planningInputContext} from '../server/planning-input-context.js';
const input=(id:string,content:string|Buffer,requirementIds=['req-4'])=>{
  const bytes=Buffer.from(content);
  return {id,name:id+'.txt',digest:createHash('sha256').update(bytes).digest('hex'),base64:bytes.toString('base64'),requirementIds};
};
it('includes whole relevant source guidance with byte identity and isolates unrelated requirements',()=>{
  const source=input('source','Actual callers are bridge/runtime-runner.mjs and server/planner.ts.');
  const result=planningInputContext([input('unrelated','Other task',['req-2']),source],['req-4']);
  expect(result).toMatchObject({permissionChanges:false,inventoryComplete:true,contentComplete:true});
  expect(result.inputs).toEqual([{id:source.id,name:source.name,digest:source.digest,size:Buffer.byteLength(Buffer.from(source.base64,'base64')),
    requirementIds:['req-4'],inputPath:'/home/agent/inputs/source/source.txt',content:Buffer.from(source.base64,'base64').toString()}]);
});
it('prioritizes newer complete inputs and accounts for every omission without slicing programs',()=>{
  const result=planningInputContext([input('old','x'.repeat(6000)),input('mid','y'.repeat(6000)),input('new','z'.repeat(6000)),input('large','q'.repeat(6001))],['req-4']);
  expect(result.contentCharacters).toBe(12000);expect(result.contentComplete).toBe(false);
  expect(result.inputs.map(i=>({id:i.id,omitted:'omitted' in i?i.omitted:undefined}))).toEqual([
    {id:'large',omitted:'per-input-budget'},{id:'new',omitted:undefined},{id:'mid',omitted:undefined},{id:'old',omitted:'total-budget'}]);
  for(const item of result.inputs.filter(i=>'omitted' in i))expect(item).not.toHaveProperty('content');
});
it('does not decode binary or invalid UTF8 into apparent source evidence',()=>{
  const result=planningInputContext([input('binary','abc\0def'),input('invalid',Buffer.from([0xff,0xfe]))],['req-4']);
  expect(result.inputs.map(i=>'omitted' in i?i.omitted:null)).toEqual(['non-utf8','binary']);
});
it('counts unicode as complete text and does not mutate the verified inputs',()=>{
  const inputs=[input('unicode','Coordenação e publicação ✓')],before=structuredClone(inputs);
  expect(planningInputContext(inputs,['req-4']).inputs[0]).toHaveProperty('content','Coordenação e publicação ✓');
  expect(inputs).toEqual(before);expect(planningInputContext(inputs,['req-9']).inputs).toEqual([]);
});
it('refuses modified bytes before they can be presented to the model',()=>{
  const forged={...input('source','original'),base64:Buffer.from('forged').toString('base64')};
  expect(()=>planningInputContext([forged],['req-4'])).toThrow('verified digest');
});
