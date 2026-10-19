import {expect,it} from 'vitest';
import {analyticalPrompt,analyticalPromptCharacterLimit,fitManagerOptionalMemory,promptCheckpoint} from '../server/analytical-prompt.js';

it('counts the exact escaped payload and leaves immutable instruction and checkpoint fields intact',()=>{
 const task={agent_id:'manager',title:'Quoted "task"',description:'\\\n"'.repeat(30000),criteria:[{name:'result.json'}]};
 const run={checkpoint:{kind:'automatic_recovery',reason:'Original evidence',artifacts:[]}};
 const manifest={taskInstructions:{descriptionHash:'a'.repeat(64)},executionInputs:[]};
 const context={manifest,content:'"\n'.repeat(1000)};
 const expected=JSON.stringify({role:task.agent_id,task:task.title,description:task.description,
  instructionManifest:manifest.taskInstructions,executionInputs:[],criteria:task.criteria,
  checkpoint:run.checkpoint,context:context.content,dependencies:[]});
 expect(analyticalPrompt(run,task,context,[])).toBe(expected);
 expect(expected.length).toBeGreaterThan(task.description.length+context.content.length);
 expect(JSON.parse(expected).description).toBe(task.description);
});

it('excludes only optional overflow while retaining every mandatory byte and smaller ranked facts',()=>{
 const required={id:'required',required:true,content:'Immutable decision\\"\n'.repeat(100)};
 const large={id:'large',required:false,content:'"'.repeat(2000)};
 const small={id:'small',required:false,content:'Useful smaller diagnosis'};
 const original=structuredClone([required,large,small]);
 const measure=(selected:typeof original)=>analyticalPrompt({},
  {agent_id:'manager',title:'Task',description:'d'.repeat(174000),criteria:[]},
  {manifest:{},content:selected.map(m=>m.content).join('\n')},[]).length;
 expect(measure(original)).toBeGreaterThan(analyticalPromptCharacterLimit);
 const fit=fitManagerOptionalMemory(original,measure);
 expect(fit.selected).toEqual([required,small]);
 expect(fit.selected[0]).toBe(original[0]);
 expect(fit.omitted).toEqual([{id:'large',characters:large.content.length,reason:'serialized_transport_budget'}]);
 expect(fit.payloadCharacters).toBeLessThanOrEqual(180000);
 expect(original).toEqual([required,large,small]);
});

it('preserves required overflow for the existing hard refusal instead of truncating mandatory facts',()=>{
 const required={id:'required',required:true,content:'r'.repeat(180001)};
 const optional={id:'optional',required:false,content:'fact'};
 const fit=fitManagerOptionalMemory([required,optional],ms=>ms.reduce((n,m)=>n+m.content.length,0));
 expect(fit.requiredFits).toBe(false);
 expect(fit.selected).toEqual([required]);
 expect(fit.payloadCharacters).toBe(180001);
 expect(required.content).toHaveLength(180001);
});

it('does not change a fitting selection, order or migration checkpoint projection',()=>{
 const memories=[{id:'a',required:true,content:'a'},{id:'b',required:false,content:'b'}];
 expect(fitManagerOptionalMemory(memories,()=>180000)).toMatchObject({selected:memories,omitted:[],requiredFits:true});
 const cp={kind:'server_migration',source:'x',proof:{runId:'run',workspace:'ws',checkedAt:'time',process:{stopped:true},candidate:{candidateDigest:'candidate',baseDigest:'base'},extra:'not sent'}};
 expect(promptCheckpoint(cp)).toEqual({kind:'server_migration',source:'x',proof:{runId:'run',workspace:'ws',checkedAt:'time',process:{stopped:true},candidateDigest:'candidate',baseDigest:'base'}});
 expect(cp.proof.extra).toBe('not sent');
});
