// Test-only deterministic adapter. No HTTP, account, credential or provider call.
import assert from 'node:assert/strict';
import {LlmAdapter} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-llm/lib/index.js';
import {defineTool} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-tools/lib/index.js';
import {access} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
export const name='guidance-wait-driver-fixture';
export const inject=['llm','tools','guidanceWaitToolReady','agentosReadonlyFsReady'];
export function apply(ctx){
 const names=ctx.tools.schemas().map(t=>t.name);for(const n of ['bash','read','wait_for_guidance'])assert.ok(names.includes(n));for(const n of ['write','edit','read_image'])assert.ok(!names.includes(n));
 const tool=ctx.tools.get('wait_for_guidance');assert.equal(tool.isConcurrencySafe({}),false);
 ctx.tools.register(defineTool({name:'fixture_boundary_gate',description:'Test fixture holds the genuine tool batch boundary.',parameters:{},isConcurrencySafe:()=>false,
  output:{schema:{type:'boolean'},render:()=>[{type:'text',text:'fixture boundary released'}]},
  execute:async(_args,exec)=>{for(;;){exec.signal.throwIfAborted();try{await access('/workspace/release-boundary');return true;}catch(e){if(e.code!=='ENOENT')throw e;}await delay(5,undefined,{signal:exec.signal});}}
 }));
 class Scripted extends LlmAdapter {
  async resolveModel(provider,id){return {provider,id,name:id,context:{contextWindow:128000},defaultMaxTokens:1024};}
  step=0;
  async *stream(options){
   options.signal.throwIfAborted();
   const first=++this.step===1;
   if(!first){const text=JSON.stringify(options.messages);if(process.env.AGENTOS_WAIT_FIXTURE_MODE==='deadline')assert.ok(text.includes('deadline'));else{assert.ok(text.includes('WAIT_FIXTURE_VALIDATED_GUIDANCE'));assert.ok(text.includes('guidance_queued'));}}
   const blocks=first?[{type:'tool-call',id:'fixture_wait',name:'wait_for_guidance',arguments:'{}'},...(process.env.AGENTOS_WAIT_FIXTURE_MODE==='deadline'?[]:[{type:'tool-call',id:'fixture_gate',name:'fixture_boundary_gate',arguments:'{}'}])]:[{type:'text',text:'WAIT_FIXTURE_COMPLETED'}];
   for(const [index,block] of blocks.entries()){yield {type:'block-start',index,blockType:block.type};yield {type:'block-end',index,block};}yield {type:'finish',reason:{kind:first?'tool-calls':'stop'}};
  }
 }
 ctx.llm.registerAdapter(['guidance-wait-scripted-fixture'],new Scripted());
}
