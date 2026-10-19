import {defineTool} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-tools/lib/index.js';
export const name='agentos-guidance-wait';
export const inject=['tools','systemPrompt','guidanceWaitBroker'];
export function apply(ctx){
 const broker=ctx.get('guidanceWaitBroker');if(broker?.available!==true)return;
 ctx.tools.register(defineTool({name:'wait_for_guidance',
  description:'Wait for an actually queued supervisor guidance notification until the fixed runtime deadline. This tool grants no authority and does not consume guidance; the SDK delivers guidance on its next step.',
  parameters:{},isConcurrencySafe:()=>false,
  output:{schema:{type:'object',additionalProperties:false,properties:{status:{type:'string',enum:['guidance_queued','deadline','runtime_closed'],required:true},actionId:{type:'string'},messageId:{type:'string'}}},render:(_args,value)=>[{type:'text',text:JSON.stringify(value)}]},
  execute:(_args,exec)=>broker.wait(exec.agent,exec.signal)
 }));
 ctx.systemPrompt.section({name:'tool:wait_for_guidance',order:ctx.systemPrompt.getSectionOrder('TOOL_READ'),text:()=> 'When the task requires live supervisor guidance, call wait_for_guidance instead of repeated shell sleeps. guidance_queued only means the validated guidance is pending; read the actual next-step input before acting. deadline is a bounded wait outcome, not task acceptance or a request to cancel.'});
 ctx.provide('guidanceWaitToolReady',{registered:true});
}
