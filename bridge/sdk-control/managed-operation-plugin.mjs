import {defineTool} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-tools/lib/index.js';
import {executeManagedOperation} from '../managed-operation.mjs';
export const name='agentos-managed-operation';
export const inject=['tools','systemPrompt'];
export function apply(ctx){
 if(process.env.AGENTOS_MANAGED_OPERATIONS!=='1')return;
 ctx.tools.register(defineTool({name:'managed_operation',
  description:'Execute one bounded long operation inside this existing isolated workspace. Waits for the owned process without model polling, writes incremental logs and an operation receipt. Repeating an identical stage reuses its receipt; uncertain execution is never launched again. A process result is not task acceptance.',
  parameters:{
   stageKey:{type:'string',required:true,description:'Stable unique key for this stage and input version; reuse the same key only for the same operation.'},
   argv:{type:'array',required:true,items:{type:'string'},description:'Absolute executable followed by literal arguments. For scripts use node or python with the script path.'},
   cwd:{type:'string',description:'Working directory within /workspace; defaults to /workspace.'},
   inputFiles:{type:'array',items:{type:'string'},description:'Additional workspace input files, including indirect script dependencies, to hash before admission and receipt reuse. Direct file arguments are bound automatically; this does not hash the whole project.'},
   deadlineMs:{type:'number',required:true,description:'Explicit technical deadline for this operation, maximum 3600000 milliseconds. This does not limit the objective or token quota.'},
  },isConcurrencySafe:()=>false,
  execute:(args,exec)=>executeManagedOperation(args,{signal:exec.signal}),
  output:{schema:{type:'object',additionalProperties:true,properties:{
   operationId:{type:'string',required:true},outcome:{type:'string',required:true},
   physicalExitConfirmed:{type:'boolean',required:true},acceptance:{type:'boolean',required:true},
   verification:{type:'string',required:true},
  }},render:(_args,value)=>[{type:'text',text:JSON.stringify(value)}]},
 }));
 ctx.systemPrompt.section({name:'tool:managed_operation',order:ctx.systemPrompt.getSectionOrder('TOOL_BASH'),
  text:()=> 'Use managed_operation for a browser scenario, test suite, or other long command instead of launching a background process and polling it with sleep/cat/date. Choose a bounded stage and its technical deadline before launch. Bind indirect script dependencies with inputFiles; direct file arguments are hashed automatically. Read the returned exit status and bound log references. A negative or uncertain operation remains negative or uncertain: preserve its evidence and use the supervisor for a changed strategy. Do not rename an uncertain stage to launch it again. Do not claim that a command receipt proves final acceptance.'});
 ctx.provide('managedOperationToolReady',{registered:true});
}
