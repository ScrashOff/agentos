import {apply as upstreamApply,Config} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-tool-fs/lib/index.js';
import {FsError} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-fs/lib/index.js';
import path from 'node:path';
import {realpath} from 'node:fs/promises';
export {Config};
export const name='agentos-readonly-fs';
export const inject=['tools','fs','systemPrompt','sandboxPolicy'];
const inside=(root,p)=>p===root||p.startsWith(root+path.sep);
export function apply(ctx,config){
 let registered=0;const seen=[];
 const tools=new Proxy(ctx.tools,{get(target,key){
  if(key==='register')return definition=>{
   if(!['read','write','edit'].includes(definition.name)||seen.includes(definition.name))throw Error('READONLY_FS_UNEXPECTED_DEFINITION');
   seen.push(definition.name);if(definition.name!=='read')return ()=>{};
   registered++;return target.register({...definition,async execute(args,exec){
    const root=await realpath(process.cwd());
    const cwd=exec.agent?.session.header.cwd??root;
    const requested=path.resolve(cwd,args.file_path);
    if(!inside(root,requested))throw new FsError('read path outside workspace','FS_SANDBOX_DENIED');
    // Provider canonicalizes existing ancestors, including a missing suffix.
    const resolved=await ctx.fs.resolve(requested,{cwd:root,signal:exec.signal});
    if(!inside(root,resolved.targetKey))throw new FsError('read target outside workspace','FS_SANDBOX_DENIED');
    return definition.execute({...args,file_path:resolved.targetKey},exec);
   }});
  };const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
 }});
 const prompt=new Proxy(ctx.systemPrompt,{get(target,key){if(key==='section')return section=>{if(!['tool:read','tool:write','tool:edit'].includes(section.name))throw Error('READONLY_FS_UNEXPECTED_SECTION');if(section.name==='tool:read')return target.section(section);return ()=>{};};const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;}});
 const facade=new Proxy(ctx,{get(target,key){if(key==='tools')return tools;if(key==='systemPrompt')return prompt;if(key==='inject')return dependencies=>{if(dependencies.length!==1||dependencies[0]!=='attachments')throw Error('READONLY_FS_UNEXPECTED_INJECTION');return ()=>{};};const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;}});
 upstreamApply(facade,config);
 if(registered!==1||seen.join(',')!=='read,write,edit')throw Error('READONLY_FS_CONTRACT_CHANGED');
 ctx.provide('agentosReadonlyFsReady',{readRegistered:true});
}
