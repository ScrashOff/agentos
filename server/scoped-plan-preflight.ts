import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import type {PlannerCatalogTransport} from '../scripts/planner-catalog-transport.mjs';
import {scopedPlanContractGuide,scopedPlanJsonSchema} from './scoped-plan-contract.js';

let bundle:Promise<string>|undefined;
/** A release process has immutable sources. Compile once, without writing build
 * output; the sandbox receives a standalone file with no build dependencies. */
async function standaloneBundle(){
  bundle??=import('vite').then(({build})=>build({configFile:false,envFile:false,publicDir:false,logLevel:'silent',
    ssr:{noExternal:['zod']},
    build:{ssr:fileURLToPath(new URL('../scripts/planner-preflight-cli.ts',import.meta.url)),
      write:false,copyPublicDir:false,target:'node22',minify:'esbuild',sourcemap:false,
      rollupOptions:{external:/^node:/,output:{format:'es',entryFileNames:'planning-preflight.mjs',inlineDynamicImports:true}}}}))
    .then(result=>{
      const output=Array.isArray(result)?result[0]:result;
      const files='output' in output?output.output:[];
      const chunk=files.length===1&&files[0].type==='chunk'?files[0]:undefined;
      if(chunk?.imports.some(specifier=>!specifier.startsWith('node:')))throw Error('PLANNING_PREFLIGHT_BUNDLE');
      const text=chunk?.code;
      if(!text||Buffer.byteLength(text)>4*1024*1024)throw Error('PLANNING_PREFLIGHT_BUNDLE');return text;
    }).catch(error=>{bundle=undefined;throw error;});
  return bundle;
}
function input(name:string,bytes:Buffer,requestId:string){
  const digest=createHash('sha256').update(bytes).digest('hex');
  const identifier=createHash('sha256').update('scoped-plan-preflight:'+requestId+':'+name+':'+digest).digest('hex').slice(0,32);
  const variant=(8|(Number.parseInt(identifier[16],16)&3)).toString(16);
  const id=`${identifier.slice(0,8)}-${identifier.slice(8,12)}-5${identifier.slice(13,16)}-${variant}${identifier.slice(17,20)}-${identifier.slice(20)}`;
  return {id,name,digest,base64:bytes.toString('base64')};
}
export async function createScopedPlannerPreflightInputs(request:{id:string;scope:string;source_revision:string},
  catalogDigest:string,inputRoot='/home/agent/inputs',transport?:PlannerCatalogTransport){
  if(!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(request.id)||
    !/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(request.scope)||!/^[a-f0-9]{64}$/.test(request.source_revision)||
    !/^[a-f0-9]{64}$/.test(catalogDigest)||!path.isAbsolute(inputRoot))throw Error('PLANNING_PREFLIGHT_BINDING');
  const schema=input('scope-plan.schema.json',Buffer.from(JSON.stringify(scopedPlanJsonSchema())),request.id);
  const binding={version:1,requestId:request.id,scope:request.scope,sourceRevision:request.source_revision,
    catalogPath:path.join(inputRoot,request.id,'planning-catalog.json'),catalogDigest,schemaDigest:schema.digest,...(transport?{transport}:{})};
  const cli=input('planning-preflight.mjs',Buffer.from(`const __AGENTOS_SCOPED_PLAN_BINDING__=${JSON.stringify(binding)};\n${await standaloneBundle()}`),request.id);
  const cliPath=path.join(inputRoot,cli.id,cli.name),schemaPath=path.join(inputRoot,schema.id,schema.name);
  return {inputs:[cli,schema],guide:{...scopedPlanContractGuide(),catalogDigest,...(transport?{catalogTransport:transport}:{}),
    cliPath,cliDigest:cli.digest,schemaPath,schemaDigest:schema.digest,
    commands:{schema:['node',cliPath,'--schema'],check:['node',cliPath,'--check','/workspace/evidence/scope-plan.json']},
    instruction:'Run the check in this session before delivering. Exit 2 returns safe field errors; correct the same draft and check again. A valid preflight permits publishing the draft only; it never accepts tasks, changes dependencies or authorizes an action. Preserve source evidence and all valid proposals.'}};
}
