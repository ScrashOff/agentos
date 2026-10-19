import {constants} from 'node:fs';
import {open,realpath} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {readPlannerCatalog,type PlannerCatalogTransport} from './planner-catalog-transport.mjs';
import {scopedPlanContractVersion,scopedPlanJsonSchema,scopedPlanPreflight,scopedPlanningRejection} from '../server/scoped-plan-contract.js';

declare const __AGENTOS_SCOPED_PLAN_BINDING__: {
  version:1;requestId:string;scope:string;sourceRevision:string;catalogPath:string;catalogDigest:string;schemaDigest:string;transport?:PlannerCatalogTransport;
};
const binding=__AGENTOS_SCOPED_PLAN_BINDING__;
const hash=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
function fail(code:string):never{throw Object.assign(Error(code),{code});}
async function readBounded(filename:string,maximum:number){
  if(!path.isAbsolute(filename)||path.normalize(filename)!==filename)fail('PLANNING_PREFLIGHT_PATH');
  // Reject symlinks and special files; there is no service credential or API.
  if(await realpath(filename)!==filename)fail('PLANNING_PREFLIGHT_PATH');
  const handle=await open(filename,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
  try{
    const before=await handle.stat();
    if(!before.isFile()||before.size>maximum)fail('PLANNING_PREFLIGHT_FILE');
    const bytes=await handle.readFile(),after=await handle.stat();
    if(bytes.length>maximum||before.size!==after.size||before.mtimeMs!==after.mtimeMs||before.ctimeMs!==after.ctimeMs)
      fail('PLANNING_PREFLIGHT_FILE_CHANGED');
    return bytes;
  }finally{await handle.close();}
}
try{
  if(hash(Buffer.from(JSON.stringify(scopedPlanJsonSchema())))!==binding.schemaDigest)fail('PLANNING_PREFLIGHT_SCHEMA_CHANGED');
  const args=process.argv.slice(2);
  if(args.length===1&&args[0]==='--schema'){
    console.log(JSON.stringify({contractVersion:scopedPlanContractVersion,schema:scopedPlanJsonSchema(),liveAuthorization:false}));
  }else{
    if(args.length!==2||args[0]!=='--check')fail('PLANNING_PREFLIGHT_ARGUMENTS');
    let catalog:Buffer;
    try{catalog=await readPlannerCatalog({path:binding.catalogPath,digest:binding.catalogDigest,transport:binding.transport});}
    catch(error:any){
      if(error?.code==='CATALOG_DIGEST_MISMATCH')fail('PLANNING_PREFLIGHT_CATALOG_CHANGED');
      if(/^CATALOG_[A-Z_]{1,45}$/.test(error?.code??''))fail('PLANNING_PREFLIGHT_'+error.code);
      throw error;
    }
    const request={id:binding.requestId,scope:binding.scope,source_revision:binding.sourceRevision,snapshot:JSON.parse(catalog.toString('utf8'))};
    if(!Array.isArray(request.snapshot?.tasks)||!Array.isArray(request.snapshot?.artifacts))fail('PLANNING_PREFLIGHT_CATALOG_INVALID');
    const draft=await readBounded(path.resolve(args[1]),8*1024*1024);
    let result;
    try{result=scopedPlanPreflight(JSON.parse(draft.toString('utf8')),request);}
    catch(error){result={validAgainstFrozenCatalog:false,liveAuthorization:false,effects:'none',...scopedPlanningRejection(error)};}
    console.log(JSON.stringify({...result,catalogDigest:binding.catalogDigest,draftDigest:hash(draft)}));
    if(!result.validAgainstFrozenCatalog)process.exitCode=2;
  }
}catch(error:any){
  const code=/^PLANNING_PREFLIGHT_[A-Z_]{1,60}$/.test(error?.code??'')?error.code:'PLANNING_PREFLIGHT_UNAVAILABLE';
  console.log(JSON.stringify({validAgainstFrozenCatalog:false,liveAuthorization:false,effects:'none',code}));process.exitCode=2;
}
