// Private, read-only bounded snapshot for an active actor. No signals or RPC.
import {readdir,realpath} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
import {readProgressFile,sameProgressFile} from './runtime-progress.mjs';
import {STAGE_POLICY,stagePath,stageTrace,waitObservation,partialReportObservation} from './runtime-stage-observation.mjs';
const DATA='/var/lib/agentos-bridge/worker-runs',ROOT='/srv/agentos/workspaces';
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
async function reports(root){
 const selected=[];let visited=0,truncated=false;
 async function walk(relative,depth){
  if(depth>3||visited>=128){truncated=true;return;}
  if(await realpath(root+'/'+relative)!==root+'/'+relative)throw Error('UNSAFE_STAGE_PATH');
  const entries=await readdir(root+'/'+relative,{withFileTypes:true});
  entries.sort((a,b)=>a.name.localeCompare(b.name));
  for(const entry of entries){
   if(++visited>128){truncated=true;break;}
   const name=relative+'/'+entry.name;if(!stagePath(name))continue;
   if(entry.isDirectory())await walk(name,depth+1);
   else if(entry.isFile()&&/(?:^|\/)\w[\w.-]*report\.json$/.test(name))selected.push(name);
  }
 }
 try{await walk('evidence',0);}catch(error){
  if(error.code!=='ENOENT')throw error;
  // A worker may not have published an evidence directory yet. This must not
  // discard a separately validated observation of a log under data/.
  if(selected.length)truncated=true;
 }
 return {paths:selected.slice(0,32),truncated:truncated||selected.length>32};
}
export async function inspectRuntimeStage(runId,workspace,io={}){
 if(!uuid.test(runId??'')||!/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(workspace??''))throw Error('INVALID_STAGE_IDENTITY');
 const read=io.readFile??readProgressFile,dir=DATA+'/'+runId,root=ROOT+'/'+workspace;
 const result={version:1,policy:STAGE_POLICY,authority:'review_only',runId,workspace,runFence:null,
  checkedAt:new Date().toISOString(),available:false,observations:[]};
 try{
  const stateFile=await read(dir+'/state.json',DATA,256*1024),state=JSON.parse(stateFile.text);
  if(state.runId!==runId||state.workspace!==workspace||state.runtime!=='harness'||state.state!=='running'||!uuid.test(state.runFence??''))return {...result,reason:'RUN_IDENTITY_CHANGED'};
  result.runFence=state.runFence;
  const startedAt=Date.parse(state.at);if(!Number.isFinite(startedAt))return {...result,reason:'RUN_START_UNAVAILABLE'};
  const trace=await read(dir+'/home/sessions/--workspace--/'+runId+'/session.v3.jsonl',dir,16*1024*1024);
  const window=stageTrace(trace.text);if(!window)return {...result,reason:'TRACE_INCOMPLETE'};
  const observations=[];
  if(window.completed.length>=4){
   const p=window.completed[0].path;
   const file=await read(root+'/'+p,root,4*1024*1024);
   const observation=waitObservation(window,file);
   if(observation&&sameProgressFile(file,await read(root+'/'+p,root,4*1024*1024)))observations.push(observation);
  }
  const listing=await(io.listReports??reports)(root);
  for(const p of listing.paths){
   if(!stagePath(p)||observations.length>=8)break;
   try{
    const file=await read(root+'/'+p,root,1024*1024),observation=partialReportObservation(p,file,startedAt);
    if(observation&&sameProgressFile(file,await read(root+'/'+p,root,1024*1024)))observations.push(observation);
   }catch{/* A partial, changing or unsafe file is not evidence. */}
  }
  if(!sameProgressFile(stateFile,await read(dir+'/state.json',DATA,256*1024)))return {...result,reason:'RUN_IDENTITY_CHANGED'};
  // A tool action after sampling may resolve the incident: discard this sample.
  const after=await read(dir+'/home/sessions/--workspace--/'+runId+'/session.v3.jsonl',dir,16*1024*1024);
  if(trace.digest!==after.digest)return {...result,reason:'TRACE_CHANGED'};
  return {...result,checkedAt:new Date().toISOString(),available:true,
   trace:{digest:trace.digest,size:trace.size,lastSequence:window.lastSequence},observations,
   truncated:listing.truncated||observations.length>=8};
 }catch{return {...result,reason:'STAGE_OBSERVATION_UNAVAILABLE'};}
}
// Realpath supports both immutable releases and the current symlink.
if(process.argv[1]&&import.meta.url===pathToFileURL(await realpath(process.argv[1])).href){
 try{console.log(JSON.stringify(await inspectRuntimeStage(process.argv[2],process.argv[3])));}
 catch{console.log(JSON.stringify({error:'INVALID_STAGE_IDENTITY'}));process.exitCode=1;}
}
