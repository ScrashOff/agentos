import {mkdir,writeFile,realpath,rename,readFile,lstat} from 'node:fs/promises';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {tx,type DB} from './db.js';
import {config} from './config.js';
import {DomainError} from './contracts.js';
import {inputBundleRoot,inputBundleDigest,inputBundleValueDigest,referencedInputCountLimit,referencedInputLimitBytes} from '../bridge/artifact-input-bundle.mjs';
import {executionInputManifest} from './execution-inputs.js';

/** Only the current frozen attempt can publish a private materialization
 * manifest. No task, case, action or acceptance is modified. */
async function owner(db:DB,run:any,task:any,context:any){
 const r=(await db.query(`SELECT r.fence,r.state,r.task_id,r.context_id,r.instruction_snapshot,r.attempt,t.goal_id,t.attempt_count
  FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1 FOR SHARE OF r,t`,[run.id])).rows[0];
 if(!r||r.fence!==run.fence||r.task_id!==task.id||r.goal_id!==task.goal_id||r.state!=='running'||r.attempt!==r.attempt_count||
  r.context_id!==context.id||inputBundleValueDigest(r.instruction_snapshot)!==inputBundleValueDigest(run.instruction_snapshot))
  throw new DomainError('INPUT_BUNDLE_OWNER_CHANGED','A tentativa mudou antes da preparação dos insumos.');
 const c=(await db.query('SELECT manifest FROM contexts WHERE id=$1 AND run_id=$2',[context.id,run.id])).rows[0];
 if(!c||inputBundleValueDigest(c.manifest)!==inputBundleValueDigest(context.manifest))
  throw new DomainError('INPUT_BUNDLE_OWNER_CHANGED','O contexto mudou antes da preparação dos insumos.');
}

export async function stageArtifactInputBundle(run:any,task:any,context:any,inputs:any[],roots?:{bundleRoot:string}){
 const root=roots?.bundleRoot??inputBundleRoot;
 if(!roots&&config.dataDir!=='/var/lib/agentos')throw new DomainError('INPUT_BUNDLE_STORE_UNAVAILABLE','Armazenamento referenciado não configurado neste host.');
 if(!inputs.length||inputs.length>referencedInputCountLimit)throw new DomainError('INPUT_BUNDLE_CAPACITY','Quantidade de insumos excede a materialização.');
 const declared=executionInputManifest(task.execution?.inputs??[]);
 const refs=[...(context.manifest.dependencyArtifacts??[]),...(context.manifest.recoveryArtifacts??[]),...(context.manifest.instructionArtifacts??[])];
 const descriptors=inputs.map(i=>{
  const known=refs.find(a=>a.id===i.id&&a.name===i.name&&a.digest===i.digest),inline=declared.find(a=>a.id===i.id&&a.name===i.name&&a.digest===i.digest);
  if(!known&&!inline)throw new DomainError('INPUT_BUNDLE_INVALID','Insumo não pertence ao manifesto da tentativa.');
  return {id:i.id,name:i.name,digest:i.digest,size:Buffer.byteLength(i.base64,'base64'),kind:known?'artifact':'inline',...(i.mime?{mime:i.mime}:{})};
 });
 if(descriptors.reduce((n,i)=>n+i.size,0)>referencedInputLimitBytes)throw new DomainError('INPUT_BUNDLE_CAPACITY','Insumos excedem a capacidade de materialização.');
 const body=Buffer.from(JSON.stringify({version:1,runId:run.id,fence:run.fence,taskId:task.id,goalId:task.goal_id,
  contextDigest:inputBundleValueDigest(context.manifest),inputs:descriptors}));
 if(body.length>1024*1024)throw new DomainError('INPUT_BUNDLE_CAPACITY','Manifesto de materialização excede a capacidade.');
 const digest=inputBundleDigest(body),dir=path.join(root,run.id+'-'+run.fence);
 await mkdir(root,{recursive:true,mode:0o700});if(await realpath(root)!==root)throw new DomainError('INPUT_BUNDLE_INVALID','Armazenamento inválido.');
 await tx(async db=>{
  await owner(db,run,task,context);
  const existing=await lstat(dir).catch(e=>{if(e.code==='ENOENT')return null;throw e;});
  if(existing){
   if(!existing.isDirectory()||existing.isSymbolicLink()||await realpath(dir)!==dir||
    !body.equals(await readFile(path.join(dir,'manifest.json'))))throw new DomainError('INPUT_BUNDLE_INVALID','Manifesto existente diverge da tentativa.');
   return;
  }
  const staging=dir+'.stage-'+randomUUID();await mkdir(staging,{mode:0o700});await mkdir(staging+'/inline',{mode:0o700});
  for(const i of descriptors.filter(i=>i.kind==='inline')){
   const source=inputs.find(a=>a.id===i.id)!;
   await writeFile(staging+'/inline/'+i.id,Buffer.from(source.base64,'base64'),{flag:'wx',mode:0o600});
  }
  await writeFile(staging+'/manifest.json',body,{flag:'wx',mode:0o600});
  await rename(staging,dir);
 });
 return {version:1,runId:run.id,fence:run.fence,digest};
}
