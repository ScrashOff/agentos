import {constants} from 'node:fs';
import {open,realpath,stat} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import path from 'node:path';

export const referencedInputLimitBytes=128*1024*1024;
export const referencedInputFileLimitBytes=8*1024*1024;
export const referencedInputCountLimit=4096;
export const inputBundleRoot='/var/lib/agentos/input-bundles';
const artifactRoot='/var/lib/agentos/artifacts';
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const hex=/^[a-f0-9]{64}$/;
const name=/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/;
export const inputBundleDigest=b=>createHash('sha256').update(b).digest('hex');
const canonical=v=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?
 Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
export const inputBundleValueDigest=v=>inputBundleDigest(JSON.stringify(canonical(v)));
const fail=code=>{throw Object.assign(Error(code),{code});};
const exact=(o,allowed)=>o&&typeof o==='object'&&!Array.isArray(o)&&Object.keys(o).every(k=>allowed.includes(k));

async function safeFile(p,max,privateFile=false){
 const fd=await open(p,constants.O_RDONLY|constants.O_NOFOLLOW);
 try{
  const s=await fd.stat();
  if(!s.isFile()||s.size>max||(privateFile&&((s.mode&0o077)!==0||s.uid!==process.getuid?.())))fail('INPUT_BUNDLE_INVALID');
  const b=await fd.readFile();if(b.length!==s.size||b.length>max)fail('INPUT_BUNDLE_INVALID');
  return b;
 }finally{await fd.close();}
}

/** Root-owned, controller-created manifest. Only UUID-addressed artifact files
 * can be loaded. The request carries no arbitrary filesystem paths or URLs.
 * Trusted root overrides are for isolated tests, never taken from params. */
export async function hydrateArtifactInputBundle(params,signal,roots={}){
 try{return await hydrate(params,signal,roots);}
 catch(e){if(signal.aborted||/^INPUT_BUNDLE_[A-Z_]+$/.test(e?.code??''))throw e;fail('INPUT_BUNDLE_INVALID');}
}
async function hydrate(params,signal,roots){
 if(!params.inputBundle)return params;
 signal.throwIfAborted();
 const ref=params.inputBundle;
 if(!exact(ref,['version','runId','fence','digest'])||ref.version!==1||
  !uuid.test(ref.runId??'')||!uuid.test(ref.fence??'')||!hex.test(ref.digest??'')||
  ref.runId!==params.runId||ref.fence!==params.runFence||
  (params.inputs!==undefined&&(!Array.isArray(params.inputs)||params.inputs.length!==0)))fail('INPUT_BUNDLE_INVALID');
 const root=roots.bundleRoot??inputBundleRoot,store=roots.artifactRoot??artifactRoot;
 if(await realpath(root)!==root||await realpath(store)!==store)fail('INPUT_BUNDLE_INVALID');
 const dir=path.join(root,ref.runId+'-'+ref.fence);
 if(await realpath(dir)!==dir)fail('INPUT_BUNDLE_INVALID');
 const s=await stat(dir);if((s.mode&0o077)!==0||s.uid!==process.getuid?.())fail('INPUT_BUNDLE_INVALID');
 const b=await safeFile(path.join(dir,'manifest.json'),1024*1024,true);
 if(inputBundleDigest(b)!==ref.digest)fail('INPUT_BUNDLE_DIGEST');
 let m;try{m=JSON.parse(b);}catch{fail('INPUT_BUNDLE_INVALID');}
 if(!exact(m,['version','runId','fence','taskId','goalId','contextDigest','inputs'])||m.version!==1||
  m.runId!==ref.runId||m.fence!==ref.fence||!uuid.test(m.taskId??'')||!uuid.test(m.goalId??'')||
  m.contextDigest!==inputBundleValueDigest(params.context?.manifest??null)||
  !Array.isArray(m.inputs)||!m.inputs.length||m.inputs.length>referencedInputCountLimit)fail('INPUT_BUNDLE_INVALID');
 const seen=new Set();let total=0;
 // Validate the full descriptor set before reading any source bytes.
 for(const i of m.inputs){
  if(!exact(i,['id','name','digest','size','kind','mime'])||!uuid.test(i.id??'')||!name.test(i.name??'')||
   !hex.test(i.digest??'')||!Number.isSafeInteger(i.size)||i.size<0||i.size>referencedInputFileLimitBytes||
   !['artifact','inline'].includes(i.kind)||(i.mime!==undefined&&(typeof i.mime!=='string'||i.mime.length>200))||seen.has(i.id))fail('INPUT_BUNDLE_INVALID');
  seen.add(i.id);total+=i.size;
 }
 if(total>referencedInputLimitBytes)fail('INPUT_BUNDLE_CAPACITY');
 const inputs=[];
 for(const i of m.inputs){
  signal.throwIfAborted();
  const base=i.kind==='artifact'?store:dir+'/inline';
  if(await realpath(base)!==base)fail('INPUT_BUNDLE_INVALID');
  const bytes=await safeFile(path.join(base,i.id),referencedInputFileLimitBytes);
  if(bytes.length!==i.size||inputBundleDigest(bytes)!==i.digest)fail('INPUT_BUNDLE_DIGEST');
  inputs.push({id:i.id,name:i.name,digest:i.digest,base64:bytes.toString('base64'),...(i.mime?{mime:i.mime}:{})});
 }
 signal.throwIfAborted();
 return {...params,inputs};
}
