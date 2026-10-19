import {afterEach,expect,it} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,mkdir,readFile,rm,symlink,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {createPlannerCatalogInputs,readPlannerCatalog,plannerCatalogInputVersion} from '../scripts/planner-catalog-transport.mjs';
import {createScopedPlannerCatalogReaderInput} from '../server/scoped-planner-context.js';
import {createScopedPlannerPreflightInputs} from '../server/scoped-plan-preflight.js';
import {scopedPlanPreflight} from '../server/scoped-plan-contract.js';
import {mergeSupervisorInputs} from '../server/execution-inputs.js';

const execute=promisify(execFile),dirs:string[]=[];
const sha=(v:Buffer|string)=>createHash('sha256').update(v).digest('hex');
const individual=8*1024*1024,total=24*1024*1024;
afterEach(async()=>{for(const dir of dirs.splice(0))await rm(dir,{recursive:true,force:true});});
function fixture(){
 const source=randomUUID(),consumer=randomUUID(),run=randomUUID(),artifact=randomUUID();
 const request={id:randomUUID(),scope:'test/catalog',source_revision:'a'.repeat(64),snapshot:{
  tasks:[{id:source,state:'accepted',stage:'work',attempt_count:1,acceptance_passed:true,has_live_run:false,
   last_run_id:run,last_run_attempt:1,last_run_state:'submitted',dependencies:[],criteria:[{id:'source',label:'Unchanged complete contract'}],
   intent:'Full Unicode source contract: ☃ 😀 '.repeat(270000),execution:{baseWorkspace:'test/catalog',workKind:'repair'}},
   {id:consumer,state:'recovery_blocked',stage:'integrate',attempt_count:2,dependencies:[],criteria:[{id:'qa',label:'Final QA remains mandatory'}],
    execution:{baseWorkspace:'test/catalog',workKind:'implementation',readOnlySource:false}}],
  artifacts:[{id:artifact,task_id:source,name:'implementation.patch',digest:'b'.repeat(64)}],inputManifest:[]}};
 const plan={version:1,requestId:request.id,sourceRevision:request.source_revision,
  reason:'Compose the already accepted repair into the existing candidate with final QA still mandatory.',tasks:[],blockers:[],dependencyChanges:[],
  actions:[{kind:'prepare_integration',taskId:consumer,expectedAttempt:2,sourceTaskId:source,expectedSourceRunId:run,
   artifactIds:[artifact],reason:'Apply the authorized accepted repair without changing the original integration criteria.'}]};
 return{request,plan};
}
async function mount(){
 const f=fixture(),dir=await mkdtemp(path.join(tmpdir(),'planner-shards-'));dirs.push(dir);
 const inputRoot=path.join(dir,'inputs'),bytes=Buffer.from(JSON.stringify(f.request.snapshot));
 expect(bytes.length).toBeGreaterThan(individual);
 const catalog=createPlannerCatalogInputs(f.request.id,bytes),catalogPath=path.join(inputRoot,f.request.id,'planning-catalog.json');
 const install=async(input:{id:string;name:string;base64:string})=>{
  const file=path.join(inputRoot,input.id,input.name);await mkdir(path.dirname(file),{recursive:true});await writeFile(file,Buffer.from(input.base64,'base64'));};
 for(const input of catalog.inputs)await install(input);
 const binding={path:catalogPath,digest:catalog.catalogDigest,transport:catalog.transport};
 return{...f,dir,inputRoot,bytes,catalog,catalogPath,binding,install};
}
async function call(file:string,args:string[]){
 try{const r=await execute(process.execPath,[file,...args],{env:{PATH:process.env.PATH??''},timeout:15000,maxBuffer:1024*1024});
  return{exit:0,result:JSON.parse(r.stdout),stderr:r.stderr};}
 catch(e:any){if(typeof e.code!=='number'||!e.stdout)throw e;return{exit:e.code,result:JSON.parse(e.stdout),stderr:e.stderr};}
}

it('keeps small catalog bytes, name, request ID and digest exactly compatible, including the 8 MiB boundary',()=>{
 for(const bytes of [Buffer.from('{"tasks":[],"criteria":"exact"}'),Buffer.alloc(individual,32)]){
  const id=randomUUID(),bundle=createPlannerCatalogInputs(id,bytes);
  expect(bundle.transport).toBeUndefined();expect(bundle.inputs).toEqual([{id,name:'planning-catalog.json',digest:sha(bytes),base64:bytes.toString('base64')}]);
  expect(bundle.catalogDigest).toBe(sha(bytes));expect(bundle.catalogBytes).toBe(bytes.length);
 }
});

it('reconstructs exact UTF-8 bytes and deterministic part identities without changing criteria or a large string',async()=>{
 const f=await mount(),before=JSON.stringify(f.request.snapshot);
 expect(f.catalog.transport?.format).toBe(plannerCatalogInputVersion);
 expect(f.catalog).toEqual(createPlannerCatalogInputs(f.request.id,f.bytes));
 expect(f.catalog.inputs.every(input=>Buffer.byteLength(input.base64,'base64')<=individual)).toBe(true);
 expect((await readPlannerCatalog(f.binding)).equals(f.bytes)).toBe(true);
 expect(JSON.stringify(JSON.parse((await readPlannerCatalog(f.binding)).toString()))).toBe(before);
 expect(mergeSupervisorInputs(f.catalog.inputs)).toEqual(f.catalog.inputs);
});

it('runs the mounted standalone reader and preflight against the same sharded catalog with no runtime imports',async()=>{
 const f=await mount(),reader=await createScopedPlannerCatalogReaderInput(f.request.id,f.catalog.catalogDigest,f.inputRoot,f.catalog.transport);
 const preflight=await createScopedPlannerPreflightInputs(f.request,f.catalog.catalogDigest,f.inputRoot,f.catalog.transport);
 for(const input of [reader,...preflight.inputs])await f.install(input);
 expect(mergeSupervisorInputs(f.catalog.inputs,[reader,...preflight.inputs])).toHaveLength(f.catalog.inputs.length+3);
 const readerPath=path.join(f.inputRoot,reader.id,reader.name),draft=path.join(f.dir,'scope-plan.json');await writeFile(draft,JSON.stringify(f.plan));
 const query=(q:any)=>call(readerPath,[f.catalogPath,f.catalog.catalogDigest,JSON.stringify(q)]);
 const directory=await query({operation:'directory',jsonPath:['tasks'],limit:1});
 expect(directory.exit).toBe(0);expect(directory.result.selection).toMatchObject({total:2,returned:1,complete:false,nextOffset:1});
 expect(directory.result.entries[0].identity.id).toBe(f.request.snapshot.tasks[0].id);
 const item=await query({operation:'read',jsonPath:['tasks',0],limit:12,maxCharacters:2000});
 const reference=item.result.entries.find((v:any)=>v.key==='intent');
 expect(reference).toMatchObject({representation:'query_child',reference:{jsonPath:['tasks',0,'intent']}});
 const child=await query({operation:'read',...reference.reference,maxCharacters:2000});
 expect(child.exit).toBe(0);expect(child.result.text).toBe(f.request.snapshot.tasks[0].intent!.slice(0,child.result.selection.returned));
 expect(child.result.catalogDigest).toBe(f.catalog.catalogDigest);
 const valid=await call(preflight.guide.cliPath,['--check',draft]);
 expect(valid.exit).toBe(0);expect(valid.result).toMatchObject(scopedPlanPreflight(f.plan,f.request));
 const wrong=structuredClone(f.plan);wrong.actions[0].expectedAttempt=99;await writeFile(draft,JSON.stringify(wrong));
 const rejected=await call(preflight.guide.cliPath,['--check',draft]);
 expect(rejected.exit).toBe(2);expect(rejected.result).toMatchObject(scopedPlanPreflight(wrong,f.request));
 expect((await readPlannerCatalog(f.binding)).equals(f.bytes)).toBe(true);
});

it.each(['index-tamper','part-tamper','missing-part','part-symlink','directory-symlink'] as const)
 ('fails closed for %s before returning any catalog bytes',async(mode)=>{
  const f=await mount(),part=f.catalog.inputs[1],file=path.join(f.inputRoot,part.id,part.name);
  if(mode==='index-tamper')await writeFile(f.catalogPath,'{}');
  if(mode==='part-tamper'){const bytes=await readFile(file);bytes[0]^=1;await writeFile(file,bytes);}
  if(mode==='missing-part')await rm(file);
  if(mode==='part-symlink'){const other=path.join(f.dir,'foreign.bin');await writeFile(other,await readFile(file));await rm(file);await symlink(other,file);}
  if(mode==='directory-symlink'){const other=path.join(f.dir,'foreign');await mkdir(other);await writeFile(path.join(other,part.name),await readFile(file));
   await rm(path.dirname(file),{recursive:true});await symlink(other,path.dirname(file));}
  await expect(readPlannerCatalog(f.binding)).rejects.toThrow(/^CATALOG_/);
 });

it.each(['reordered','duplicated','traversal-name','traversal-id','bad-size','bad-total','foreign-request','logical-digest'] as const)
 ('rejects structurally invalid %s even if the index digest is rebound in the test',async(mode)=>{
  const f=await mount(),index=JSON.parse((await readFile(f.catalogPath)).toString());
  if(mode==='reordered')index.parts.reverse();
  if(mode==='duplicated')index.parts[1]={...index.parts[0]};
  if(mode==='traversal-name')index.parts[0].name='../foreign';
  if(mode==='traversal-id')index.parts[0].id='../../foreign';
  if(mode==='bad-size')index.parts[0].size=individual+1;
  if(mode==='bad-total')index.catalogBytes=total+1;
  if(mode==='foreign-request')index.requestId=randomUUID();
  if(mode==='logical-digest')index.catalogDigest='f'.repeat(64);
  const bytes=Buffer.from(JSON.stringify(index));await writeFile(f.catalogPath,bytes);
  const binding={...f.binding,transport:{...f.catalog.transport!,indexDigest:sha(bytes)}};
  await expect(readPlannerCatalog(binding)).rejects.toThrow(/^CATALOG_/);
 });

it('retains the aggregate guard for index, parts, evidence and tools; refuses oversized catalogs without partial output',async()=>{
 expect(()=>createPlannerCatalogInputs(randomUUID(),Buffer.alloc(total+1))).toThrow('SUPERVISION_INPUTS_TOO_LARGE');
 expect(()=>createPlannerCatalogInputs(randomUUID(),Buffer.alloc(total))).toThrow('SUPERVISION_INPUTS_TOO_LARGE');
 const f=await mount(),evidence=Array.from({length:2},()=>{const bytes=Buffer.alloc(individual,1);
  return{id:randomUUID(),name:'evidence.bin',digest:sha(bytes),base64:bytes.toString('base64')};});
 let rejected:unknown;try{mergeSupervisorInputs(evidence,f.catalog.inputs);}catch(error){rejected=error;}
 expect(rejected).toMatchObject({code:'SUPERVISION_INPUTS_TOO_LARGE'});
});

it('uses the same refusal for tampered parts in the real reader and preflight; never treats the index as a catalog',async()=>{
 const f=await mount(),reader=await createScopedPlannerCatalogReaderInput(f.request.id,f.catalog.catalogDigest,f.inputRoot,f.catalog.transport),
  preflight=await createScopedPlannerPreflightInputs(f.request,f.catalog.catalogDigest,f.inputRoot,f.catalog.transport);
 for(const input of [reader,...preflight.inputs])await f.install(input);
 const file=path.join(f.inputRoot,f.catalog.inputs[1].id,f.catalog.inputs[1].name),bytes=await readFile(file);bytes[0]^=1;await writeFile(file,bytes);
 const r=await call(path.join(f.inputRoot,reader.id,reader.name),[f.catalogPath,f.catalog.catalogDigest,JSON.stringify({operation:'directory',jsonPath:['tasks']})]);
 const draft=path.join(f.dir,'scope-plan.json');await writeFile(draft,JSON.stringify(f.plan));
 const p=await call(preflight.guide.cliPath,['--check',draft]);
 expect(r).toMatchObject({exit:1,result:{error:'CATALOG_PART_DIGEST'}});
 expect(p).toMatchObject({exit:2,result:{validAgainstFrozenCatalog:false,code:'PLANNING_PREFLIGHT_CATALOG_PART_DIGEST'}});
 await expect(readPlannerCatalog({path:f.catalogPath,digest:f.catalog.catalogDigest})).rejects.toThrow('CATALOG_DIGEST_MISMATCH');
});
