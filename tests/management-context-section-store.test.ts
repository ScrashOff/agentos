import {afterAll,beforeAll,beforeEach,expect,it} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {pool,migrate,tx} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision} from '../server/supervision.js';
import {installManagement} from '../server/management.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagerContextSections,persistManagerContextSections,readManagerContextSection} from '../server/manager-context-section-store.js';
import {managerContextSectionDigest,type ManagerContextSectionName} from '../server/manager-context-sections.js';
import {managementContextRequests,readManagementRetrievalRequest} from '../server/management-retrieval.js';
import {retainManagementEvidence,readManagementEvidenceLedger} from '../server/management-evidence-ledger.js';
import {readMaterialDecisionOutcome} from '../server/management-material-decision.js';
import {deliveryDigest} from '../server/delivery-supervision.js';

beforeAll(async()=>{
 expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
 await migrate();await installBacklogPlanner();await installSupervision();await installManagement();await installManagerContextSections(pool);
});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());
const noArtifacts=async(_id:string):Promise<{bytes:Buffer}>=>{throw Error('NO_ARTIFACT_IO_EXPECTED');};
const sha=(text:string)=>createHash('sha256').update(text).digest('hex');
const noise=(size:number)=>Array.from({length:Math.ceil(size/64)},(_,i)=>sha(String(i))).join('').slice(0,size);
const blob=(value:any,section:ManagerContextSectionName='progress')=>{
 const text=JSON.stringify(value);return {section,text,characters:text.length,digest:managerContextSectionDigest(section,text)};
};
const request=(source:ReturnType<typeof blob>,offset=0)=>({kind:'context_section' as const,section:source.section,digest:source.digest,offset});
async function goal(){const id=randomUUID();await pool.query(
 "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Section','Exact operational observation','section-store',$1::text)",[id]);return id;}
async function count(goalId:string){return (await pool.query('SELECT count(*)::int n FROM management_context_sections WHERE goal_id=$1',[goalId])).rows[0].n;}

it('persists idempotently and reads exact same-goal immutable pages without a run or artifact',async()=>{
 const g=await goal(),other=await goal(),source=blob({milestones:[{detail:noise(26000)}]}),before=(await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n;
 await tx(db=>persistManagerContextSections(db,g,[source]));await tx(db=>persistManagerContextSections(db,g,[source]));
 expect(await count(g)).toBe(1);
 let offset=0,text='';
 do{
  const page=await readManagementRetrievalRequest(pool,g,noArtifacts,request(source,offset));
  expect(page).toMatchObject({status:'OK',digest:source.digest,totalCharacters:source.characters,request:request(source,offset)});
  expect(page).not.toHaveProperty('runId');expect(page).not.toHaveProperty('artifactId');
  expect(page.text.length).toBeLessThanOrEqual(12000);expect(page.text).toBe(source.text.slice(offset,offset+12000));
  text+=page.text;offset=page.nextOffset;
 }while(offset!==null);
 expect(text).toBe(source.text);expect(JSON.parse(text)).toEqual({milestones:[{detail:noise(26000)}]});
 expect(await readManagementRetrievalRequest(pool,other,noArtifacts,request(source))).toMatchObject({status:'NOT_FOUND'});
 expect(await readManagerContextSection(pool,g,{...request(source),section:'frontier'})).toMatchObject({status:'INTEGRITY_FAILED'});
 const end=await readManagementRetrievalRequest(pool,g,noArtifacts,request(source,source.characters));
 expect(end).toMatchObject({status:'OK',text:'',nextOffset:null,truncated:false});
 const beyond=await readManagementRetrievalRequest(pool,g,noArtifacts,request(source,source.characters+1));
 expect(beyond).toMatchObject({status:'PAGE_OUT_OF_RANGE',nextOffset:0});expect(beyond).not.toHaveProperty('text');
 expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(before);
});

it.each(['digest','characters','section','non-json','noncanonical','duplicate','over-limit'])(
 'rejects %s before writing any portion of a batch',async mode=>{
 const g=await goal(),good=blob({safe:true}),bad:any=blob({unsafe:false},'frontier');
 if(mode==='digest')bad.digest='a'.repeat(64);
 if(mode==='characters')bad.characters++;
 if(mode==='section')bad.section='futureAuthority';
 if(mode==='non-json'){bad.text='undefined';bad.characters=bad.text.length;bad.digest=sha(bad.section+'\n'+bad.text);}
 if(mode==='noncanonical'){bad.text='{ "safe": false }';bad.characters=bad.text.length;bad.digest=sha(bad.section+'\n'+bad.text);}
 if(mode==='over-limit')Object.assign(bad,blob({text:'😀'.repeat(300000)},'frontier'));
 const inputs=mode==='duplicate'?[good,good]:[good,bad];
 await expect(tx(db=>persistManagerContextSections(db,g,inputs))).rejects.toThrow(/MANAGER_CONTEXT_SECTION_(INVALID|TOO_LARGE)/);
 expect(await count(g)).toBe(0);
});

it('rolls back every section if the final transaction fails, without a partial durable reference source',async()=>{
 const g=await goal(),sources=[blob({a:1}),blob({b:2},'frontier')];
 await expect(tx(async db=>{await persistManagerContextSections(db,g,sources);throw Error('ADMISSION_REJECTED_AFTER_STORE');}))
  .rejects.toThrow('ADMISSION_REJECTED_AFTER_STORE');
 expect(await count(g)).toBe(0);
 for(const source of sources)expect(await readManagerContextSection(pool,g,request(source))).toMatchObject({status:'NOT_FOUND'});
});

it('serializes concurrent identical section batches and never overwrites the immutable winner',async()=>{
 const g=await goal(),sources=[blob({a:noise(2000)}),blob({b:noise(2100)},'frontier')];
 await Promise.all([tx(db=>persistManagerContextSections(db,g,sources)),tx(db=>persistManagerContextSections(db,g,[...sources].reverse()))]);
 expect(await count(g)).toBe(2);
 const before=(await pool.query('SELECT digest,section,text,characters,created_at FROM management_context_sections WHERE goal_id=$1 ORDER BY digest',[g])).rows;
 const forged={...sources[0],text:JSON.stringify({different:true})};forged.characters=forged.text.length;
 await expect(tx(db=>persistManagerContextSections(db,g,[forged]))).rejects.toThrow('MANAGER_CONTEXT_SECTION_INVALID');
 expect((await pool.query('SELECT digest,section,text,characters,created_at FROM management_context_sections WHERE goal_id=$1 ORDER BY digest',[g])).rows).toEqual(before);
});

it.each(['text','characters','section'])('refuses corrupted stored %s and does not replace it on conflict',async field=>{
 const g=await goal(),source=blob({exact:'Original operational observation'});
 await tx(db=>persistManagerContextSections(db,g,[source]));
 if(field==='text')await pool.query('UPDATE management_context_sections SET text=$3 WHERE goal_id=$1 AND digest=$2',[g,source.digest,JSON.stringify({exact:'corrupt'})]);
 if(field==='characters')await pool.query('UPDATE management_context_sections SET characters=characters+1 WHERE goal_id=$1 AND digest=$2',[g,source.digest]);
 if(field==='section')await pool.query("UPDATE management_context_sections SET section='frontier' WHERE goal_id=$1 AND digest=$2",[g,source.digest]);
 const read=await readManagementRetrievalRequest(pool,g,noArtifacts,request(source));
 expect(read.status).toBe('INTEGRITY_FAILED');expect(read).not.toHaveProperty('text');
 await expect(tx(db=>persistManagerContextSections(db,g,[source]))).rejects.toThrow('MANAGER_CONTEXT_SECTION_CONFLICT');
 expect(await count(g)).toBe(1);
});

it('preserves escaped NUL/lone surrogates and exact UTF16 page boundaries through PostgreSQL and the ledger',async()=>{
 const g=await goal(),source=blob({text:'A'.repeat(12000)+'😀'+'B'.repeat(1200),literal:'\ud800\u0000',marker:{$n:7}});
 await tx(db=>persistManagerContextSections(db,g,[source]));
 const split=source.text.indexOf('😀')+1,focus={caseId:randomUUID(),taskId:randomUUID(),scope:'fixture/section'};
 const first=await readManagementRetrievalRequest(pool,g,noArtifacts,request(source,split-12000));
 expect(first.text.length).toBe(12000);expect(first.text.charCodeAt(first.text.length-1)).toBe(0xd83d);
 const second=await readManagementRetrievalRequest(pool,g,noArtifacts,request(source,split));
 expect(second.text.charCodeAt(0)).toBe(0xde00);
 await retainManagementEvidence(pool,g,{results:[first,second]},noArtifacts,focus);
 const retained=await readManagementEvidenceLedger(pool,g,focus.scope,focus,noArtifacts);
 expect(retained.entries).toHaveLength(1);expect(retained.entries[0]).not.toHaveProperty('runId');
 expect(retained.entries[0].pages[0].text).toBe(source.text.slice(split-12000));
 expect(retained.entries[0].complete).toBe(false);
 const stored=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-evidence:'+g])).rows[0].value;
 expect(stored.version).toBe(2);expect(JSON.parse(stored.entries[0].pages[0].textJson)).toBe(source.text.slice(split-12000));
 const pg=(await pool.query('SELECT text FROM management_context_sections WHERE goal_id=$1 AND digest=$2',[g,source.digest])).rows[0].text;
 expect(pg).toBe(source.text);expect(JSON.parse(pg).literal).toBe('\ud800\u0000');
});

it('retains only verified same-focus section pages and distinguishes two immutable versions',async()=>{
 const g=await goal(),one=blob({count:1}),two=blob({count:2}),focus={caseId:randomUUID(),taskId:randomUUID(),scope:'fixture/section'};
 await tx(db=>persistManagerContextSections(db,g,[one]));await tx(db=>persistManagerContextSections(db,g,[two]));
 for(const source of [one,two]){
  const page=await readManagementRetrievalRequest(pool,g,noArtifacts,request(source));
  await retainManagementEvidence(pool,g,{results:[page]},noArtifacts,focus);
 }
 let retained=await readManagementEvidenceLedger(pool,g,focus.scope,focus,noArtifacts);
 expect(retained.entries).toHaveLength(2);expect(new Set(retained.entries.map((entry:any)=>entry.digest))).toEqual(new Set([one.digest,two.digest]));
 expect((await readManagementEvidenceLedger(pool,g,focus.scope,{...focus,caseId:randomUUID()},noArtifacts)).entries).toEqual([]);
 const page=await readManagementRetrievalRequest(pool,g,noArtifacts,request(one));
 await expect(retainManagementEvidence(pool,g,{results:[{...page,text:'forged'}]},noArtifacts,focus)).rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
 await expect(retainManagementEvidence(pool,g,{results:[{...page,runId:randomUUID()}]},noArtifacts,focus)).rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
 await pool.query('UPDATE management_context_sections SET text=$3 WHERE goal_id=$1 AND digest=$2',[g,one.digest,JSON.stringify({count:99})]);
 retained=await readManagementEvidenceLedger(pool,g,focus.scope,focus,noArtifacts);
 expect(retained.entries).toHaveLength(1);expect(retained.entries[0].digest).toBe(two.digest);
});

it('does not admit malformed request paths, offsets or digests through the shared reader schema',()=>{
 const source=blob({safe:true}),valid=request(source);
 expect(managementContextRequests.safeParse([valid]).success).toBe(true);
 for(const patch of [{section:'../../secrets'},{section:'tasks'},{offset:-1},{offset:0.5},{offset:10000001},{digest:'uuid-not-sha'}])
  expect(managementContextRequests.safeParse([{...valid,...patch}]).success).toBe(false);
});

it('keeps an operational section unrelated to a material gap even when its body mentions the exact focal case',async()=>{
 const g=await goal(),task=randomUUID(),run=randomUUID(),caseId=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
  VALUES($1,$2,'consumer','Consumer','Keep acceptance','research.web','failed','work','[]',1)`,[task,g]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'failed','harness')",[run,task,randomUUID()]);
 await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,'Material decision','[]')",[caseId,g,task,run]);
 const binding={goalId:g,caseId,consumerTaskId:task,consumerRunId:run,consumerAttempt:1,obligations:[{
  obligationId:randomUUID(),generation:sha('generation'),handoffId:randomUUID(),producerTaskId:randomUUID(),producerRunId:randomUUID(),
  producerManifestDigest:sha('manifest'),repairArtifacts:[{id:randomUUID(),name:'implementation.patch',digest:sha('patch'),size:1},
   {id:randomUUID(),name:'repair-report.json',digest:sha('report'),size:2}]}]};
 const hint={version:1,authority:'review_only',...binding,identityDigest:deliveryDigest(binding),actionValidationRequired:true};
 const source=blob({caseId,taskId:task,obligationId:binding.obligations[0].obligationId});
 await tx(db=>persistManagerContextSections(db,g,[source]));
 const page=await readManagementRetrievalRequest(pool,g,noArtifacts,request(source));expect(page.status).toBe('OK');
 const snapshot={cases:[{id:caseId,taskId:task,materialConsumptionReview:hint,evidence:[]}],operation:{focus:{caseId,taskId:task},
  recoveredContext:{results:[page]}}};
 const coordination={management:{contextRequests:[request(source)],materialContextGap:{caseId,consumerRunId:run,
  identityDigest:hint.identityDigest,reason:'The global operational section was requested for this observation.',requestIndices:[0]}}};
 const before=(await pool.query('SELECT state,attempt_count,criteria FROM tasks WHERE id=$1',[task])).rows[0];
 const result:any=await readMaterialDecisionOutcome(pool,g,randomUUID(),'MANAGEMENT_V1\n'+JSON.stringify(snapshot),coordination);
 expect(result).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false});
 expect(result).not.toHaveProperty('contextGap');expect(result.requiredDecision.materialReview).toEqual(hint);
 // Extra address-shaped fields are not an authorization bridge to the new kind.
 const injected:any={...request(source),caseId,taskId:task,obligationId:binding.obligations[0].obligationId};
 const normalized=managementContextRequests.parse([injected])[0];
 expect(normalized).not.toHaveProperty('caseId');expect(normalized).not.toHaveProperty('taskId');
 const again:any=await readMaterialDecisionOutcome(pool,g,randomUUID(),'MANAGEMENT_V1\n'+JSON.stringify(snapshot),
  {management:{...coordination.management,contextRequests:[injected]}});
 expect(again).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false});expect(again).not.toHaveProperty('contextGap');
 expect((await pool.query('SELECT state,attempt_count,criteria FROM tasks WHERE id=$1',[task])).rows[0]).toEqual(before);
});
