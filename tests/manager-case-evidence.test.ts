import {afterAll,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {migrate,pool,type DB} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installSupervision} from '../server/supervision.js';
import {managerCaseEvidenceSource,readManagerCaseEvidence} from '../server/manager-case-evidence.js';
import {projectManagerCases} from '../server/manager-material-consumption.js';
import {readManagementRetrieval,managementContextRequests,managementRetrievalCharacterLimit} from '../server/management-retrieval.js';

beforeAll(async()=>{await migrate();await installBacklogPlanner();await installSupervision();await installManagement();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
const hash=(s:string)=>createHash('sha256').update(s).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?
 Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
async function fixture(evidence:any[]=[{kind:'SCOPED_DELIVERY_ACTION',summary:'Full original ü 😀 '.repeat(350),
 action:{reason:'A complete current proposal.',marker:'^12',literal:{$v:2}}},{kind:'UNKNOWN_CURRENT_KIND',instruction:'Keep every field.'}]){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Case evidence','Exact source','case-evidence',$1::text)",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
  VALUES($1,$2,'consumer','Consumer','Preserve all criteria.','research.web','failed','work','[]',
   '{"baseWorkspace":"case/scope","workKind":"repair"}',1)`,[task,goal]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'failed','harness')",[run,task,randomUUID()]);
 await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,'Review proposals',$5)",[caseId,goal,task,run,JSON.stringify(evidence)]);
 const row=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
 const binding=managerCaseEvidenceSource(row)!;
 const request={kind:'case_evidence' as const,caseId,evidenceIndex:0,digest:binding.digest,offset:0};
 return {goal,task,run,caseId,row,binding,request};
}
async function packet(goal:string,requests:any[],focus?:any){
 await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
  ['management-state:'+goal,JSON.stringify({contextRequests:requests,...(focus?{contextRequestFocus:focus}:{})})]);
}
const noArtifacts=vi.fn(async(_id:string):Promise<{bytes:Buffer}>=>{throw Error('ARTIFACT_READER_MUST_NOT_RUN');});

it('binds the original case array before the existing 1800-character summary projection',async()=>{
 const f=await fixture(),projected=projectManagerCases([{...f.row,current_run_id:f.run,current_attempt:1}])[0];
 expect(projected.evidence[0].summary).toHaveLength(1800);
 expect(projected.evidenceSource).toEqual(f.binding);
 const page=await readManagerCaseEvidence(pool,f.goal,f.request);
 expect(page.status).toBe('OK');
 const value=JSON.parse(page.text!);
 expect(value.summary).toEqual(f.row.evidence[0].summary);
 expect(value.action.literal).toEqual({$v:2});expect(value.action.marker).toBe('^12');
 expect(page.digest).toBe(hash(page.text!));
 expect(page.entryDigest).toBe(f.binding.entryDigests[0]);
 expect(page.arrayDigest).toBe(f.binding.arrayDigest);
 expect(page).not.toHaveProperty('runId');expect(page).not.toHaveProperty('artifactId');
});

it('returns and reconstructs an entire multi-page original through the actual bounded retrieval entry point',async()=>{
 const f=await fixture([{kind:'SCOPED_DELIVERY_ACTION',summary:'Unicode 😀 and quotes " \\ \n'.repeat(2200)}]);
 let offset:any=0,all='';
 do{await packet(f.goal,[{...f.request,offset}]);
  const recovered=await readManagementRetrieval(pool,f.goal,noArtifacts),page=recovered.results[0];
  expect(page.status).toBe('OK');expect(page.text.length).toBeLessThanOrEqual(12000);
  expect(page.entryDigest).toBe(f.binding.entryDigests[0]);expect(page.evidenceDigest).toBe(f.binding.digest);
  all+=page.text;offset=page.nextOffset;
 }while(offset!==null);
 expect(JSON.parse(all)).toEqual(f.row.evidence[0]);expect(hash(all)).toBe(f.binding.entryDigests[0]);
 expect(noArtifacts).not.toHaveBeenCalled();
});

it('is goal-bound even when a valid case identifier and digest from another goal are supplied',async()=>{
 const f=await fixture(),other=await fixture();
 expect(await readManagerCaseEvidence(pool,other.goal,f.request)).toEqual({status:'NOT_FOUND'});
});

it('allows explicit same-goal evidence outside the focused case without borrowing another review',async()=>{
 const f=await fixture(),focus={caseId:randomUUID(),taskId:randomUUID(),scope:'case/other'};
 await packet(f.goal,[f.request],focus);
 const value=await readManagementRetrieval(pool,f.goal,noArtifacts,focus);
 expect(value.results[0]).toMatchObject({status:'OK',caseId:f.caseId,evidenceDigest:f.binding.digest});
});

for(const change of ['append','reorder','entry'] as const)it(`rejects a stale full-array digest after ${change}, including later pages`,async()=>{
 const f=await fixture(),values=structuredClone(f.row.evidence);
 if(change==='append')values.push({kind:'NEW_CURRENT_PROOF'});
 if(change==='reorder')values.reverse();
 if(change==='entry')values[0].summary+='Changed original';
 await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[f.caseId,JSON.stringify(values)]);
 const result=await readManagerCaseEvidence(pool,f.goal,{...f.request,offset:100});
 expect(result).toMatchObject({status:'STALE_CASE_EVIDENCE',caseId:f.caseId,nextOffset:0});
 expect(result).not.toHaveProperty('text');expect(result.evidenceDigest).not.toBe(f.binding.digest);
});

it('binds case, goal, task and source-run ownership independently of equal evidence text',async()=>{
 const f=await fixture();
 for(const field of ['id','goal_id','task_id','source_run_id'])
  expect(managerCaseEvidenceSource({...f.row,[field]:randomUUID()})?.digest).not.toBe(f.binding.digest);
 await pool.query('UPDATE supervision_cases SET source_run_id=NULL WHERE id=$1',[f.caseId]);
 expect((await readManagerCaseEvidence(pool,f.goal,f.request)).status).toBe('STALE_CASE_EVIDENCE');
});

it('uses canonical original JSONB values, preserving arrays and distinguishing entries',async()=>{
 const f=await fixture();
 expect(f.binding.arrayDigest).toBe(hash(JSON.stringify(canonical(f.row.evidence))));
 expect(f.binding.entryDigests[0]).not.toBe(f.binding.entryDigests[1]);
 const p=await readManagerCaseEvidence(pool,f.goal,{...f.request,evidenceIndex:1});
 expect(JSON.parse(p.text!)).toEqual(f.row.evidence[1]);
 expect(p.entryDigest).toBe(f.binding.entryDigests[1]);
});

it('does not replace absent/malformed source identity with a projected hash',async()=>{
 const f=await fixture();
 expect(managerCaseEvidenceSource({...f.row,evidence:{not:'an array'}})).toBeNull();
 expect(managerCaseEvidenceSource({...f.row,source_run_id:undefined})).toBeNull();
 expect(managerCaseEvidenceSource({...f.row,task_id:'unknown'})).toBeNull();
 expect((await readManagerCaseEvidence(pool,f.goal,{...f.request,evidenceIndex:100})).status).toBe('NOT_FOUND');
});

it('bounds entry count before constructing per-entry digest metadata for tiny scalar arrays',async()=>{
 const f=await fixture(),evidence=Array.from({length:10001},()=>0);
 expect(managerCaseEvidenceSource({...f.row,evidence})).toBeNull();
 await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1',[f.caseId,JSON.stringify(evidence)]);
 expect(await readManagerCaseEvidence(pool,f.goal,f.request)).toEqual({status:'TOO_LARGE'});
});

it('requires a digest and keeps the existing four-request limit',async()=>{
 const f=await fixture(),{digest:_digest,...missing}=f.request;
 expect(managementContextRequests.safeParse([missing]).success).toBe(false);
 expect(managementContextRequests.safeParse(Array.from({length:5},()=>f.request)).success).toBe(false);
 for(const patch of [{evidenceIndex:-1},{offset:-1},{digest:'not-a-digest'}])
  await expect(readManagerCaseEvidence(pool,f.goal,{...f.request,...patch})).rejects.toThrow('CASE_EVIDENCE_REQUEST_INVALID');
});

it('keeps four large requested entries inside the existing escaped 24000-character response budget',async()=>{
 const f=await fixture(Array.from({length:4},(_,index)=>({kind:'CURRENT_'+index,text:'"\\😀\n'.repeat(6000)})));
 await packet(f.goal,Array.from({length:4},(_,evidenceIndex)=>({...f.request,evidenceIndex})));
 const result=await readManagementRetrieval(pool,f.goal,noArtifacts);
 expect(result.results).toHaveLength(4);expect(JSON.stringify(result).length).toBeLessThanOrEqual(managementRetrievalCharacterLimit);
 for(const [index,page] of result.results.entries()){
  expect(page.status).toBe('OK');expect(page.entryDigest).toBe(f.binding.entryDigests[index]);
  expect(page.truncated).toBe(true);expect(page.nextOffset).toBe(page.text.length);
 }
});

it('never converts a DB failure into successful evidence or an unrelated not-found response',async()=>{
 const f=await fixture(),error=Object.assign(Error('database fault'),{code:'XX000'});
 const db={query:async()=>{throw error;}} as unknown as DB;
 await expect(readManagerCaseEvidence(db,f.goal,f.request)).rejects.toBe(error);
});

it('leaves authoritative case/task/run/settings/events and acceptance untouched while reading',async()=>{
 const f=await fixture();await packet(f.goal,[f.request]);
 const snapshot=async()=>({
  c:(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.caseId])).rows,
  t:(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows,
  r:(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows,
  s:(await pool.query('SELECT * FROM settings WHERE id=$1',['management-state:'+f.goal])).rows,
  e:(await pool.query('SELECT * FROM events ORDER BY id')).rows,
  a:(await pool.query('SELECT * FROM acceptances ORDER BY id')).rows,
 });
 const before=await snapshot();await readManagementRetrieval(pool,f.goal,noArtifacts);
 expect(await snapshot()).toEqual(before);
});
