import {afterAll,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {installManagement} from '../server/management.js';
import {installSupervision} from '../server/supervision.js';
import {managerCaseEvidenceSource} from '../server/manager-case-evidence.js';
import {readManagementRetrievalRequest} from '../server/management-retrieval.js';
import {retainManagementEvidence,readManagementEvidenceLedger} from '../server/management-evidence-ledger.js';

beforeAll(async()=>{await migrate();await installBacklogPlanner();await installSupervision();await installManagement();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
const noArtifacts=async(_id:string):Promise<{bytes:Buffer}>=>{throw Error('NO_ARTIFACT_IO_EXPECTED');};
const ledgerKey=(goal:string)=>'management-evidence:'+goal;
async function fixture(text='A precise source with unchanged authority. '.repeat(80)){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Ledger focus','Exact pages','ledger-focus',$1::text)",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
  VALUES($1,$2,'consumer','Consumer',$3,'research.web','failed','work','[]',
  '{"baseWorkspace":"ledger/focus","workKind":"repair"}',1)`,[task,goal,text]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'failed','harness')",[run,task,randomUUID()]);
 await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
  VALUES($1,$2,$3,$4,'Decision pending',$5)`,[caseId,goal,task,run,JSON.stringify([{kind:'CURRENT_DECISION',text}])]);
 const row=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
 const source=managerCaseEvidenceSource(row)!;
 const request={kind:'case_evidence' as const,caseId,evidenceIndex:0,digest:source.digest,offset:0};
 const page=await readManagementRetrievalRequest(pool,goal,noArtifacts,request);
 expect(page.status).toBe('OK');expect(typeof page.text).toBe('string');
 return {goal,task,run,caseId,request,page,focus:{caseId,scope:'ledger/focus',taskId:task}};
}
const focusAt=(f:Awaited<ReturnType<typeof fixture>>,step:number)=>step%2?
 {remainingCases:step,taskId:f.task,selectionReason:'review_quantum',scope:f.focus.scope,
  sourceFingerprint:String(step).repeat(64),caseId:f.caseId}:
 {...f.focus,sourceFingerprint:String(step).repeat(64),selectionReason:'stable_decision',remainingCases:step};
const interval=(page:any,offset:number,end:number)=>({...page,request:{...page.request,offset},
 text:page.text.slice(offset,end),nextOffset:end,truncated:true});
// Reproduce the old key exactly: it included every scheduling field and its
// insertion order. The migration must derive identity from fields, not this key.
function legacyEntry(page:any,focus:any,updatedAt='2026-10-12T19:00:00.000Z'){
 const request=Object.fromEntries(Object.entries({...page.request,
  ...(page.request.kind==='task_metadata'?{part:page.request.part??'decision'}:{})})
  .filter(([key])=>key!=='offset'&&key!=='snapshotDigest').sort(([a],[b])=>a.localeCompare(b)));
 return {key:JSON.stringify([request,page.runId??null,page.artifactId??null,page.digest,page.snapshotDigest??null,focus??null]),
  request:page.request,runId:page.runId,artifactId:page.artifactId,digest:page.digest,snapshotDigest:page.snapshotDigest,
  totalCharacters:page.totalCharacters,...(focus?{focus}:{}),pages:[{offset:page.request.offset??0,text:page.text}],updatedAt};
}
async function seed(goal:string,entries:any[],version=2){
 const value={version,entries:entries.map(entry=>({...entry,pages:entry.pages.map((page:any)=>version===2?
  {offset:page.offset,textJson:JSON.stringify(page.text)}:page)})),fullSourcesPreserved:true};
 await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
  [ledgerKey(goal),JSON.stringify(value)]);
 return stored(goal);
}
async function stored(goal:string){return (await pool.query('SELECT value FROM settings WHERE id=$1',[ledgerKey(goal)])).rows[0]?.value;}
const read=(f:Awaited<ReturnType<typeof fixture>>,focus:any=f.focus)=>
 readManagementEvidenceLedger(pool,f.goal,f.focus.scope,focus,noArtifacts);

it('accumulates exact pages when selection reason, fingerprint, remaining count and property order change',async()=>{
 const f=await fixture();
 for(const [step,offset,end] of [[0,0,60],[1,40,100],[2,80,140]])
  await retainManagementEvidence(pool,f.goal,{results:[interval(f.page,offset,end)]},noArtifacts,focusAt(f,step));
 const ledger=await read(f,focusAt(f,9));
 expect(ledger.entries).toHaveLength(1);
 expect(ledger.entries[0]).toMatchObject({focus:f.focus,complete:false,availableRanges:[{offset:0,characters:140}],
  pages:[{offset:0,text:f.page.text.slice(0,140)}]});
 const value=await stored(f.goal);
 expect(value.entries).toHaveLength(1);expect(value.entries[0].focus).toEqual(f.focus);
 expect(JSON.parse(value.entries[0].key).at(-1)).toEqual(f.focus);
});

it('keeps canonical null scope distinct from omitted scope while coalescing its pages',async()=>{
 const f=await fixture(),focus={...f.focus,scope:null};
 await seed(f.goal,[legacyEntry(interval(f.page,0,40),{...focus,selectionReason:'priority'})]);
 const observedFocus={...focus,selectionReason:'stable_decision'};
 await retainManagementEvidence(pool,f.goal,{results:[interval(f.page,40,80)]},noArtifacts,observedFocus);
 const value=await stored(f.goal);
 expect(value.entries).toHaveLength(1);expect(value.entries[0].focus).toEqual(focus);
 const retained=await readManagementEvidenceLedger(pool,f.goal,undefined,focus,noArtifacts);
 expect(retained.entries).toHaveLength(1);
 expect(retained.entries[0].pages).toEqual([{offset:0,text:f.page.text.slice(0,80)}]);
 expect((await readManagementEvidenceLedger(pool,f.goal,undefined,{caseId:f.caseId,taskId:f.task},noArtifacts)).entries).toEqual([]);
});

it.each([1,2])('coalesces compatible legacy v%s keys on read without writing and persists the canonical group on retain',async(version)=>{
 const f=await fixture();
 const before=await seed(f.goal,[legacyEntry(interval(f.page,0,80),focusAt(f,0)),
  legacyEntry(interval(f.page,40,120),focusAt(f,1),'2026-10-12T19:01:00.000Z')],version);
 expect(before.entries[0].key).not.toBe(before.entries[1].key);
 const ledger=await read(f);
 expect(ledger.entries).toHaveLength(1);
 expect(ledger.entries[0].pages).toEqual([{offset:0,text:f.page.text.slice(0,120)}]);
 expect(await stored(f.goal)).toEqual(before);
 await retainManagementEvidence(pool,f.goal,{results:[interval(f.page,120,200)]},noArtifacts,focusAt(f,2));
 const after=await stored(f.goal);
 expect(after.version).toBe(2);expect(after.entries).toHaveLength(1);
 expect(after.entries[0].focus).toEqual(f.focus);
 expect(JSON.parse(after.entries[0].pages[0].textJson)).toBe(f.page.text.slice(0,200));
});

it.each(['overlap','totalCharacters'])('refuses a legacy %s conflict before writing and leaves all previous evidence unchanged',async(conflict)=>{
 const f=await fixture(),first=legacyEntry(interval(f.page,0,80),focusAt(f,0));
 const second=legacyEntry(interval(f.page,40,120),focusAt(f,1));
 if(conflict==='overlap')second.pages[0].text='!'+second.pages[0].text.slice(1);
 else second.totalCharacters++;
 const before=await seed(f.goal,[first,second]);
 await expect(read(f)).rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
 await expect(retainManagementEvidence(pool,f.goal,{results:[interval(f.page,120,200)]},noArtifacts,f.focus))
  .rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
 expect(await stored(f.goal)).toEqual(before);
});

it('also preserves the entire ledger when a new page conflicts after a compatible legacy merge',async()=>{
 const f=await fixture();
 const before=await seed(f.goal,[legacyEntry(interval(f.page,0,80),focusAt(f,0)),legacyEntry(interval(f.page,40,120),focusAt(f,1))]);
 const page={...interval(f.page,100,180),text:'!'+f.page.text.slice(101,180)};
 await expect(retainManagementEvidence(pool,f.goal,{results:[page]},noArtifacts,focusAt(f,2)))
  .rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
 expect(await stored(f.goal)).toEqual(before);
});

it('never merges different cases, scopes, focal tasks, attempts, runs, hashes or unbound legacy entries',async()=>{
 const f=await fixture(),taskPage=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,
  {kind:'task',taskId:f.task,expectedAttempt:1,offset:0});
 const page=interval(taskPage,0,60),base=legacyEntry(page,f.focus);
 const entries=[base,
  legacyEntry(page,{...f.focus,caseId:randomUUID()}),
  legacyEntry(page,{...f.focus,scope:'ledger/another'}),
  legacyEntry(page,{...f.focus,taskId:randomUUID()}),
  legacyEntry({...page,request:{...page.request,expectedAttempt:2}},f.focus),
  legacyEntry({...page,runId:randomUUID()},f.focus),
  legacyEntry({...page,digest:'a'.repeat(64)},f.focus),
  legacyEntry({...page,snapshotDigest:'b'.repeat(64)},f.focus),
  legacyEntry(page,undefined)];
 await seed(f.goal,entries);
 await retainManagementEvidence(pool,f.goal,{results:[]},noArtifacts,f.focus);
 const value=await stored(f.goal);
 expect(value.entries).toHaveLength(entries.length);
 expect(new Set(value.entries.map((entry:any)=>entry.key)).size).toBe(entries.length);
 expect(value.entries.filter((entry:any)=>!entry.focus)).toHaveLength(1);
 // The reader keeps its ordinary same-goal/current-attempt validation; keys do
 // not turn different source identities into reusable evidence.
 const current=await read(f);
 expect(current.entries).toHaveLength(2); // Bound + legitimate legacy unbound.
 expect(current.entries.every((entry:any)=>entry.runId===f.run&&entry.digest===taskPage.digest)).toBe(true);
 expect((await readManagementEvidenceLedger(pool,randomUUID(),f.focus.scope,f.focus,noArtifacts)).entries).toEqual([]);
});

it('keeps goal partitions independent even for identical immutable artifact source metadata',async()=>{
 const a=await fixture(),b=await fixture();
 const page={status:'OK',request:{kind:'artifact',taskId:a.task,expectedAttempt:1,name:'report.txt',offset:0},
  runId:a.run,artifactId:randomUUID(),digest:'c'.repeat(64),totalCharacters:20,text:'first goal'};
 await retainManagementEvidence(pool,a.goal,{results:[page]});
 const before=await stored(a.goal);
 await retainManagementEvidence(pool,b.goal,{results:[{...page,text:'other goal'}]});
 expect(await stored(a.goal)).toEqual(before);
 expect(JSON.parse((await stored(b.goal)).entries[0].pages[0].textJson)).toBe('other goal');
 expect((await readManagementEvidenceLedger(pool,b.goal,a.focus.scope,a.focus,noArtifacts)).entries).toEqual([]);
});

it('merges legacy UTF-16 halves exactly and does not claim an unobserved gap is complete',async()=>{
 const f=await fixture('x'.repeat(100)+'\u{1f600}'+'y'.repeat(300));
 const split=f.page.text.indexOf('\u{1f600}')+1;
 expect(split).toBeGreaterThan(1);
 await seed(f.goal,[legacyEntry(interval(f.page,0,split),focusAt(f,0)),
  legacyEntry(interval(f.page,split,split+40),focusAt(f,1)),
  legacyEntry(interval(f.page,split+60,split+80),focusAt(f,2))]);
 const ledger=await read(f);
 expect(ledger.entries).toHaveLength(1);
 expect(ledger.entries[0].complete).toBe(false);
 expect(ledger.entries[0].pages).toEqual([{offset:0,text:f.page.text.slice(0,split+40)},
  {offset:split+60,text:f.page.text.slice(split+60,split+80)}]);
 expect(ledger.entries[0].pages[0].text).not.toContain('\ufffd');
 await retainManagementEvidence(pool,f.goal,{results:[interval(f.page,split+40,split+60)]},noArtifacts,focusAt(f,3));
 expect((await read(f)).entries[0].pages).toEqual([{offset:0,text:f.page.text.slice(0,split+80)}]);
});

it('does not join another source through a forged legacy key',async()=>{
 const f=await fixture(),a=legacyEntry(interval(f.page,0,40),focusAt(f,0));
 const b=legacyEntry({...interval(f.page,40,80),digest:'d'.repeat(64)},focusAt(f,1));
 b.key=a.key;
 await seed(f.goal,[a,b]);
 await retainManagementEvidence(pool,f.goal,{results:[interval(f.page,40,80)]},noArtifacts,f.focus);
 const value=await stored(f.goal);
 expect(value.entries).toHaveLength(2);
 expect(value.entries.find((entry:any)=>entry.digest===f.page.digest).pages)
  .toEqual([{offset:0,textJson:JSON.stringify(f.page.text.slice(0,80))}]);
 expect((await read(f)).entries).toHaveLength(1);
});

it.each([false,true])('bounds a legacy union beyond 64000 characters even when the incoming source is different: %s',async(otherSource)=>{
 const text='Large exact task contract. '.repeat(3600),f=await fixture(text);
 const persisted=(await pool.query('SELECT description FROM tasks WHERE id=$1',[f.task])).rows[0].description as string;
 expect(persisted.startsWith(text)).toBe(true);
 const taskPage=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,
  {kind:'task',taskId:f.task,expectedAttempt:1,offset:0});
 expect(taskPage.totalCharacters).toBe(persisted.length);
 const whole={...taskPage,text:persisted};
 await seed(f.goal,[legacyEntry(interval(whole,0,40000),focusAt(f,0)),
  legacyEntry(interval(whole,40000,80000),focusAt(f,1))]);
 await retainManagementEvidence(pool,f.goal,{results:otherSource?[interval(f.page,0,40)]:[]},noArtifacts,f.focus);
 const value=await stored(f.goal),entry=value.entries.find((item:any)=>item.request.kind==='task');
 expect(entry.pages).toHaveLength(1);
 expect(JSON.parse(entry.pages[0].textJson)).toBe(persisted.slice(0,64000));
 expect(entry.omittedCharacters).toBe(16000);
 const delivered=(await read(f)).entries.find((item:any)=>item.request.kind==='task');
 expect(delivered.complete).toBe(false);
 expect(delivered.availableRanges).toEqual([{offset:0,characters:64000}]);
 expect(delivered.continueRequest.offset).toBe(delivered.pages[0].text.length);
});

it('keeps at most eight ranges after merging untouched legacy entries and reports the omitted bytes',async()=>{
 const f=await fixture(),entries=[0,1].map(step=>({...legacyEntry(interval(f.page,0,20),focusAt(f,step)),
  pages:Array.from({length:5},(_,index)=>{const offset=(step*5+index)*40;return {offset,text:f.page.text.slice(offset,offset+20)};})}));
 await seed(f.goal,entries);
 await retainManagementEvidence(pool,f.goal,{results:[]},noArtifacts,f.focus);
 const value=await stored(f.goal);
 expect(value.entries).toHaveLength(1);expect(value.entries[0].pages).toHaveLength(8);
 expect(value.entries[0].pages.map((page:any)=>page.offset)).toEqual([80,120,160,200,240,280,320,360]);
 expect(value.entries[0].omittedCharacters).toBe(40);
 const delivered=(await read(f)).entries[0];
 expect(delivered.complete).toBe(false);expect(delivered.availableRanges).toHaveLength(8);
});
