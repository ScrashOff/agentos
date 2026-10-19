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
async function fixture(text='Exact decision evidence — preserve the consumer and its acceptance contract.'){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Ledger','Decision context','evidence-ledger',$1::text)",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
  VALUES($1,$2,'consumer','Consumer','Original task contract','research.web','failed','work','[]',
  '{"baseWorkspace":"ledger/source","workKind":"repair"}',1)`,[task,goal]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'failed','harness')",[run,task,randomUUID()]);
 await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,'Decision needed',$5)",
  [caseId,goal,task,run,JSON.stringify([{kind:'CURRENT_DECISION',text}])]);
 const row=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
 const source=managerCaseEvidenceSource(row)!;
 const request={kind:'case_evidence' as const,caseId,evidenceIndex:0,digest:source.digest,offset:0};
 return {goal,task,run,caseId,request,focus:{caseId,taskId:task,scope:'ledger/source'}};
}

it('retains mutable case pages without inventing a run and revalidates them before a later review',async()=>{
 const f=await fixture();
 const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,f.request);
 expect(page).not.toHaveProperty('runId');
 await retainManagementEvidence(pool,f.goal,{results:[page]},noArtifacts,f.focus);
 const retained=await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts);
 expect(retained.entries).toHaveLength(1);
 expect(retained.entries[0]).toMatchObject({complete:true,digest:page.digest,pages:[{offset:0,text:page.text}]});
 expect((await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,{...f.focus,caseId:randomUUID()},noArtifacts)).entries).toEqual([]);
 expect((await readManagementEvidenceLedger(pool,randomUUID(),f.focus.scope,f.focus,noArtifacts)).entries).toEqual([]);
 await pool.query("UPDATE supervision_cases SET evidence=evidence||'[ {\"kind\":\"NEW_CURRENT_EVIDENCE\"} ]'::jsonb WHERE id=$1",[f.caseId]);
 expect((await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts)).entries).toEqual([]);
});

it('coalesces exact overlapping intervals and preserves missing ranges without claiming full delivery',async()=>{
 const f=await fixture('Unicode 😀 source '.repeat(1200));
 for(const offset of [0,6000]){
  const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,{...f.request,offset});
  await retainManagementEvidence(pool,f.goal,{results:[{...page,text:page.text.slice(0,8000)}]},noArtifacts,f.focus);
 }
 const retained=await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts);
 expect(retained.entries).toHaveLength(1);
 expect(retained.entries[0].complete).toBe(false);
 expect(retained.entries[0].availableRanges).toEqual([{offset:0,characters:14000}]);
 expect(retained.entries[0].pages[0].text.length).toBe(14000);
 const original=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,f.request);
 expect(retained.entries[0].pages[0].text.slice(0,original.text.length)).toBe(original.text);
 const stored=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-evidence:'+f.goal])).rows[0].value;
 expect(stored.version).toBe(2);
 expect(JSON.parse(stored.entries[0].pages[0].textJson)).toBe(retained.entries[0].pages[0].text);
 const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,{...f.request,offset:100});
 await expect(retainManagementEvidence(pool,f.goal,{results:[{...page,text:'incorrect bytes'}]},noArtifacts,f.focus))
  .rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
});

it('retains exact UTF-16 page boundaries without replacing a split surrogate and still reads legacy cache',async()=>{
 const f=await fixture('A'.repeat(7960)+'😀'+'B'.repeat(9000));
 const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,f.request);
 const split=page.text.indexOf('😀')+1;
 expect(split).toBeGreaterThan(1);
 const first={...page,text:page.text.slice(0,split)};
 await retainManagementEvidence(pool,f.goal,{results:[first]},noArtifacts,f.focus);
 let retained=await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts);
 expect(retained.entries[0].pages[0].text).toBe(first.text);
 expect(retained.entries[0].pages[0].text.charCodeAt(split-1)).toBe(0xd83d);
 const second=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,{...f.request,offset:split});
 expect(second.text.charCodeAt(0)).toBe(0xde00);
 await retainManagementEvidence(pool,f.goal,{results:[second]},noArtifacts,f.focus);
 retained=await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts);
 const exact=first.text+second.text;
 expect(retained.entries[0].pages[0].text).toBe(exact);
 expect(retained.entries[0].complete).toBe(true);
 const stored=(await pool.query('SELECT value FROM settings WHERE id=$1',['management-evidence:'+f.goal])).rows[0].value;
 const legacy={...stored,version:1,entries:stored.entries.map((entry:any)=>({...entry,
  pages:entry.pages.map((p:any)=>({offset:p.offset,text:JSON.parse(p.textJson)}))}))};
 await pool.query('UPDATE settings SET value=$2 WHERE id=$1',['management-evidence:'+f.goal,JSON.stringify(legacy)]);
 expect((await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts)).entries[0].pages[0].text).toBe(exact);
});

it('rejects stale task text even when the task attempt has not changed',async()=>{
 const f=await fixture(),request={kind:'task' as const,taskId:f.task,expectedAttempt:1,offset:0};
 const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,request);
 await retainManagementEvidence(pool,f.goal,{results:[page]},noArtifacts,f.focus);
 expect((await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts)).entries).toHaveLength(1);
 await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[f.task,'Additional requirement for the same attempt']);
 expect((await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts)).entries).toEqual([]);
});

it('refuses task pages as soon as an actor becomes live',async()=>{
 const f=await fixture(),request={kind:'task_metadata' as const,taskId:f.task,expectedAttempt:1,offset:0};
 const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,request);
 await retainManagementEvidence(pool,f.goal,{results:[page]},noArtifacts,f.focus);
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
 expect((await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts)).entries).toEqual([]);
});

it('keeps a usable exact interval when coalesced decision evidence exceeds the prompt budget',async()=>{
 const f=await fixture('Long source with escaped quotes " and Unicode 😀. '.repeat(900));
 for(const offset of [0,12000,24000,36000]){
  const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,{...f.request,offset});
  await retainManagementEvidence(pool,f.goal,{results:[page]},noArtifacts,f.focus);
 }
 const retained=await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts);
 expect(JSON.stringify(retained).length).toBeLessThanOrEqual(24000);
 expect(retained.entries).toHaveLength(1);
 expect(retained.entries[0].complete).toBe(false);
 expect(retained.entries[0].pages[0].text.length).toBeGreaterThan(1000);
 expect(retained.entries[0].continueRequest.offset).toBe(retained.entries[0].pages[0].text.length);
 expect(retained.entries[0].omittedCachedCharacters).toBeGreaterThan(0);
});

it('merges an initial task page and a continuation bound to the returned digest',async()=>{
 const f=await fixture();
 await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[f.task,'A requirement '.repeat(1300)]);
 const first=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,
  {kind:'task',taskId:f.task,expectedAttempt:1,offset:0});
 await retainManagementEvidence(pool,f.goal,{results:[first]},noArtifacts,f.focus);
 const next=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,
  {...first.request,offset:first.nextOffset,snapshotDigest:first.snapshotDigest});
 await retainManagementEvidence(pool,f.goal,{results:[next]},noArtifacts,f.focus);
 const retained=await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts);
 expect(retained.entries).toHaveLength(1);
 expect(retained.entries[0]).toMatchObject({complete:true,totalCharacters:first.totalCharacters});
});

it('ignores malformed cache requests without hiding valid current evidence',async()=>{
 const f=await fixture(),page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,f.request);
 await retainManagementEvidence(pool,f.goal,{results:[page]},noArtifacts,f.focus);
 await pool.query(`UPDATE settings SET value=jsonb_set(value,'{entries}',value->'entries'||$2::jsonb) WHERE id=$1`,
  ['management-evidence:'+f.goal,JSON.stringify([{request:{kind:'case_evidence',caseId:'not-a-uuid'},pages:[]}])]);
 expect((await readManagementEvidenceLedger(pool,f.goal,f.focus.scope,f.focus,noArtifacts)).entries).toHaveLength(1);
});
