import {afterAll,afterEach,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {mkdir,writeFile,rm} from 'node:fs/promises';
import path from 'node:path';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision} from '../server/supervision.js';
import {makeApp} from '../server/app.js';
import {operationsChanges,operationsEvidence,operationsInspect,operationsRead,operationsSnapshot} from '../server/operations-api.js';
import {authorizeOperationsRequest,decodeOperationsCursor,encodeOperationsCursor,operationDigest,operationHash,
 operationsPrincipal,redactOperationsText,type OperationsPrincipal} from '../server/operations-contract.js';

const operator:OperationsPrincipal={id:'test-operator',goalIds:null,projects:[],scopes:['observe','evidence']};
const originalAccess=process.env.AGENTOS_OPERATIONS_ACCESS_JSON;
const files:string[]=[];
beforeAll(async()=>{
 expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
 await migrate();await installSupervision();
});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();delete process.env.AGENTOS_OPERATIONS_ACCESS_JSON;});
afterEach(async()=>{for(const file of files.splice(0))await rm(file,{force:true});
 if(originalAccess===undefined)delete process.env.AGENTOS_OPERATIONS_ACCESS_JSON;else process.env.AGENTOS_OPERATIONS_ACCESS_JSON=originalAccess;});
afterAll(()=>pool.end());

async function fixture(){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),context=randomUUID();
 const frozenDescription='FROZEN-PRIVATE-INSTRUCTION '+ 'PRIVATE-INSTRUCTION-'.repeat(5000);
 const inputBytes=Buffer.from('PRIVATE-INPUT-BASE64'),inputBase64=inputBytes.toString('base64');
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'A bounded objective','Private objective source','operations-test',$1::text)",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
  VALUES($1,$2,'work','Task one',$4,'qa.backend','running','work',
  '[{"id":"proof","type":"artifact","name":"proof.json","label":"Proof"}]',$3,1)`,
  [task,goal,JSON.stringify({kind:'harness',workspace:'operations/source',inputs:[{id:randomUUID(),name:'context.txt',digest:operationHash(inputBytes),base64:inputBase64}]}),frozenDescription]);
 // The normal database trigger validates input bytes and freezes the canonical
 // task. Never invent a snapshot or disable its immutable-history guard.
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,model,started_at,context_id)
  VALUES($1,$2,1,$3,1,'running','harness','glm-5.3-flash',now(),$4)`,[run,task,fence,context]);
 await pool.query("UPDATE tasks SET description='CURRENT-PRIVATE-INSTRUCTION' WHERE id=$1",[task]);
 return {goal,task,run,fence,context,frozenDescription,inputBase64};
}
async function artifact(f:Awaited<ReturnType<typeof fixture>>,text='{"passed":false}',mime='application/json'){
 const id=randomUUID(),bytes=Buffer.from(text),digest=operationHash(bytes);
 await pool.query('INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',[id,f.run,'proof.json',mime,digest,bytes.length]);
 return {id,bytes,digest};
}
const evidenceRequest=(f:Awaited<ReturnType<typeof fixture>>,a:{id:string;digest:string},extra={})=>
 ({goalId:f.goal,reference:{kind:'artifact',artifactId:a.id,expectedDigest:a.digest},...extra});
async function setting(goalId:string,kind:string,value:unknown){
 await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value',
  [kind+':'+goalId,JSON.stringify(value)]);
}

it('returns bounded source-free state and preserves the distinction between execution and delivery',async()=>{
 const f=await fixture();
 await pool.query('INSERT INTO settings VALUES($1,$2)', ['operation-progress:'+f.goal,JSON.stringify({observedAt:'2026-10-17T00:00:00Z',
  delivery:{integrated:0,verified:0,total:5},stagnation:{detected:true,unchangedReviews:4,threshold:3},inputDigests:['PRIVATE-INPUT-DIGEST']})]);
 await setting(f.goal,'work-frontier',{complete:false,unchangedReviews:4,observedAt:'2026-10-17T00:00:00Z',
  receipts:[{private:'PRIVATE-FRONTIER-RECEIPT'}],entries:[{instruction:'PRIVATE-FRONTIER-ENTRY'}]});
 const snapshot=await operationsSnapshot(f.goal,operator),body=JSON.stringify(snapshot);
 expect(snapshot.summary.executions.live).toHaveLength(1);expect(snapshot.summary.delivery).toMatchObject({integrated:0,verified:0,total:5,stagnation:true});
 expect(snapshot.freshness.heartbeatIsProgress).toBe(false);expect(snapshot.eventHistoryComplete).toBe(false);
 for(const secret of ['PRIVATE-INSTRUCTION','PRIVATE-INPUT-BASE64',f.inputBase64,'FROZEN-PRIVATE','PRIVATE-INPUT-DIGEST',
  'PRIVATE-FRONTIER-RECEIPT','PRIVATE-FRONTIER-ENTRY'])expect(body).not.toContain(secret);
 expect(Buffer.byteLength(body)).toBeLessThan(16000);
 expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(1);
});
it('reports unavailable progress as unavailable, without claiming any business acceptance',async()=>{
 const f=await fixture(),s=await operationsSnapshot(f.goal,operator);
 expect(s.summary.delivery).toEqual({source:'unavailable',observedAt:null,integrated:null,verified:null,total:null,lastDeliveryAt:null,
  stagnation:null,unchangedReviews:null,stagnationObservation:{source:'unavailable',available:false,observedAt:null,reviewedAt:null,
   reviewRunId:null,complete:null,threshold:null,meaning:'reviews_without_new_final_criterion_receipt'}});
 expect(s.summary.inputChange).toMatchObject({source:'unavailable',available:false,observedAt:null,unchangedReviews:null,stagnation:null});
 expect(s.summary.eta.status).toBe('unknown');expect(s.summary.needsHuman.classification).toBe('not_inferred_from_failure');
 await pool.query('INSERT INTO settings VALUES($1,$2)',['operation-progress:'+f.goal,JSON.stringify({delivery:{total:5}})]);
 const partial=await operationsSnapshot(f.goal,operator);
 expect(partial.summary.delivery).toMatchObject({integrated:null,verified:null,total:5,stagnation:null,unchangedReviews:null});
 expect(partial.summary.inputChange).toMatchObject({source:'persisted-operation-progress',available:false,stagnation:null});
});
it('keeps final delivery stagnation when new inputs reset the operation-progress counter, with separate provenance',async()=>{
 const f=await fixture(),deliveryReview=randomUUID(),inputReview=randomUUID();
 await setting(f.goal,'operation-progress',{observedAt:'2026-10-17T04:06:00Z',reviewedAt:'2026-10-17T04:06:02Z',lastReviewRunId:inputReview,
  delivery:{integrated:1,verified:0,total:5},lastDeliveryAt:'2026-10-13T08:34:11Z',inputFingerprint:'a'.repeat(64),
  stagnation:{detected:false,unchangedReviews:0,threshold:3}});
 const frontier={complete:false,unchangedReviews:3232,observedAt:'2026-10-17T04:04:18Z',reviewedAt:'2026-10-17T04:04:23Z',reviewRunId:deliveryReview};
 await setting(f.goal,'work-frontier',frontier);
 const s=await operationsSnapshot(f.goal,operator);
 expect(s.summary.delivery).toMatchObject({source:'persisted-operation-progress',observedAt:'2026-10-17T04:06:00.000Z',
  integrated:1,verified:0,total:5,lastDeliveryAt:'2026-10-13T08:34:11.000Z',stagnation:true,unchangedReviews:3232,
  stagnationObservation:{source:'persisted-work-frontier',available:true,observedAt:'2026-10-17T04:04:18.000Z',
   reviewedAt:'2026-10-17T04:04:23.000Z',reviewRunId:deliveryReview,complete:false,threshold:3}});
 expect(s.summary.inputChange).toEqual({source:'persisted-operation-progress',available:true,observedAt:'2026-10-17T04:06:00.000Z',
  reviewedAt:'2026-10-17T04:06:02.000Z',lastReviewRunId:inputReview,inputFingerprint:'a'.repeat(64),
  unchangedReviews:0,threshold:3,stagnation:false,meaning:'reviews_without_new_delivery_or_input'});
 expect(s.freshness).toMatchObject({deliveryObservedAt:'2026-10-17T04:06:00.000Z',deliveryStagnationObservedAt:'2026-10-17T04:04:18.000Z',
  inputChangeObservedAt:'2026-10-17T04:06:00.000Z',heartbeatIsProgress:false});
 expect((await pool.query('SELECT value FROM settings WHERE id=$1',['work-frontier:'+f.goal])).rows[0].value).toEqual(frontier);
 expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1',[f.task])).rows[0].n).toBe(0);
});
it.each([
 {complete:false,unchangedReviews:2,stagnation:false},
 {complete:false,unchangedReviews:3,stagnation:true},
 {complete:false,unchangedReviews:100,stagnation:true},
 {complete:true,unchangedReviews:100,stagnation:false},
])('uses the canonical delivery predicate for $unchangedReviews reviews and complete=$complete',async({complete,unchangedReviews,stagnation})=>{
 const f=await fixture();await setting(f.goal,'work-frontier',{complete,unchangedReviews});
 const s=await operationsSnapshot(f.goal,operator);
 expect(s.summary.delivery).toMatchObject({source:'unavailable',integrated:null,verified:null,total:null,stagnation,unchangedReviews,
  stagnationObservation:{source:'persisted-work-frontier',available:true,complete,threshold:3}});
 expect(s.summary.inputChange).toMatchObject({source:'unavailable',available:false,stagnation:null});
});
it.each([
 {complete:false},
 {unchangedReviews:4},
 {complete:'false',unchangedReviews:4},
 {complete:false,unchangedReviews:'4'},
 {complete:false,unchangedReviews:-1},
 {complete:false,unchangedReviews:1.5},
 {complete:false,unchangedReviews:Number.MAX_SAFE_INTEGER+1},
])('does not convert malformed frontier metadata into a healthy or zero-delivery claim: %j',async(frontier)=>{
 const f=await fixture();await setting(f.goal,'work-frontier',frontier);
 await setting(f.goal,'operation-progress',{stagnation:{detected:false,unchangedReviews:0,threshold:3}});
 const s=await operationsSnapshot(f.goal,operator);
 expect(s.summary.delivery).toMatchObject({stagnation:null,unchangedReviews:null,
  stagnationObservation:{source:'persisted-work-frontier',available:false,threshold:null}});
 expect(s.summary.inputChange).toMatchObject({available:true,stagnation:false,unchangedReviews:0});
});
it('detects a frontier-only delivery transition and does not require a task, input or acceptance change',async()=>{
 const f=await fixture();await setting(f.goal,'work-frontier',{complete:false,unchangedReviews:2});
 const before=await operationsSnapshot(f.goal,operator);
 await setting(f.goal,'work-frontier',{complete:false,unchangedReviews:3});
 const after=await operationsChanges(f.goal,operator,before.cursor);
 expect(after.changed).toBe(true);expect(after.changes.map(c=>c.section)).toEqual(['delivery']);
 expect(after.changes[0].current).toMatchObject({stagnation:true,unchangedReviews:3});
 expect((await operationsChanges(f.goal,operator,after.nextCursor)).changes).toEqual([]);
});
it('keeps valid delivery evidence when input metadata is malformed and does not invent its provenance',async()=>{
 const f=await fixture();await setting(f.goal,'work-frontier',{complete:false,unchangedReviews:9});
 await setting(f.goal,'operation-progress',{observedAt:'invalid-time',reviewedAt:'invalid-time',lastReviewRunId:'not-a-uuid',
  inputFingerprint:'PRIVATE-INVALID-FINGERPRINT',stagnation:{detected:false,unchangedReviews:'0',threshold:3}});
 const s=await operationsSnapshot(f.goal,operator);
 expect(s.summary.delivery).toMatchObject({stagnation:true,unchangedReviews:9});
 expect(s.summary.inputChange).toMatchObject({source:'persisted-operation-progress',available:false,observedAt:null,reviewedAt:null,
  lastReviewRunId:null,inputFingerprint:null,unchangedReviews:null,threshold:null,stagnation:null});
 expect(JSON.stringify(s)).not.toContain('PRIVATE-INVALID-FINGERPRINT');
});
it('reports input novelty separately without clearing an unchanged final-delivery counter',async()=>{
 const f=await fixture();await setting(f.goal,'work-frontier',{complete:false,unchangedReviews:8});
 await setting(f.goal,'operation-progress',{inputFingerprint:'a'.repeat(64),stagnation:{detected:true,unchangedReviews:3,threshold:3}});
 const before=await operationsSnapshot(f.goal,operator);
 await setting(f.goal,'operation-progress',{inputFingerprint:'b'.repeat(64),stagnation:{detected:false,unchangedReviews:0,threshold:3}});
 const after=await operationsChanges(f.goal,operator,before.cursor),latest=await operationsSnapshot(f.goal,operator);
 expect(after.changed).toBe(true);expect(after.changes.map(c=>c.section)).toEqual(['inputChange']);
 expect(after.changes[0].current).toMatchObject({stagnation:false,unchangedReviews:0,inputFingerprint:'b'.repeat(64)});
 expect(latest.summary.delivery).toEqual(before.summary.delivery);
 expect(latest.summary.delivery).toMatchObject({stagnation:true,unchangedReviews:8});
});
it('has stable deltas for heartbeats and detects state changes outside displayed detail',async()=>{
 const f=await fixture(),before=await operationsSnapshot(f.goal,operator);
 await pool.query('UPDATE runs SET heartbeat_at=now() WHERE id=$1',[f.run]);
 expect((await operationsChanges(f.goal,operator,before.cursor)).changed).toBe(false);
 const extra=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'hidden','Other','Do work','qa.backend','blocked','[]')`,[extra,f.goal]);
 const after=await operationsChanges(f.goal,operator,before.cursor);
 expect(after.changed).toBe(true);expect(after.changes.some(c=>c.section==='tasks')).toBe(true);
 expect((await operationsChanges(f.goal,operator,after.nextCursor)).changes).toEqual([]);
});
it('detects a late commit even after a higher event id committed before the cursor',async()=>{
 const f=await fixture(),client=await pool.connect();
 try{
  await client.query('BEGIN');
  await client.query("UPDATE tasks SET state='recovery_blocked',updated_at=clock_timestamp() WHERE id=$1",[f.task]);
  const early=(await client.query("INSERT INTO events(kind,entity_id,payload) VALUES('EARLY_UNCOMMITTED',$1,'{}') RETURNING id",[f.goal])).rows[0].id;
  const later=(await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('LATER_COMMITTED',$1,'{}') RETURNING id",[f.goal])).rows[0].id;
  expect(BigInt(later)).toBeGreaterThan(BigInt(early));
  const before=await operationsSnapshot(f.goal,operator);
  await client.query('COMMIT');
  const after=await operationsChanges(f.goal,operator,before.cursor);
  expect(after.changed).toBe(true);expect(after.changes.find(c=>c.section==='tasks')?.current.states).toContainEqual({state:'recovery_blocked',count:1});
  expect(after.semantics).toBe('current_state_reconciliation');expect(after.eventHistoryComplete).toBe(false);
 }finally{await client.query('ROLLBACK').catch(()=>{});client.release();}
});
it('detects new immutable evidence and acceptance without relying on task updated_at',async()=>{
 const f=await fixture(),before=await operationsSnapshot(f.goal,operator);await artifact(f);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,'[]','fixture')",[randomUUID(),f.task,f.run]);
 const after=await operationsChanges(f.goal,operator,before.cursor);
 expect(after.changes.some(c=>c.section==='evidence')).toBe(true);
 expect(after.changes.find(c=>c.section==='evidence')?.recentRuns?.[0].id).toBe(f.run);
});
it('detects a deferred canonical rejection recorded without changing the task or existing case, and drops stale identities',async()=>{
 const f=await fixture(),caseId=randomUUID(),acceptanceId=randomUUID();
 await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1",[f.run]);
 await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[f.task]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,'[]','fixture')",[acceptanceId,f.task,f.run]);
 await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence,state) VALUES($1,$2,$3,$4,'existing owner','[]','decided')",[caseId,f.goal,f.task,f.run]);
 const unchanged=(await pool.query('SELECT t.updated_at task_updated,c.updated_at case_updated FROM tasks t JOIN supervision_cases c ON c.task_id=t.id WHERE t.id=$1',[f.task])).rows[0];
 const before=await operationsSnapshot(f.goal,operator);
 const value={version:1,authority:'review_only',goalId:f.goal,taskId:f.task,runId:f.run,runFence:f.fence,contextVersion:f.context,
  expectedAttempt:1,acceptanceId,causeDigest:'c'.repeat(64),requestKey:'test-rejection',ownerCaseId:caseId,ownerState:'decided',
  disposition:'deferred_existing_decision',routed:false,observedAt:new Date().toISOString(),acceptance:false,
  rejection:{kind:'repair_source_proof_rejected',code:'REPAIR_SOURCE_PROOF_PATCH_PATH',failedCriterion:{id:'proof'},
   report:{id:randomUUID(),name:'repair-report.json',digest:'a'.repeat(64),size:30},privateDetail:'MUST-NOT-EXPOSE'}};
 await pool.query('INSERT INTO settings VALUES($1,$2)',['canonical-repair-rejection:'+f.run,JSON.stringify(value)]);
 const delta=await operationsChanges(f.goal,operator,before.cursor),supervisor=delta.changes.find(c=>c.section==='supervisor');
 expect(supervisor?.current.pendingRejections).toHaveLength(1);
 expect(supervisor?.current.pendingRejections[0].observation).toMatchObject({authority:'review_only',acceptance:false,
  ownerCaseId:caseId,disposition:'deferred_existing_decision',routed:false,code:'REPAIR_SOURCE_PROOF_PATCH_PATH'});
 expect(JSON.stringify(delta)).not.toContain('MUST-NOT-EXPOSE');
 expect((await pool.query('SELECT t.updated_at task_updated,c.updated_at case_updated FROM tasks t JOIN supervision_cases c ON c.task_id=t.id WHERE t.id=$1',[f.task])).rows[0]).toEqual(unchanged);
 const detail=await operationsInspect(f.goal,operator,'run',f.run);expect(detail.item.pendingRejection.acceptanceId).toBe(acceptanceId);
 await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
 expect((await operationsSnapshot(f.goal,operator)).summary.supervisor.pendingRejections).toEqual([]);
 expect((await operationsInspect(f.goal,operator,'run',f.run)).item.pendingRejection).toBeNull();
});
it('binds signed cursors to principal, grants, objective, expiry and payload integrity',async()=>{
 const f=await fixture(),s=await operationsSnapshot(f.goal,operator);
 expect(()=>decodeOperationsCursor(s.cursor,randomUUID(),operator)).toThrow();
 expect(()=>decodeOperationsCursor(s.cursor,f.goal,{...operator,id:'other'})).toThrow();
 expect(()=>decodeOperationsCursor(s.cursor,f.goal,{...operator,scopes:['observe']})).toThrow();
 expect(()=>decodeOperationsCursor(s.cursor.slice(0,-2)+'aa',f.goal,operator)).toThrow();
 const cursor=encodeOperationsCursor(f.goal,operator,{revision:operationDigest({}),sections:{}},1000);
 expect(()=>decodeOperationsCursor(cursor,f.goal,operator,3601001)).toThrowError(/expirou/);
});
it('executes observation SQL in a read-only transaction',async()=>{
 await expect(operationsRead(db=>db.query("INSERT INTO events(kind,payload) VALUES('FORBIDDEN','{}')"))).rejects.toMatchObject({code:'25006'});
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='FORBIDDEN'")).rows[0].n).toBe(0);
});
it('enforces objective grants on snapshots, exact runs and artifacts',async()=>{
 const a=await fixture(),b=await fixture(),art=await artifact(b);
 const scoped:OperationsPrincipal={id:'goal-a',goalIds:[a.goal],projects:[],scopes:['observe','evidence']};
 await expect(operationsSnapshot(b.goal,scoped)).rejects.toMatchObject({code:'OPERATIONS_SCOPE'});
 await expect(operationsInspect(a.goal,scoped,'run',b.run)).rejects.toMatchObject({code:'NOT_FOUND'});
 await expect(operationsEvidence(scoped,evidenceRequest({...b,goal:a.goal},art),async()=>({bytes:art.bytes}))).rejects.toMatchObject({code:'NOT_FOUND'});
 await expect(operationsEvidence({...scoped,scopes:['observe']},evidenceRequest(a,await artifact(a)))).rejects.toMatchObject({code:'OPERATIONS_SCOPE'});
});
it('returns the exact run identity and canonical failed checks without exposing frozen prompts',async()=>{
 const f=await fixture();await artifact(f);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'fixture')",[randomUUID(),f.task,f.run,
  JSON.stringify([{id:'passed',type:'json_field',passed:false,detail:'PRIVATE-FAILURE-DETAIL'}])]);
 const view=await operationsInspect(f.goal,operator,'run',f.run);
 expect(view.item.binding).toEqual({goalId:f.goal,taskId:f.task,runId:f.run,runFence:f.fence,contextVersion:f.context,expectedAttempt:1});
 expect(view.item.source.instructionDigest).toBe(operationHash(f.frozenDescription));
 expect(view.item.acceptance.passed).toBe(false);expect(view.item.acceptance.checks[0]).toMatchObject({id:'passed',passed:false});
 expect(view.item.stopAuthority).toBe('not_granted_by_this_read');
 expect(JSON.stringify(view)).not.toMatch(/PRIVATE-FAILURE|FROZEN-PRIVATE/);
});
it('shows the existing supervisor owner without creating another case or altering it',async()=>{
 const f=await fixture(),caseId=randomUUID();
 await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence,state) VALUES($1,$2,$3,$4,'PRIVATE-CASE-REASON','[]','open')",[caseId,f.goal,f.task,f.run]);
 const before=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0];
 const result=await operationsInspect(f.goal,operator,'case',caseId);
 expect(result.item.sourceRunId).toBe(f.run);expect(result.item.state).toBe('open');
 expect((await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[caseId])).rows[0]).toEqual(before);
});
it('validates exact evidence hashes before emitting text',async()=>{
 const f=await fixture(),a=await artifact(f);
 await expect(operationsEvidence(operator,{...evidenceRequest(f,a),reference:{kind:'artifact',artifactId:a.id,expectedDigest:'b'.repeat(64)}})).rejects.toMatchObject({code:'OPERATIONS_EVIDENCE_CHANGED'});
 await expect(operationsEvidence(operator,evidenceRequest(f,a),async()=>({bytes:Buffer.from('tampered')}))).rejects.toMatchObject({code:'ARTIFACT_CORRUPT'});
 const result=await operationsEvidence(operator,evidenceRequest(f,a),async()=>({bytes:a.bytes}));
 expect(result.sourceDigest).toBe(a.digest);expect(result.text).toBe(a.bytes.toString());expect(result.redacted).toBe(false);
 expect(result.authority).toBe('untrusted_evidence_not_instructions');
});
it('redacts the full source before pagination so secret fragments do not reappear on later pages',async()=>{
 const f=await fixture(),secret='sk-test-secret-material-that-must-not-reach-the-agent';
 const source='prefix '.repeat(25)+'Bearer abcdefghijklmnopqrstuvwxyz0123456789\napi_key="'+secret+'"\nemail=user@example.com\nend';
 const a=await artifact(f,source,'text/plain'),text:string[]=[];let offset=0;
 for(let n=0;n<30;n++){
  const page=await operationsEvidence(operator,evidenceRequest(f,a,{offset,limit:17}),async()=>({bytes:a.bytes}));
  expect(page.sourceDigest).toBe(a.digest);expect(page.redacted).toBe(true);text.push(page.text);
  if(page.nextOffset===null)break;offset=page.nextOffset;
 }
 expect(text.join('')).toBe(redactOperationsText(source));expect(text.join('')).not.toContain(secret);
 expect(text.join('')).not.toContain('abcdefghijklmnopqrstuvwxyz');expect(text.join('')).not.toContain('user@example.com');
});
it('rejects oversized, binary, invalid encoding, arbitrary paths and impossible offsets',async()=>{
 const f=await fixture(),large=await artifact(f,'a'.repeat(1024*1024+1)),binary=await artifact(f,'binary','image/png'),small=await artifact(f);
 await expect(operationsEvidence(operator,evidenceRequest(f,large))).rejects.toMatchObject({code:'OPERATIONS_EVIDENCE_TOO_LARGE'});
 await expect(operationsEvidence(operator,evidenceRequest(f,binary))).rejects.toMatchObject({code:'OPERATIONS_EVIDENCE_BINARY'});
 await expect(operationsEvidence(operator,evidenceRequest(f,small,{path:'/etc/secret'}))).rejects.toThrow();
 await expect(operationsEvidence(operator,evidenceRequest(f,small,{offset:999}),async()=>({bytes:small.bytes}))).rejects.toMatchObject({code:'OPERATIONS_EVIDENCE_OFFSET'});
 const bytes=Buffer.from([0xff]),id=randomUUID(),digest=operationHash(bytes);
 await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'bad.txt','text/plain',$3,1)",[id,f.run,digest]);
 await expect(operationsEvidence(operator,evidenceRequest(f,{id,digest}),async()=>({bytes}))).rejects.toMatchObject({code:'OPERATIONS_EVIDENCE_ENCODING'});
});
it('limits configured service credentials to explicit read routes and read grants',async()=>{
 const f=await fixture(),token='test-operations-service-credential-12345';
 process.env.AGENTOS_OPERATIONS_ACCESS_JSON=JSON.stringify([{id:'assistant',tokenSha256:operationHash(token),goalIds:[f.goal],scopes:['observe']}]);
 const req={url:'/api/operations/v1/goals/'+f.goal+'/snapshot',method:'GET',headers:{authorization:'Bearer '+token}};
 expect(authorizeOperationsRequest(req)).toBe(true);expect(operationsPrincipal(req)).toMatchObject({id:'assistant',goalIds:[f.goal],scopes:['observe']});
 for(const url of ['/api/tasks/'+f.task+'/retry','/api/tasks/'+f.task+'/interrupt-for-recovery','/api/operations/v1/interventions'])
  expect(authorizeOperationsRequest({...req,url,method:'POST'})).toBe(false);
});
it('integrates actual HTTP routes with legacy auth and scoped read-only service auth',async()=>{
 const f=await fixture(),other=await fixture(),token='test-scoped-read-only-1234567890';
 process.env.AGENTOS_OPERATIONS_ACCESS_JSON=JSON.stringify([{id:'assistant',tokenSha256:operationHash(token),goalIds:[f.goal],scopes:['observe','evidence']}]);
 const app=makeApp(undefined as any);try{
  const url='/api/operations/v1/goals/'+f.goal+'/snapshot';
  expect((await app.inject({method:'GET',url})).statusCode).toBe(401);
  const headers={authorization:'Bearer '+token};
  const result=await app.inject({method:'GET',url,headers});expect(result.statusCode).toBe(200);expect(result.json().summary.goal.id).toBe(f.goal);
  expect((await app.inject({method:'GET',url:'/api/operations/v1/goals/'+other.goal+'/snapshot',headers})).statusCode).toBe(403);
  expect((await app.inject({method:'POST',url:'/api/tasks/'+f.task+'/retry',headers,payload:{checkpoint:'Do not perform',effectsReviewed:true}})).statusCode).toBe(401);
  expect((await app.inject({method:'GET',url,headers:{authorization:'Bearer '+config.apiToken}})).statusCode).toBe(200);
  const capabilities=(await app.inject({method:'GET',url:'/api/operations/v1/capabilities',headers})).json();
  expect(capabilities.implemented).toHaveLength(4);expect(capabilities.mcpHttp).toBe(false);expect(capabilities.mcpStdioBridge).toBe(true);expect(capabilities.mutations.request_intervention).toBe(false);
 }finally{await app.close();}
});
it('reads an actual persisted artifact through the authenticated route and canonical hash reader',async()=>{
 const f=await fixture(),a=await artifact(f,'{"passed":false,"password":"private-password"}');
 const file=path.join(config.dataDir,'artifacts',a.id);await mkdir(path.dirname(file),{recursive:true});await writeFile(file,a.bytes);files.push(file);
 const app=makeApp(undefined as any);try{
  const response=await app.inject({method:'POST',url:'/api/operations/v1/evidence/read',headers:{authorization:'Bearer '+config.apiToken},payload:evidenceRequest(f,a)});
  expect(response.statusCode).toBe(200);expect(response.json().sourceDigest).toBe(a.digest);expect(response.body).not.toContain('private-password');
 }finally{await app.close();}
});
