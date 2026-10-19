import {randomUUID,createHash} from 'node:crypto';
import {beforeAll,afterAll,it,expect} from 'vitest';
import {pool,migrate} from '../server/db.js';
import {projectManagerValidationHistory} from '../server/manager-validation-history.js';
import {projectManagerReview} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {managementContextRequests,readManagementRetrievalRequest} from '../server/management-retrieval.js';
import {retainManagementEvidence,readManagementEvidenceLedger} from '../server/management-evidence-ledger.js';

const hash=(text:string)=>createHash('sha256').update(text).digest('hex');
const noise=(seed:string,size:number)=>Array.from({length:Math.ceil(size/64)},(_,i)=>hash(seed+':'+i)).join('').slice(0,size);
const noArtifacts=async(_id:string):Promise<{bytes:Buffer}>=>{throw Error('ARTIFACT_IO_FORBIDDEN');};
beforeAll(async()=>{await migrate();});
afterAll(async()=>{await pool.end();});
function fixture(){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID();
 const rows=Array.from({length:5},(_,index)=>({goalId:goal,taskId:task,runId:index===0?run:randomUUID(),
  actionId:randomUUID(),state:'completed',expectedAttempt:74-index,candidateDigest:hash('candidate:'+index),
  unknownControl:{mustRemain:'unknown control '+index},reason:'Preserve all criteria.',
  result:{state:'completed',passed:false,acceptance:false,code:'SOURCE_DRIFT',stale:index>0,
   diagnostic:noise('diagnostic:'+index,11000),sourceDrift:{exact:noise('drift:'+index,6000)},
   resultDigest:hash('result:'+index),unknownAuthority:{doNotChange:true}}}));
 const snapshot={cases:[{id:randomUUID(),taskId:task,currentRun:{id:run,attempt:74,state:'submitted'},evidence:[]}],
  operation:{focus:{taskId:task,scope:'test/scope'},tasks:[{id:task,expectedAttempt:74,stage:'integrate',
   criteria:[{id:'independent-qa',type:'artifact',name:'verification.json'}],intent:{text:'Preserve original unstructured instructions.'}}],
   candidateValidations:rows,requiredDecision:{taskId:task,consumerRunId:run,consumerAttempt:74}}};
 return {goal,task,run,rows,snapshot};
}

it('pages only verbose older terminal diagnostics while retaining the complete current validation and every control field',()=>{
 const f=fixture(),before=structuredClone(f.snapshot),out=projectManagerValidationHistory(f.snapshot);
 expect(out.operation.candidateValidations[0]).toEqual(f.rows[0]);
 for(let i=1;i<5;i++){
  const row=out.operation.candidateValidations[i];
  const {diagnostic,sourceDrift,...control}=row.result;
  const {diagnostic:_diagnostic,sourceDrift:_sourceDrift,...originalControl}=f.rows[i].result;
  expect(control).toEqual(originalControl);
  expect({...row,result:f.rows[i].result}).toEqual(f.rows[i]);
  for(const detail of [diagnostic,sourceDrift])expect(detail).toMatchObject({
   kind:'MANAGER_VALIDATION_HISTORY_DETAIL_V1',complete:false,fullSourcePreserved:true,
   recovery:{request:{kind:'candidate_validation',actionId:f.rows[i].actionId,snapshotDigest:hash(JSON.stringify(f.rows[i])),offset:0}}});
 }
 expect(out.operation.tasks).toEqual(f.snapshot.operation.tasks);
 expect(out.operation.requiredDecision).toEqual(f.snapshot.operation.requiredDecision);
 expect(out.cases).toEqual(f.snapshot.cases);expect(f.snapshot).toEqual(before);
 expect(projectManagerValidationHistory(out)).toEqual(out);
});

it.each(['running','uncertain','unknown','same-attempt','missing-task','referenced-run','referenced-action','oversized'])(
 'keeps the complete %s observation instead of treating it as dispensable history',mode=>{
 const f=fixture(),row=f.rows[1];
 if(['running','uncertain','unknown'].includes(mode))row.state=mode;
 if(mode==='same-attempt')row.expectedAttempt=74;
 if(mode==='missing-task')row.taskId=randomUUID();
 if(mode==='referenced-run')(f.snapshot.operation.requiredDecision as any).sourceRunId=row.runId;
 if(mode==='referenced-action')(f.snapshot.cases[0].evidence as any[]).push({validationActionId:row.actionId});
 if(mode==='oversized')row.result.diagnostic='x'.repeat(1048577);
 expect(projectManagerValidationHistory(f.snapshot).operation.candidateValidations[1]).toEqual(row);
});

it('fits an oversized review without increasing the cap or changing current criteria, decisions or failed outcomes',()=>{
 const f=fixture();(f.snapshot.operation as any).unknownCurrentContract={exact:noise('required',62000)};
 const prefix='MANAGEMENT_V1\n'+noise('instructions',30000)+'\n';
 expect(JSON.stringify(prefix+JSON.stringify(f.snapshot)).length).toBeGreaterThan(150000);
 const review=projectManagerReview(f.snapshot,prefix,0,150000);
 expect(review.withinBudget).toBe(true);
 const out=expandSharedManagerContext(JSON.parse(review.description.slice(prefix.length)));
 expect(out.operation.candidateValidations[0]).toEqual(f.rows[0]);
 expect(out.operation.tasks).toEqual(f.snapshot.operation.tasks);
 expect(out.operation.requiredDecision).toEqual(f.snapshot.operation.requiredDecision);
 expect(out.operation.unknownCurrentContract).toEqual((f.snapshot.operation as any).unknownCurrentContract);
 expect(out.operation.candidateValidations.every((r:any)=>r.result.passed===false&&r.result.acceptance===false)).toBe(true);
});

it('returns exact same-goal pages without the private request, refuses changed digests, and revalidates retained history',async()=>{
 const f=fixture(),row=f.rows[1],key='candidate-validation:'+row.actionId;
 // The reference is computed from the exact canonical jsonb view used by the
 // ordinary production observer; JSON object spelling must agree on both sides.
 await pool.query('INSERT INTO settings VALUES($1,$2)',[key,JSON.stringify({...row,request:{secret:'MUST_NOT_LEAVE_SETTINGS'}})]);
 const canonical=(await pool.query("SELECT value-'request' value FROM settings WHERE id=$1",[key])).rows[0].value;
 const text=JSON.stringify(canonical),request={kind:'candidate_validation' as const,actionId:row.actionId,snapshotDigest:hash(text),offset:0};
 expect(managementContextRequests.safeParse([request]).success).toBe(true);
 expect(managementContextRequests.safeParse([{...request,snapshotDigest:undefined}]).success).toBe(false);
 let restored='',offset:number|null=0;
 while(offset!==null){const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,{...request,offset});
  expect(page.status).toBe('OK');restored+=page.text;offset=page.nextOffset;}
 expect(restored).toBe(text);expect(restored).not.toContain('MUST_NOT_LEAVE_SETTINGS');
 expect((await readManagementRetrievalRequest(pool,randomUUID(),noArtifacts,request)).status).toBe('NOT_FOUND');
 expect((await readManagementRetrievalRequest(pool,f.goal,noArtifacts,{...request,offset:text.length+1})).status).toBe('PAGE_OUT_OF_RANGE');
 const page=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,request),focus={caseId:f.snapshot.cases[0].id,taskId:f.task,scope:'test/scope'};
 await retainManagementEvidence(pool,f.goal,{results:[page]},noArtifacts,focus);
 const cache=await readManagementEvidenceLedger(pool,f.goal,'test/scope',focus,noArtifacts);
 expect(cache.entries).toHaveLength(1);expect(cache.entries[0].pages[0].text).toBe(page.text);
 expect((await readManagementEvidenceLedger(pool,f.goal,'test/scope',{...focus,caseId:randomUUID()},noArtifacts)).entries).toEqual([]);
 await pool.query("UPDATE settings SET value=jsonb_set(value,'{result,code}','\"CHANGED\"') WHERE id=$1",[key]);
 const stale=await readManagementRetrievalRequest(pool,f.goal,noArtifacts,request);
 expect(stale.status).toBe('STALE_CONTEXT');expect(stale.text).toBeUndefined();
 expect((await readManagementEvidenceLedger(pool,f.goal,'test/scope',focus,noArtifacts)).entries).toEqual([]);
 await expect(retainManagementEvidence(pool,f.goal,{results:[page]},noArtifacts,focus)).rejects.toThrow('MANAGEMENT_EVIDENCE_CONFLICT');
});
