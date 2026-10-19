import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx,lock} from '../server/db.js';
import {config} from '../server/config.js';
import {backlogObligationIdentity,claimBacklogObligation,findExistingBacklogObligation,type BacklogObligationInput} from '../server/work-obligations.js';
import {assertRetryProgress,retryProgressSignature} from '../server/retry-progress.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');});
afterAll(async()=>{await pool.end();});
async function fixture() {
  const goalId=randomUUID(),consumerId=randomUUID(),sourceId=randomUUID(),artifactId=randomUUID();
  await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('obligation-agent','Test','test','[]','test',1,'Test fixture')");
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Deliver document','Source-bound review','obligation-test',$2)",[goalId,goalId]);
  const consumer={id:consumerId,goal_id:goalId,description:'Publish the verified document',stage:'integrate',covers:['document'],
    criteria:[{id:'verified',label:'Evidence',type:'json_field',name:'report.json',field:'verified',equals:true}],
    execution:{baseWorkspace:'documents/release'}};
  const source={id:sourceId,goal_id:goalId,attempt_count:1,execution:{baseWorkspace:'documents/release'}};
  const input:BacklogObligationInput={goalId,taskId:randomUUID(),consumer,source,workKind:'diagnostic',
    description:'Compare the broken reference against its captured source and publish the exact divergent line.',
    context:{inputs:[{id:artifactId,name:'source.json',digest:'a'.repeat(64)}],
      provenance:[{artifactId,taskId:sourceId,runId:randomUUID(),name:'source.json',role:'accepted_source',digest:'a'.repeat(64)}],omitted:[]}};
  const insert=async(db:any,id:string,state='ready')=>db.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria)
    VALUES($1,$2,$3,'Focal diagnosis','Exact discriminating hypothesis','obligation-agent',$4,'[]')`,[id,goalId,id,state]);
  const claim=(next=input,fail=false)=>tx(async db=>{await lock(db);const receipt=await claimBacklogObligation(db,next);
    if(receipt.code==='OBLIGATION_RESERVED')await insert(db,next.taskId);
    if(fail)throw Error('ROLLBACK_FIXTURE');return receipt;});
  return {input,claim,insert};
}

it('uses the same reservation across task IDs, presentation keys, routes and equivalent artifact IDs',async()=>{
  const f=await fixture();
  const preview=(input=f.input)=>tx(async db=>{
    await db.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    return findExistingBacklogObligation(db,input);
  });
  expect(await preview()).toBeNull();
  expect((await pool.query('SELECT 1 FROM events')).rowCount).toBe(0);
  const first=await f.claim();
  const other=structuredClone(f.input);other.taskId=randomUUID();other.consumer.title='A different label';
  other.consumer.criteria[0].id='new-label-id';other.consumer.criteria[0].label='Renamed';
  other.consumer.account_id='another-account';other.consumer.execution.workforceLane='another-route';
  const artifactId=randomUUID();other.context!.inputs[0].id=artifactId;other.context!.provenance[0].artifactId=artifactId;
  other.context!.provenance[0].runId=randomUUID();
  expect(backlogObligationIdentity(other)).toEqual(backlogObligationIdentity(f.input));
  expect(await preview(other)).toMatchObject({code:'OBLIGATION_REUSE_REQUIRED',taskId:first.taskId});
  expect(await f.claim(other)).toMatchObject({code:'OBLIGATION_REUSE_REQUIRED',taskId:first.taskId});
  expect((await pool.query('SELECT count(*)::int n FROM tasks WHERE goal_id=$1',[f.input.goalId])).rows[0].n).toBe(1);
});

it('serializes competing reservations and rolls a reservation back with failed task creation',async()=>{
  const f=await fixture();
  await expect(f.claim(f.input,true)).rejects.toThrow('ROLLBACK_FIXTURE');
  expect((await pool.query("SELECT 1 FROM events WHERE kind='BACKLOG_OBLIGATION_RESERVED'")).rowCount).toBe(0);
  const receipts=await Promise.all([f.claim(),f.claim({...f.input,taskId:randomUUID()})]);
  expect(receipts.map(r=>r.code).sort()).toEqual(['OBLIGATION_RESERVED','OBLIGATION_REUSE_REQUIRED']);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='BACKLOG_OBLIGATION_BOUND'")).rowCount).toBe(1);
});

it.each(['hypothesis','input','criterion','instruction','work-kind','scope'])('permits a material %s change',async(change)=>{
  const f=await fixture();await f.claim();const next=structuredClone(f.input);next.taskId=randomUUID();
  if(change==='hypothesis')next.description+=' Test the distinct parser hypothesis rather than the transport hypothesis.';
  if(change==='input'){next.context!.inputs[0].digest='b'.repeat(64);next.context!.provenance[0].digest='b'.repeat(64);}
  if(change==='criterion')next.consumer.criteria[0].equals=false;
  if(change==='instruction')next.consumer.description='Publish the independently translated document';
  if(change==='work-kind')next.workKind='repair';
  if(change==='scope'){next.consumer.execution.baseWorkspace='documents/another';next.source.execution.baseWorkspace='documents/another';}
  expect(await f.claim(next)).toMatchObject({code:'OBLIGATION_RESERVED',taskId:next.taskId});
});

it('does not invent equivalence for omitted, mismatched or unbound source evidence',async()=>{
  const f=await fixture();
  for(const mutation of ['missing-source','digest','omission','scope']){
    const next=structuredClone(f.input);
    if(mutation==='missing-source')next.source=null;
    if(mutation==='digest')next.context!.provenance[0].digest='b'.repeat(64);
    if(mutation==='omission')next.context!.omitted=[{role:'accepted_source'}];
    if(mutation==='scope')next.source.execution.baseWorkspace='different/scope';
    expect(await f.claim(next)).toMatchObject({code:'OBLIGATION_UNBOUND'});
  }
  expect((await pool.query('SELECT 1 FROM events')).rowCount).toBe(0);
});

it('does not count quota/transport as functional failure or reset equivalent retry accounting with a new task UUID',async()=>{
  const f=await fixture();await f.claim();
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.input.taskId])).rows[0];
  const run={id:randomUUID(),state:'failed',error_code:'EXECUTION_FAILED'},proof={candidate:{candidateDigest:'a'.repeat(64)}};
  const signature=await retryProgressSignature(pool,task,run,proof.candidate.candidateDigest);
  const identity=backlogObligationIdentity(f.input)!;
  // Historical equivalent task IDs do not provide novelty. These are trusted
  // controller receipts from the same obligation, not worker declarations.
  for(let n=0;n<6;n++)await pool.query(`INSERT INTO events(kind,entity_id,payload) VALUES('OBLIGATION_RETRY_ADMITTED',$1,$2)`,
    [identity.reservationKey,JSON.stringify({signature,taskId:randomUUID()})]);
  await expect(tx(async db=>{await lock(db);await assertRetryProgress(db,task,run,proof);})).rejects.toThrow('REPEATED_NO_PROGRESS');
  for(const error_code of ['QUOTA_EXCEEDED','RATE_LIMIT','MODEL_TRANSPORT_ERROR','MODEL_REQUEST_TIMEOUT'])
    await tx(async db=>{await lock(db);await assertRetryProgress(db,task,{...run,error_code},proof);});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='EXECUTION_RETRY_ADMITTED'")).rowCount).toBe(0);
  // New substantive input remains a valid reason to continue, not a hard cap.
  const changed={...task,execution:{inputs:[{name:'new-source',digest:'b'.repeat(64)}]}};
  await tx(async db=>{await lock(db);await assertRetryProgress(db,changed,run,proof);});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='EXECUTION_RETRY_ADMITTED'")).rowCount).toBe(1);
});
