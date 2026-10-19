import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision} from '../server/supervision.js';
import {requestRecoveryReview} from '../server/recovery-supervision.js';
import {verificationRoutingCode} from '../server/verification-routing-failure.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());
async function fixture(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1,'QA routing','Preserve failed QA',$2,$2,'active')`,[goal,'qa-'+goal]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,'qa','QA','Read-only evidence','qa.backend','recovery_blocked','verify','[]',$3,11)`,
    [task,goal,JSON.stringify({kind:'harness',workspace:'qa/copy',sourceWorkspace:'qa/source',readOnlySource:true})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,instruction_snapshot)
    SELECT $1,t.id,11,$3,'harness',1,'submitted',jsonb_build_object('schema',1,'task',to_jsonb(t)) FROM tasks t WHERE t.id=$2`,[run,task,randomUUID()]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence)
    VALUES($1,$2,$3,$4,'open','Original generic failure','[]')`,[caseId,goal,task,run]);
  return {goal,task,run,caseId};
}
it('keeps known routing causes but never persists arbitrary exception messages',()=>{
  expect(verificationRoutingCode(Error('REPAIR_TARGET_MISSING'))).toBe('REPAIR_TARGET_MISSING');
  expect(verificationRoutingCode({code:'TASK_CHANGED',message:'private-token'})).toBe('TASK_CHANGED');
  expect(verificationRoutingCode(Error('private-token /etc/private'))).toBe('VERIFICATION_ROUTING_UNKNOWN');
});
it('enriches an existing exact case once without changing failed QA, run or acceptance',async()=>{
  const f=await fixture(),before=(await pool.query('SELECT state,instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0];
  for(let n=0;n<3;n++)expect(await requestRecoveryReview(f.task,f.run,'VERIFICATION_ROUTING_FAILED','Retain QA',
    {routingFailureCode:'REPAIR_TARGET_MISSING'})).toEqual({caseId:f.caseId,state:'open'});
  const c=(await pool.query('SELECT evidence,reason FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0];
  expect(c.reason).toBe('Original generic failure');expect(c.evidence).toHaveLength(1);
  expect(c.evidence[0]).toMatchObject({code:'REPAIR_TARGET_MISSING',sourceTaskId:f.task,sourceRunId:f.run,sourceAttempt:11,sourceWorkspaceMatches:true});
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='SUPERVISION_EVIDENCE_RECORDED'")).rows[0].n).toBe(1);
  expect((await pool.query('SELECT state,instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1',[f.task])).rows[0].n).toBe(0);
});
it.each(['stale','cancelled','paused','foreign_case'])('never attaches evidence to an unauthorized %s episode',async mode=>{
  const f=await fixture();let sourceRun=f.run;
  if(mode==='stale')sourceRun=randomUUID();
  if(mode==='cancelled')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.task,JSON.stringify({runId:f.run})]);
  if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='foreign_case')await pool.query('UPDATE supervision_cases SET source_run_id=NULL WHERE id=$1',[f.caseId]);
  await requestRecoveryReview(f.task,sourceRun,'VERIFICATION_ROUTING_FAILED','Retain QA',{routingFailureCode:'VERIFICATION_CANDIDATE_UNBOUND'});
  expect((await pool.query('SELECT evidence FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0].evidence).toEqual([]);
});
