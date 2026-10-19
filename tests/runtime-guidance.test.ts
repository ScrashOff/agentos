import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { installManagement } from '../server/management.js';
import { seedRegistry } from '../server/registry.js';
import { guidanceSchema, proposeRuntimeGuidance, reconcileRuntimeGuidance } from '../server/runtime-guidance.js';

let f: any;

beforeAll(async () => { await migrate(); await installManagement(); });
beforeEach(async () => {
  const goal = randomUUID(), managerTask = randomUUID(), managerRun = randomUUID();
  const taskId = randomUUID(), runId = randomUUID(), fence = randomUUID(), contextVersion = randomUUID();
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'runtime-guidance:%'");
  await seedRegistry();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Guidance','Goal','guidance-test',$2)", [goal,goal]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2),($3,$4)', [
    'management:' + goal, JSON.stringify({ enabled: true }),
    'workforce:' + goal, JSON.stringify({ enabled: true }),
  ]);
  for (const [id, key, state, stage, execution, attempt] of [
    [managerTask, 'manager', 'accepted', 'coordinate', null, 1],
    [taskId, 'target', 'running', 'work', JSON.stringify({ kind: 'harness' }), 1],
  ] as any[]) {
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
      VALUES($1,$2,$3,$3,'Task contract','qa.backend',$4,$5,'[]',$6,$7)`, [id, goal, key, state, stage, execution, attempt]);
  }
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')", [managerRun, managerTask, randomUUID()]);
  await pool.query('INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
    [randomUUID(), managerRun, 'coordination.json', 'application/json', 'a'.repeat(64), 1]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')", [randomUUID(), managerTask, managerRun]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,1,$3,'harness',1,'running',$4)", [runId, taskId, fence, contextVersion]);
  await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)', [contextVersion, runId, '{}', 'immutable task instructions']);
  const evidenceId = (await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('TEST_EVIDENCE',$1,'{}') RETURNING id", [goal])).rows[0].id;
  f = { goal, managerRun, taskId, runId, fence, contextVersion, evidenceId };
});
afterAll(async () => { await pool.end(); });

const proposal = () => ({ kind: 'guide', taskId: f.taskId, expectedAttempt: 1, runId: f.runId,
  runFence: f.fence, contextVersion: f.contextVersion,
  instruction: 'Check the observed failing request and preserve its exact output for the next step.',
  reason: 'The current attempt has a concrete failure that can be addressed without changing its contract.',
  facts: [{ evidenceRef: 'event:' + f.evidenceId, statement: 'The goal evidence records the failing request.' }] });

describe('runtime guidance manager', () => {
  it('does not erase known consumption with stale or foreign later observations',async()=>{
    const p=await proposeRuntimeGuidance(f.goal,f.managerRun,0,proposal());const messageId=randomUUID();
    const valid={actionId:p.actionId,goalId:f.goal,taskId:f.taskId,runId:f.runId,runFence:f.fence,contextVersion:f.contextVersion,state:'consumed',messageId};
    await reconcileRuntimeGuidance(async()=>valid);
    for(const stale of [{...valid,messageId:randomUUID()},{...valid,state:'dispatching',messageId:undefined},{...valid,state:'uncertain'},null]){
      await reconcileRuntimeGuidance(async(method)=>{expect(method).toBe('worker.guidanceReceipt');return stale;});
      const value=(await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value;
      expect(value.state).toBe('consumed');expect(value.remoteMessageId).toBe(messageId);
    }
  });
  it('preserves the in-flight owner receipt when a parallel observer sees a changed binding',async()=>{
    const p=await proposeRuntimeGuidance(f.goal,f.managerRun,0,proposal());
    let entered!:()=>void,finish!:()=>void;
    const started=new Promise<void>(resolve=>entered=resolve),release=new Promise<void>(resolve=>finish=resolve);
    const owner=reconcileRuntimeGuidance(async(method)=>{
      expect(method).toBe('worker.guide');entered();await release;
      return {actionId:p.actionId,goalId:f.goal,taskId:f.taskId,runId:f.runId,runFence:f.fence,contextVersion:f.contextVersion,state:'queued_next_step',messageId:randomUUID()};
    });
    await started;
    try{
      await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.runId,randomUUID()]);
      let observations=0;await reconcileRuntimeGuidance(async()=>{observations++;throw Error('LIVE_OWNER_STOLEN');});
      expect(observations).toBe(0);
      expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('dispatching');
    }finally{finish();await owner;}
    expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('queued_next_step');
  });
  it('recovers a lost guide RPC reply through the durable receipt without replaying the send',async()=>{
    const p=await proposeRuntimeGuidance(f.goal,f.managerRun,0,proposal());const methods:string[]=[];
    await reconcileRuntimeGuidance(async(method)=>{methods.push(method);throw Object.assign(Error('reply lost'),{code:'GUIDANCE_OUTCOME_UNCONFIRMED'});});
    const messageId=randomUUID();let remoteState='queued_next_step';
    const reader=async(method:string)=>{methods.push(method);if(method!=='worker.guidanceReceipt')throw Error('AMBIGUOUS_SEND_REPLAYED');
      return {actionId:p.actionId,goalId:f.goal,taskId:f.taskId,runId:f.runId,runFence:f.fence,contextVersion:f.contextVersion,state:remoteState,messageId};};
    await reconcileRuntimeGuidance(reader);
    expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('queued_next_step');
    remoteState='consumed';await reconcileRuntimeGuidance(reader);
    expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('consumed');
    await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[f.taskId]);
    await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[f.runId]);
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",[randomUUID(),f.taskId,f.runId]);
    await reconcileRuntimeGuidance(reader);
    expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('effectverified');
    expect(methods.filter(x=>x==='worker.guide')).toHaveLength(1);
  });
  it('does not accept foreign receipts or turn a missing ambiguous receipt into another send',async()=>{
    const p=await proposeRuntimeGuidance(f.goal,f.managerRun,0,proposal());const methods:string[]=[];
    await reconcileRuntimeGuidance(async(method)=>{methods.push(method);throw Error('lost reply');});
    for(const override of [null,{runFence:randomUUID()},{contextVersion:randomUUID()},{messageId:undefined}]){
      await reconcileRuntimeGuidance(async(method)=>{methods.push(method);return override===null?null:{actionId:p.actionId,goalId:f.goal,taskId:f.taskId,runId:f.runId,runFence:f.fence,contextVersion:f.contextVersion,state:'consumed',messageId:randomUUID(),...override};});
      expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('uncertain');
    }
    expect(methods.filter(x=>x==='worker.guide')).toHaveLength(1);
    expect(methods.slice(1).every(x=>x==='worker.guidanceReceipt')).toBe(true);
  });

  it('validates the contract and stores one replay-safe proposal before any bridge dispatch', async () => {
    expect(guidanceSchema.safeParse(proposal()).success).toBe(true);
    const results = await Promise.all([
      proposeRuntimeGuidance(f.goal, f.managerRun, 4, proposal()),
      proposeRuntimeGuidance(f.goal, f.managerRun, 4, proposal()),
    ]);
    expect(results[0]).toEqual(results[1]);
    expect(results[0].code).toBe('PROPOSED');
    const actionId = results[0].actionId;
    expect((await pool.query('SELECT value->>\'state\' state FROM settings WHERE id=$1', ['runtime-guidance:' + actionId])).rows[0].state).toBe('proposed');
    let calls: string[] = [];
    const result = await reconcileRuntimeGuidance(async (method: string) => {
      calls.push(method);
      return { actionId, runId: f.runId, runFence: f.fence, goalId: f.goal, taskId: f.taskId,
        contextVersion: f.contextVersion, state: 'queued_next_step', messageId: randomUUID() };
    });
    expect(calls).toEqual(['worker.guide']);
    expect(result.dispatched).toBe(1);
    expect((await pool.query('SELECT value->>\'state\' state FROM settings WHERE id=$1', ['runtime-guidance:' + actionId])).rows[0].state).toBe('queued_next_step');
  });

  it('rejects foreign evidence and stale context; a dispatching crash only checks the receipt', async () => {
    const foreign = randomUUID();
    await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Foreign','Goal','other',$2)", [foreign,foreign]);
    const foreignEvent = (await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('FOREIGN_EVIDENCE',$1,'{}') RETURNING id", [foreign])).rows[0].id;
    const bad = await proposeRuntimeGuidance(f.goal, f.managerRun, 0,
      { ...proposal(), facts: [{ evidenceRef: 'event:' + foreignEvent, statement: 'Unrelated evidence exists.' }] });
    expect(bad.code).toBe('GUIDANCE_EVIDENCE_INVALID');

    const staleProposal = await proposeRuntimeGuidance(f.goal, f.managerRun, 1, proposal());
    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [f.runId, randomUUID()]);
    let methods: string[] = [];
    await reconcileRuntimeGuidance(async (method: string) => { methods.push(method); return null; });
    expect(methods).not.toContain('worker.guide');
    expect((await pool.query('SELECT value->>\'state\' state FROM settings WHERE id=$1', ['runtime-guidance:' + staleProposal.actionId])).rows[0].state).toBe('rejected');

    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [f.runId, f.contextVersion]);
    const crashProposal = await proposeRuntimeGuidance(f.goal, f.managerRun, 5, proposal());
    await pool.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"dispatching\"') WHERE id=$1", ['runtime-guidance:' + crashProposal.actionId]);
    await reconcileRuntimeGuidance(async (method: string) => { methods.push(method); return null; });
    expect(methods.at(-1)).toBe('worker.guidanceReceipt');
  });

  it('keeps stop, cancellation, and manager policy guards authoritative', async () => {
    await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)", [f.taskId, JSON.stringify({ runId: f.runId })]);
    const cancelled = await proposeRuntimeGuidance(f.goal, f.managerRun, 2, proposal());
    expect(cancelled.code).toBe('CANCEL_REQUESTED');
    await pool.query("DELETE FROM events WHERE kind='CANCEL_REQUESTED'");
    await pool.query('UPDATE settings SET value=$2 WHERE id=$1', ['management:' + f.goal, JSON.stringify({ enabled: false })]);
    const disabled = await proposeRuntimeGuidance(f.goal, f.managerRun, 3, proposal());
    expect(disabled.code).toBe('MANAGEMENT_DISABLED');
  });
  it('concurrent reconcilers send once, distinguish consumption and require same-attempt acceptance for effect',async()=>{
    const p=await proposeRuntimeGuidance(f.goal,f.managerRun,0,proposal());
    let sends=0;const messageId=randomUUID();let state='queued_next_step';
    const dispatcher=async(method:string)=>{if(method==='worker.guide'){sends++;await new Promise(r=>setTimeout(r,30));}
      return {...f,actionId:p.actionId,goalId:f.goal,runId:f.runId,runFence:f.fence,taskId:f.taskId,contextVersion:f.contextVersion,state,messageId};};
    await Promise.all([reconcileRuntimeGuidance(dispatcher),reconcileRuntimeGuidance(dispatcher),reconcileRuntimeGuidance(dispatcher)]);
    expect(sends).toBe(1);
    state='consumed';await reconcileRuntimeGuidance(dispatcher);
    expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('consumed');
    await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[f.taskId]);
    await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[f.runId]);
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,'[]','test')",[randomUUID(),f.taskId,f.runId]);
    await reconcileRuntimeGuidance(dispatcher);
    expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('consumed');
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",[randomUUID(),f.taskId,f.runId]);
    await reconcileRuntimeGuidance(dispatcher);
    expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('effectverified');
    expect(sends).toBe(1);
  });
  it('never repeats an ambiguous send and does not mistake a claimed message for consumption',async()=>{
    const p=await proposeRuntimeGuidance(f.goal,f.managerRun,0,proposal());const methods:string[]=[];
    await reconcileRuntimeGuidance(async(method)=>{methods.push(method);throw Object.assign(Error('uncertain'),{code:'GUIDANCE_OUTCOME_UNCONFIRMED'});});
    await reconcileRuntimeGuidance(async(method)=>{methods.push(method);return {...f,actionId:p.actionId,goalId:f.goal,taskId:f.taskId,runFence:f.fence,state:'dispatching'};});
    expect(methods).toEqual(['worker.guide','worker.guidanceReceipt']);
    expect((await pool.query('SELECT value FROM settings WHERE id=$1',['runtime-guidance:'+p.actionId])).rows[0].value.state).toBe('uncertain');
  });
});
