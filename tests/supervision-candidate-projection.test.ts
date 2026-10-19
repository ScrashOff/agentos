import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {pool,migrate,event} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision,detectSupervisionCases,readSupervisionDetectionCandidates} from '../server/supervision.js';
import {supervisionEvidenceFingerprint} from '../server/supervision-wait.js';

beforeAll(async()=>{await migrate();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterEach(()=>vi.restoreAllMocks());
afterAll(()=>pool.end());

type Options={state?:string;kind?:string;count?:number;code?:string;needsAction?:boolean;
  manual?:boolean;autonomy?:boolean;enabled?:boolean;goalState?:string;live?:boolean};
async function candidate(o:Options={}) {
  const goal=randomUUID(),task=randomUUID(),project='supervision-projection-'+goal;
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1::uuid,'Projection','Preserve the original work',$2,$1::text,$3)`,[goal,project,o.goalState??'active']);
  await pool.query('INSERT INTO settings VALUES($1,$2)', ['supervision:'+goal,JSON.stringify({enabled:o.enabled??true})]);
  if(o.autonomy!==undefined)await pool.query('INSERT INTO settings VALUES($1,$2)', ['autonomy:'+project,JSON.stringify({enabled:o.autonomy})]);
  const bytes=Buffer.from('historical worker input with immutable source bytes\n'.repeat(4000));
  const execution={kind:o.kind??'harness',workspace:project+'/worker',baseWorkspace:project+'/base',
    testFile:'qa-protocol/discriminating.test.mjs',...(o.needsAction?{continuation:{state:'needs_action'}}:{}),
    inputs:[{id:randomUUID(),name:'frozen-input.txt',digest:createHash('sha256').update(bytes).digest('hex'),size:bytes.length,base64:bytes.toString('base64')}]};
  const count=o.count??3,attempt=count+(o.live?1:0);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,recovery)
    VALUES($1,$2,'work','Real work','Preserve every criterion','qa.backend',$3,'work',$4,$5,$6,$7)`,
  [task,goal,o.live?'running':o.state??'failed',JSON.stringify([{id:'proof',label:'Real proof',type:'artifact',name:'verification.json'}]),
    JSON.stringify(execution),attempt,JSON.stringify(o.manual?{mode:'manual'}:{mode:'automatic'})]);
  let run='';
  for(let i=1;i<=attempt;i++){
    run=randomUUID();
    await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,summary,instruction_snapshot)
      SELECT $1,t.id,$3,$4,'harness',1,$5,$6,'Original failure evidence',
        jsonb_build_object('task',to_jsonb(t),'executionInputs',$7::jsonb) FROM tasks t WHERE t.id=$2`,
    [run,task,i,randomUUID(),o.live&&i===attempt?'running':'failed',o.code??'MODEL_TIMEOUT',
      JSON.stringify(execution.inputs.map(({base64,...metadata})=>metadata))]);
  }
  return {goal,task,run,attempt,execution};
}
async function existingCase(f:Awaited<ReturnType<typeof candidate>>,state:string,appliedAttempt?:number){
  const id=randomUUID();
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,applied_attempt)
    VALUES($1,$2,$3,$4,$5,'Existing authority','[]',$6)`,[id,f.goal,f.task,f.run||null,state,appliedAttempt??null]);
  if(state==='waiting_evidence'){
    const t=(await pool.query('SELECT id,required_memories,execution FROM tasks WHERE id=$1',[f.task])).rows[0];
    await pool.query('UPDATE supervision_cases SET wait_fingerprint=$2 WHERE id=$1',[id,await supervisionEvidenceFingerprint(pool,t)]);
  }
  return id;
}
async function observeControlReads(){
  const client=await pool.connect(),prototype=Object.getPrototypeOf(client),original=prototype.query;
  client.release();const controls:string[]=[];
  vi.spyOn(prototype,'query').mockImplementation(function(this:any,...args:any[]){
    if(typeof args[0]==='string'&&args[0].includes('SELECT control.kind,'))controls.push(args[1][0]);
    return original.apply(this,args);
  });
  return controls;
}
async function historicalHashes(){
  const rows=(await pool.query(`SELECT id,md5(instruction_snapshot::text) snapshot FROM runs ORDER BY id`)).rows;
  expect(rows.every(r=>typeof r.snapshot==='string'&&r.snapshot.length===32)).toBe(true);
  return rows;
}

it('projects only identities and excludes all pending case states before reading worker controls',async()=>{
  const valid=await candidate(),unsupported=await candidate({kind:'demo'}),closed=await candidate({state:'accepted'});
  const pending=[];
  for(const state of ['open','decided','delegated','waiting_evidence']){
    const f=await candidate();await existingCase(f,state);pending.push(f);
  }
  const disabled=await candidate({enabled:false}),paused=await candidate({autonomy:false}),manual=await candidate({manual:true});
  const explicit=await candidate({manual:true,autonomy:true}),inactive=await candidate({goalState:'paused'});
  const rows=await readSupervisionDetectionCandidates(pool);
  expect(rows.map(t=>t.id).sort()).toEqual([valid.task,unsupported.task,explicit.task].sort());
  expect(rows.every(t=>Object.keys(t).sort().join(',')==='goal_id,id,state')).toBe(true);
  expect(JSON.stringify(rows).length).toBeLessThan(700);
  const controls=await observeControlReads();await detectSupervisionCases();
  expect(controls.sort()).toEqual([valid.task,unsupported.task,explicit.task].sort());
  const cases=(await pool.query("SELECT task_id FROM supervision_cases WHERE state='open'")).rows.map(r=>r.task_id);
  expect(cases).toContain(valid.task);expect(cases).toContain(explicit.task);expect(cases).not.toContain(unsupported.task);
  expect(controls).not.toContain(closed.task);
  for(const f of [...pending,disabled,paused,manual,inactive])expect(controls).not.toContain(f.task);
});

it('keeps cancellation, quota, applied-attempt and proven disk preflight guards ahead of large control reads',async()=>{
  const none=await candidate({count:0}),quota=await candidate({code:'QUOTA_EXCEEDED'}),cancel=await candidate();
  await event(pool,'CANCEL_REQUESTED',cancel.task,{runId:cancel.run});
  const recent=await candidate();await existingCase(recent,'applied',2);
  const disk=await candidate({code:'DISK_CAPACITY'});
  await event(pool,'HARNESS_NOT_STARTED',disk.task,{runId:disk.run,confirmed:true,code:'DISK_CAPACITY'});
  const unprovenDisk=await candidate({code:'DISK_CAPACITY'}),valid=await candidate();
  const before=await historicalHashes(),controls=await observeControlReads();
  await detectSupervisionCases();
  expect(controls.sort()).toEqual([unprovenDisk.task,valid.task].sort());
  const newCases=(await pool.query("SELECT task_id FROM supervision_cases WHERE state='open' ORDER BY task_id")).rows.map(r=>r.task_id);
  expect(newCases).toEqual([unprovenDisk.task,valid.task].sort());
  for(const f of [none,quota,cancel,recent,disk])expect(controls).not.toContain(f.task);
  expect(await historicalHashes()).toEqual(before);
});

it('preserves live attempts, needs_action and the three-failure threshold with identical causal evidence',async()=>{
  const live=await candidate({live:true}),needs=await candidate({count:1,needsAction:true});
  const insufficient=await candidate({count:2}),enough=await candidate();
  const before=await historicalHashes(),controls=await observeControlReads();
  await detectSupervisionCases();
  expect(controls.sort()).toEqual([live.task,needs.task,insufficient.task,enough.task].sort());
  const cases=(await pool.query('SELECT task_id,evidence,reason FROM supervision_cases ORDER BY task_id')).rows;
  expect(cases.map(c=>c.task_id)).toEqual([live.task,needs.task,enough.task].sort());
  expect(cases.find(c=>c.task_id===live.task).evidence.map((e:any)=>e.attempt)).toEqual([3,2,1]);
  expect(cases.find(c=>c.task_id===needs.task).evidence.map((e:any)=>e.attempt)).toEqual([1]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[live.task])).rows[0].state).toBe('running');
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[live.run])).rows[0].state).toBe('running');
  expect(await historicalHashes()).toEqual(before);
  controls.length=0;expect(await detectSupervisionCases()).toEqual([]);
  expect(controls).toEqual([insufficient.task]);
});
