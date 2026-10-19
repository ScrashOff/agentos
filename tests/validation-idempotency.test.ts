import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {validateTask} from '../server/service.js';
import {readPendingValidations,readValidationSnapshot,validationReceiptId} from '../server/validation-identity.js';

const criterion={id:'result',type:'json_field',name:'result.json',field:'verified',equals:true,label:'Verified result'};
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await mkdir(path.join(config.dataDir,'artifacts'),{recursive:true});
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'validation-receipt:%'");
  await seedRegistry();
});
afterAll(()=>pool.end());

async function fixture(options:{criteria?:any[];historical?:number;value?:boolean}={}){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),attempt=(options.historical??0)+1;
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Validation','Preserve evidence','validation-idempotency',$1::text)",[goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count)
    VALUES($1,$2,'task','Validation task','Original contract','qa.backend','submitted',$3,$4)`,
    [task,goal,JSON.stringify(options.criteria??[criterion]),attempt]);
  const historical=[];
  for(let n=1;n<attempt;n++){
    const id=randomUUID();historical.push(id);
    await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version)
      VALUES($1,$2,$3,'submitted',$4,'harness',1)`,[id,task,n,randomUUID()]);
  }
  await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version)
    VALUES($1,$2,$3,'submitted',$4,'harness',1)`,[run,task,attempt,fence]);
  if(options.value!==undefined)await artifact(run,options.value);
  return{goal,task,run,fence,attempt,historical};
}
async function artifact(run:string,value:boolean,name='result.json'){
  const id=randomUUID(),bytes=Buffer.from(JSON.stringify({verified:value})),digest=createHash('sha256').update(bytes).digest('hex');
  await writeFile(path.join(config.dataDir,'artifacts',id),bytes,{flag:'wx'});
  await pool.query('INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
    [id,run,name,'application/json',digest,bytes.length]);
  return id;
}
async function counts(task:string){
  const a=(await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1',[task])).rows[0].n;
  const e=(await pool.query("SELECT count(*)::int n FROM events WHERE entity_id=$1 AND kind IN('VALIDATION_PENDING','TASK_ACCEPTED')",[task])).rows[0].n;
  return{acceptances:a,events:e};
}

it('nominates the current attempt once despite five historical submitted runs, then becomes idle',async()=>{
  const f=await fixture({historical:5,value:false});
  expect(await readPendingValidations()).toEqual([{id:f.task}]);
  expect(await validateTask(f.task)).toMatchObject({passed:false,duplicate:false});
  for(let n=0;n<3;n++){
    expect(await readPendingValidations()).toEqual([]);
    expect(await validateTask(f.task)).toMatchObject({passed:false,duplicate:true});
  }
  expect(await counts(f.task)).toEqual({acceptances:1,events:1});
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=ANY($1::uuid[])',[f.historical])).rows[0].n).toBe(0);
});

it.each([false,true])('serializes concurrent validators into one receipt and preserves the %s result',async value=>{
  const f=await fixture({value});
  const results=await Promise.all(Array.from({length:5},()=>validateTask(f.task)));
  expect(results.every(r=>r.passed===value)).toBe(true);
  expect(results.filter(r=>!r.duplicate)).toHaveLength(1);
  expect(await counts(f.task)).toEqual({acceptances:1,events:1});
});

it('re-evaluates newly published immutable proof after a negative result and retains its history',async()=>{
  const f=await fixture();
  await validateTask(f.task);const original=(await pool.query('SELECT * FROM acceptances WHERE task_id=$1',[f.task])).rows[0];
  await artifact(f.run,true);
  expect(await readPendingValidations()).toEqual([{id:f.task}]);
  expect(await validateTask(f.task)).toMatchObject({passed:true,duplicate:false});
  expect((await pool.query('SELECT * FROM acceptances WHERE id=$1',[original.id])).rows[0]).toEqual(original);
  expect(await counts(f.task)).toEqual({acceptances:2,events:2});
  expect(await readPendingValidations()).toEqual([]);
});

it('separates explicit review attestation from deterministic validation',async()=>{
  const f=await fixture({value:true,criteria:[criterion,{id:'review',type:'review',label:'Independent review'}]});
  expect((await validateTask(f.task)).passed).toBe(false);
  expect(await readPendingValidations()).toEqual([]);
  expect(await validateTask(f.task,'reviewer:test')).toMatchObject({passed:true,duplicate:false});
  expect(await validateTask(f.task,'reviewer:test')).toMatchObject({passed:true,duplicate:true});
  expect(await counts(f.task)).toEqual({acceptances:2,events:2});
});

it('does not let an older receipt hide subsequently recorded feedback',async()=>{
  const f=await fixture({value:false});await validateTask(f.task);
  const feedback=randomUUID();
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,false,'[]','later-feedback')`,[feedback,f.task,f.run]);
  expect(await readPendingValidations()).toEqual([{id:f.task}]);
  expect((await validateTask(f.task)).duplicate).toBe(false);
  expect((await pool.query('SELECT reviewer FROM acceptances WHERE id=$1',[feedback])).rows[0].reviewer).toBe('later-feedback');
  expect(await counts(f.task)).toEqual({acceptances:3,events:2});
});

it('revalidates contract and fence changes and refuses receipt reuse across validator versions',async()=>{
  const f=await fixture({value:false});await validateTask(f.task);
  await pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1',[f.task,JSON.stringify([{...criterion,label:'Refined label'}])]);
  expect(await readPendingValidations()).toEqual([{id:f.task}]);await validateTask(f.task);
  await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
  expect(await readPendingValidations()).toEqual([{id:f.task}]);await validateTask(f.task);
  const snapshot=await readValidationSnapshot(pool,f.task);
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{input,version}','\"deterministic-contract/old\"') WHERE id=$1",[validationReceiptId(snapshot!)]);
  expect(await readPendingValidations()).toEqual([{id:f.task}]);await validateTask(f.task);
  expect(await counts(f.task)).toEqual({acceptances:4,events:4});
});

it('never validates a historical result when the current attempt is active or missing',async()=>{
  const f=await fixture({historical:5,value:false});
  await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
  expect(await readPendingValidations()).toEqual([]);
  await expect(validateTask(f.task)).rejects.toMatchObject({code:'NOT_SUBMITTED'});
  await pool.query('UPDATE tasks SET attempt_count=attempt_count+1 WHERE id=$1',[f.task]);
  expect(await readPendingValidations()).toEqual([]);
  expect(await counts(f.task)).toEqual({acceptances:0,events:0});
});

// Hold the real authority lock until validation has read bytes and queued its
// commit, then change exactly one input. No timing-only race or mocked CAS.
it.each(['criteria','artifact','fence','attempt','feedback'] as const)('rejects a stale %s identity after artifact I/O',async mutation=>{
  const f=await fixture({value:false}),owner=await pool.connect();
  let pending:Promise<any>|undefined;
  try{
    await owner.query('BEGIN');await owner.query('SELECT pg_advisory_xact_lock(4280001)');
    const ownerPid=(await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
    pending=validateTask(f.task).then(value=>({value}),error=>({error}));
    let blocked=false;
    for(let n=0;n<200;n++){
      // pg_locks spans all databases. A production waiter using the same lock
      // key must not release this barrier before our validator reaches commit.
      // Query live lock state, avoiding pg_stat_activity's transaction snapshot.
      blocked=!!(await owner.query(`SELECT 1 FROM pg_locks waiter
        WHERE waiter.locktype='advisory' AND waiter.classid=0 AND waiter.objid=4280001
          AND waiter.objsubid=1 AND NOT waiter.granted
          AND waiter.database=(SELECT oid FROM pg_database WHERE datname=current_database())
          AND $1::int=ANY(pg_blocking_pids(waiter.pid)) LIMIT 1`,[ownerPid])).rowCount;
      if(blocked)break;await delay(10);
    }
    expect(blocked).toBe(true);
    if(mutation==='criteria')await owner.query('UPDATE tasks SET criteria=$2 WHERE id=$1',[f.task,JSON.stringify([{...criterion,equals:false}])]);
    if(mutation==='artifact')await owner.query('UPDATE artifacts SET digest=$2 WHERE run_id=$1',[f.run,'f'.repeat(64)]);
    if(mutation==='fence')await owner.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.run,randomUUID()]);
    if(mutation==='attempt')await owner.query('UPDATE tasks SET attempt_count=attempt_count+1 WHERE id=$1',[f.task]);
    if(mutation==='feedback')await owner.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
      VALUES($1,$2,$3,false,'[]','concurrent-review')`,[randomUUID(),f.task,f.run]);
    await owner.query('COMMIT');
    expect((await pending).error).toMatchObject({code:'STATE_CHANGED'});
    expect(await counts(f.task)).toEqual({acceptances:mutation==='feedback'?1:0,events:0});
  }finally{
    await owner.query('ROLLBACK');owner.release();await pending;
  }
});
