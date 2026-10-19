import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,lock} from '../server/db.js';
import {startRun,markUncertain} from '../server/service.js';
import {seedRegistry} from '../server/registry.js';
import {config} from '../server/config.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
it('starts liveness at admission after waiting for the scheduler lock, rather than at transaction creation',async()=>{
  const goal=randomUUID(),task=randomUUID(),run=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Clock','Clock','test-clock',$2)",[goal,goal]);
  await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'clock','Clock','Clock','qa.backend','queued','[]')",[task,goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'queued')",[run,task,randomUUID()]);
  const owner=await pool.connect();let started:ReturnType<typeof startRun>|undefined;
  try{
    await owner.query('BEGIN');await lock(owner);
    started=startRun(run,'test-clock-worker');
    await vi.waitFor(async()=>expect((await owner.query("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND objid=4280001 AND NOT granted")).rows[0].n).toBeGreaterThan(0));
    const releasedAt=(await owner.query('SELECT clock_timestamp() at')).rows[0].at;
    await owner.query('COMMIT');
    const admitted=await started;
    expect(admitted.started_at.getTime()).toBeGreaterThanOrEqual(releasedAt.getTime());
    expect(admitted.heartbeat_at.getTime()).toBeGreaterThanOrEqual(releasedAt.getTime());
    await markUncertain();
    expect((await pool.query('SELECT state FROM runs WHERE id=$1',[run])).rows[0].state).toBe('running');
  }finally{await owner.query('ROLLBACK');owner.release();await started;}
});
