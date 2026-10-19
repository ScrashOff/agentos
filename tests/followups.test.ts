import { beforeAll, beforeEach, afterAll, it, expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installFollowups, reconcileFollowups } from '../server/followups.js';

beforeAll(async () => {
  await migrate();
  await installFollowups();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
  await seedRegistry();
});
afterAll(async () => {
  await pool.end();
});
async function fixture() {
  const previous = randomUUID(),
    next = randomUUID(),
    task = randomUUID(),
    run = randomUUID();
  for (const [id, state] of [
    [previous, 'active'],
    [next, 'waiting_predecessor'],
  ])
    await pool.query(
      "INSERT INTO goals(id,title,description,project,idempotency_key,state) VALUES($1::uuid,'Goal','Delivery','followup-test',$1::text,$2)",
      [id, state],
    );
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count) VALUES($1,$2,'verify','Verify','Check evidence','qa.backend','accepted','verify','[]',1)",
    [task, previous],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",
    [run, task, randomUUID()],
  );
  await pool.query(
    "INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','test')",
    [randomUUID(), task, run],
  );
  await pool.query(
    'INSERT INTO goal_followups(goal_id,predecessor_id,required_verifiers) VALUES($1,$2,$3)',
    [next, previous, JSON.stringify([task])],
  );
  return { previous, next, task, run };
}
it('waits for actual predecessor completion and activates exactly once across concurrent polls', async () => {
  const f = await fixture();
  expect(await reconcileFollowups()).toEqual([]);
  await expect(pool.query("UPDATE goals SET state='active' WHERE id=$1", [f.next])).rejects.toThrow(
    'FOLLOWUP_PENDING',
  );
  await pool.query("UPDATE goals SET state='completed' WHERE id=$1", [f.previous]);
  const results = await Promise.all([reconcileFollowups(), reconcileFollowups()]);
  expect(results.flat()).toEqual([f.next]);
  expect((await pool.query("SELECT * FROM events WHERE kind='FOLLOWUP_ACTIVATED'")).rowCount).toBe(
    1,
  );
});
it('rejects stale passes and falsely completed predecessors with pending or uncertain work', async () => {
  const f = await fixture();
  await pool.query("UPDATE goals SET state='completed' WHERE id=$1", [f.previous]);
  await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1', [f.task]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,2,'failed',$3,'harness',1)",
    [randomUUID(), f.task, randomUUID()],
  );
  expect(await reconcileFollowups()).toEqual([]);
  await pool.query("UPDATE tasks SET state='running' WHERE id=$1", [f.task]);
  expect(await reconcileFollowups()).toEqual([]);
});
it.each(['paused', 'cancelled'])('does not resume a %s followup', async (state) => {
  const f = await fixture();
  await pool.query("UPDATE goals SET state='completed' WHERE id=$1", [f.previous]);
  await pool.query('UPDATE goals SET state=$2 WHERE id=$1', [f.next, state]);
  expect(await reconcileFollowups()).toEqual([]);
});
