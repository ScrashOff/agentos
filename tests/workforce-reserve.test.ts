import {randomUUID} from 'node:crypto';
import {beforeAll,afterAll,expect,it} from 'vitest';
import {pool,migrate} from '../server/db.js';
import {reservesWorkforceBacklog,workforceReserveSql} from '../server/workforce-reserve.js';
beforeAll(async()=>{await migrate();});
afterAll(async()=>{await pool.end();});
it('keeps the PostgreSQL reserve count consistent across failure, waits and live reconciliation',async()=>{
 const result=await pool.query(`WITH tasks(id,state) AS (VALUES
 ($1::uuid,'recovery_blocked'),($2::uuid,'failed'),($3::uuid,'waiting_quota'),($4::uuid,'recovering_artifacts'),($5::uuid,'accepted')),
 runs(task_id,state) AS (VALUES($1::uuid,'reconciling'))
 SELECT t.id,t.state,EXISTS(SELECT 1 FROM runs r WHERE r.task_id=t.id AND r.state IN('queued','running','cancelling','reconciling')) has_live_run,
 (${workforceReserveSql}) reserved FROM tasks t`,Array.from({length:5},()=>randomUUID()));
 expect(result.rows.map(x=>x.reserved)).toEqual([true,false,true,true,false]);
 for(const row of result.rows)expect(reservesWorkforceBacklog(row)).toBe(row.reserved);
});
it('keeps dependency waits in the ledger without spending ready reserve, and retains any real live reservation',async()=>{
 const ids=Array.from({length:7},()=>randomUUID());
 const result=await pool.query(`WITH tasks(id,state) AS (VALUES
  ($1::uuid,'failed'),($2::uuid,'accepted'),($3::uuid,'ready'),($4::uuid,'blocked_dependencies'),
  ($5::uuid,'recovery_blocked'),($6::uuid,'blocked_dependencies'),($7::uuid,'ready')),
  dependencies(task_id,depends_on) AS (VALUES($3::uuid,$1::uuid),($4::uuid,$1::uuid),($5::uuid,$1::uuid),($6::uuid,$2::uuid)),
  runs(task_id,state) AS (VALUES($5::uuid,'reconciling'))
  SELECT t.id,t.state,EXISTS(SELECT 1 FROM runs r WHERE r.task_id=t.id AND r.state IN('queued','running','cancelling','reconciling')) has_live_run,
   EXISTS(SELECT 1 FROM dependencies d JOIN tasks parent ON parent.id=d.depends_on WHERE d.task_id=t.id AND parent.state<>'accepted') dependency_blocked,
   (${workforceReserveSql}) reserved FROM tasks t`,ids);
 expect(result.rows.map(row=>row.reserved)).toEqual([false,false,false,false,true,true,true]);
 for(const row of result.rows)expect(reservesWorkforceBacklog(row)).toBe(row.reserved);
});
