import { randomUUID } from 'node:crypto';
import { beforeAll, afterAll, expect, it } from 'vitest';
import { pool, migrate, tx } from '../server/db.js';
import { installRuntimeLookupIndexes } from '../server/runtime-lookup-indexes.js';
import {seedRegistry} from '../server/registry.js';

beforeAll(async () => { await migrate(); await seedRegistry(); });
afterAll(async () => { await pool.end(); });

it('serializes independent installers and leaves all runtime lookup indexes ready', async () => {
  await Promise.all([installRuntimeLookupIndexes(pool), installRuntimeLookupIndexes(pool)]);
  const rows = (await pool.query(`SELECT c.relname,i.indisvalid,i.indisready FROM pg_index i
    JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname=ANY($1::text[])`, [[
    'artifacts_run_name_idx','acceptances_run_created_idx','events_run_activity_idx','events_kind_created_idx','tasks_goal_scope_idx',
  ]])).rows;
  expect(rows).toHaveLength(5);
  expect(rows.every(x => x.indisvalid && x.indisready)).toBe(true);
});

it('uses a workspace index to select a scope beside large immutable task contexts',async()=>{
  const goal=randomUUID();
  try{
    await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Scoped lookup','Scoped lookup','lookup-test',$2)",[goal,goal]);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
      SELECT gen_random_uuid(),$1,'history-'||n,'History','Preserved','qa.backend','accepted','[]',
        jsonb_build_object('baseWorkspace','lookup-test/scope-'||n,'kind','harness','workKind','diagnostic',
          'contextSnapshot',repeat('immutable worker context ',10000)) FROM generate_series(1,256)n`,[goal]);
    await pool.query('ANALYZE tasks');
    const sql="SELECT id,execution->>'baseWorkspace' scope FROM tasks WHERE goal_id=$1 AND execution->>'baseWorkspace'=$2";
    const rows=(await pool.query(sql,[goal,'lookup-test/scope-17'])).rows;
    expect(rows).toHaveLength(1);
    expect(rows[0].scope).toBe('lookup-test/scope-17');
    const plan=(await pool.query('EXPLAIN (FORMAT JSON) '+sql,[goal,'lookup-test/scope-17'])).rows[0]['QUERY PLAN'];
    expect(JSON.stringify(plan)).toContain('tasks_goal_scope_idx');
    expect((await pool.query("SELECT execution->>'contextSnapshot' context FROM tasks WHERE id=$1",[rows[0].id])).rows[0].context)
      .toBe('immutable worker context '.repeat(10000));
  }finally{
    await pool.query('DELETE FROM tasks WHERE goal_id=$1',[goal]);
    await pool.query('DELETE FROM goals WHERE id=$1',[goal]);
    await pool.query('ANALYZE tasks');
  }
});

it('locates the latest activity for one run without including another run or heartbeat', async () => {
  const run = randomUUID(), other = randomUUID();
  const inserted: string[] = [];
  try {
    for (const [kind, runId, date] of [
      ['OPENCODE_TOOL_EVENT',run,'2026-01-01T00:00:01Z'],
      ['MODEL_RESPONSE',run,'2026-01-01T00:00:02Z'],
      ['MODEL_RESPONSE',other,'2026-01-01T00:00:03Z'],
      ['HEARTBEAT',run,'2026-01-01T00:00:04Z'],
    ]) {
      inserted.push((await pool.query('INSERT INTO events(kind,entity_id,payload,created_at) VALUES($1,$2,$3,$4) RETURNING id',
        [kind,run,JSON.stringify({runId}),date])).rows[0].id);
    }
    // Exercise the selective per-run lookup against a real activity history.
    // Four rows allow PostgreSQL to prefer the equally valid kind index;
    // requiring one physical plan on that empty fixture is nondeterministic.
    inserted.push(...(await pool.query(`INSERT INTO events(kind,entity_id,payload,created_at)
      SELECT 'MODEL_RESPONSE',$1,jsonb_build_object('runId',gen_random_uuid()::text),
        '2026-01-01T00:00:05Z'::timestamptz+g*interval '1 microsecond'
      FROM generate_series(1,4096)g RETURNING id`,[run])).rows.map(row=>row.id));
    await pool.query('ANALYZE events');
    await tx(async db => {
      const sql=`SELECT max(created_at) last_activity FROM events WHERE payload->>'runId'=$1
        AND kind IN('OPENCODE_TOOL_EVENT','HARNESS_TOOL_EVENT','ARTIFACT_CREATED','MODEL_RESPONSE')`;
      const result = (await db.query(sql,[run])).rows[0];
      expect(result.last_activity.toISOString()).toBe('2026-01-01T00:00:02.000Z');
      const explain = (await db.query('EXPLAIN (FORMAT JSON) '+sql,[run])).rows[0]['QUERY PLAN'];
      expect(JSON.stringify(explain)).toContain('events_run_activity_idx');
    });
  } finally {
    await pool.query('DELETE FROM events WHERE id=ANY($1::bigint[])',[inserted]);
    await pool.query('ANALYZE events');
  }
});

it('supports artifact and acceptance lookups by run using the installed indexes', async () => {
  await tx(async db => {
    await db.query('SET LOCAL enable_seqscan=off');
    const run = randomUUID();
    const artifact = (await db.query('EXPLAIN (FORMAT JSON) SELECT id,name,digest FROM artifacts WHERE run_id=$1 AND name=$2',
      [run,'implementation.patch'])).rows[0]['QUERY PLAN'];
    const acceptance = (await db.query('EXPLAIN (FORMAT JSON) SELECT passed FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC LIMIT 1',
      [run])).rows[0]['QUERY PLAN'];
    expect(JSON.stringify(artifact)).toContain('artifacts_run_name_idx');
    expect(JSON.stringify(acceptance)).toContain('acceptances_run_created_idx');
  });
});
