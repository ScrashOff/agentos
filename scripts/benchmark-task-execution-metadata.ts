// Explicit isolated benchmark. Never accepts the production database or starts
// a controller/model; root runs this under the normal test-database lock.
import { randomUUID, randomBytes, createHash } from 'node:crypto';
const url = new URL(process.env.AGENTOS_TEST_DATABASE_URL ?? 'http://invalid');
if (!['127.0.0.1', 'localhost'].includes(url.hostname) || url.port !== '15482' || url.pathname !== '/agentos_test')
  throw Error('BENCHMARK_REQUIRES_EXPLICIT_ISOLATED_DATABASE');
const args = Object.fromEntries(process.argv.slice(2).map(value => {
  const match = /^--(tasks|input-mib)=([0-9]+)$/.exec(value);
  if (!match) throw Error('BENCHMARK_ARGUMENT');
  return [match[1], Number(match[2])];
}));
const count = args.tasks ?? 64, inputMiB = args['input-mib'] ?? 2;
if (count < 1 || count > 128 || inputMiB < 1 || inputMiB > 8) throw Error('BENCHMARK_BOUNDS');
process.env.DATABASE_URL = url.toString();
const { pool, migrate } = await import('../server/db.js');
const { seedRegistry } = await import('../server/registry.js');
const { installTaskExecutionMetadata } = await import('../server/task-execution-metadata.js');
const goalId = randomUUID(), samples: Record<string, any>[] = [];
const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const legacy = `SELECT t.id,CASE WHEN t.execution IS NULL THEN NULL ELSE jsonb_build_object(
  'baseWorkspace',x."baseWorkspace",'workspace',x.workspace,'workKind',x."workKind") END execution
  FROM tasks t LEFT JOIN LATERAL jsonb_to_record(t.execution) AS x("baseWorkspace" text,workspace text,"workKind" text) ON true
  WHERE t.goal_id=$1 ORDER BY t.id`;
const projected = `SELECT t.id,CASE WHEN NOT m.execution_present THEN NULL ELSE jsonb_build_object(
  'baseWorkspace',x."baseWorkspace",'workspace',x.workspace,'workKind',x."workKind") END execution
  FROM tasks t JOIN task_execution_metadata m ON m.task_id=t.id
  LEFT JOIN LATERAL jsonb_to_record(m.control) AS x("baseWorkspace" text,workspace text,"workKind" text) ON true
  WHERE t.goal_id=$1 ORDER BY t.id`;
try {
  await migrate(); await installTaskExecutionMetadata(); await seedRegistry();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Metadata benchmark','Isolated benchmark','metadata-benchmark',$1::text)`, [goalId]);
  const createdAt = performance.now();
  for (let index = 0; index < count; index++) {
    const bytes = randomBytes(inputMiB * 1024 * 1024), id = randomUUID();
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
      VALUES($1::uuid,$2,$1::text,'Benchmark','No runtime','research.requirements','ready','work','[]',$3)`,
    [id, goalId, JSON.stringify({ kind: 'opencode', baseWorkspace: 'benchmark/scope-' + index % 5,
      workspace: 'benchmark/' + id, workKind: 'diagnostic', inputs: [{ id: randomUUID(), name: 'bytes.bin', digest: sha(bytes), base64: bytes.toString('base64') }] })]);
  }
  const insertMs = Math.round(performance.now() - createdAt);
  const oldRows = (await pool.query(legacy, [goalId])).rows, newRows = (await pool.query(projected, [goalId])).rows;
  if (JSON.stringify(oldRows) !== JSON.stringify(newRows) || newRows.length !== count) throw Error('BENCHMARK_SEMANTIC_MISMATCH');
  for (let round = 0; round < 3; round++) for (const [name, sql] of [['legacy', legacy], ['projected', projected]]) {
    const started = performance.now();
    const plan = (await pool.query('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + sql, [goalId])).rows[0]['QUERY PLAN'][0];
    samples.push({ name, round, wallMs: Math.round(performance.now() - started), executionMs: plan['Execution Time'],
      planningMs: plan['Planning Time'], sharedHitBlocks: plan.Plan['Shared Hit Blocks'], sharedReadBlocks: plan.Plan['Shared Read Blocks'] });
  }
  const bytes = (await pool.query(`SELECT sum(octet_length(control::text)+octet_length(input_digests::text))::bigint projection_bytes
    FROM task_execution_metadata WHERE goal_id=$1`, [goalId])).rows[0];
  console.log(JSON.stringify({ ok: true, isolated: true, tasks: count, inputMiB, insertMs,
    resultHash: sha(JSON.stringify(newRows)), projectionBytes: bytes.projection_bytes, samples }));
} finally {
  await pool.query('DELETE FROM tasks WHERE goal_id=$1', [goalId]);
  await pool.query('DELETE FROM goals WHERE id=$1', [goalId]);
  await pool.end();
}
