import { afterAll, beforeAll, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { migrate, pool } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installPublishedRebase } from '../server/published-rebase.js';

beforeAll(async () => {
  await migrate();
  await seedRegistry();
  await installPublishedRebase();
});

afterAll(async () => { await pool.end(); });

it('holds a rebase-owned task/dependency contract and rejects a new run in real PostgreSQL', async () => {
  const c = await pool.connect();
  const goal = randomUUID(), task = randomUUID(), dep = randomUUID(), actionId = randomUUID();
  try {
    await c.query('BEGIN');
    await c.query(
      "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Rebase guard test','Fixture','agentos',$2)",
      [goal, goal],
    );
    const criteria = [{ id: 'same', type: 'json_field', name: 'implementation.json', field: 'readyForValidation', equals: true }];
    const execution = { kind: 'harness', workspace: 'agentos/o-rebase-test', sourceWorkspace: 'agentos/srcfixture', baseWorkspace: 'agentos/base' };
    await c.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
       VALUES($1,$3,'target','Target','Fixture','qa.backend','failed','integrate',$4,$5,2),
             ($2,$3,'dependency','Dependency','Fixture','qa.backend','accepted','work','[]','{}',1)`,
      [task, dep, goal, JSON.stringify(criteria), JSON.stringify(execution)],
    );
    await c.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)', [task, dep]);
    await c.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
      'published-rebase:' + actionId,
      JSON.stringify({ actionId, taskId: task, state: 'proposed', requestDigest: 'a'.repeat(64) }),
    ]);

    await c.query('SAVEPOINT before_run');
    await expect(c.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,3,$3,'harness',1)",
      [randomUUID(), task, randomUUID()],
    )).rejects.toThrow('PUBLISHED_REBASE_OWNS_TASK');
    await c.query('ROLLBACK TO SAVEPOINT before_run');
    await c.query('SAVEPOINT before_task');
    await expect(c.query('UPDATE tasks SET description=$2 WHERE id=$1', [task, 'contract mutation']))
      .rejects.toThrow('PUBLISHED_REBASE_OWNS_TASK');
    await c.query('ROLLBACK TO SAVEPOINT before_task');
    await c.query('SAVEPOINT before_dependency');
    await expect(c.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)', [task, dep]))
      .rejects.toThrow('PUBLISHED_REBASE_OWNS_DEPENDENCIES');
    await c.query('ROLLBACK TO SAVEPOINT before_dependency');
    await c.query('SAVEPOINT before_receipt_rename');
    await expect(c.query('UPDATE settings SET id=$2 WHERE id=$1', ['published-rebase:' + actionId, 'ordinary-setting']))
      .rejects.toThrow('PUBLISHED_REBASE_RECEIPT_IMMUTABLE');
    await c.query('ROLLBACK TO SAVEPOINT before_receipt_rename');
    await c.query('SAVEPOINT before_receipt_delete');
    await expect(c.query('DELETE FROM settings WHERE id=$1', ['published-rebase:' + actionId]))
      .rejects.toThrow('PUBLISHED_REBASE_RECEIPT_IMMUTABLE');
    await c.query('ROLLBACK TO SAVEPOINT before_receipt_delete');
    await c.query('ROLLBACK');
  } finally {
    await c.query('ROLLBACK').catch(() => {});
    c.release();
  }
});

it('allows only the bound source-change CAS and preserves accepted inputs and historical run in PostgreSQL', async () => {
  const c = await pool.connect();
  const goal = randomUUID(), task = randomUUID(), run = randomUUID();
  const deps = Array.from({ length: 30 }, () => randomUUID());
  const fence = randomUUID(), context = randomUUID(), actionId = randomUUID();
  const currentWorkspace = 'agentos/o-prior-rebase-test';
  const targetWorkspace = 'agentos/o-rb-' + actionId.replaceAll('-', '');
  const baselineWorkspace = 'agentos/pub' + 'b'.repeat(64);
  const requestDigest = 'c'.repeat(64), publishedBaselineDigest = 'b'.repeat(64);
  const sourceManifestDigest = 'd'.repeat(64), workspaceManifestDigest = 'e'.repeat(64);
  const preparedReceiptDigest = 'f'.repeat(64);
  try {
    await c.query('BEGIN');
    await c.query(
      "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Rebase CAS test','Fixture','agentos',$2)",
      [goal, goal],
    );
    const criteria = [{ id: 'same', type: 'json_field', name: 'implementation.json', field: 'readyForValidation', equals: true }];
    const execution = { kind: 'harness', workspace: currentWorkspace, sourceWorkspace: 'agentos/srcprior', baseWorkspace: 'agentos/base', continuation: { historical: true } };
    await c.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,max_attempts,reason,next_attempt_at)
       VALUES($1,$2,'target','Target','Fixture','qa.backend','failed','integrate',$3,$4,2,2,'prior failure',now())`,
      [task, goal, JSON.stringify(criteria), JSON.stringify(execution)],
    );
    await c.query(
      `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,max_attempts,reason,next_attempt_at)
       SELECT dep_id,$1,'dependency-'||ordinality,'Dependency','Accepted fixture','qa.backend','accepted','work','[]','{}',1,3,'accepted source',NULL
       FROM unnest($2::uuid[]) WITH ORDINALITY AS deps(dep_id,ordinality)`,
      [goal, deps],
    );
    await c.query('INSERT INTO dependencies(task_id,depends_on) SELECT $1,dep_id FROM unnest($2::uuid[]) AS deps(dep_id)', [task, deps]);
    await c.query(
      "INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,context_id,finished_at) VALUES($1,$2,2,'submitted',$3,'harness',1,$4,now())",
      [run, task, fence, context],
    );
    const beforeRun = (await c.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [run])).rows[0].instruction_snapshot;
    const beforeTask = (await c.query('SELECT description,instruction_revision_id FROM tasks WHERE id=$1', [task])).rows[0];

    const prepared = { receiptDigest: preparedReceiptDigest, sourceChanged: true };
    const ownedReceipt = {
      actionId, taskId: task, goalId: goal, state: 'proposed', requestDigest,
      currentWorkspace, targetWorkspace, baselineWorkspace,
      publishedBaselineDigest, sourceManifestDigest, workspaceManifestDigest,
    };
    await c.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
      'published-rebase:' + actionId, JSON.stringify(ownedReceipt),
    ]);
    await c.query("UPDATE settings SET value=value||'{\"state\":\"preparing\"}'::jsonb WHERE id=$1", ['published-rebase:' + actionId]);
    await c.query("UPDATE settings SET value=value||'{\"state\":\"uncertain\"}'::jsonb WHERE id=$1", ['published-rebase:' + actionId]);
    await c.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1', [
      'published-rebase:' + actionId,
      JSON.stringify({ state: 'prepared', prepared, preparedAt: new Date().toISOString() }),
    ]);
    await c.query("UPDATE settings SET value=value||'{\"state\":\"applying\"}'::jsonb WHERE id=$1", ['published-rebase:' + actionId]);

    const nextExecution = {
      ...execution, workspace: targetWorkspace, sourceWorkspace: baselineWorkspace,
      publishedRebase: {
        version: 1, actionId, requestDigest, publishedBaselineDigest,
        sourceManifestDigest, workspaceManifestDigest, preparedReceiptDigest,
        priorWorkspace: currentWorkspace, baselineWorkspace, workspace: targetWorkspace,
      },
    };
    await c.query(
      "UPDATE tasks SET execution=$2,state='ready',reason='Rebase isolado preparado a partir da publicação verificada; critérios e dependências preservados.',next_attempt_at=NULL,max_attempts=GREATEST(max_attempts,attempt_count+1),updated_at=now() WHERE id=$1",
      [task, JSON.stringify(nextExecution)],
    );
    await c.query("UPDATE settings SET value=value||jsonb_build_object('state','applied','appliedAt',now()) WHERE id=$1", ['published-rebase:' + actionId]);

    const result = await c.query('SELECT * FROM tasks WHERE id=$1', [task]);
    expect(result.rows[0].state).toBe('ready');
    expect(result.rows[0].execution).toEqual(nextExecution);
    expect(result.rows[0].criteria).toEqual(criteria);
    expect(result.rows[0].description).toBe(beforeTask.description);
    expect(result.rows[0].instruction_revision_id).toBe(beforeTask.instruction_revision_id);
    expect(result.rows[0].attempt_count).toBe(2);
    expect(result.rows[0].max_attempts).toBe(3);
    expect(result.rows[0].next_attempt_at).toBeNull();
    expect((await c.query('SELECT depends_on FROM dependencies WHERE task_id=$1 ORDER BY depends_on', [task])).rows)
      .toEqual(deps.sort().map(dep => ({ depends_on: dep })));
    expect((await c.query('SELECT id,state,fence,context_id FROM runs WHERE id=$1', [run])).rows).toEqual([
      { id: run, state: 'submitted', fence, context_id: context },
    ]);
    expect((await c.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [run])).rows[0].instruction_snapshot).toEqual(beforeRun);
    expect((await c.query('SELECT count(*)::int AS count FROM acceptances WHERE task_id=$1', [task])).rows[0].count).toBe(0);
    expect((await c.query("SELECT value->>'state' AS state FROM settings WHERE id=$1", ['published-rebase:' + actionId])).rows[0].state).toBe('applied');

    await c.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,3,$3,'harness',1)",
      [randomUUID(), task, randomUUID()],
    );
    await c.query('ROLLBACK');
  } finally {
    await c.query('ROLLBACK').catch(() => {});
    c.release();
  }
});

