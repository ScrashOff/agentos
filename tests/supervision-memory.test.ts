import { beforeAll, beforeEach, afterAll, it, expect, vi, afterEach } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installSupervision } from '../server/supervision.js';
import { assembleContext, saveMemory, hash } from '../server/memory.js';
import { config } from '../server/config.js';
import * as contextRank from '../server/context-rank.js';
import {
  appliedSupervisionMemoryReferences,
  supervisionMemorySnapshot,
  assertSupervisionMemorySnapshot,
} from '../server/supervision-memory.js';

beforeAll(async () => {
  await migrate();
  await installSupervision();
});
beforeEach(async () => {
  await pool.query('TRUNCATE goals,agents,accounts,events,memories CASCADE');
  await seedRegistry();
});
afterEach(() => {
  vi.restoreAllMocks();
  config.layaUrl = '';
});
afterAll(() => pool.end());

async function fixture(references: any[] = [], required: string[] = []) {
  const goal = randomUUID(),
    task = randomUUID(),
    source = randomUUID(),
    run = randomUUID(),
    caseId = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Current goal','Scope','supervision-memory-test',$2)",
    [goal, goal],
  );
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count,execution)
    VALUES($1,$2,'consumer','consumer','Current integration','qa.backend','running','[]',2,$3)`,
    [task, goal, JSON.stringify({ kind: 'harness', supervision: { caseId, sourceRunId: source } })],
  );
  await pool.query(
    'UPDATE tasks SET required_memories=$2 WHERE id=$1', [task,JSON.stringify(required)],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,created_at) VALUES($1,$2,1,$3,'harness',1,'failed',now()-interval '2 minutes')",
    [source, task, randomUUID()],
  );
  await pool.query(
    `INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,applied_attempt,created_at,updated_at)
    VALUES($1,$2,$3,NULL,'applied','New evidence',$4,1,now()-interval '1 minute',now()-interval '1 minute')`,
    [caseId, goal, task, JSON.stringify(references)],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'running')",
    [run, task, randomUUID()],
  );
  return {
    goal,
    task,
    source,
    run,
    caseId,
    taskRow: (await pool.query('SELECT * FROM tasks WHERE id=$1', [task])).rows[0],
    runRow: (await pool.query('SELECT * FROM runs WHERE id=$1', [run])).rows[0],
  };
}
async function observation(
  content = 'Exact runtime delta; preserve original error and G1008 remains unresolved.',
  project = 'supervision-memory-test',
) {
  return saveMemory({
    project,
    title: 'Native startup observation',
    content,
    kind: 'observation',
    verification: 'unverified',
    source: 'operator:isolated-observation',
  });
}
async function setEvidence(f: Awaited<ReturnType<typeof fixture>>, entries: any[]) {
  await pool.query('UPDATE supervision_cases SET evidence=$2 WHERE id=$1', [
    f.caseId,
    JSON.stringify(entries),
  ]);
}

it('delivers applied evidence outside the recent/optional budget and deduplicates required IDs without promoting observations', async () => {
  const prior = await saveMemory({
    project: 'supervision-memory-test',
    title: 'Required owner decision',
    content: 'd'.repeat(54500),
    kind: 'decision',
    verification: 'confirmed',
    source: 'owner',
  });
  const patch = await observation('Exact guarded runtime patch.\n' + 'p'.repeat(2990));
  const gameInfo = await observation('G1008 after native GameInfo.\n' + 'g'.repeat(1990));
  await pool.query(
    "UPDATE memories SET created_at=now()-interval '1 year' WHERE id=ANY($1::uuid[])",
    [[patch.id, gameInfo.id]],
  );
  await pool.query(
    "INSERT INTO memories(id,project,title,content,kind,verification,source) SELECT gen_random_uuid(),'supervision-memory-test','consumer history','consumer obsolete hypothesis '||repeat('x',3000),'observation','unverified','test' FROM generate_series(1,205)",
  );
  const f = await fixture([
    { memoryId: patch.id },
    { memoryId: gameInfo.id },
    { memoryId: patch.id },
  ],[prior.id, patch.id]);
  const originalTask = structuredClone(f.taskRow);
  const context = await assembleContext(f.runRow, f.taskRow, []);
  expect(context.manifest.selected.map((entry) => entry.id).sort()).toEqual(
    [prior.id, patch.id, gameInfo.id].sort(),
  );
  expect(context.manifest.selected.every((entry) => entry.required)).toBe(true);
  expect(context.manifest.memorySelection.requiredCharacters).toBeLessThanOrEqual(60000);
  expect(
    context.manifest.memorySelection.omitted.some(
      (entry) => entry.reason === 'optional_context_budget',
    ),
  ).toBe(true);
  expect(context.manifest.supervisionMemory?.memories).toEqual([
    {
      id: patch.id,
      version: 1,
      hash: hash(
        (await pool.query('SELECT content FROM memories WHERE id=$1', [patch.id])).rows[0].content,
      ),
      kind: 'observation',
      verification: 'unverified',
      source: 'operator:isolated-observation',
    },
    {
      id: gameInfo.id,
      version: 1,
      hash: hash(
        (await pool.query('SELECT content FROM memories WHERE id=$1', [gameInfo.id])).rows[0]
          .content,
      ),
      kind: 'observation',
      verification: 'unverified',
      source: 'operator:isolated-observation',
    },
  ]);
  expect(f.taskRow).toEqual(originalTask);
  expect(
    (await pool.query('SELECT required_memories FROM tasks WHERE id=$1', [f.task])).rows[0]
      .required_memories,
  ).toEqual([prior.id, patch.id]);
  expect(
    (await pool.query('SELECT kind,verification FROM memories WHERE id=$1', [gameInfo.id])).rows[0],
  ).toEqual({ kind: 'observation', verification: 'unverified' });
  expect(context.content.match(new RegExp('Memória ' + patch.id, 'g'))).toHaveLength(1);
});

it('delivers all ten evidence memories despite the optional count cap', async () => {
  const memories = await Promise.all(
    Array.from({ length: 10 }, (_, i) => observation('Evidence ' + i)),
  );
  const f = await fixture(memories.map((memory) => ({ memoryId: memory.id })));
  const context = await assembleContext(f.runRow, f.taskRow, []);
  expect(context.manifest.selected.filter((entry) => entry.required)).toHaveLength(10);
  expect(context.manifest.supervisionMemory?.memories).toHaveLength(10);
});

it.each(['cross-project', 'missing', 'superseded', 'expired', 'wrong-version', 'wrong-hash'])(
  'fails closed for %s evidence without creating a context',
  async (invalid) => {
    const memory = await observation(
      'Actual evidence',
      invalid === 'cross-project' ? 'private-other-project' : 'supervision-memory-test',
    );
    const f = await fixture([
      {
        memoryId: invalid === 'missing' ? randomUUID() : memory.id,
        ...(invalid === 'wrong-version' ? { memoryVersion: 2 } : {}),
        ...(invalid === 'wrong-hash' ? { memoryHash: '0'.repeat(64) } : {}),
      },
    ]);
    if (invalid === 'superseded')
      await saveMemory({
        project: 'supervision-memory-test',
        title: 'Replacement',
        content: 'Next revision',
        kind: 'observation',
        verification: 'unverified',
        source: 'test',
        supersedes: memory.id,
      });
    if (invalid === 'expired')
      await pool.query("UPDATE memories SET expires_at=now()-interval '1 second' WHERE id=$1", [
        memory.id,
      ]);
    await expect(assembleContext(f.runRow, f.taskRow, [])).rejects.toMatchObject({
      code: 'REQUIRED_CONTEXT_MISSING',
    });
    expect((await pool.query('SELECT 1 FROM contexts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
    expect(
      (await pool.query('SELECT context_id FROM runs WHERE id=$1', [f.run])).rows[0].context_id,
    ).toBeNull();
  },
);

it('keeps the existing required context limit for supervisor evidence', async () => {
  const huge = await observation('x'.repeat(60001));
  const f = await fixture([{ memoryId: huge.id }]);
  await expect(assembleContext(f.runRow, f.taskRow, [])).rejects.toMatchObject({
    code: 'CONTEXT_TOO_LARGE',
  });
  expect((await pool.query('SELECT 1 FROM contexts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
});

it('ignores IDs in descriptions, nested evidence and unrelated task/goal cases', async () => {
  const secret = await observation('Private foreign task evidence', 'private-other-project');
  const f = await fixture([{ instruction: 'Read ' + secret.id, nested: { memoryId: secret.id } }]);
  f.taskRow.description = 'Arbitrary text memoryId:' + secret.id;
  f.taskRow.execution.supervision.caseId = randomUUID();
  const context = await assembleContext(f.runRow, f.taskRow, []);
  expect(context.manifest.supervisionMemory?.memories).toEqual([]);
  expect(context.content).not.toContain(secret.id);
  const other = await fixture([{ memoryId: secret.id }]);
  await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1', [
    f.task,
    JSON.stringify({ supervision: { caseId: other.caseId, sourceRunId: other.source } }),
  ]);
  const unrelated = await appliedSupervisionMemoryReferences(
    pool,
    f.runRow,
    f.taskRow,
    'supervision-memory-test',
  );
  expect(unrelated).toBeNull();
});

it.each(['open', 'decided', 'delegated', 'waiting_evidence', 'forwarded'])(
  'does not require evidence from a %s case',
  async (state) => {
    const f = await fixture([{ memoryId: randomUUID() }]);
    await pool.query('UPDATE supervision_cases SET state=$2 WHERE id=$1', [f.caseId, state]);
    expect(
      await appliedSupervisionMemoryReferences(
        pool,
        f.runRow,
        f.taskRow,
        'supervision-memory-test',
      ),
    ).toBeNull();
  },
);

it('rejects stale handoff source/attempt and an older case after a newer revision exists', async () => {
  const f = await fixture([{ memoryId: randomUUID() }]);
  await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1', [
    f.task,
    JSON.stringify({ supervisionHandoff: { caseId: f.caseId, sourceRunId: randomUUID() } }),
  ]);
  expect(
    await appliedSupervisionMemoryReferences(pool, f.runRow, f.taskRow, 'supervision-memory-test'),
  ).toBeNull();
  await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1', [
    f.task,
    JSON.stringify(f.taskRow.execution),
  ]);
  await pool.query('UPDATE supervision_cases SET applied_attempt=2 WHERE id=$1', [f.caseId]);
  expect(
    await appliedSupervisionMemoryReferences(pool, f.runRow, f.taskRow, 'supervision-memory-test'),
  ).toBeNull();
  await pool.query('UPDATE supervision_cases SET applied_attempt=1 WHERE id=$1', [f.caseId]);
  await pool.query(
    `INSERT INTO supervision_cases(id,goal_id,task_id,state,reason,evidence) VALUES($1,$2,$3,'open','New revision','[]')`,
    [randomUUID(), f.goal, f.task],
  );
  expect(
    await appliedSupervisionMemoryReferences(pool, f.runRow, f.taskRow, 'supervision-memory-test'),
  ).toBeNull();
});

it('rechecks the persisted revision and memory version before committing its manifest', async () => {
  const memory = await observation();
  const f = await fixture([{ memoryId: memory.id }]);
  const authority = await appliedSupervisionMemoryReferences(
    pool,
    f.runRow,
    f.taskRow,
    'supervision-memory-test',
  );
  const rows = (await pool.query('SELECT * FROM memories WHERE id=$1', [memory.id])).rows;
  const snapshot = supervisionMemorySnapshot(authority, rows);
  await assertSupervisionMemorySnapshot(
    pool,
    f.runRow,
    f.taskRow,
    'supervision-memory-test',
    snapshot,
  );
  await pool.query('UPDATE memories SET content=$2 WHERE id=$1', [memory.id, 'Changed bytes']);
  await expect(
    assertSupervisionMemorySnapshot(pool, f.runRow, f.taskRow, 'supervision-memory-test', snapshot),
  ).rejects.toMatchObject({ code: 'SUPERVISION_CONTEXT_CHANGED' });
  await pool.query('UPDATE memories SET content=$2 WHERE id=$1', [memory.id, rows[0].content]);
  await setEvidence(f, [{ memoryId: memory.id }, { memoryId: randomUUID() }]);
  await expect(
    assertSupervisionMemorySnapshot(pool, f.runRow, f.taskRow, 'supervision-memory-test', snapshot),
  ).rejects.toMatchObject({ code: 'SUPERVISION_CONTEXT_CHANGED' });
});

it.each(['case_revision', 'memory_revision'])(
  'does not commit context after a concurrent %s during ranking',
  async (revision) => {
    const memory = await observation();
    const optional = await observation('consumer Current integration');
    const f = await fixture([{ memoryId: memory.id }]);
    config.layaUrl = 'test-only';
    const rank = vi.spyOn(contextRank, 'rankContext').mockImplementationOnce(async () => {
      if (revision === 'case_revision') {
        await setEvidence(f, [{ memoryId: memory.id }, { memoryId: randomUUID() }]);
      } else {
        await saveMemory({
          project: 'supervision-memory-test',
          title: 'Replacement',
          content: 'New evidence',
          kind: 'observation',
          verification: 'unverified',
          source: 'test',
          supersedes: memory.id,
        });
      }
      return { [optional.id]: 1 };
    });
    await expect(assembleContext(f.runRow, f.taskRow, [])).rejects.toMatchObject({
      code:
        revision === 'case_revision' ? 'SUPERVISION_CONTEXT_CHANGED' : 'REQUIRED_CONTEXT_MISSING',
    });
    expect(rank).toHaveBeenCalledOnce();
    expect((await pool.query('SELECT 1 FROM contexts WHERE run_id=$1', [f.run])).rowCount).toBe(0);
    expect(
      (await pool.query('SELECT context_id FROM runs WHERE id=$1', [f.run])).rows[0].context_id,
    ).toBeNull();
  },
);

it('does not inject evidence appended after the current attempt was created', async () => {
  const memory = await observation();
  const f = await fixture([{ memoryId: memory.id }]);
  await pool.query('UPDATE supervision_cases SET updated_at=now() WHERE id=$1', [f.caseId]);
  expect(
    await appliedSupervisionMemoryReferences(pool, f.runRow, f.taskRow, 'supervision-memory-test'),
  ).toBeNull();
});

it('only builds a new attempt manifest and preserves the prior run context and prompt data', async () => {
  const memory = await observation();
  const f = await fixture([{ memoryId: memory.id }]);
  const oldContext = randomUUID(),
    oldManifest = { selected: [], at: 'before-supervision' };
  await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)', [
    oldContext,
    f.source,
    JSON.stringify(oldManifest),
    'Frozen prior prompt',
  ]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1', [f.source, oldContext]);
  const previous = (await pool.query('SELECT * FROM contexts WHERE id=$1', [oldContext])).rows[0];
  const priorRun = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.source])).rows[0];
  expect(
    await appliedSupervisionMemoryReferences(pool, priorRun, f.taskRow, 'supervision-memory-test'),
  ).toBeNull();
  await assembleContext(f.runRow, f.taskRow, []);
  expect((await pool.query('SELECT * FROM contexts WHERE id=$1', [oldContext])).rows[0]).toEqual(
    previous,
  );
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.source])).rows[0]).toEqual(
    priorRun,
  );
  expect(
    (
      await pool.query('SELECT description,execution,required_memories FROM tasks WHERE id=$1', [
        f.task,
      ])
    ).rows[0],
  ).toEqual({
    description: f.taskRow.description,
    execution: f.taskRow.execution,
    required_memories: [],
  });
});

it('delivers delegated evidence only to an explicitly listed child with a valid same-goal decision', async () => {
  const memory = await observation();
  const f = await fixture([{ memoryId: memory.id }]);
  const reviewer = randomUUID(),
    decision = randomUUID(),
    child = randomUUID(),
    childRun = randomUUID();
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,stage)
    VALUES($1,$2,'reviewer','Supervisor','Structured review','qa.backend','submitted','[]','coordinate')`,
    [reviewer, f.goal],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')",
    [decision, reviewer, randomUUID()],
  );
  await pool.query(
    `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count,execution)
    VALUES($1,$2,'child','Focal repair','Focal repair','qa.backend','running','[]',1,'{}')`,
    [child, f.goal],
  );
  await pool.query(
    'UPDATE supervision_cases SET state=$2,decision_run_id=$3,support_task_ids=$4 WHERE id=$1',
    [f.caseId, 'delegated', decision, JSON.stringify([child])],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",
    [childRun, child, randomUUID()],
  );
  const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [child])).rows[0];
  const run = (await pool.query('SELECT * FROM runs WHERE id=$1', [childRun])).rows[0];
  const context = await assembleContext(run, task, []);
  expect(context.manifest.supervisionMemory).toMatchObject({
    caseId: f.caseId,
    relation: 'delegated_child',
    authorityTaskId: f.task,
  });
  expect(context.manifest.selected.find((entry) => entry.id === memory.id)?.required).toBe(true);
  expect(
    await appliedSupervisionMemoryReferences(pool, f.runRow, f.taskRow, 'supervision-memory-test'),
  ).toBeNull();
  await pool.query(
    "UPDATE supervision_cases SET support_task_ids='[]',support_task_id=$2 WHERE id=$1",
    [f.caseId, child],
  );
  expect(
    await appliedSupervisionMemoryReferences(pool, run, task, 'supervision-memory-test'),
  ).toMatchObject({ caseId: f.caseId });
  await pool.query('UPDATE supervision_cases SET decision_run_id=NULL WHERE id=$1', [f.caseId]);
  expect(
    await appliedSupervisionMemoryReferences(pool, run, task, 'supervision-memory-test'),
  ).toBeNull();
});
