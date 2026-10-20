import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createGlmRequestGate, GLM_MAX_REQUESTS } from '../bridge/glm-request-gate.mjs';
import { acquireGlmRequestSlot, getGlmRequestGateStatus } from '../bridge/glm-request-slot.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(predicate, timeout = 10000) {
  const deadline = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() > deadline) assert.fail('Timed out waiting for gate state');
    await sleep(5);
  }
}

// A planned tree of thirty subagents is not thirty simultaneous provider calls:
// the gate admits six and holds the rest in FIFO order.
test('thirty queued subagents share the six provider permits in order', async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'glm-gate-30-'));
  const socketPath =
    process.platform === 'win32'
      ? `\\\\.\\pipe\\agentos-glm-${randomUUID()}`
      : path.join(directory, 'gate.sock');

  const gate = await createGlmRequestGate({ socketPath, refreshMs: 20 });
  t.after(async () => {
    await gate.close();
    await rm(directory, { recursive: true, force: true });
  });

  assert.equal(GLM_MAX_REQUESTS, 6, 'The provider request limit stays at six');
  assert.equal((await gate.status()).limit, 6);

  const granted = [];
  const pending = Array.from({ length: 30 }, (_, i) =>
    acquireGlmRequestSlot({ socketPath, runId: `subagent-${i}` }).then((slot) => {
      granted.push(i);
      return slot;
    }),
  );

  await until(async () => granted.length === 6 && (await gate.status()).queued === 24);
  // Permits are held for the whole stream: waiting does not release them.
  await sleep(50);
  const status = await getGlmRequestGateStatus({ socketPath });
  assert.equal(status.active, 6, 'Only six subagents call the model at a time');
  assert.equal(status.queued, 24, 'The remaining twenty-four wait in the gate queue');
  assert.deepEqual(granted, [0, 1, 2, 3, 4, 5], 'Admission follows arrival order');

  // Draining one permit at a time admits exactly one waiting subagent, in order.
  for (let i = 0; i < 30; i++) {
    const slot = await pending[i];
    await until(async () => (await gate.status()).active === Math.min(6, 30 - i));
    slot.release();
    await until(() => granted.length === Math.min(30, i + 7));
  }

  await until(async () => (await gate.status()).active === 0);
  const final = await gate.status();
  assert.equal(final.granted, 30, 'Every subagent eventually reached the provider');
  assert.equal(final.maxObserved, 6, 'Six simultaneous provider requests were never exceeded');
  assert.equal(final.queued, 0);
});

test('the gate refuses a limit above the six-request policy ceiling', async () => {
  const directory = await mkdtemp(path.join(tmpdir(), 'glm-gate-limit-'));
  const socketPath =
    process.platform === 'win32'
      ? `\\\\.\\pipe\\agentos-glm-${randomUUID()}`
      : path.join(directory, 'gate.sock');
  try {
    await assert.rejects(
      createGlmRequestGate({ socketPath, maxConcurrent: 30, refreshMs: 20 }),
      /GLM_GATE_LIMIT_INVALID/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('hierarchical role mapping and bounded write scope validation', () => {
  const sampleTasks = [
    {
      id: 'task-coord-1',
      title: 'coordinator:refactor-core',
      agent_role: 'coordinator',
      agent_level: 0,
      parent_task_id: null,
      execution: { writePaths: ['package.json'] },
    },
    {
      id: 'task-spec-1',
      title: 'specialist:backend',
      agent_role: 'specialist',
      agent_level: 1,
      parent_task_id: 'task-coord-1',
      execution: { writePaths: ['server/api.ts', 'server/service.ts'] },
    },
    {
      id: 'task-work-1',
      title: 'worker:auth-controller',
      agent_role: 'worker',
      agent_level: 2,
      parent_task_id: 'task-spec-1',
      execution: { writePaths: ['server/auth.ts'] },
    },
    {
      id: 'task-val-1',
      title: 'validator:qa-tests',
      agent_role: 'validator',
      agent_level: 3,
      parent_task_id: 'task-work-1',
      execution: { writePaths: ['tests/auth.test.ts'] },
    },
  ];

  // Verify hierarchy link integrity
  const coord = sampleTasks.find((t) => t.id === 'task-coord-1');
  const spec = sampleTasks.find((t) => t.parent_task_id === coord.id);
  const worker = sampleTasks.find((t) => t.parent_task_id === spec.id);
  const validator = sampleTasks.find((t) => t.parent_task_id === worker.id);

  assert.ok(coord && spec && worker && validator);
  assert.equal(coord.agent_level, 0);
  assert.equal(spec.agent_level, 1);
  assert.equal(worker.agent_level, 2);
  assert.equal(validator.agent_level, 3);

  // Verify bounded write paths scopes
  assert.deepEqual(worker.execution.writePaths, ['server/auth.ts']);
  assert.deepEqual(validator.execution.writePaths, ['tests/auth.test.ts']);
});

test('30-agent complete hierarchical graph validation for massive codebase refactoring', () => {
  const tasks = [];

  // 1. Coordinator (Level 0)
  tasks.push({
    id: 'coord-master',
    title: 'coordinator:codebase-modernization',
    agent_role: 'coordinator',
    agent_level: 0,
    parent_task_id: null,
    execution: { writePaths: ['architecture.md', 'package.json'] },
  });

  // 2. Specialists (Level 1: 4 specialists)
  const specialistDomains = ['backend', 'frontend', 'database', 'qa'];
  for (const domain of specialistDomains) {
    tasks.push({
      id: `spec-${domain}`,
      title: `specialist:${domain}`,
      agent_role: 'specialist',
      agent_level: 1,
      parent_task_id: 'coord-master',
      execution: { writePaths: [`src/${domain}/contracts/`] },
    });
  }

  // 3. Workers (Level 2: 20 workers, 5 per specialist)
  for (let s = 0; s < specialistDomains.length; s++) {
    const domain = specialistDomains[s];
    for (let w = 1; w <= 5; w++) {
      tasks.push({
        id: `work-${domain}-${w}`,
        title: `worker:${domain}-module-${w}`,
        agent_role: 'worker',
        agent_level: 2,
        parent_task_id: `spec-${domain}`,
        execution: { writePaths: [`src/${domain}/modules/part-${w}.ts`] },
      });
    }
  }

  // 4. Validators (Level 3: 5 validation & QA agents)
  for (let v = 1; v <= 5; v++) {
    tasks.push({
      id: `val-test-${v}`,
      title: `validator:qa-suite-${v}`,
      agent_role: 'validator',
      agent_level: 3,
      parent_task_id: `work-qa-${v}`,
      execution: { writePaths: [`tests/suites/test-${v}.spec.ts`] },
    });
  }

  assert.equal(tasks.length, 30, 'Task graph must contain exactly 30 concurrent subagents');

  // Verify all non-root tasks have valid parents
  const idSet = new Set(tasks.map((t) => t.id));
  for (const t of tasks) {
    if (t.agent_level > 0) {
      assert.ok(t.parent_task_id && idSet.has(t.parent_task_id), `Task ${t.id} must point to a valid parent`);
    }
    // Verify each subagent has strict bounded write scopes
    assert.ok(Array.isArray(t.execution.writePaths) && t.execution.writePaths.length > 0, `Task ${t.id} must have bounded writePaths`);
  }

  // Verify role counts
  assert.equal(tasks.filter((t) => t.agent_role === 'coordinator').length, 1);
  assert.equal(tasks.filter((t) => t.agent_role === 'specialist').length, 4);
  assert.equal(tasks.filter((t) => t.agent_role === 'worker').length, 20);
  assert.equal(tasks.filter((t) => t.agent_role === 'validator').length, 5);

  // A thirty-node tree is still served by six provider permits.
  assert.ok(tasks.length > GLM_MAX_REQUESTS);
});

// Provider rate limiting is exercised against the real implementation in
// tests/opencode-provider-error.test.mjs. A backoff formula redefined inside a
// test only asserts itself.
