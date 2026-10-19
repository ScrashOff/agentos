import { it, expect } from 'vitest';
import { validateFollowupPlan, followupPlansEqual } from '../server/followup-plan.js';

function fixture() {
  const tasks: any[] = [];
  for (let i = 0; i < 10; i++) {
    const base = 'test/game-' + i;
    for (const [phase, stage, parents, source] of [
      ['backend', 'work', [], base],
      ['native', 'work', [], base],
      ['integrate', 'integrate', ['backend', 'native'], base],
      ['rtp', 'verify', ['integrate'], 'test/o' + i + '-integrate'],
      ['qa', 'verify', ['integrate', 'rtp'], 'test/o' + i + '-integrate'],
    ] as const)
      tasks.push({
        id: 'id-' + i + '-' + phase,
        key: 'g' + i + '-' + phase,
        title: 'Game ' + i + ' ' + phase,
        description: 'Implement and independently test the assigned scoped behavior.',
        agentId: 'qa.backend',
        accountId: 'account',
        model: 'test-model',
        stage,
        execution: {
          workspace: 'test/o' + i + '-' + phase,
          sourceWorkspace: source,
          baseWorkspace: base,
        },
        dependencies: parents.map((p) => 'id-' + i + '-' + p),
        criteria: [
          {
            id: 'check',
            label: 'Real evidence',
            type: 'json_field',
            name: 'report.json',
            field: 'passed',
            equals: true,
          },
        ],
      });
  }
  return {
    tasks,
    request: {
      message: 'Conclude all ten games independently',
      project: 'test',
      mode: 'execute',
      workspaces: Array.from({ length: 10 }, (_, i) => 'test/game-' + i),
      requestedCapacity: 20,
      idempotencyKey: 'ten-game-contract',
    },
    plan: {
      title: 'Ten games',
      summary: 'Twenty workers, integrations and independent verifications.',
      requirements: [
        { id: 'acceptance', description: 'Every game must pass its acceptance tests.' },
      ],
      tasks: tasks.map((t) => ({ key: t.key, covers: ['acceptance'] })),
    },
    agents: [{ id: 'qa.backend', runtime: 'harness' }],
    accounts: [
      {
        id: 'account',
        provider: 'codex',
        bridge_id: 'account',
        enabled: true,
        models: ['test-model'],
      },
    ],
  };
}
it('validates all fifty nodes and twenty requested slots through the public contract', () => {
  const f = fixture();
  const result = validateFollowupPlan(f.request, f.plan, f.tasks, f.agents, f.accounts);
  expect(result.plan.tasks).toHaveLength(50);
  expect(result.request.requestedCapacity).toBe(20);
  expect(result.plan.tasks.find((t) => t.key === 'g0-qa')?.sourceTask).toBe('g0-integrate');
});
it('does not create a revision after a JSONB round trip but detects a changed acceptance contract', () => {
  const f = fixture();
  const { plan } = validateFollowupPlan(f.request, f.plan, f.tasks, f.agents, f.accounts);
  const stored = JSON.parse(JSON.stringify(plan));
  const reversed = Object.fromEntries(Object.entries(stored).reverse());
  expect(followupPlansEqual(plan, reversed)).toBe(true);
  stored.tasks[0].criteria[0].equals = false;
  expect(followupPlansEqual(plan, stored)).toBe(false);
});
it('rejects incomplete final validation, cross-game sources and dependency cycles', () => {
  const f = fixture();
  const qa = f.tasks.find((t) => t.key === 'g0-qa');
  const rtp = f.tasks.find((t) => t.key === 'g0-rtp');
  qa.execution.sourceWorkspace = 'test/o1-integrate';
  rtp.execution.sourceWorkspace = 'test/o1-integrate';
  expect(() => validateFollowupPlan(f.request, f.plan, f.tasks, f.agents, f.accounts)).toThrow();
  qa.execution.sourceWorkspace = rtp.execution.sourceWorkspace = 'test/o0-integrate';
  f.tasks.find((t) => t.key === 'g0-backend').dependencies = [qa.id];
  expect(() => validateFollowupPlan(f.request, f.plan, f.tasks, f.agents, f.accounts)).toThrow(
    'circulares',
  );
});
