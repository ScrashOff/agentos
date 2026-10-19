import { expect, it } from 'vitest';
import { isNativeOnlyAccount, quotaFor, supportsAccountExecution } from '../server/accounts.js';
import { chooseWorkforceRoute, type WorkforcePolicy } from '../server/workforce-policy.js';

const policy: WorkforcePolicy = {
  enabled: true,
  glmAccountId: 'glm',
  glmTarget: 12,
  workerAccounts: ['muse-1', 'muse-2', 'qwen'],
  coordinatorAccountId: 'codex',
  coordinatorModel: 'gpt-6-sol',
};
const muse = (id = 'muse-1', extra: Record<string, unknown> = {}) => ({
  id,
  provider: 'muse',
  bridge_id: id,
  enabled: true,
  model: 'muse-fixture',
  models: ['muse-fixture'],
  allow_unknown: true,
  ...extra,
});
const task = { execution: { kind: 'harness', workforceLane: 'muse' } };

it('routes Muse workers only to authorized native-capable accounts', () => {
  const accounts = [muse('muse-unauthorized'), muse()];
  expect(chooseWorkforceRoute(task, accounts, policy)).toEqual({
    accountId: 'muse-1',
    model: 'muse-fixture',
  });
  expect(
    chooseWorkforceRoute({ ...task, allowed_accounts: ['muse-2'] }, accounts, policy),
  ).toBeNull();
  expect(chooseWorkforceRoute(task, [muse('muse-unauthorized')], policy)).toBeNull();
  expect(chooseWorkforceRoute(task, [muse('muse-1', { enabled: false })], policy)).toBeNull();
});

it('never routes native Muse subscriptions to analytical calls, coordinators or OpenCode jobs', () => {
  const account = muse();
  expect(isNativeOnlyAccount(account)).toBe(true);
  expect(isNativeOnlyAccount({ family: 'muse' })).toBe(true);
  expect(supportsAccountExecution(account)).toBe(false);
  expect(supportsAccountExecution(account, { kind: 'harness' })).toBe(true);
  for (const execution of [
    undefined,
    { kind: 'opencode' },
    { workforceLane: 'muse' },
    { kind: 'opencode', workforceLane: 'muse' },
  ])
    expect(chooseWorkforceRoute({ execution }, [account], policy)).toBeNull();
  expect(
    chooseWorkforceRoute(
      { stage: 'coordinate', execution: { kind: 'harness' } },
      [account],
      policy,
    ),
  ).toBeNull();
  expect(
    chooseWorkforceRoute(
      { execution: { kind: 'harness', workforceLane: 'qwen' } },
      [account],
      policy,
    ),
  ).toBeNull();
});

it('allows authorized Muse accounts for unpinned harness workers', () => {
  expect(
    chooseWorkforceRoute({ execution: { kind: 'harness' } }, [muse()], policy)?.accountId,
  ).toBe('muse-1');
});

it('respects Muse quota windows, authentication and explicitly allowed unknown quota', () => {
  const quota = (remainingPercent: number) => ({
    status: 'ok',
    groups: [{ label: 'Muse', windows: [{ remainingPercent }] }],
  });
  expect(quotaFor(muse('muse-1', { quota: quota(42) }), 'muse-fixture')).toEqual({
    status: 'available',
    remaining: 42,
  });
  const exhausted = muse('muse-1', { quota: quota(0) });
  expect(chooseWorkforceRoute(task, [exhausted, muse('muse-2')], policy)?.accountId).toBe('muse-2');
  for (const account of [
    exhausted,
    muse('muse-1', { connection_status: 'reconnect_required' }),
    muse('muse-1', { allow_unknown: false }),
  ])
    expect(chooseWorkforceRoute(task, [account], policy)).toBeNull();
});

it('keeps the selected model and honors continuation cooldowns across Muse accounts', () => {
  const accounts = [muse(), muse('muse-2')];
  const cooling = {
    ...task,
    account_id: 'muse-1',
    model: 'muse-fixture',
    execution: {
      ...task.execution,
      continuationRoutes: [{ accountId: 'muse-1', model: 'muse-fixture', until: '2030-01-01' }],
    },
  };
  expect(chooseWorkforceRoute(cooling, accounts, policy, Date.parse('2026-10-05'))?.accountId).toBe(
    'muse-2',
  );
  expect(chooseWorkforceRoute(cooling, accounts, policy, Date.parse('2030-01-02'))?.accountId).toBe(
    'muse-1',
  );
});
