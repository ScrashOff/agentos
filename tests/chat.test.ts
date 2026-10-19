import { beforeAll, beforeEach, afterEach, afterAll, it, expect, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { accountBridge } from '../server/bridge.js';
import { syncAccounts, quotaFor, chooseChatAccount } from '../server/accounts.js';
import { ChatEngine, newChat, chatDetail } from '../server/chat.js';
import { seedRegistry } from '../server/registry.js';
import { makeApp } from '../server/app.js';
const account = {
  id: 'harness:openai-codex-2',
  provider: 'openai-codex-2',
  family: 'codex',
  label: 'Codex fixture',
  source: 'harness',
  connected: true,
  identity: { email: 'fixture@example.test' },
  models: ['fixture-model'],
  defaultModel: 'fixture-model',
  authStatus: 'connected',
  quota: {
    status: 'ok',
    updated: Date.now(),
    groups: [{ label: 'codex', windows: [{ remainingPercent: 90 }] }],
  },
};
const engine = new ChatEngine();
beforeAll(async () => {
  await migrate();
});
beforeEach(async () => {
  await pool.query('TRUNCATE chats,goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  config.bridgeHost = 'test-fixture';
});
afterEach(async () => {
  await engine.close();
  vi.restoreAllMocks();
  config.bridgeHost = '';
  config.remoteExecution = false;
});
afterAll(async () => {
  await pool.end();
});
it('inherits metadata and models without storing credentials in PostgreSQL', async () => {
  vi.spyOn(accountBridge, 'request').mockResolvedValue({
    accounts: [account],
    checkedAt: new Date().toISOString(),
  });
  await syncAccounts(true);
  const row = (await pool.query('SELECT * FROM accounts WHERE bridge_id=$1', [account.id])).rows[0];
  expect(row.identity.email).toBe('fixture@example.test');
  expect(row.models).toEqual(['fixture-model']);
  expect(row.source).toBe('harness');
  expect(row.quota_status).toBe('available');
  expect(row).not.toHaveProperty('credential');
});
it('uses the selected AGY model quota instead of the unrelated family bucket', () => {
  const a = {
    provider: 'agy',
    quota: {
      status: 'ok',
      groups: [
        { label: 'Gemini', windows: [{ remainingPercent: 0 }] },
        { label: 'Claude e GPT', windows: [{ remainingPercent: 80 }] },
      ],
    },
  };
  expect(quotaFor(a, 'gemini-3.8-flash-high').status).toBe('exhausted');
  expect(quotaFor(a, 'claude-sonnet-4-6').status).toBe('available');
});
it('keeps native Muse accounts available for workers but excludes them from automatic and explicit chat', async () => {
  vi.spyOn(accountBridge, 'request').mockResolvedValue({
    accounts: [{ ...account, id: 'agentos:muse:test', family: 'muse', source: 'agentos' }, account],
  });
  await syncAccounts(true);
  expect((await chooseChatAccount()).account.provider).toBe('codex');
  await expect(chooseChatAccount('agentos-muse-test')).rejects.toMatchObject({
    code: 'NATIVE_RUNTIME_ONLY',
  });
  expect(
    (await pool.query("SELECT provider FROM accounts WHERE id='agentos-muse-test'")).rows[0]
      .provider,
  ).toBe('muse');
});
it('persists a real-shaped tool roundtrip and deduplicates the user message', async () => {
  config.remoteExecution = true;
  let generated = 0;
  vi.spyOn(accountBridge, 'request').mockImplementation(async (method: string) => {
    if (method === 'state') return { accounts: [account] };
    if (method === 'worker.workspaces') return [{ id: 'test/game' }];
    generated++;
    return {
      message: {
        role: 'assistant',
        content:
          generated === 1
            ? [{ type: 'toolCall', id: 'tool-1', name: 'workspace_status', arguments: {} }]
            : [{ type: 'text', text: 'Estado consultado.' }],
        stopReason: 'stop',
        timestamp: Date.now(),
        usage: { input: 10, output: 5 },
      },
    };
  });
  await syncAccounts(true);
  const chat = await newChat(),
    input = {
      message: 'Veja o progresso',
      requestKey: randomUUID(),
      accountId: 'harness-openai-codex-2',
      model: 'fixture-model',
    };
  const sent = await Promise.all([engine.send(chat.id, input), engine.send(chat.id, input)]);
  expect(sent.filter((s) => s.duplicate)).toHaveLength(1);
  await expect
    .poll(
      async () => (await chatDetail(chat.id)).messages.find((m) => m.role === 'assistant')?.state,
    )
    .toBe('completed');
  expect(generated).toBe(2);
  expect((await chatDetail(chat.id)).messages.at(-1).content).toBe('Estado consultado.');
  expect(await engine.send(chat.id, input)).toEqual({ duplicate: true });
  expect((await chatDetail(chat.id)).messages).toHaveLength(2);
  await expect(engine.send(chat.id, { ...input, message: 'Different' })).rejects.toMatchObject({
    code: 'IDEMPOTENCY_CONFLICT',
  });
});
it('new logins and API keys use the authenticated bridge and are not persisted locally', async () => {
  const spy = vi
    .spyOn(accountBridge, 'request')
    .mockImplementation(async (method: string) =>
      method === 'state'
        ? { accounts: [account] }
        : method === 'login.start'
          ? { id: randomUUID(), status: 'pending' }
          : { accountId: 'test' },
    );
  const app = makeApp();
  const headers = { authorization: 'Bearer ' + config.apiToken };
  try {
    const denied = await app.inject({
      method: 'POST',
      url: '/api/connections/login',
      payload: { family: 'kimi', label: 'Kimi extra' },
    });
    expect(denied.statusCode).toBeGreaterThanOrEqual(400);
    const response = await app.inject({
      method: 'POST',
      url: '/api/connections/login',
      headers,
      payload: { family: 'kimi', label: 'Kimi extra' },
    });
    expect(response.statusCode).toBe(200);
    expect(spy).toHaveBeenCalledWith('login.start', { family: 'kimi', label: 'Kimi extra' });
    const muse = await app.inject({
      method: 'POST',
      url: '/api/connections/login',
      headers,
      payload: { family: 'muse', label: 'Muse adicional' },
    });
    expect(muse.statusCode).toBe(200);
    expect(spy).toHaveBeenCalledWith('login.start', { family: 'muse', label: 'Muse adicional' });
    const saved = await app.inject({
      method: 'POST',
      url: '/api/connections/api-key',
      headers,
      payload: {
        family: 'openai',
        label: 'Key fixture',
        model: 'test',
        baseUrl: 'https://example.test/v1',
        key: 'test-only-credential',
      },
    });
    expect(saved.statusCode).toBe(200);
    expect(JSON.stringify((await pool.query('SELECT * FROM accounts')).rows)).not.toContain(
      'test-only-credential',
    );
  } finally {
    await app.close();
  }
});
