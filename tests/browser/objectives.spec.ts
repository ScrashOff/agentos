import { test, expect } from '@playwright/test';
for (const mode of ['plan', 'execute'])
  test(`natural objective submits explicit ${mode} mode and scope`, async ({ page }) => {
    let body: any;
    const chat = {
      id: '11111111-1111-4111-8111-111111111111',
      title: 'Teste de interface',
      project: 'ui-test',
    };
    await page.route('**/api/workspaces', (r) =>
      r.fulfill({ json: [{ id: 'ui-test/base', name: 'base' }] }),
    );
    await page.route('**/api/chats', (r) =>
      r.fulfill({ json: r.request().method() === 'POST' ? chat : [] }),
    );
    await page.route('**/api/chats/' + chat.id, (r) => r.fulfill({ json: { chat, messages: [] } }));
    await page.route('**/api/objectives', async (r) => {
      body = r.request().postDataJSON();
      await r.fulfill({ json: { objectiveId: '22222222-2222-4222-8222-222222222222' } });
    });
    await page.goto('/');
    await page.getByLabel('Tipo de pedido').selectOption(mode);
    await page.getByLabel('Workspace da tarefa').selectOption('ui-test/base');
    await page.getByLabel('Frentes simultâneas').fill('2');
    await page
      .getByLabel('Mensagem', { exact: true })
      .fill('Corrija o carregamento e valide a candidata.');
    await page.getByLabel('Enviar mensagem').click();
    await expect.poll(() => body?.mode).toBe(mode);
    expect(body.workspaces).toEqual(['ui-test/base']);
    expect(body.project).toBe('ui-test');
    expect(body.requestedCapacity).toBe(2);
    expect(body.chatId).toBe(chat.id);
    await page.setViewportSize({ width: 390, height: 900 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
      true,
    );
  });
