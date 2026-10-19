import { test, expect } from '@playwright/test';

test('simple chat and additional connection forms are the default interface', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'O que vamos fazer?' })).toBeVisible();
  await expect(page.getByLabel('Mensagem', { exact: true })).toBeVisible();
  await expect(page.getByLabel('Conta do chat').locator('option')).not.toHaveCount(1);
  await page.getByLabel('Conta do chat').selectOption('harness-openai-codex-2');
  await expect(page.getByLabel('Modelo do chat')).toHaveValue('gpt-6-luna');
  await page.screenshot({ path: 'artifacts/screenshots/chat-desktop.png', fullPage: true });
  await page.getByRole('button', { name: 'Conectar conta', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Conectar conta' });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByLabel('Serviço').locator('option')).toHaveCount(7);
  await dialog.getByLabel('Serviço').selectOption('qwen');
  await expect(dialog.getByLabel('Endpoint da API')).toHaveValue(
    'https://token-plan.maas.qwencloudapi.com/compatible-mode/v1',
  );
  await expect(dialog.getByLabel('Chave da API')).toHaveAttribute('type', 'password');
  await dialog.getByLabel('Serviço').selectOption('openai');
  await expect(dialog.getByLabel('Endpoint da API')).toHaveValue('');
  await dialog.getByLabel('Serviço').selectOption('kimi');
  await expect(dialog.getByRole('button', { name: 'Iniciar login' })).toBeVisible();
  await page.screenshot({ path: 'artifacts/screenshots/connect-account.png', fullPage: true });
  await dialog.getByLabel('Fechar conexão').click();
  await page.locator('nav').getByRole('button', { name: 'Contas e cotas' }).click();
  await expect(page.locator('.connected-card')).toHaveCount(9);
  await expect(page.getByRole('button', { name: /Adicionar outra conta/ })).toBeVisible();
});

for (const width of [390, 820]) {
  test(`chat remains usable at width ${width}`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto('/');
    await expect(page.getByLabel('Mensagem', { exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Nova conversa', exact: true })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
      true,
    );
    await page.getByRole('button', { name: 'Conectar conta', exact: true }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
      true,
    );
    await page.screenshot({ path: `artifacts/screenshots/chat-${width}.png`, fullPage: true });
  });
}

test('Codex login offers its method selector and a real device authorization link', async ({
  page,
}) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Conectar conta', exact: true }).click();
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Iniciar login' }).click();
  const selector = dialog.getByRole('combobox', { name: /Select OpenAI Codex login method/ });
  await expect(selector).toBeVisible();
  const value = await selector
    .locator('option')
    .evaluateAll(
      (options) =>
        (options.find((o) => /headless/i.test(o.textContent ?? '')) as HTMLOptionElement).value,
    );
  await selector.selectOption(value);
  await dialog.getByRole('button', { name: 'Enviar autorização' }).click();
  const link = dialog.getByRole('link', { name: 'Abrir página de autorização' });
  await expect(link).toHaveAttribute('href', /^https:\/\/auth\.openai\.com\//, { timeout: 20000 });
  await expect(dialog.locator('.login-code')).toBeVisible();
  await dialog.getByLabel('Fechar conexão').click();
  await expect(dialog).toHaveCount(0);
});
