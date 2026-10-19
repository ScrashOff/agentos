import { test, expect } from '@playwright/test';
test('an isolated operation runs through the DAG, context and artifact download', async ({
  page,
}) => {
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto('/');
  await page.locator('nav').getByRole('button', { name: 'Visão geral', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Visão geral', exact: true })).toBeVisible();
  await expect(page.getByText('Control plane operacional')).toBeVisible();
  await page.getByRole('button', { name: 'Objetivos e tarefas', exact: false }).click();
  await page.getByRole('button', { name: 'Exemplo técnico', exact: true }).click();
  await expect(
    page.getByRole('heading', { name: 'Validar a operação do AgentOS', exact: true, level: 1 }),
  ).toBeVisible();
  await expect(page.getByText('6 de 6 tarefas aceitas', { exact: true })).toBeVisible({
    timeout: 30000,
  });
  await page.screenshot({ path: 'artifacts/screenshots/goal-desktop.png', fullPage: true });
  await page.getByRole('button', { name: 'Memória e contexto', exact: true }).click();
  await page.locator('.context-list button').first().click();
  await expect(page.getByRole('dialog')).toContainText('Contrato da demonstração');
  await expect(page.getByRole('dialog')).toContainText('Manifesto de seleção');
  await page.getByRole('button', { name: 'Fechar', exact: true }).click();
  await page.locator('.tabs').getByRole('button', { name: 'Artefatos', exact: true }).click();
  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.locator('.artifact-card').first().click(),
  ]);
  expect(download.suggestedFilename()).toBe('result.json');
  expect(await download.failure()).toBe(null);
  await page.locator('nav').getByRole('button', { name: 'Visão geral', exact: true }).click();
  await page.screenshot({ path: 'artifacts/screenshots/dashboard-desktop.png', fullPage: true });
  for (const name of [
    'Organização',
    'Registro de agentes',
    'Execuções',
    'Artefatos',
    'Memória',
    'Contas e cotas',
    'Eventos',
    'Configurações',
  ]) {
    await page.locator('nav').getByRole('button', { name, exact: true }).click();
    await expect(page.getByRole('heading', { name, exact: true })).toBeVisible();
  }
  expect(errors).toEqual([]);
});
test('mobile navigation and form work without horizontal page overflow', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await page.getByRole('button', { name: 'Abrir menu' }).click();
  await page.locator('nav').getByRole('button', { name: 'Visão geral', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Visão geral', exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.screenshot({ path: 'artifacts/screenshots/dashboard-mobile.png', fullPage: true });
  await page.getByRole('button', { name: 'Abrir menu' }).click();
  await page.locator('nav').getByRole('button', { name: 'Memória', exact: true }).click();
  await page.getByRole('button', { name: 'Adicionar memória' }).click();
  const title = 'Decisão de teste mobile ' + Date.now();
  await expect(page.getByRole('dialog', { name: 'Registrar conhecimento' })).toBeVisible();
  await page.getByLabel('Título', { exact: true }).fill(title);
  await page
    .getByLabel('Conhecimento', { exact: true })
    .fill('Fixture de interface: manter o ambiente isolado.');
  await page.getByLabel('Projeto', { exact: true }).fill('ui-validation');
  await page.getByRole('button', { name: 'Salvar', exact: true }).click();
  await expect(page.getByRole('dialog')).toHaveCount(0);
  await expect(page.getByRole('heading', { name: title, exact: true })).toBeVisible();
});
