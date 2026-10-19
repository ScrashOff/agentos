import { test, expect } from '@playwright/test';
test('cycles have their own navigation, execution details and editable steps', async ({ page }) => {
  await page.goto('http://127.0.0.1:4282/#cycles');
  await page.getByRole('button', { name: /Publicações aprovadas no Bitrix/ }).click();
  await expect(page.getByRole('heading', { name: 'Execuções', exact: true })).toBeVisible();
  await page.locator('.cycle-run-list button').first().click();
  await expect(page.getByText(/etapas concluídas/)).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Preparar revisão exata' })).toBeVisible();
  await page.screenshot({ path: '.local/cycles-execution.png', fullPage: true });
  await page.getByRole('button', { name: 'Voltar ao ciclo', exact: true }).click();
  await page.getByRole('button', { name: 'Editar fluxo' }).click();
  await expect(page.getByRole('heading', { name: 'Editar fluxo', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Adicionar etapa' }).click();
  await expect(page.getByText('Etapa 4', { exact: true })).toBeVisible();
  await page.screenshot({ path: '.local/cycles-editor.png', fullPage: true });
});
