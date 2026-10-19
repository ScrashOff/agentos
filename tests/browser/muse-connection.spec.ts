import { expect, test } from '@playwright/test';

test('Muse login is available while its native subscription stays out of the chat picker', async ({
  page,
}) => {
  let completed = false;
  await page.route('**/api/connections', async (route) =>
    route.fulfill({
      json: {
        accounts: [
          {
            id: 'muse-test',
            label: 'Muse fixture',
            provider: 'muse',
            source: 'agentos',
            enabled: true,
            model: 'muse-fixture',
            models: ['muse-fixture'],
            identity: {},
            quota: {},
          },
        ],
      },
    }),
  );
  await page.route('**/api/connections/login**', async (route) => {
    const pathname = new URL(route.request().url()).pathname;
    if (pathname.endsWith('/active')) return route.fulfill({ json: null });
    if (route.request().method() === 'POST') {
      expect(route.request().postDataJSON().family).toBe('muse');
    }
    return route.fulfill({
      json: {
        id: 'muse-login-fixture',
        family: 'muse',
        status: completed ? 'completed' : 'pending',
        notices: [
          { url: 'https://auth.meta.com/device', instructions: 'Autorize sua conta Muse.' },
        ],
      },
    });
  });
  await page.goto('/');
  await expect(page.getByLabel('Conta do chat').locator('option[value="muse-test"]')).toHaveCount(
    0,
  );
  await page.getByRole('button', { name: 'Conectar conta', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Conectar conta' });
  await dialog.getByLabel('Serviço').selectOption('muse');
  await expect(dialog.getByText(/Conecte sua conta Muse Code para executar agentes/)).toBeVisible();
  await dialog.getByRole('button', { name: 'Iniciar login' }).click();
  await expect(dialog.getByRole('link', { name: 'Abrir página de autorização' })).toHaveAttribute(
    'href',
    'https://auth.meta.com/device',
  );
  completed = true;
  await expect(
    dialog.getByText(
      'Muse Code já pode executar agentes nas tarefas. Use outra conta para conversar no chat.',
    ),
  ).toBeVisible();
});
