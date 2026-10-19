import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests/browser',
  fullyParallel: false,
  workers: 1,
  timeout: 40000,
  use: {
    baseURL: 'http://127.0.0.1:4280',
    viewport: { width: 1536, height: 1100 },
    headless: true,
    launchOptions: { channel: 'chrome' },
    trace: 'retain-on-failure',
  },
  reporter: 'list',
});
