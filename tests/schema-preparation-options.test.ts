import { beforeAll, afterAll, it, expect } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { pool, migrate } from '../server/db.js';
import { config } from '../server/config.js';
import { parseSchemaPreparationOptions, schemaPreparationPgOptions,
  assertSchemaPreparationTimeouts } from '../scripts/schema-preparation-options.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const cli = ['--import', path.join(root, 'node_modules/tsx/dist/loader.mjs'),
  path.join(root, 'scripts/prepare-schema.ts')];
beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
});
afterAll(() => pool.end());

it('keeps 5s by default and permits exactly 30s only through the explicit maintenance flag', () => {
  const ordinary = parseSchemaPreparationOptions([]);
  expect(ordinary).toEqual({ maintenanceLockWait: false, lockWaitMs: 5000 });
  expect(schemaPreparationPgOptions(ordinary)).toBe('-c lock_timeout=5000 -c statement_timeout=180000');
  const maintenance = parseSchemaPreparationOptions(['--maintenance-lock-wait']);
  expect(maintenance).toEqual({ maintenanceLockWait: true, lockWaitMs: 30000 });
  expect(schemaPreparationPgOptions(maintenance)).toBe('-c lock_timeout=30000 -c statement_timeout=180000');
});

it.each([
  ['--same-source-as', '/source', '--maintenance-lock-wait'],
  ['--maintenance-lock-wait', '--same-source-as', '/source'],
].map(args => ({ args })))('preserves compatibility verification with flags in either order: $args', ({ args }) => {
  expect(parseSchemaPreparationOptions(args)).toEqual({ sameSourceAs: '/source', maintenanceLockWait: true, lockWaitMs: 30000 });
  expect(parseSchemaPreparationOptions(['--same-source-as', '/source']))
    .toEqual({ sameSourceAs: '/source', maintenanceLockWait: false, lockWaitMs: 5000 });
});

it.each([
  ['--maintenance-lock-wait=30s'], ['--maintenance-lock-wait', '0'],
  ['--maintenance-lock-wait', '30000'], ['--maintenance-lock-wait', '60000'],
  ['--maintenance-lock-wait', '--maintenance-lock-wait'], ['--same-source-as'],
  ['--same-source-as', ''], ['--same-source-as', ' '],
  ['--same-source-as', '--maintenance-lock-wait'],
  ['--same-source-as', '/a', '--same-source-as', '/b'], ['--force'], ['/source'],
].map(args => ({ args })))('rejects unknown, duplicate, missing or arbitrary timeout arguments: $args', ({ args }) => {
  expect(() => parseSchemaPreparationOptions(args)).toThrowError(expect.objectContaining({ code: 'MIGRATION_ARGUMENTS' }));
});

it.each([0, 1, 5000, 30001, 60000, Infinity])('refuses forged maintenance limits: %s', lockWaitMs => {
  expect(() => schemaPreparationPgOptions({ maintenanceLockWait: true, lockWaitMs } as any))
    .toThrowError(expect.objectContaining({ code: 'MIGRATION_ARGUMENTS' }));
});

it.each([false, true])('requires the exact effective database timeouts for maintenance=%s', maintenance => {
  const options = parseSchemaPreparationOptions(maintenance ? ['--maintenance-lock-wait'] : []);
  const expected = { lock_timeout: maintenance ? '30s' : '5s', statement_timeout: '3min' };
  expect(() => assertSchemaPreparationTimeouts(options, expected)).not.toThrow();
  for (const effective of [{ ...expected, lock_timeout: '0' }, { ...expected, lock_timeout: '1min' },
    { ...expected, lock_timeout: maintenance ? '5s' : '30s' }, { ...expected, statement_timeout: '0' },
    { ...expected, statement_timeout: '5min' }])
    expect(() => assertSchemaPreparationTimeouts(options, effective))
      .toThrowError(expect.objectContaining({ code: 'MIGRATION_TIMEOUT_CONFIGURATION' }));
});

it.each([false, true])('runs the real CLI with verified limits and complete receipts for maintenance=%s', async maintenance => {
  const prepared = await promisify(execFile)(process.execPath,
    [...cli, ...(maintenance ? ['--maintenance-lock-wait'] : [])],
    { cwd: root, timeout: 20000, env: { ...process.env, PGOPTIONS: '-c lock_timeout=0 -c statement_timeout=0' } });
  const receipt = JSON.parse(prepared.stdout.trim().split('\n').at(-1)!);
  expect(receipt).toMatchObject({ ok: true, state: 'complete', backlog: { state: 'complete' },
    sameSourceAsVerified: false, maintenanceLockWait: maintenance, lockWaitMs: maintenance ? 30000 : 5000 });
});

it('rejects invalid arguments before importing database configuration', async () => {
  await expect(promisify(execFile)(process.execPath, [...cli, '--maintenance-lock-wait=60000'],
    { cwd: root, timeout: 10000, env: { ...process.env, DATABASE_URL: 'invalid database address', AGENTOS_ROLE: 'invalid-role' } }))
    .rejects.toMatchObject({ code: 1, stderr: expect.stringContaining('"code":"MIGRATION_ARGUMENTS"') });
});

it('fails before migration if a connection URL overrides the verified timeout and preserves receipts', async () => {
  const receiptRows = () => pool.query("SELECT id,value FROM settings WHERE id IN('schema:migration-active','schema:backlog-active') ORDER BY id");
  const before = (await receiptRows()).rows;
  const url = new URL(config.databaseUrl);
  url.searchParams.set('options', '-c lock_timeout=1 -c statement_timeout=180000');
  await expect(promisify(execFile)(process.execPath, [...cli, '--maintenance-lock-wait'],
    { cwd: root, timeout: 10000, env: { ...process.env, DATABASE_URL: url.toString() } }))
    .rejects.toMatchObject({ code: 1, stderr: expect.stringContaining('"code":"MIGRATION_TIMEOUT_CONFIGURATION"') });
  expect((await receiptRows()).rows).toEqual(before);
});
