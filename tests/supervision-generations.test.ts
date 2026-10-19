import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {installSupervision} from '../server/supervision.js';
import {supervisionGenerationReady} from '../server/supervision-generations.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installSupervision();
});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');});
afterAll(async()=>{await pool.query('ALTER TABLE supervision_cases DROP CONSTRAINT IF EXISTS test_legacy_supervision_pair');await pool.end();});

it('retains the legacy uniqueness during additive migration so old controllers cannot observe generations',async()=>{
  await pool.query('ALTER TABLE supervision_cases ADD CONSTRAINT test_legacy_supervision_pair UNIQUE(task_id,source_run_id)');
  try {
    expect(await supervisionGenerationReady(pool)).toBe(false);
    await installSupervision();
    expect(await supervisionGenerationReady(pool)).toBe(false);
    expect((await pool.query(`SELECT attname FROM pg_attribute WHERE attrelid='supervision_cases'::regclass
      AND attname IN('request_key','parent_case_id') ORDER BY attname`)).rows).toEqual([{attname:'parent_case_id'},{attname:'request_key'}]);
    expect((await pool.query("SELECT indisvalid FROM pg_index WHERE indexrelid='supervision_case_generation'::regclass")).rows[0].indisvalid).toBe(true);
  } finally {await pool.query('ALTER TABLE supervision_cases DROP CONSTRAINT test_legacy_supervision_pair');}
  expect(await supervisionGenerationReady(pool)).toBe(true);
});
