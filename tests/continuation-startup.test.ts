import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {ContinuationSupervisor} from '../server/continuation.js';
import {prepareSchema,migrationReceiptKey} from '../server/schema-readiness.js';

beforeAll(async()=>{await migrate();});
beforeEach(async()=>{
  config.bridgeHost='test-only';
  await pool.query('DELETE FROM settings WHERE id=ANY($1::text[])',[
    [migrationReceiptKey,'artifact-recovery','continuation-supervisor']]);
});
afterEach(()=>{vi.restoreAllMocks();config.bridgeHost='';});
afterAll(async()=>{await pool.query('DELETE FROM settings WHERE id=$1',[migrationReceiptKey]);await pool.end();});

async function definitions(){
  return (await pool.query(`SELECT oid::text,xmin::text,proname FROM pg_proc
    WHERE pronamespace='public'::regnamespace AND proname LIKE 'agentos_%' ORDER BY oid`)).rows;
}

it.each(['missing','mismatch','partial'])('refuses %s migration proof before owner acquisition or continuation dispatch',async(mode)=>{
  if(mode!=='missing'){
    await prepareSchema();
    const patch=mode==='mismatch'?{fingerprint:'f'.repeat(64)}:{state:'applying'};
    await pool.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1',
      [migrationReceiptKey,JSON.stringify(patch)]);
  }
  const before=await definitions(),supervisor=new ContinuationSupervisor();
  const scan=vi.spyOn(supervisor,'scan').mockResolvedValue();
  const probe=await pool.connect();
  try{
    await expect(supervisor.start()).rejects.toMatchObject({code:'MIGRATION_REQUIRED'});
    expect(scan).not.toHaveBeenCalled();
    expect((await probe.query('SELECT pg_try_advisory_lock(4280050) owned')).rows[0].owned).toBe(true);
    await probe.query('SELECT pg_advisory_unlock(4280050)');
    expect((await pool.query("SELECT 1 FROM settings WHERE id IN('artifact-recovery','continuation-supervisor')")).rowCount).toBe(0);
    expect(await definitions()).toEqual(before);
  }finally{await supervisor.close();probe.release();}
});

it('starts with exact complete proof while task readers would block redundant policy DDL',async()=>{
  await prepareSchema();
  const before=await definitions(),supervisor=new ContinuationSupervisor();
  const scan=vi.spyOn(supervisor,'scan').mockResolvedValue();
  const reader=await pool.connect();
  await reader.query('BEGIN');await reader.query('LOCK TABLE tasks IN ACCESS SHARE MODE');
  const starting=supervisor.start();
  try{
    expect(await Promise.race([starting,delay(1500).then(()=>{throw Error('STARTUP_WAITED_FOR_TASK_DDL');})])).toBe(true);
    expect(scan).toHaveBeenCalledTimes(1);
    const claimed=(await reader.query('SELECT pg_try_advisory_lock(4280050) owned')).rows[0].owned;
    if(claimed)await reader.query('SELECT pg_advisory_unlock(4280050)');
    expect(claimed).toBe(false);
    expect((await pool.query("SELECT value FROM settings WHERE id='artifact-recovery'")).rows[0].value).toEqual({enabled:true});
    expect(await definitions()).toEqual(before);
  }finally{
    await reader.query('ROLLBACK');reader.release();await starting.catch(()=>{});await supervisor.close();
  }
  const probe=await pool.connect();
  try{
    expect((await probe.query('SELECT pg_try_advisory_lock(4280050) owned')).rows[0].owned).toBe(true);
    await probe.query('SELECT pg_advisory_unlock(4280050)');
  }finally{probe.release();}
});

it('preserves disabled continuation startup without opening an owner when no bridge is configured',async()=>{
  config.bridgeHost='';
  const supervisor=new ContinuationSupervisor(),scan=vi.spyOn(supervisor,'scan').mockResolvedValue();
  const connect=vi.spyOn(pool,'connect');
  expect(await supervisor.start()).toBe(false);
  expect(connect).not.toHaveBeenCalled();expect(scan).not.toHaveBeenCalled();
});

it('refuses real backlog startup without schema proof before running its other installers or loops',async()=>{
  const before=await definitions();
  const root=fileURLToPath(new URL('../',import.meta.url));
  await expect(promisify(execFile)(process.execPath,[
    '--import',fileURLToPath(new URL('../node_modules/tsx/dist/loader.mjs',import.meta.url)),
    fileURLToPath(new URL('../scripts/backlog-loop.ts',import.meta.url)),
  ],{cwd:root,timeout:10000,env:{...process.env,AGENTOS_BRIDGE_HOST:'',AGENTOS_REMOTE_EXECUTION:'0'}}))
    .rejects.toMatchObject({code:1,stderr:expect.stringContaining('MIGRATION_REQUIRED')});
  expect(await definitions()).toEqual(before);
  expect((await pool.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted
    AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objid=4280066`)).rowCount).toBe(0);
});
