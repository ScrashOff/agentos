import {readFile,realpath,access,stat} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {pool,tx,lock,event} from '../server/db.js';
import {config} from '../server/config.js';
import {workerSourceDigest} from '../server/worker-readiness.js';
import {requireBacklogSchemaReady} from '../server/backlog-schema-readiness.js';

// Explicit second migration phase. The additive preparer leaves old readers
// compatible; only confirmed adoption by every controller permits generations.
const execute=promisify(execFile),root=await realpath(fileURLToPath(new URL('../',import.meta.url)));
const services=['agentos-workers','agentos-opencode','agentos-backlog','agentos-cycles'];
async function gone(pid:number){
  if(!pid)return true;
  try{await access('/proc/'+pid);return false;}catch(error:any){if(error.code==='ENOENT')return true;throw error;}
}
async function confirm(record:any,digest:string){
  if(await realpath('/opt/agentos/current')!==root || record.candidate!==root || record.digest!==digest ||
    record.workersPendingRestart!==false)throw Error('GENERATION_ROLLOUT_REQUIRED');
  for(const service of [...services,'agentos-api']){
    const {stdout}=await execute('systemctl',['show',service,'--property=MainPID,ActiveState'],{timeout:10000});
    const status=Object.fromEntries(stdout.trim().split('\n').map(line=>line.split('=')));
    const pid=Number(status.MainPID);
    if(status.ActiveState!=='active' || !pid || await realpath('/proc/'+pid+'/cwd')!==root)
      throw Error('GENERATION_CONTROLLER_NOT_ADOPTED');
    if(service==='agentos-api')continue;
    const phase=record.workerServices?.[service],proof=phase?.proof;
    const readiness=(await pool.query('SELECT value FROM settings WHERE id=$1',['worker-readiness:'+service])).rows[0]?.value;
    if(phase?.phase!=='adopted' || !proof?.oldPidGone || proof?.pid!==pid || proof?.cwd!==root ||
      proof?.digest!==digest || readiness?.pid!==pid || readiness?.digest!==digest || readiness?.candidate!==root ||
      !await gone(phase.previousPid) || !Array.isArray(readiness.locks) || !readiness.locks.length)
      throw Error('GENERATION_CONTROLLER_PROOF_REQUIRED');
    const owners=(await pool.query(`SELECT count(*)::int n FROM jsonb_to_recordset($1::jsonb)
      AS owner(key int,"backendPid" int) JOIN pg_locks l ON l.locktype='advisory' AND l.classid=0
      AND l.objsubid=1 AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
      AND l.objid=owner.key::oid AND l.pid=owner."backendPid" AND l.granted`,[JSON.stringify(readiness.locks)])).rows[0].n;
    if(owners!==readiness.locks.length)throw Error('GENERATION_OWNER_PROOF_REQUIRED');
  }
}
try{
  if(process.platform!=='linux')throw Error('SERVER_ONLY');
  const url=new URL(config.databaseUrl);
  if(!['localhost','127.0.0.1'].includes(url.hostname)||url.port!=='15482'||url.pathname!=='/agentos')throw Error('AGENTOS_DATABASE_REQUIRED');
  if(!process.argv.includes('--publication-lock-held')){
    const {stdout}=await execute('flock',['--no-fork','--nonblock','/var/lib/agentos/releases/publication.lock',
      process.execPath,...process.execArgv,fileURLToPath(import.meta.url),'--publication-lock-held'],{timeout:60000});
    process.stdout.write(stdout);
  }else{
    const inode=(await stat('/var/lib/agentos/releases/publication.lock',{bigint:true})).ino.toString();
    const owns=(await readFile('/proc/locks','utf8')).split('\n').some(line=>{
      const parts=line.trim().split(/\s+/);
      return parts[1]==='FLOCK' && parts[3]==='WRITE' && parts[4]===String(process.pid) && parts[5]?.split(':')[2]===inode;
    });
    if(!owns)throw Error('PUBLICATION_LOCK_REQUIRED');
    await requireBacklogSchemaReady();
    const record=JSON.parse(await readFile('/var/lib/agentos/releases/last-worker-update.json','utf8'));
    const digest=await workerSourceDigest(root);
    await confirm(record,digest);
    const result=await tx(async db=>{
      if(!(await db.query('SELECT pg_try_advisory_xact_lock(4280101) owned')).rows[0].owned)throw Error('SCHEMA_PREPARER_OWNED');
      await db.query("SET LOCAL lock_timeout='3s'");
      await db.query("SET LOCAL statement_timeout='20s'");
      await lock(db);
      await requireBacklogSchemaReady(db);
      await confirm(record,digest);
      const index=(await db.query(`SELECT indisvalid,indisready FROM pg_index
        WHERE indexrelid=to_regclass('public.supervision_case_generation')`)).rows[0];
      if(!index?.indisvalid || !index?.indisready)throw Error('GENERATION_INDEX_REQUIRED');
      const old=(await db.query(`SELECT c.conname FROM pg_constraint c
        WHERE c.conrelid='supervision_cases'::regclass AND c.contype='u'
        AND (SELECT array_agg(a.attname::text ORDER BY a.attname) FROM unnest(c.conkey) k(attnum)
          JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum)=ARRAY['source_run_id','task_id']::text[]`)).rows;
      for(const row of old)await db.query('ALTER TABLE supervision_cases DROP CONSTRAINT "'+row.conname.replaceAll('"','""')+'"');
      const receipt={candidate:root,digest,activatedAt:new Date().toISOString(),removedConstraints:old.map(row=>row.conname),rolloutToken:record.rolloutToken};
      if(old.length){
        await db.query("INSERT INTO settings(id,value) VALUES('supervision-generations:activation',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",[JSON.stringify(receipt)]);
        await event(db,'SUPERVISION_GENERATIONS_ACTIVATED','agentos',receipt);
      }
      return {ok:true,alreadyActivated:old.length===0,...receipt};
    });
    console.log(JSON.stringify(result));
  }
}catch(error:any){
  const value=error.code??error.message;
  console.error(JSON.stringify({ok:false,code:/^[A-Z][A-Z0-9_]{2,80}$/.test(value??'')?value:'GENERATION_ACTIVATION_FAILED'}));process.exitCode=1;
}finally{await pool.end();}
