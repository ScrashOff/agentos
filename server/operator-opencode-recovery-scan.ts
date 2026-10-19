import {tx,type DB} from './db.js';

/** Operational scan position only: no authority, progress event, lease, retry
 * or model state. A rejected oldest candidate cannot monopolize the batch. */
export async function operatorOpenCodeRecoveryBatch(
 phase:'dispatch'|'review'|'handoff',select:(db:DB,cursor:string)=>Promise<{id:string}[]>,
){
 return tx(async db=>{
  const key='operator-opencode-recovery-scan:'+phase;
  await db.query(`INSERT INTO settings(id,value) VALUES($1,'{"version":1}') ON CONFLICT(id) DO NOTHING`,[key]);
  const prior=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[key])).rows[0]?.value;
  const cursor=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(prior?.lastRunId??'')
   ?prior.lastRunId:'00000000-0000-0000-0000-000000000000';
  const rows=await select(db,cursor);
  if(rows.length)await db.query('UPDATE settings SET value=$2 WHERE id=$1',[
   key,JSON.stringify({version:1,lastRunId:rows.at(-1)!.id})]);
  return rows;
 });
}
