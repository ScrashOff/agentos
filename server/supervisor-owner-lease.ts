import {createHash,randomUUID} from 'node:crypto';
export type SupervisorOwnerLease={ownerNonce:string;backendPid:number;isAlive:()=>Promise<boolean>;release:()=>Promise<void>};
export function supervisorLeaseKey(runId:string){return [428037,createHash('sha256').update(runId).digest().readInt32BE(0)] as const;}
export function supervisorLeaseAcquirer(pool:{connect:()=>Promise<any>}){
 return async(runId:string):Promise<SupervisorOwnerLease|null>=>{
  const client=await pool.connect();let alive=true,released=false;const died=()=>{alive=false;};client.on('error',died);client.on('end',died);const keys=supervisorLeaseKey(runId);
  try{const row=(await client.query('SELECT pg_try_advisory_lock($1,$2) owned,pg_backend_pid() pid',keys)).rows[0];
   if(!row.owned){client.removeListener('error',died);client.removeListener('end',died);client.release();return null;}
   return {ownerNonce:randomUUID(),backendPid:row.pid,
    isAlive:async()=>{if(!alive||released)return false;try{const proof=(await client.query(`SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()
      AND classid::bigint=$1::bigint AND objid::bigint=$2::bigint AND objsubid=2 AND granted) owned`,[keys[0]>>>0,keys[1]>>>0])).rows[0];return alive&&!released&&proof?.owned===true;}catch{alive=false;return false;}},
    release:async()=>{if(released)return;released=true;try{if(alive)await client.query('SELECT pg_advisory_unlock($1,$2)',keys);}catch{alive=false;}finally{client.removeListener('error',died);client.removeListener('end',died);client.release(!alive);}}
   };
  }catch(e){client.removeListener('error',died);client.removeListener('end',died);client.release(true);throw e;}
 };
}
