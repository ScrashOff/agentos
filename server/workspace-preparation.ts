import { randomUUID } from 'node:crypto';
import { pool, tx, lock, event, type DB } from './db.js';
import { accountBridge } from './bridge.js';
import { recordStorageFailure } from './storage-recovery.js';

/** Preparation has its own persisted queue; no copies or SSH inside admission. */
export async function installWorkspacePreparation(db: DB = pool) {
  await db.query(`CREATE TABLE IF NOT EXISTS workspace_preparations(
    id uuid PRIMARY KEY,goal_id uuid NOT NULL REFERENCES goals(id),workspace text NOT NULL UNIQUE,
    source_workspace text NOT NULL,state text NOT NULL DEFAULT 'pending',owner uuid,
    attempt int NOT NULL DEFAULT 0,requested_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,finished_at timestamptz,next_attempt_at timestamptz,reason text,
    receipt jsonb NOT NULL DEFAULT '{}')`);
}
export async function requestWorkspacePreparation(goalId: string, workspace: string, source: string) {
  if(!/^[a-z0-9_-]+\/o[a-z0-9_-]+$/.test(workspace) || !/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(source) ||
    workspace.split('/')[0]!==source.split('/')[0] || workspace===source)throw Error('PREPARATION_SCOPE_INVALID');
  return tx(async db => {
    await lock(db);
    await db.query(`INSERT INTO workspace_preparations(id,goal_id,workspace,source_workspace)
      VALUES($1,$2,$3,$4) ON CONFLICT(workspace) DO NOTHING`, [randomUUID(),goalId,workspace,source]);
    const row=(await db.query('SELECT * FROM workspace_preparations WHERE workspace=$1',[workspace])).rows[0];
    if(row.goal_id!==goalId || row.source_workspace!==source) throw Error('PREPARATION_SOURCE_CHANGED');
    return row.state==='prepared';
  });
}
export class WorkspacePreparationWorker {
  private active=new Set<string>();
  private closed=false;
  private claiming=false;
  async tick() {
    if(this.closed || this.claiming || this.active.size>=3)return;
    if((await pool.query("SELECT value->>'state' state FROM settings WHERE id='storage:agentos-workspaces'")).rows[0]?.state==='blocked')return;
    this.claiming=true;
    try {
      const rows=await tx(async db=>{
        await lock(db);
        // Owners hold per-job session locks. A controller restart only reclaims
        // a copy when that owner is gone; workspace.prepare is idempotent and
        // the bridge independently locks the destination and source.
        const running=Number((await db.query("SELECT count(*) n FROM workspace_preparations WHERE state='preparing'")).rows[0].n);
        const available=Math.min(3-this.active.size,3-running);
        if(available<=0)return [];
        return (await db.query(`SELECT p.* FROM workspace_preparations p JOIN goals g ON g.id=p.goal_id
          WHERE p.state IN('pending','retry_scheduled') AND g.state='active'
          AND (p.next_attempt_at IS NULL OR p.next_attempt_at<=now())
          ORDER BY p.requested_at LIMIT $1 FOR UPDATE OF p SKIP LOCKED`,[available])).rows;
      });
      for(const row of rows){
        if(this.closed)break;
        this.active.add(row.id);
        void this.prepare(row).finally(()=>{
          this.active.delete(row.id);
          // Refill a copy slot when it opens, independently of a potentially
          // slow planning/supervision iteration in the main controller.
          if(!this.closed)void this.tick().catch(()=>{});
        });
      }
    }finally{this.claiming=false;}
  }
  private async prepare(row:any) {
    const client=await pool.connect(),token=randomUUID();let owned=false;
    try{
      owned=(await client.query('SELECT pg_try_advisory_lock(hashtextextended($1,4280082)) owned',[row.id])).rows[0].owned;
      if(!owned)return;
      const claimed=await tx(async db=>{
        await lock(db);
        if(Number((await db.query("SELECT count(*) n FROM workspace_preparations WHERE state='preparing'")).rows[0].n)>=3)return false;
        return !!(await db.query(`UPDATE workspace_preparations SET state='preparing',owner=$2,attempt=attempt+1,started_at=now()
          WHERE id=$1 AND state IN('pending','retry_scheduled') RETURNING id`,[row.id,token])).rowCount;
      });
      if(!claimed)return;
      try{
        await accountBridge.request('worker.prepare',{workspace:row.workspace,sourceWorkspace:row.source_workspace},{timeout:180000});
        await tx(async db=>{
          const changed=await db.query(`UPDATE workspace_preparations SET state='prepared',finished_at=now(),reason=NULL,
            receipt=jsonb_build_object('workspace',workspace,'sourceWorkspace',source_workspace,'confirmed',true)
            WHERE id=$1 AND owner=$2 AND state='preparing'`,[row.id,token]);
          if(changed.rowCount)await event(db,'WORKSPACE_PREPARED',row.goal_id,{preparationId:row.id,workspace:row.workspace});
        });
      }catch(e:any){
        const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(e.code??'')?e.code:'PREPARATION_FAILED';
        if(code==='DISK_CAPACITY')await recordStorageFailure(pool,null);
        await pool.query(`UPDATE workspace_preparations SET state='retry_scheduled',reason=$3,
          next_attempt_at=now()+interval '30 seconds',finished_at=now() WHERE id=$1 AND owner=$2`,[row.id,token,code]);
      }
    }catch{ /* durable row remains visible; next reconcile checks actual owner */ }
    finally{if(owned)await client.query('SELECT pg_advisory_unlock(hashtextextended($1,4280082))',[row.id]).catch(()=>{});client.release();}
  }
  async reconcileOwners(){
    // Try the exact session lock before reclaiming, never use an age heuristic.
    for(const row of (await pool.query("SELECT id FROM workspace_preparations WHERE state='preparing'")).rows){
      const client=await pool.connect();let owned=false;
      try{
        owned=(await client.query('SELECT pg_try_advisory_lock(hashtextextended($1,4280082)) owned',[row.id])).rows[0].owned;
        if(owned)await client.query("UPDATE workspace_preparations SET state='retry_scheduled',owner=NULL,next_attempt_at=now(),reason='PREPARATION_OWNER_GONE' WHERE id=$1 AND state='preparing'",[row.id]);
      }finally{if(owned)await client.query('SELECT pg_advisory_unlock(hashtextextended($1,4280082))',[row.id]);client.release();}
    }
  }
  requestStop(){this.closed=true;}
  async close(){this.requestStop();while(this.active.size || this.claiming)await new Promise(r=>setTimeout(r,50));}
}
