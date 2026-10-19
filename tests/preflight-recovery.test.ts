import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {hash} from '../server/memory.js';
import {reconcileUnstartedPreflight} from '../server/preflight-recovery.js';
beforeAll(migrate);
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(async()=>{await pool.end();});
async function fixture() {
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),context=randomUUID(),artifact=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'A general task','Complete it','preflight',$2)",[goal,goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,execution,attempt_count)
  VALUES($1,$2,'work','Work','Preserve the contract','engineering.backend.node','[]','recovery_blocked',
   '{"kind":"harness","workspace":"preflight/work"}',1)`,[task,goal]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,agent_version,runtime,error_code)
  VALUES($1,$2,1,'failed',$3,1,'harness','SUPERVISION_INPUTS_TOO_LARGE')`,[run,task,randomUUID()]);
 const bytes=Buffer.alloc(8*1024*1024+1),digest=hash(bytes);
 await pool.query(`INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'')`,
  [context,run,JSON.stringify({executionInputs:[],dependencyArtifacts:[{id:artifact,name:'input.log',digest}]})]);
 await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,context]);
 const reader=vi.fn().mockResolvedValue({id:artifact,name:'input.log',digest,bytes});
 return {run,task,reader};
}
it('reconciles an older proven preflight failure once without marking a process stopped or changing the attempt',async()=>{
 const f=await fixture(),before=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
 expect(await reconcileUnstartedPreflight(f.run,f.reader)).toBe(true);
 expect(await reconcileUnstartedPreflight(f.run,f.reader)).toBe(true);
 const receipts=(await pool.query("SELECT payload FROM events WHERE kind='HARNESS_NOT_STARTED' AND payload->>'runId'=$1",[f.run])).rows;
 expect(receipts).toHaveLength(1);
 expect(receipts[0].payload).toMatchObject({confirmed:true,phase:'reconciled_preflight',processInspectionStillRequired:true});
 expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
});
it('does not infer prelaunch failure from the error label, missing observation or contradictory runtime activity',async()=>{
 const f=await fixture();
 const validReader=vi.fn().mockResolvedValue({id:randomUUID(),name:'small.log',digest:hash('small'),bytes:Buffer.from('small')});
 expect(await reconcileUnstartedPreflight(f.run,validReader)).toBe(false);
 await pool.query("INSERT INTO events(kind,payload) VALUES('TOOL_CALL',$1)",[JSON.stringify({runId:f.run})]);
 expect(await reconcileUnstartedPreflight(f.run,f.reader)).toBe(false);
 expect(f.reader).not.toHaveBeenCalled();
 expect((await pool.query("SELECT 1 FROM events WHERE kind='HARNESS_NOT_STARTED'")).rows).toEqual([]);
});
