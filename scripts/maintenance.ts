import {pool,tx,lock,event} from '../server/db.js';
const mode=process.argv[2];
try {
 if(mode==='drain')await tx(async db=>{await lock(db);
  await db.query(`CREATE OR REPLACE FUNCTION agentos_maintenance_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
   IF EXISTS(SELECT 1 FROM settings WHERE id='maintenance' AND value->>'draining'='true') THEN RAISE EXCEPTION 'MAINTENANCE_DRAIN: admissions suspended for controlled transfer'; END IF;
   RETURN NEW; END $$;
   DROP TRIGGER IF EXISTS agentos_maintenance_admission ON runs;
   CREATE TRIGGER agentos_maintenance_admission BEFORE INSERT ON runs FOR EACH ROW EXECUTE FUNCTION agentos_maintenance_admission();`);
  await db.query("INSERT INTO settings(id,value) VALUES('maintenance',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",[JSON.stringify({draining:true,reason:'Transferência controlada do AgentOS para agentos-server',startedAt:new Date().toISOString()})]);
  await event(db,'CONTROL_PLANE_DRAIN',null,{reason:'server-migration'});
 });
 if(mode==='resume')await tx(async db=>{await lock(db);await db.query("UPDATE settings SET value=value||'{\"draining\":false}'::jsonb WHERE id='maintenance'");await event(db,'CONTROL_PLANE_RESUMED',null,{host:process.env.COMPUTERNAME??'server'});});
 console.log(JSON.stringify({maintenance:(await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0]?.value,active:(await pool.query("SELECT r.id,r.state,t.key,r.model,t.execution->>'workspace' workspace FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.state IN('queued','running','cancelling','reconciling')")).rows,planning:(await pool.query("SELECT id,state FROM planning_runs WHERE state='running'")).rows,chats:(await pool.query("SELECT id,state FROM chat_messages WHERE state IN('queued','running','cancelling')")).rows}));
}finally{await pool.end();}
