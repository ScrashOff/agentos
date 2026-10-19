import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {pool,migrate,event,type DB} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision,readSupervisedResumeCandidates} from '../server/supervision.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();await installSupervision();await installTaskExecutionMetadata();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id='artifact-recovery'");
});
afterAll(()=>pool.end());

// The published selection, kept literally as an independent equivalence oracle.
// Return the complete original row so projection cannot hide input corruption.
const legacySql=`SELECT t.*,r.id run_id,r.fence,r.state run_state,r.error_code,r.attempt run_attempt
    FROM tasks t JOIN goals g ON g.id=t.goal_id JOIN settings s ON s.id='supervision:'||g.id
    LEFT JOIN settings a ON a.id='autonomy:'||g.project
    JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    WHERE g.state='active' AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
    AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
    AND (t.state IN('failed','submitted','recovery_blocked') OR
      (t.state='recovering_artifacts' AND r.runtime='harness' AND r.state='failed'
        AND r.error_code IN('ARTIFACT_PATH','ARTIFACT_NAME','ARTIFACT_NAME_COLLISION','ARTIFACT_MISSING','RESULT_FORMAT','RESERVED_ARTIFACT_NAME')))
    AND r.state IN('failed','submitted')
    AND t.execution->>'kind' IN('harness','opencode')
    AND (t.execution ? 'supervision' OR t.execution ? 'supervisionHandoff')
    AND NOT (COALESCE(t.execution->>'readOnlySource','false')='true' AND t.stage IN('verify','review'))
    AND EXISTS(SELECT 1 FROM supervision_cases applied WHERE applied.task_id=t.id AND applied.goal_id=t.goal_id
      AND applied.state='applied' AND applied.applied_attempt=r.attempt
      AND applied.id::text IN(t.execution->'supervision'->>'caseId',t.execution->'supervisionHandoff'->>'caseId')
      AND COALESCE(CASE WHEN t.execution->'supervisionHandoff'->>'caseId'=applied.id::text
        THEN t.execution->'supervisionHandoff'->>'sourceRunId' ELSE t.execution->'supervision'->>'sourceRunId' END,
        applied.source_run_id::text)=r.id::text)
    AND NOT EXISTS(SELECT 1 FROM runs active WHERE active.task_id=t.id AND active.state IN('queued','running','reconciling','cancelling'))
    AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text)
    AND NOT EXISTS(SELECT 1 FROM events WHERE kind='SUPERVISION_RESUME_DEFERRED' AND entity_id=t.id::text
      AND payload->>'sourceRunId'=r.id::text AND created_at>now()-interval '15 seconds')
    AND NOT EXISTS(SELECT 1 FROM supervision_cases c WHERE c.task_id=t.id AND c.state IN('open','decided','delegated','waiting_evidence'))
    AND NOT EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=t.id AND p.state<>'accepted')
    ORDER BY t.updated_at LIMIT 4`;

async function fixture(index=1){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),caseId=randomUUID(),project='resume-projection-'+goal;
  const bytes=randomBytes(48*1024),execution={kind:'harness',workspace:project+'/worker',baseWorkspace:project+'/base',
    supervision:{caseId,sourceRunId:run},inputs:[{id:randomUUID(),name:'immutable.txt',digest:createHash('sha256').update(bytes).digest('hex'),
      size:bytes.length,base64:bytes.toString('base64')}]};
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key,state)
    VALUES($1,'Resume selection','Original acceptance remains authoritative',$2,$3,'active')`,[goal,project,goal]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,updated_at)
    VALUES($1,$2,'work','Original task','Do not change inputs or criteria','qa.backend','failed','integrate',
      '[{"id":"evidence","type":"artifact","name":"candidate.json"}]',$3,77,'2026-01-01'::timestamptz+$4*interval '1 second')`,
    [task,goal,JSON.stringify(execution),index]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code)
    VALUES($1,$2,77,$3,'harness',1,'failed','MODEL_TIMEOUT')`,[run,task,randomUUID()]);
  await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,applied_attempt)
    VALUES($1,$2,$3,$4,'applied','A concrete revised approach','[]',77)`,[caseId,goal,task,run]);
  return {goal,task,run,caseId,project,execution};
}
async function equivalent(db:DB=pool){
  const old=(await db.query(legacySql)).rows,current=await readSupervisedResumeCandidates(db);
  expect(current).toEqual(old);return current;
}

it.each(['failed','submitted','recovery_blocked','recovering_artifacts','opencode'])
('preserves complete selected rows and packaging/runtime semantics: %s',async mode=>{
  const f=await fixture();
  if(mode==='opencode')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{kind}','\"opencode\"') WHERE id=$1",[f.task]);
  else await pool.query('UPDATE tasks SET state=$2 WHERE id=$1',[f.task,mode]);
  if(mode==='submitted')await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[f.run]);
  if(mode==='recovering_artifacts')await pool.query("UPDATE runs SET error_code='RESULT_FORMAT' WHERE id=$1",[f.run]);
  const before=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0];
  const rows=await equivalent();expect(rows.map(t=>t.id)).toEqual([f.task]);
  expect(rows[0].execution.inputs).toEqual(f.execution.inputs);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
});

it.each(['disabled','paused','manual','autonomy','cancel','recent-deferred','pending-case','dependency','live','wrong-attempt','wrong-source','no-directive','qa','wrong-runtime-packaging'])
('retains the same exclusion and never admits an older or unauthorized candidate: %s',async mode=>{
  const f=await fixture();
  if(mode==='disabled')await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['supervision:'+f.goal]);
  if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(mode==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);
  if(mode==='autonomy')await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":false}')",['autonomy:'+f.project]);
  if(mode==='cancel')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
  if(mode==='recent-deferred')await event(pool,'SUPERVISION_RESUME_DEFERRED',f.task,{sourceRunId:f.run});
  if(mode==='pending-case')await pool.query("UPDATE supervision_cases SET state='decided' WHERE id=$1",[f.caseId]);
  if(mode==='dependency'){const parent=await fixture(2);await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.task,parent.task]);
    await pool.query("UPDATE tasks SET state='cancelled' WHERE id=$1",[parent.task]);}
  if(mode==='live')await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,78,$3,'harness',1,'running')`,[randomUUID(),f.task,randomUUID()]);
  if(mode==='wrong-attempt')await pool.query('UPDATE supervision_cases SET applied_attempt=76 WHERE id=$1',[f.caseId]);
  if(mode==='wrong-source')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision,sourceRunId}',to_jsonb($2::text)) WHERE id=$1",[f.task,randomUUID()]);
  if(mode==='no-directive')await pool.query("UPDATE tasks SET execution=execution-'supervision' WHERE id=$1",[f.task]);
  if(mode==='qa')await pool.query("UPDATE tasks SET stage='verify',execution=execution||'{\"readOnlySource\":true}' WHERE id=$1",[f.task]);
  if(mode==='wrong-runtime-packaging'){
    await pool.query("UPDATE tasks SET state='recovering_artifacts' WHERE id=$1",[f.task]);
    await pool.query("UPDATE runs SET runtime='opencode',error_code='RESULT_FORMAT' WHERE id=$1",[f.run]);
  }
  expect(await equivalent()).toEqual([]);
});

it('keeps the applied-source fallback and handoff precedence exactly as published',async()=>{
  const f=await fixture();
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision}',jsonb_build_object('caseId',$2::text)) WHERE id=$1",[f.task,f.caseId]);
  expect((await equivalent()).map(t=>t.id)).toEqual([f.task]);
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervisionHandoff}',jsonb_build_object('caseId',$2::text,'sourceRunId',$3::text)) WHERE id=$1",[f.task,f.caseId,randomUUID()]);
  expect(await equivalent()).toEqual([]);
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervisionHandoff,sourceRunId}',to_jsonb($2::text)) WHERE id=$1",[f.task,f.run]);
  expect((await equivalent()).map(t=>t.id)).toEqual([f.task]);
});

it('preserves ordered final limit after excluding older blocked and unsupported tasks',async()=>{
  const fixtures=[];for(let i=0;i<8;i++)fixtures.push(await fixture(i));
  await pool.query("UPDATE tasks SET execution=execution||'{\"kind\":\"demo\"}' WHERE id=$1",[fixtures[0].task]);
  await pool.query("UPDATE tasks SET execution=execution-'supervision' WHERE id=$1",[fixtures[1].task]);
  expect((await equivalent()).map(t=>t.id)).toEqual(fixtures.slice(2,6).map(f=>f.task));
});

it.each([null,'[]','"harness"','{"kind":"demo"}'])('preserves exclusion of null or unsupported execution %s',async value=>{
  const f=await fixture();await pool.query('UPDATE tasks SET execution=$2::jsonb WHERE id=$1',[f.task,value]);
  expect(await equivalent()).toEqual([]);
});

it.each(['missing','version','goal'])('fails closed on incomplete transactional metadata: %s',async mode=>{
  const f=await fixture();
  if(mode==='missing')await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[f.task]);
  if(mode==='version')await pool.query('UPDATE task_execution_metadata SET version=0 WHERE task_id=$1',[f.task]);
  if(mode==='goal'){
    const other=await fixture(2);await pool.query('UPDATE task_execution_metadata SET goal_id=$2 WHERE task_id=$1',[f.task,other.goal]);
  }
  await expect(readSupervisedResumeCandidates()).rejects.toThrow('TASK_EXECUTION_METADATA_MISSING');
});

it('is a read-only selector with the same contracts in one repeatable-read snapshot',async()=>{
  await fixture();const db=await pool.connect();
  try{await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    expect(await equivalent(db)).toHaveLength(1);await db.query('COMMIT');
  }finally{await db.query('ROLLBACK');db.release();}
});
