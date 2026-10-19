import {pool,tx,lock,event,type DB} from './db.js';
import {accountBridge} from './bridge.js';
import {assertStopped} from './continuation.js';

const resource='storage:agentos-workspaces';
const live=['queued','running','cancelling','reconciling'];
export async function recordStorageFailure(db:DB,runId:string|null){
 const old=(await db.query('SELECT value FROM settings WHERE id=$1',[resource])).rows[0]?.value;
 await db.query(`INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=settings.value||excluded.value`,
  [resource,JSON.stringify({state:'blocked',code:'DISK_CAPACITY',lastFailureAt:new Date().toISOString(),sourceRunId:runId})]);
 if(old?.state!=='blocked')await event(db,'STORAGE_CAPACITY_BLOCKED',resource,{runId,code:'DISK_CAPACITY'});
}
export function validStorageObservation(v:any){
 const age=Date.now()-Date.parse(v?.checkedAt);
 return v?.version===1 && v.resource==='agentos-workspaces' && Number.isFinite(age) && age>=-5000 && age<60000 &&
  ['freeBytes','reservedBytes','availableBytes','headroomBytes'].every(k=>Number.isSafeInteger(v[k])&&v[k]>=0) &&
  v.headroomBytes>=2*1024**3 && v.availableBytes===Math.max(0,v.freeBytes-v.reservedBytes) &&
  v.available===(v.availableBytes>=v.headroomBytes);
}

/** Resource availability is an observed fact. It cannot stand in for a model
 * decision, a process stop receipt, an accepted dependency or task acceptance. */
export async function reconcileStorageRecovery(inspect:(id:string,native:boolean)=>Promise<any>,
 request:typeof accountBridge.request=accountBridge.request.bind(accountBridge)){
 const rows=(await pool.query(`SELECT t.id,t.goal_id,t.state,t.execution->>'workspace' workspace,
  t.execution->>'sourceWorkspace' source_workspace,r.id run_id,r.runtime,r.fence
  FROM tasks t JOIN goals g ON g.id=t.goal_id JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
  LEFT JOIN settings a ON a.id='autonomy:'||g.project
  WHERE g.state='active' AND r.state='failed' AND r.error_code='DISK_CAPACITY'
  AND t.state IN('failed','recovery_blocked','waiting_resource','blocked_dependencies')
  AND COALESCE(a.value->>'enabled','true')='true'
  AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
  AND (t.execution->'resourceRecovery'->>'nextAt' IS NULL OR t.execution->'resourceRecovery'->>'nextAt'<=$1)
  ORDER BY t.updated_at,t.id LIMIT 6`,[new Date().toISOString()])).rows;
 const old=(await pool.query('SELECT value FROM settings WHERE id=$1',[resource])).rows[0]?.value;
 if(!rows.length && old?.state!=='blocked')return [];
 let observation:any;
 try{observation=await request('worker.storage',{}, {timeout:15000});}catch{return [];}
 if(!validStorageObservation(observation))return [];
 const applied=await tx(async db=>{
  await lock(db);
  const latest=(await db.query('SELECT value FROM settings WHERE id=$1',[resource])).rows[0]?.value;
  if(Date.parse(latest?.lastFailureAt??'')>Date.parse(observation.checkedAt))return false;
  const state=observation.available?'available':'blocked';
  await db.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=settings.value||excluded.value',
   [resource,JSON.stringify({...observation,state})]);
  if(latest?.state!==state)await event(db,observation.available?'STORAGE_CAPACITY_RESTORED':'STORAGE_CAPACITY_BLOCKED',resource,observation);
  return true;
 });
 if(!applied)return [];
 const recovered=[];
 for(const row of rows){
  let code=observation.available?'RESOURCE_RECOVERY_PENDING':'DISK_CAPACITY';
  try{
   if(!observation.available)continue;
   if(!(await pool.query(`SELECT 1 FROM events WHERE entity_id=$1 AND payload->>'runId'=$2
    AND kind=$3 AND payload->>'confirmed'='true'
    AND payload->>(CASE WHEN $3='OPENCODE_NOT_STARTED' THEN 'errorCode' ELSE 'code' END)='DISK_CAPACITY'
    AND NOT EXISTS(SELECT 1 FROM events x WHERE x.payload->>'runId'=$2 AND x.kind IN
    ('CANCEL_REQUESTED','RUNTIME_STARTED','HARNESS_STARTED','MODEL_RESPONSE','TOOL_CALL','TOOL_RESULT','HARNESS_TOOL_EVENT','OPENCODE_TOOL_EVENT'))`,
    [row.id,row.run_id,row.runtime==='opencode'?'OPENCODE_NOT_STARTED':'HARNESS_NOT_STARTED'])).rowCount)continue;
   // Materialization is idempotent and owns the workspace/storage locks. It
   // cannot execute tools or a model. It may still refuse a large new copy.
   if(row.source_workspace)await request('worker.prepare',{workspace:row.workspace,sourceWorkspace:row.source_workspace},{timeout:180000});
   const proof=await inspect(row.run_id,row.runtime==='opencode');
   observation=await request('worker.storage',{}, {timeout:15000});
   const done=await tx(async db=>{
    await lock(db);
    const t=(await db.query(`SELECT t.* FROM tasks t JOIN goals g ON g.id=t.goal_id
     LEFT JOIN settings a ON a.id='autonomy:'||g.project WHERE t.id=$1 AND g.state='active'
     AND COALESCE(a.value->>'enabled','true')='true' AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true') FOR UPDATE OF t`,[row.id])).rows[0];
    const r=(await db.query('SELECT * FROM runs WHERE id=$1 AND fence=$2',[row.run_id,row.fence])).rows[0];
    if(!t||!r||r.state!=='failed'||r.error_code!=='DISK_CAPACITY'||t.attempt_count!==r.attempt||
     !['failed','recovery_blocked','waiting_resource','blocked_dependencies'].includes(t.state)||
     t.execution.workspace!==row.workspace||(t.execution.sourceWorkspace??null)!==row.source_workspace)return false;
    assertStopped(proof,r,t);
    if(!validStorageObservation(observation)||!observation.available)return false;
    const storage=(await db.query('SELECT value FROM settings WHERE id=$1',[resource])).rows[0]?.value;
    if(Date.parse(storage?.lastFailureAt??'')>Date.parse(observation.checkedAt))return false;
    // Never use the error label alone to infer that the failed attempt did no work.
    const notStarted=(await db.query(`SELECT 1 FROM events WHERE entity_id=$1 AND payload->>'runId'=$2
     AND kind=$3 AND payload->>'confirmed'='true'
     AND payload->>(CASE WHEN $3='OPENCODE_NOT_STARTED' THEN 'errorCode' ELSE 'code' END)='DISK_CAPACITY'`,
     [t.id,r.id,r.runtime==='opencode'?'OPENCODE_NOT_STARTED':'HARNESS_NOT_STARTED'])).rowCount;
    if(!notStarted)return false;
    if((await db.query(`SELECT 1 FROM runs x JOIN tasks w ON w.id=x.task_id WHERE x.state=ANY($3::text[])
      AND (x.task_id=$1 OR w.execution->>'workspace'=$4)
     UNION ALL SELECT 1 FROM events WHERE payload->>'runId'=$2 AND kind IN
      ('CANCEL_REQUESTED','RUNTIME_STARTED','HARNESS_STARTED','MODEL_RESPONSE','TOOL_CALL','TOOL_RESULT','HARNESS_TOOL_EVENT','OPENCODE_TOOL_EVENT')
     UNION ALL SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=$1 AND p.state<>'accepted' LIMIT 1`,
     [t.id,r.id,live,t.execution.workspace])).rowCount)return false;
    const cases=(await db.query(`SELECT * FROM supervision_cases WHERE task_id=$1
     AND state IN('open','decided','delegated','waiting_evidence') FOR UPDATE`,[t.id])).rows;
    // Do not erase an applied/pending semantic decision or independently running
    // support. Only unassigned incidents about this exact preflight are retired.
    if(cases.some(c=>c.source_run_id!==r.id||c.decision_run_id||c.support_task_id||c.support_task_ids?.length||!['open','waiting_evidence'].includes(c.state)||
      !Array.isArray(c.evidence)||!c.evidence.length||c.evidence.some((item:any)=>item.code!=='DISK_CAPACITY'))) {
     if(!(await db.query("SELECT 1 FROM events WHERE kind='STORAGE_RECOVERY_EVIDENCE' AND entity_id=$1 AND payload->>'runId'=$2",[t.id,r.id])).rowCount)
      await event(db,'STORAGE_RECOVERY_EVIDENCE',t.id,{runId:r.id,observation,supervisionPreserved:true});
     return false;
    }
    for(const c of cases)await db.query("UPDATE supervision_cases SET state='superseded',reason='Pré-execução bloqueada apenas por armazenamento; capacidade e parada confirmadas.',updated_at=now() WHERE id=$1",[c.id]);
    await db.query(`UPDATE tasks SET state='ready',next_attempt_at=NULL,max_attempts=GREATEST(max_attempts,attempt_count+1),
      reason='Capacidade de armazenamento recuperada; retomada da tarefa original.',updated_at=now(),
      execution=jsonb_set(execution,'{continuation}',$2) WHERE id=$1`,[t.id,JSON.stringify({state:'queued',owner:'scheduler',runId:r.id,reasonCode:'STORAGE_CAPACITY_RESTORED',updatedAt:new Date().toISOString()})]);
    await event(db,'STORAGE_PREFLIGHT_REQUEUED',t.id,{runId:r.id,observation,checkedAt:proof.checkedAt,workspace:proof.workspace,process:proof.process,criteriaPreserved:true});
    return true;
   });
   if(done)recovered.push(row.id);
  }catch(e:any){code=/^[A-Z][A-Z0-9_]{2,80}$/.test(e.code??e.message??'')?(e.code??e.message):'STORAGE_OBSERVATION_FAILED';}
  finally{
   if(!recovered.includes(row.id))await pool.query(`UPDATE tasks SET updated_at=now(),execution=jsonb_set(execution,'{resourceRecovery}',$3)
    WHERE id=$1 AND attempt_count=(SELECT attempt FROM runs WHERE id=$2) AND state IN('failed','recovery_blocked','waiting_resource','blocked_dependencies')`,
    [row.id,row.run_id,JSON.stringify({state:'waiting_resource',owner:'controller',reasonCode:code,runId:row.run_id,nextAt:new Date(Date.now()+60000).toISOString(),message:observation.available?'Armazenamento recuperado; conferindo parada e supervisão existente.':'Aguardando capacidade do volume; sem nova tentativa de modelo.'})]);
  }
 }
 return recovered;
}
