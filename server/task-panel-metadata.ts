import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import type {Pool} from 'pg';
import {pool,type DB} from './db.js';

// Independent from semantic task_execution_metadata: observation timestamps in
// continuation must never enter management materiality or change its receipt.
export const taskPanelMetadataVersion=1;
export const taskPanelMetadataReceipt='schema:task-panel-metadata';
const ownerLock=4280107;
const failure=(code:string)=>Object.assign(new Error(code),{code});
const sourceHash=async()=>createHash('sha256').update(await readFile(new URL('./task-panel-metadata.ts',import.meta.url))).digest('hex');

export const panelExecutionColumns=`panel_metadata.task_id panel_metadata_id,
 panel_metadata.version panel_metadata_version,(panel_metadata.goal_id=t.goal_id) panel_metadata_goal_valid,
 panel_metadata.projection_valid panel_metadata_valid,panel_metadata.execution execution`;
export const panelExecutionJoin='LEFT JOIN task_panel_metadata panel_metadata ON panel_metadata.task_id=t.id';
export function checkedPanelExecution<T extends Record<string,any>>(rows:T[]):T[]{
 return rows.map(row=>{
  const {panel_metadata_id:id,panel_metadata_version:version,panel_metadata_goal_valid:goalValid,
    panel_metadata_valid:valid,...value}=row;
  if(id!==row.id||version!==taskPanelMetadataVersion||goalValid!==true)throw failure('TASK_PANEL_METADATA_MISSING');
  if(valid!==true)throw failure('TASK_PANEL_METADATA_INVALID');
  return value as T;
 });
}

async function shape(db:DB){
 const r=(await db.query(`SELECT to_regclass('public.task_panel_metadata') IS NOT NULL present,
  to_regprocedure('public.agentos_panel_execution_project(jsonb)') IS NOT NULL projector,
  to_regprocedure('public.agentos_panel_execution_sync()') IS NOT NULL synchronizer,
  EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.tasks'::regclass AND tgname='agentos_panel_execution_sync'
   AND tgenabled IN('O','A') AND NOT tgisinternal AND tgtype=21 AND tgattr=''::int2vector AND tgqual IS NULL
   AND tgfoid=to_regprocedure('public.agentos_panel_execution_sync()')) trigger_ready`)).rows[0];
 return r.present&&r.projector&&r.synchronizer&&r.trigger_ready;
}
async function definitionHash(db:DB){
 const rows=(await db.query(`SELECT pg_get_functiondef('public.agentos_panel_execution_project(jsonb)'::regprocedure) projector,
  pg_get_functiondef('public.agentos_panel_execution_sync()'::regprocedure) synchronizer,pg_get_triggerdef(oid) trigger_definition
  FROM pg_trigger WHERE tgrelid='public.tasks'::regclass AND tgname='agentos_panel_execution_sync'`)).rows;
 return createHash('sha256').update(JSON.stringify(rows)).digest('hex');
}

/** Read-only startup verification. It neither installs DDL nor silently returns
 * empty metadata when the explicit preparer has not completed its backfill. */
export async function requireTaskPanelMetadataReady(db:DB=pool){
 const receipt=(await db.query('SELECT value FROM settings WHERE id=$1',[taskPanelMetadataReceipt])).rows[0]?.value;
 if(receipt?.version!==taskPanelMetadataVersion||receipt?.state!=='complete'||receipt?.sourceHash!==await sourceHash()||
  !(await shape(db))||receipt?.definitionHash!==await definitionHash(db))throw failure('TASK_PANEL_METADATA_REQUIRED');
 if((await db.query(`SELECT 1 FROM tasks t LEFT JOIN task_panel_metadata m ON m.task_id=t.id
  WHERE m.task_id IS NULL OR m.version<>$1 OR m.goal_id<>t.goal_id LIMIT 1`,[taskPanelMetadataVersion])).rowCount)
  throw failure('TASK_PANEL_METADATA_INCOMPLETE');
 return receipt;
}

/** Task-row locks pin the execution being projected. A concurrent writer either
 * finishes first or refreshes the new projection after this transaction commits. */
export async function backfillTaskPanelMetadataBatch(db:DB,limit=16){
 if(!Number.isInteger(limit)||limit<1||limit>128)throw failure('TASK_PANEL_METADATA_BATCH');
 const result=await db.query(`WITH batch AS MATERIALIZED(
  SELECT t.id,t.goal_id,t.execution FROM tasks t LEFT JOIN task_panel_metadata m ON m.task_id=t.id
  WHERE m.task_id IS NULL OR m.version<>$1 OR m.goal_id<>t.goal_id ORDER BY t.id LIMIT $2 FOR UPDATE OF t SKIP LOCKED
 ), projected AS MATERIALIZED(SELECT id,goal_id,agentos_panel_execution_project(execution) p FROM batch)
 INSERT INTO task_panel_metadata(task_id,goal_id,version,projection_valid,execution)
 SELECT id,goal_id,$1,(p->>'valid')::boolean,p->'execution' FROM projected
 ON CONFLICT(task_id) DO UPDATE SET goal_id=excluded.goal_id,version=excluded.version,
  projection_valid=excluded.projection_valid,execution=excluded.execution RETURNING task_id`,[taskPanelMetadataVersion,limit]);
 return result.rows.map(row=>row.task_id as string);
}

/** Explicit additive preparation, compatible with processes reading the old
 * tasks.execution field and with the unchanged semantic metadata v1. */
export async function installTaskPanelMetadata(database:Pool=pool){
 try{return await requireTaskPanelMetadataReady(database);}
 catch(error:any){if(!['TASK_PANEL_METADATA_REQUIRED','TASK_PANEL_METADATA_INCOMPLETE'].includes(error.code))throw error;}
 const db=await database.connect();let owned=false;
 try{
  await db.query('SELECT pg_advisory_lock($1)',[ownerLock]);owned=true;
  try{return await requireTaskPanelMetadataReady(db);}
  catch(error:any){if(!['TASK_PANEL_METADATA_REQUIRED','TASK_PANEL_METADATA_INCOMPLETE'].includes(error.code))throw error;}
  const expectedSourceHash=await sourceHash();
  const previousDefinitionHash=await shape(db)?await definitionHash(db):null;
  await db.query('BEGIN');
  try{
   await db.query(`INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value`,
    [taskPanelMetadataReceipt,JSON.stringify({version:taskPanelMetadataVersion,state:'preparing',sourceHash:expectedSourceHash})]);
   await db.query(`CREATE TABLE IF NOT EXISTS task_panel_metadata(
    task_id uuid PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    goal_id uuid NOT NULL REFERENCES goals(id) ON DELETE CASCADE,version integer NOT NULL,
    projection_valid boolean NOT NULL,execution jsonb NOT NULL);
    CREATE OR REPLACE FUNCTION agentos_panel_execution_project(value jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE projected jsonb;
    BEGIN
     -- Preserve the old read-model casts and recursive null stripping exactly.
     -- Invalid historical shapes stay writable but cannot masquerade as {}.
     IF value IS NOT NULL AND jsonb_typeof(value)<>'object' THEN
      RETURN jsonb_build_object('valid',false,'execution','{}'::jsonb);
     END IF;
     BEGIN
      SELECT jsonb_strip_nulls(to_jsonb(x)) INTO projected FROM jsonb_to_record(value)
       AS x(kind text,"baseWorkspace" text,workspace text,"workKind" text,"workforceLane" text,
        "readOnlySource" boolean,continuation jsonb);
     EXCEPTION WHEN invalid_text_representation OR invalid_parameter_value OR numeric_value_out_of_range THEN
      RETURN jsonb_build_object('valid',false,'execution','{}'::jsonb);
     END;
     RETURN jsonb_build_object('valid',true,'execution',COALESCE(projected,'{}'::jsonb));
    END $$;
    CREATE OR REPLACE FUNCTION agentos_panel_execution_sync() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE p jsonb;
    BEGIN
     p:=agentos_panel_execution_project(NEW.execution);
     INSERT INTO task_panel_metadata(task_id,goal_id,version,projection_valid,execution)
      VALUES(NEW.id,NEW.goal_id,${taskPanelMetadataVersion},(p->>'valid')::boolean,p->'execution')
      ON CONFLICT(task_id) DO UPDATE SET goal_id=excluded.goal_id,version=excluded.version,
       projection_valid=excluded.projection_valid,execution=excluded.execution;
     RETURN NEW;
    END $$;
    DO $$ BEGIN
     IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.tasks'::regclass AND tgname='agentos_panel_execution_sync') THEN
      CREATE TRIGGER agentos_panel_execution_sync AFTER INSERT OR UPDATE ON tasks
       FOR EACH ROW EXECUTE FUNCTION agentos_panel_execution_sync();
     END IF;
    END $$;`);
   if(previousDefinitionHash!==await definitionHash(db))await db.query('UPDATE task_panel_metadata SET version=0');
   await db.query('COMMIT');
  }catch(error){await db.query('ROLLBACK');throw error;}
  while((await backfillTaskPanelMetadataBatch(db)).length){/* Explicit resumable batches only. */}
  if(await sourceHash()!==expectedSourceHash)throw failure('TASK_PANEL_METADATA_SOURCE_CHANGED');
  if(!(await shape(db))||(await db.query(`SELECT 1 FROM tasks t LEFT JOIN task_panel_metadata m ON m.task_id=t.id
   WHERE m.task_id IS NULL OR m.version<>$1 OR m.goal_id<>t.goal_id LIMIT 1`,[taskPanelMetadataVersion])).rowCount)throw failure('55P03');
  const receipt={version:taskPanelMetadataVersion,state:'complete',sourceHash:expectedSourceHash,
   definitionHash:await definitionHash(db),completedAt:new Date().toISOString()};
  await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
   [taskPanelMetadataReceipt,JSON.stringify(receipt)]);
  return receipt;
 }finally{if(owned)await db.query('SELECT pg_advisory_unlock($1)',[ownerLock]).catch(()=>{});db.release();}
}
