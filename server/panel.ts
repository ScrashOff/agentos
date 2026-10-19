import { pool, type DB } from './db.js';
import { config } from './config.js';
import { DomainError } from './contracts.js';
import { compactWorkFrontier } from './work-frontier.js';
import {deliveryPanel} from './delivery-panel.js';
import {panelExecutionColumns,panelExecutionJoin,checkedPanelExecution} from './task-panel-metadata.js';

// Explicit read models: never transfer execution inputs or frozen agent context
// just to display status. Full legacy API contracts remain unchanged.
const taskFields = `t.id,t.goal_id,t.key,t.title,t.agent_id,t.account_id,t.model,t.state,t.stage,
 t.priority,t.attempt_count,t.max_attempts,t.next_attempt_at,t.created_at,t.updated_at,
 left(t.reason,1200) reason,t.resources,t.criteria,t.covers,
 ${panelExecutionColumns}`;
// A checked, transactionally maintained read model includes continuation without
// decoding every task's embedded inputs on each status/detail request.
const taskProjection=panelExecutionJoin;
const runFields = `r.id,r.task_id,r.attempt,r.state,r.worker_id,r.account_id,r.runtime,r.agent_version,
 r.created_at,r.started_at,r.finished_at,r.heartbeat_at,r.context_id,left(r.summary,800) summary,
 r.error_code,r.usage,r.model,jsonb_strip_nulls(jsonb_build_object('kind',r.checkpoint->'kind',
 'sourceRunId',r.checkpoint->'sourceRunId')) checkpoint`;
const settingsFilter = `id IN('capacity','remote-capacity','continuation-supervisor','backlog-engine','maintenance')
 OR id LIKE 'objective-capacity:%' OR id LIKE 'planning-demand:%' OR id LIKE 'workforce:%' OR id LIKE 'workforce-health:%' OR id LIKE 'management:%'
 OR id LIKE 'management-state:%' OR id LIKE 'operation-progress:%'
 OR id LIKE 'exclusive-workstreams:%'`;

async function consistent<T>(read: (db: DB, cursor: string) => Promise<T>) {
  const db = await pool.connect();
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const cursor = (await db.query("SELECT COALESCE(max(id),0)::text cursor FROM events")).rows[0].cursor;
    const value = await read(db, cursor);
    await db.query('COMMIT');
    return value;
  } catch (error) { await db.query('ROLLBACK'); throw error; }
  finally { db.release(); }
}
async function panelSettings(db: DB) {
  const rows = (await db.query(`SELECT id,CASE WHEN id LIKE 'operation-progress:%'
   THEN value - 'inputDigests' - 'deliveryReceipts' ELSE value END value FROM settings WHERE (${settingsFilter})
   AND (id NOT LIKE '%:%' OR EXISTS(SELECT 1 FROM goals
    WHERE goals.id::text=substring(settings.id from position(':' in settings.id)+1)
    OR (settings.id LIKE 'exclusive-workstreams:%' AND goals.project=substring(settings.id from position(':' in settings.id)+1))))`)).rows;
  return Object.fromEntries(rows.map(row => [row.id, row.value]));
}
export async function panelState() {
  return consistent(async (db, eventCursor) => {
    const goals = (await db.query(`SELECT g.*,count(t.id)::int task_count,
     count(t.id) FILTER(WHERE t.state='accepted')::int accepted_count
     FROM goals g LEFT JOIN tasks t ON t.goal_id=g.id AND t.state<>'superseded'
     GROUP BY g.id ORDER BY g.created_at DESC`)).rows;
    const tasks = checkedPanelExecution((await db.query(`SELECT ${taskFields} FROM tasks t ${taskProjection}
     WHERE t.state NOT IN('accepted','cancelled','superseded') ORDER BY t.created_at,t.id`)).rows);
    const runs = (await db.query(`SELECT ${runFields} FROM tasks t
     JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
     WHERE t.state NOT IN('accepted','cancelled','superseded') ORDER BY r.created_at DESC`)).rows;
    return {
      goals,tasks,runs,
      agents:(await db.query('SELECT * FROM agents ORDER BY id')).rows,
      accounts:(await db.query('SELECT * FROM accounts ORDER BY id')).rows,
      events:(await db.query(`WITH latest AS MATERIALIZED(
        SELECT * FROM events WHERE id<=$1 ORDER BY id DESC LIMIT 50)
       SELECT e.* FROM latest e WHERE e.kind<>'EXECUTION_OBSERVED' OR e.id=(
        SELECT max(newer.id) FROM latest newer WHERE newer.kind='EXECUTION_OBSERVED'
        AND newer.payload->>'runId' IS NOT DISTINCT FROM e.payload->>'runId')
       ORDER BY e.id DESC`,[eventCursor])).rows,
      memories:[],contexts:[],artifacts:[],settings:await panelSettings(db),
      engine:(await db.query("SELECT value FROM settings WHERE id='scheduler-health'")).rows[0]?.value ?? null,
      integrations:{harness:!!config.dshCommand || (config.remoteExecution && !!config.bridgeHost),
        laya:!!config.layaUrl || !!config.bridgeHost,recall:(!!config.recallUrl && !!config.recallKey) || !!config.bridgeHost},
      eventCursor,summary:true,serverTime:new Date().toISOString(),
    };
  });
}
export async function panelGoal(id: string) {
  return consistent(async (db,eventCursor) => {
    const goal=(await db.query('SELECT * FROM goals WHERE id=$1',[id])).rows[0];
    if(!goal) throw new DomainError('NOT_FOUND','Objetivo não encontrado.',404);
    const tasks=checkedPanelExecution((await db.query(`SELECT ${taskFields} FROM tasks t ${taskProjection} WHERE t.goal_id=$1
      AND t.state<>'superseded' ORDER BY t.created_at,t.key`,[id])).rows);
    const dependencies=(await db.query('SELECT d.* FROM dependencies d JOIN tasks t ON t.id=d.task_id WHERE t.goal_id=$1',[id])).rows;
    const runs=(await db.query(`SELECT ${runFields} FROM tasks t
      JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
      WHERE t.goal_id=$1 AND t.state NOT IN('accepted','cancelled','superseded') ORDER BY r.created_at DESC`,[id])).rows;
    const acceptances=(await db.query(`SELECT a.* FROM tasks t
      JOIN LATERAL(SELECT id FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
      JOIN LATERAL(SELECT * FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC LIMIT 1)a ON true
      WHERE t.goal_id=$1 AND t.state NOT IN('accepted','cancelled','superseded')`,[id])).rows;
    const stored=(await db.query('SELECT value FROM settings WHERE id=$1',['work-frontier:'+id])).rows[0]?.value;
    return {goal,tasks,retiredTasks:[],dependencies,runs,acceptances,
      objective:(await db.query('SELECT id,status,mode,request,created_at FROM objectives WHERE id=$1',[id])).rows[0] ?? null,
      revisions:(await db.query('SELECT * FROM plan_revisions WHERE objective_id=$1 ORDER BY revision DESC LIMIT 2',[id])).rows,
      planning:(await db.query('SELECT id,state,error,created_at FROM planning_runs WHERE objective_id=$1 ORDER BY created_at DESC LIMIT 10',[id])).rows,
      planningAttempts:[],frontier:stored ? compactWorkFrontier(stored) : null,
      deliveryFlow:await deliveryPanel(db,id),
      eventCursor,summary:true,serverTime:new Date().toISOString()};
  });
}

/** Stable attempt pagination. The newest attempt cursor is inclusive only on
 * the first page; later pages cannot repeat rows if a new attempt is created. */
export async function panelTask(id: string,before?: number,limit=20) {
  return consistent(async (db,eventCursor) => {
    const task=checkedPanelExecution((await db.query(`SELECT ${taskFields},t.description FROM tasks t ${taskProjection} WHERE t.id=$1`,[id])).rows)[0];
    if(!task) throw new DomainError('NOT_FOUND','Tarefa não encontrada.',404);
    const runs=(await db.query(`SELECT r.id,r.task_id,r.attempt,r.state,r.account_id,r.runtime,r.model,r.created_at,
      r.started_at,r.finished_at,r.heartbeat_at,r.context_id,r.summary,r.error_code,r.usage,r.checkpoint
      FROM runs r WHERE r.task_id=$1 AND ($2::int IS NULL OR r.attempt<$2) ORDER BY r.attempt DESC LIMIT $3`,[id,before ?? null,limit+1])).rows;
    const hasMore=runs.length>limit; runs.splice(limit);
    const ids=runs.map(run=>run.id);
    const acceptances=(await db.query('SELECT * FROM acceptances WHERE run_id=ANY($1::uuid[]) ORDER BY created_at DESC',[ids])).rows;
    const artifacts=(await db.query('SELECT * FROM artifacts WHERE run_id=ANY($1::uuid[]) ORDER BY created_at DESC',[ids])).rows;
    const total=(await db.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[id])).rows[0].n;
    return {task,runs,acceptances,artifacts,eventCursor,
      page:{total,nextBefore:hasMore ? runs.at(-1)?.attempt : null},serverTime:new Date().toISOString()};
  });
}

export async function panelCollection(kind:'artifacts'|'memories'|'contexts'|'runs',before?:string,limit=50,goalId?:string) {
  const table=kind; // union validated by HTTP route; identifiers never come from unvalidated input.
  const columns=kind==='contexts' ? `id,run_id,created_at,jsonb_build_object('selector',manifest->'selector',
    'estimatedTokens',manifest->'estimatedTokens','selected',jsonb_build_array(),'summary',true) manifest` : kind==='runs'
    ? 'id,task_id,attempt,state,model,account_id,runtime,created_at,started_at,finished_at,summary,error_code,context_id,(SELECT title FROM tasks WHERE id=runs.task_id) task_title' : '*';
  return consistent(async(db)=>{
    const scope=kind==='memories' ? `EXISTS(SELECT 1 FROM goals g WHERE g.id=$3 AND g.project=entry.project)` :
      kind==='runs' ? `EXISTS(SELECT 1 FROM tasks t WHERE t.id=entry.task_id AND t.goal_id=$3)` :
      `EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=entry.run_id AND t.goal_id=$3)`;
    const rows=(await db.query(`SELECT ${columns} FROM ${table} WHERE
      ($1::uuid IS NULL OR (created_at,id)<(SELECT created_at,id FROM ${table} WHERE id=$1))
      AND ($3::uuid IS NULL OR ${scope.replaceAll('entry.',table+'.')})
      ORDER BY created_at DESC,id DESC LIMIT $2`,[before ?? null,limit+1,goalId ?? null])).rows;
    const more=rows.length>limit;rows.splice(limit);
    const total=(await db.query(`SELECT count(*)::int n FROM ${table} entry WHERE ($1::uuid IS NULL OR ${scope.replaceAll('$3','$1')})`,[goalId ?? null])).rows[0].n;
    return {rows,page:{total,nextBefore:more ? rows.at(-1)?.id : null}};
  });
}
