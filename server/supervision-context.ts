import type { DB } from './db.js';
import { readArtifact } from './service.js';
import {requireTaskExecutionMetadataReady,taskExecutionMetadataVersion} from './task-execution-metadata.js';
export { mergeSupervisorInputs } from './execution-inputs.js';

const MAX_BYTES = 24 * 1024 * 1024;
const repairNames = [
  'implementation.patch',
  'repair-report.json',
  'protocol-report.json',
  'repair-tests.log',
  'candidate.json',
  'changes.patch',
];
const failedNames = [
  'runtime-composition.json',
  'candidate.json',
  'changes.patch',
  // Repair candidates live under data/, outside the canonical source delta.
  // Preserve their attempted patch and regression as rejected evidence so the
  // next worker can investigate the actual partial repair without accepting it.
  'implementation.patch',
  'repair-report.json',
  'repair-tests.log',
  'protocol-report.json',
  'protocol-tests.log',
  'test-source.mjs',
  'verification.json',
  'recovery-report.json',
  'final-integration-report.json',
  'boot-identity-probe.json',
  'canonical-protocol.log',
  'recovery-cross-module.log',
];

/** Discover the graph using small relational IDs before decoding workspace
 * metadata. The materialized row boundary prevents PostgreSQL from applying
 * JSON predicates to every task or decoding the same input payload per edge.
 * The second traversal retains the original per-hop workspace boundary. */
export async function readSupervisionRepairPeers(db:DB,source:any,target:any) {
  return (await db.query(
      `WITH RECURSIVE ancestor_ids(id) AS (
        SELECT d.depends_on FROM dependencies d JOIN tasks p ON p.id=d.depends_on
        WHERE d.task_id=ANY($1::uuid[]) AND p.goal_id=$3
        UNION
        SELECT d.depends_on FROM dependencies d JOIN ancestor_ids parent ON d.task_id=parent.id
        JOIN tasks p ON p.id=d.depends_on WHERE p.goal_id=$3
      ), ancestor_rows AS MATERIALIZED (
        SELECT p.id,p.title,p.created_at,p.attempt_count,p.state,p.execution
        FROM ancestor_ids ancestor JOIN tasks p ON p.id=ancestor.id WHERE p.goal_id=$3
      ), scoped AS MATERIALIZED (
        SELECT p.id,p.title,p.created_at,p.attempt_count,p.state,meta."workKind" work_kind
        FROM ancestor_rows p CROSS JOIN LATERAL jsonb_to_record(
          CASE WHEN jsonb_typeof(p.execution)='object' THEN p.execution ELSE '{}'::jsonb END
        ) AS meta("baseWorkspace" text,"workKind" text)
        WHERE meta."baseWorkspace"=$4
      ), parents(id) AS (
        SELECT d.depends_on FROM dependencies d JOIN scoped p ON p.id=d.depends_on
        WHERE d.task_id=ANY($1::uuid[])
        UNION
        SELECT d.depends_on FROM dependencies d JOIN parents parent ON d.task_id=parent.id
        JOIN scoped p ON p.id=d.depends_on
      ) SELECT p.id,p.title,r.id run_id FROM parents parent JOIN scoped p ON p.id=parent.id
    JOIN runs r ON r.task_id=p.id AND r.attempt=p.attempt_count WHERE p.id<>$2
    AND p.state='accepted' AND p.work_kind='repair'
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed)
    ORDER BY p.created_at,p.id`,
      [[target.id, source.id], source.id, target.goal_id, target.execution.baseWorkspace],
  )).rows;
}

/** Preserve the staged handoff selection while filtering on the canonical raw
 * projection. Check missing/stale metadata before projected filters can hide it;
 * the returned run is still the actual current accepted attempt. No artifact
 * bytes, task execution payload or acceptance policy are changed here. */
export async function readStagedSupervisionRepairs(db:DB,source:any,target:any){
  await requireTaskExecutionMetadataReady(db);
  const rows=(await db.query(`WITH relational AS MATERIALIZED (
    SELECT p.id,p.title,p.created_at,p.goal_id,r.id run_id,metadata.task_id metadata_id,
      metadata.goal_id metadata_goal,metadata.version metadata_version,metadata.control
    FROM deferred_handoffs h JOIN tasks p ON p.id=h.producer_task_id
    JOIN runs r ON r.task_id=p.id AND r.attempt=p.attempt_count
    LEFT JOIN task_execution_metadata metadata ON metadata.task_id=p.id
    WHERE h.consumer_task_id=$1 AND h.goal_id=$2 AND h.state IN('staged','replanning_required')
      AND p.id<>$3 AND p.state='accepted' AND r.state='submitted'
      AND (SELECT passed FROM acceptances v WHERE v.run_id=r.id ORDER BY created_at DESC LIMIT 1)=true
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=p.id AND live.state IN('queued','running','cancelling','reconciling'))
  ), guard AS MATERIALIZED (
    SELECT CASE WHEN EXISTS(SELECT 1 FROM relational WHERE metadata_id IS DISTINCT FROM id
      OR metadata_version IS DISTINCT FROM ${taskExecutionMetadataVersion} OR metadata_goal IS DISTINCT FROM goal_id)
      THEN 'TASK_EXECUTION_METADATA_MISSING' END selection_error
  ) SELECT guard.selection_error staged_selection_error,selected.id,selected.title,selected.run_id
    FROM guard LEFT JOIN LATERAL (
      SELECT id,title,run_id,created_at FROM relational WHERE guard.selection_error IS NULL
        AND control->>'workKind'='repair' AND control->>'baseWorkspace'=$4
    )selected ON guard.selection_error IS NULL ORDER BY selected.created_at,selected.id`,
    [target.id,target.goal_id,source.id,target.execution.baseWorkspace])).rows;
  const error=rows.find(row=>row.staged_selection_error)?.staged_selection_error;
  if(error)throw Object.assign(new Error(error),{code:error});
  return rows.filter(row=>row.id!==null).map(({staged_selection_error:_sentinel,...row})=>row);
}

// A repair needs the accepted modules already produced for its consumer, plus
// the actual rejected report. Neither category changes the consumer's acceptance.
export async function supervisionInputs(db: DB, source: any, target: any) {
  const refs = (
    await db.query(
      `SELECT a.id,a.name,r.task_id,r.id run_id,true required,'accepted_source' role
    FROM artifacts a JOIN runs r ON r.id=a.run_id WHERE r.task_id=$1 AND r.attempt=$2 AND a.name<>'task-instructions-history.json'
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed) ORDER BY a.name`,
      [source.id, source.attempt_count],
    )
  ).rows;
  const peers = await readSupervisionRepairPeers(db,source,target);
  if((await db.query("SELECT to_regclass('public.deferred_handoffs') present")).rows[0].present){
    const staged=await readStagedSupervisionRepairs(db,source,target);
    for(const repair of staged)if(!peers.some(p=>p.id===repair.id))peers.push(repair);
  }
  for (const p of peers)
    refs.push(
      ...(
        await db.query(
          `SELECT id,name,$2::uuid task_id,run_id,false required,'accepted_repair' role
    FROM artifacts WHERE run_id=$1 AND name=ANY($3::text[]) ORDER BY name`,
          [p.run_id, p.id, repairNames],
        )
      ).rows,
    );
  const failed = (
    await db.query(
      `SELECT id FROM runs WHERE task_id=$1 AND state IN('failed','submitted')
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=runs.id AND NOT v.passed) ORDER BY attempt DESC LIMIT 1`,
      [target.id],
    )
  ).rows[0];
  if (failed)
    refs.push(
      ...(
        await db.query(
          `SELECT id,name,$2::uuid task_id,run_id,false required,'rejected_evidence' role
    FROM artifacts WHERE run_id=$1 AND name=ANY($3::text[]) ORDER BY name`,
          [failed.id, target.id, failedNames],
        )
      ).rows,
    );
  const inputs: any[] = [];
  const provenance: any[] = [];
  const omitted: any[] = [];
  let bytes = 0;
  for (const ref of refs) {
    if (inputs.some((a) => a.id === ref.id)) continue;
    const a = await readArtifact(ref.id);
    if (bytes + a.bytes.length > MAX_BYTES) {
      if (ref.required) throw Error('SUPERVISION_SOURCE_TOO_LARGE');
      omitted.push({ id: ref.id, role: ref.role, reason: 'input_bytes_limit' });
      continue;
    }
    inputs.push({ id: ref.id, name: a.name, digest: a.digest, base64: a.bytes.toString('base64') });
    provenance.push({
      artifactId: ref.id,
      taskId: ref.task_id,
      runId: ref.run_id,
      name: a.name,
      role: ref.role,
      digest: a.digest,
    });
    bytes += a.bytes.length;
  }
  return { inputs, provenance, omitted };
}
