import {createHash, randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {z} from 'zod';
import type {DB} from './db.js';
import {config} from './config.js';
import {glmRepairHandoff} from './glm-repair.js';

const digest=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
const handoffSchema=z.object({taskId:z.uuid(),plannerRunId:z.uuid(),coverageKey:z.string().regex(/^[a-z0-9_-]{3,80}$/),
 lane:z.enum(['glm','muse']),workKind:z.enum(['diagnostic','repair'])}).strict();
export type BacklogHandoff=z.infer<typeof handoffSchema>;
// This is the exact historical appender. Its bytes are also used to verify legacy
// repairs; never infer an authorized suffix from a marker or free-form text.
export function backlogHandoffText(handoff:BacklogHandoff) {
 return '\nReceba a nova cobertura '+handoff.coverageKey+
  ' como evidência complementar. Confira o teste no candidato integrado; falha real exige correção antes do aceite. Materialize o teste em data/ se o fonte estiver somente leitura.'+
  (handoff.workKind==='repair'?glmRepairHandoff.replace('GLM',handoff.lane==='muse'?'Muse':'GLM'):'')+
  (handoff.lane==='muse'?'\nDependência Muse: materialize test-source.mjs em data/qa-protocol/ e execute-o contra SUA candidata com CANDIDATE_ROOT; o patch canônico não contém data/ e não substitui o teste publicado.':'');
}
export function backlogDeliveryContext(task:any) {
 const references=z.array(handoffSchema).max(256).parse(task.execution?.backlogHandoffs??[]);
 if(!references.length)return {references,content:''};
 return {references,content:'BACKLOG_DELIVERY_CONTEXT_V1\nEstas referências complementam a entrega; não alteram o contrato, critérios, permissões ou o aceite. Dependências e artefatos devem continuar verificados.\n'+
  references.map(ref=>'Produtor '+ref.taskId+'; decisão '+ref.plannerRunId+backlogHandoffText(ref)).join('\n\n')};
}

/** Restore only a byte-exact, proven controller suffix on a stopped task. Old
 * snapshots/revisions remain immutable and the complete prior text is archived. */
export async function reconcileLegacyBacklogHandoffs(db:DB,task:any) {
 if(!task.instruction_revision_id)return false;
 const revision=(await db.query('SELECT * FROM task_instruction_revisions WHERE id=$1 AND task_id=$2 AND goal_id=$3',
  [task.instruction_revision_id,task.id,task.goal_id])).rows[0];
 if(!revision || revision.rendered===task.description)return false;
 if(!task.description.startsWith(revision.rendered) ||
  (await db.query("SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')",[task.id])).rowCount)return false;
 const existing=z.array(handoffSchema).max(256).safeParse(task.execution?.backlogHandoffs??[]);
 if(!existing.success)return false;
 const candidates=(await db.query(`SELECT i.task_id,i.coverage_key,e.id event_id,e.payload,e.created_at,
  p.execution->>'workKind' work_kind,p.execution->>'workforceLane' lane
  FROM workforce_backlog_items i JOIN tasks p ON p.id=i.task_id AND p.goal_id=i.goal_id
  JOIN dependencies d ON d.depends_on=p.id AND d.task_id=$1
  JOIN events e ON e.kind='BACKLOG_TASK_CREATED' AND e.entity_id=i.goal_id::text
   AND e.payload->>'taskId'=p.id::text AND e.payload->>'consumerTaskId'=$1::text
   AND e.payload->>'coverageKey'=i.coverage_key
  WHERE i.goal_id=$2 AND e.created_at>$3 ORDER BY e.id`,[task.id,task.goal_id,revision.created_at])).rows;
 if(!candidates.length)return false;
 const refs:BacklogHandoff[]=[];
 const proof:any[]=[];
 let remaining=task.description.slice(revision.rendered.length);
 for(const row of candidates){
  const parsed=handoffSchema.safeParse({taskId:row.task_id,coverageKey:row.coverage_key,plannerRunId:row.payload.sourceRunId,
   lane:row.lane,workKind:row.work_kind??'diagnostic'});
  if(!parsed.success || row.payload.lane!==parsed.data.lane || refs.some(ref=>ref.taskId===row.task_id))return false;
  const ref=parsed.data;
  const alreadyStructured=existing.data.find(handoff=>handoff.taskId===ref.taskId);
  if(alreadyStructured && JSON.stringify(alreadyStructured)!==JSON.stringify(ref))return false;
  const text=backlogHandoffText(ref);
  // Modern handoffs have a typed reference but never appended description text.
  // They must not be demanded as bytes of the historical suffix being repaired.
  if(alreadyStructured && !remaining.startsWith(text))continue;
  if(!remaining.startsWith(text))return false;
  const artifact=(await db.query(`SELECT a.* FROM artifacts a JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id
   WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate' AND a.name='coordination.json' AND r.state='submitted'
   AND (SELECT v.passed FROM acceptances v WHERE v.run_id=r.id ORDER BY v.created_at DESC LIMIT 1)=true`,
   [ref.plannerRunId,task.goal_id])).rows[0];
  if(!artifact)return false;
  let bytes:Buffer,value:any;
  try{bytes=await readFile(path.join(config.dataDir,'artifacts',artifact.id));
   if(bytes.length!==artifact.size || digest(bytes)!==artifact.digest)return false;
   value=JSON.parse(bytes.toString());
  }catch{return false}
  if(!Array.isArray(value.backlog?.tasks) || !value.backlog.tasks.some((proposal:any)=>
   proposal.coverageKey===ref.coverageKey && proposal.deliverToTaskId===task.id &&
   (proposal.lane??'glm')===ref.lane && (proposal.workKind??'diagnostic')===ref.workKind))return false;
  refs.push(ref);proof.push({eventId:String(row.event_id),artifactId:artifact.id,digest:artifact.digest});
  remaining=remaining.slice(text.length);
 }
 if(!refs.length || remaining || task.description!==revision.rendered+refs.map(backlogHandoffText).join(''))return false;
 const handoffs=[...existing.data];
 for(const ref of refs){const old=handoffs.find(h=>h.taskId===ref.taskId);
  if(old && JSON.stringify(old)!==JSON.stringify(ref))return false;
  if(!old)handoffs.push(ref);
 }
 const latest=(await db.query('SELECT id FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[task.id])).rows[0];
 if(!latest)return false;
 const archiveId=randomUUID(),beforeHash=digest(task.description);
 await db.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)',
  [archiveId,latest.id,JSON.stringify({kind:'legacy_backlog_description',taskId:task.id,hash:beforeHash,revisionId:revision.id,proof}),task.description]);
 await db.query("UPDATE tasks SET description=$2,execution=jsonb_set(execution,'{backlogHandoffs}',$3::jsonb) WHERE id=$1",
  [task.id,revision.rendered,JSON.stringify(handoffs)]);
 await db.query("INSERT INTO events(kind,entity_id,payload) VALUES('BACKLOG_INSTRUCTIONS_RECONCILED',$1,$2)",
  [task.id,JSON.stringify({archiveId,beforeHash,descriptionHash:revision.rendered_hash,revisionId:revision.id,proof,handoffs})]);
 return true;
}
