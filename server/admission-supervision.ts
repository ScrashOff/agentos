import {randomUUID} from 'node:crypto';
import {event,type DB} from './db.js';
import {canonicalRepairReviewIsFresh} from './canonical-repair-rejection.js';
import {operatorOpenCodeReviewIsFresh} from './operator-opencode-review.js';

/** A rejected admission has no new run. Its previous closed case therefore
 * needs fresh supervision instead of waiting for another failed attempt. */
export async function requestAdmissionSupervision(db:DB) {
 const rows=(await db.query(`SELECT t.id,t.goal_id,t.instruction_revision_id,r.id run_id,r.attempt,
 e.id event_id,e.created_at event_at,e.payload,c.id case_id,c.state case_state,c.reason case_reason,
 c.decision,c.decision_run_id,c.applied_attempt,c.evidence
 FROM tasks t JOIN goals g ON g.id=t.goal_id
 JOIN settings s ON s.id='supervision:'||g.id
 LEFT JOIN settings a ON a.id='autonomy:'||g.project
 JOIN LATERAL(SELECT id,attempt,state FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
 JOIN LATERAL(SELECT id,created_at,payload FROM events WHERE kind='RUN_ADMISSION_BLOCKED'
  AND entity_id=t.id::text AND payload->>'goalId'=g.id::text
  AND payload->>'instructionRevisionId' IS NOT DISTINCT FROM t.instruction_revision_id::text
  ORDER BY id DESC LIMIT 1)e ON true
 LEFT JOIN supervision_cases c ON c.task_id=t.id AND c.source_run_id=r.id
 WHERE g.state='active' AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
 AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
 AND t.state IN('recovery_blocked','blocked_dependencies','waiting_resource','ready','retry_scheduled')
 AND t.execution->>'kind' IN('harness','opencode')
 AND r.attempt=t.attempt_count AND r.state IN('failed','submitted')
 AND e.payload->>'code' IN('TASK_INSTRUCTIONS_CHANGED','INSTRUCTION_CASE_CHANGED','INSTRUCTION_CASE_INVALID',
  'INSTRUCTION_SOURCE_INVALID','INSTRUCTION_DECISION_UNVERIFIED','SUPERVISION_INPUTS_INVALID',
  'SUPERVISION_INPUT_DIGEST','SUPERVISION_INPUTS_TOO_LARGE','SUPERVISION_INPUT_COLLISION')
 AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','cancelling','reconciling'))
 AND NOT EXISTS(SELECT 1 FROM events cancel WHERE cancel.kind='CANCEL_REQUESTED' AND cancel.payload->>'runId'=r.id::text)
 AND NOT EXISTS(SELECT 1 FROM supervision_cases pending WHERE pending.task_id=t.id AND pending.state IN('open','decided','delegated','waiting_evidence'))
 AND NOT EXISTS(SELECT 1 FROM supervision_cases newer WHERE newer.task_id=t.id AND c.id IS NOT NULL AND newer.created_at>c.created_at)
 FOR UPDATE OF t`)).rows;
 const added:string[]=[];
 for(const row of rows){
  if((row.evidence??[]).some((item:any)=>item.admissionEventId===String(row.event_id)))continue;
  const caseId=row.case_id??randomUUID();
  const evidence={kind:'RUN_ADMISSION_BLOCKED',admissionEventId:String(row.event_id),
   at:new Date(row.event_at).toISOString(),code:row.payload.code,runId:row.run_id,
   attempt:row.attempt,instructionRevisionId:row.instruction_revision_id,
   message:'A admissão não criou outra tentativa. Revise a proveniência atual; preserve os critérios e snapshots. Uma decisão anterior a este evento não pode retomá-la.'};
  const reason='Admissão exige nova revisão de proveniência: '+row.payload.code;
  if(row.case_id){
   await db.query("UPDATE supervision_cases SET state='open',reason=$2,evidence=evidence||$3::jsonb,updated_at=now() WHERE id=$1",
    [caseId,reason,JSON.stringify([evidence])]);
  }else{
   await db.query('INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,$5,$6)',
    [caseId,row.goal_id,row.id,row.run_id,reason,JSON.stringify([evidence])]);
  }
  await db.query("UPDATE tasks SET state='recovery_blocked',reason=$2,updated_at=now() WHERE id=$1",[row.id,reason]);
  await event(db,'ADMISSION_SUPERVISION_REQUESTED',row.id,{caseId,admissionEventId:String(row.event_id),
   instructionRevisionId:row.instruction_revision_id,sourceRunId:row.run_id,
   previousCase:row.case_id?{state:row.case_state,reason:row.case_reason,decision:row.decision,
    decisionRunId:row.decision_run_id,appliedAttempt:row.applied_attempt}:null});
  added.push(caseId);
 }
 return added;
}

export function admissionReviewIsFresh(evidence:any[],createdAt:string|Date) {
 if(!canonicalRepairReviewIsFresh(evidence,createdAt))return false;
 if(!operatorOpenCodeReviewIsFresh(evidence,createdAt))return false;
 const dates=evidence.filter(item=>item?.kind==='RUN_ADMISSION_BLOCKED').map(item=>Date.parse(item.at));
 if(dates.some(date=>!Number.isFinite(date)))return false;
 const minimum=Math.max(0,...dates);
 return !minimum || new Date(createdAt).getTime()>=minimum;
}
