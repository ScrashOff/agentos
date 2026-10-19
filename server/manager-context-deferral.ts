import {createHash} from 'node:crypto';
import type {DB} from './db.js';
import {managerFocusSourceFingerprint} from './manager-focus.js';

export const managerContextDeferralMilliseconds=120000;
const sha=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const uuid=/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const digest=/^[a-f0-9]{64}$/;
export type ContextCaseBinding={case_id:string;case_contract:string;task_id:string;task_contract:string;
 source_run_id:string|null;instruction_revision_id:string|null;run_id:string|null;run_contract:string|null;
 artifact_binding:unknown};
export type ContextDeferral={caseId:string;taskId:string;sourceFingerprint:string;projectionVersion:string;
 code:'CONTEXT_TOO_LARGE';sourceHash:string;characters:number;maximum:number;checkedAt:string;nextCheckAt:string};
export type ContextDeferralLedger={version:1;entries:ContextDeferral[]};

/** Exact semantic identities, never instruction snapshots or artifact bodies.
 * Heartbeats and observation timestamps are deliberately excluded: a live
 * unrelated case must not invalidate every forty-second analytical snapshot.
 * Execution control/input digests use the existing canonical small projection. */
export async function readContextCaseBindings(db:DB,goalId:string,ids:string[]):Promise<ContextCaseBinding[]> {
 if(!ids.length)return [];
 return (await db.query(`SELECT c.id case_id,c.task_id,
  encode(sha256(convert_to(jsonb_build_array(c.state,c.reason,c.evidence,c.decision_run_id,c.decision)::text,'UTF8')),'hex') case_contract,
  encode(sha256(convert_to(jsonb_build_array(t.state,t.attempt_count,t.title,t.description,t.criteria,t.stage,t.agent_id,
    t.account_id,t.model,t.allowed_accounts,t.required_memories,t.covers,t.resources,t.recovery,t.max_attempts,
    t.instruction_revision_id,m.version,m.goal_id,m.execution_type,m.execution_present,m.inputs_valid,m.control,m.input_digests)::text,'UTF8')),'hex') task_contract,
  c.source_run_id,t.instruction_revision_id,r.id run_id,
  CASE WHEN r.id IS NOT NULL THEN encode(sha256(convert_to(jsonb_build_array(r.id,r.fence,r.attempt,r.state,r.error_code,
    left(r.summary,1000))::text,'UTF8')),'hex') END run_contract,
  COALESCE((SELECT jsonb_agg(jsonb_build_array(a.id,a.name,a.digest,a.size) ORDER BY a.name,a.id)
    FROM (SELECT id,name,digest,size FROM artifacts WHERE run_id=r.id
      ORDER BY CASE name WHEN 'missing-inputs.json' THEN 0 WHEN 'repair-report.json' THEN 1
        WHEN 'protocol-report.json' THEN 2 WHEN 'verification.json' THEN 3 ELSE 4 END,size,id LIMIT 8)a),'[]'::jsonb) artifact_binding
  FROM supervision_cases c JOIN tasks t ON t.id=c.task_id LEFT JOIN task_execution_metadata m ON m.task_id=t.id
  LEFT JOIN LATERAL(SELECT id,fence,attempt,state,error_code,summary FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
  WHERE c.goal_id=$1 AND c.state='open' AND c.id=ANY($2::uuid[]) ORDER BY c.id`,[goalId,ids])).rows;
}
export function contextCaseBindingsMatch(before:ContextCaseBinding[],after:ContextCaseBinding[]) {
 return sha(before)===sha(after);
}
export function contextCaseSourceFingerprint(entry:any,binding:ContextCaseBinding) {
 return sha([managerFocusSourceFingerprint(entry),entry.source_run_id??null,entry.reason??null,
  entry.base_workspace??null,entry.current_summary??null,entry.evidence,
  binding.instruction_revision_id,binding.case_contract,binding.task_contract,binding.run_id,binding.run_contract,binding.artifact_binding]);
}
/** A malformed, expired or materially changed receipt cannot hide a case.
 * Only known, current receipts affect selection. No accepted-review history is
 * created here, and pending retrieval requests stay owned by their case. */
export function selectContextDeferrals(cases:any[],bindings:ContextCaseBinding[],raw:any,
 projectionVersion:string,now:number) {
 const byId=new Map(bindings.map(binding=>[binding.case_id,binding]));
 const current=new Map(cases.map(entry=>[entry.id,entry]));
 const values=raw?.version===1&&Array.isArray(raw.entries)?raw.entries:[];
 const counts=new Map<string,number>();
 for(const entry of values)if(typeof entry?.caseId==='string')counts.set(entry.caseId,(counts.get(entry.caseId)??0)+1);
 const entries:ContextDeferral[]=values.filter((receipt:any)=>{
  const entry=current.get(receipt?.caseId),binding=byId.get(receipt?.caseId);
  const checked=Date.parse(receipt?.checkedAt),next=Date.parse(receipt?.nextCheckAt);
  return entry&&binding&&counts.get(receipt.caseId)===1&&uuid.test(receipt.caseId)&&uuid.test(receipt.taskId)&&
   receipt.taskId===entry.task_id&&receipt.taskId===binding.task_id&&receipt.code==='CONTEXT_TOO_LARGE'&&
   receipt.projectionVersion===projectionVersion&&digest.test(receipt.sourceHash??'')&&
   digest.test(receipt.sourceFingerprint??'')&&receipt.sourceFingerprint===contextCaseSourceFingerprint(entry,binding)&&
   Number.isSafeInteger(receipt.characters)&&Number.isSafeInteger(receipt.maximum)&&receipt.maximum>0&&receipt.characters>receipt.maximum&&
   Number.isFinite(checked)&&Number.isFinite(next)&&checked<=now&&next-checked===managerContextDeferralMilliseconds;
 });
 const blockedIds=new Set(entries.filter(entry=>Date.parse(entry.nextCheckAt)>now).map(entry=>entry.caseId));
 return {ledger:{version:1 as const,entries},eligible:cases.filter(entry=>!blockedIds.has(entry.id)),
  deferred:entries.filter(entry=>blockedIds.has(entry.caseId)),
  nextCheckAt:entries.filter(entry=>blockedIds.has(entry.caseId)).map(entry=>entry.nextCheckAt).sort()[0]??null};
}
export function addContextDeferral(ledger:ContextDeferralLedger,entry:any,binding:ContextCaseBinding|undefined,
 projectionVersion:string,sourceHash:string,characters:number,maximum:number,now:number):ContextDeferralLedger {
 if(!entry||!binding||entry.id!==binding.case_id||entry.task_id!==binding.task_id||
   !Number.isFinite(now)||!digest.test(sourceHash)||!Number.isSafeInteger(characters)||
   !Number.isSafeInteger(maximum)||maximum<=0||characters<=maximum)throw Error('MANAGER_CONTEXT_DEFERRAL_INVALID');
 const value:ContextDeferral={caseId:entry.id,taskId:entry.task_id,sourceFingerprint:contextCaseSourceFingerprint(entry,binding),
  projectionVersion,code:'CONTEXT_TOO_LARGE',sourceHash,characters,maximum,checkedAt:new Date(now).toISOString(),
  nextCheckAt:new Date(now+managerContextDeferralMilliseconds).toISOString()};
 return {version:1,entries:[...ledger.entries.filter(old=>old.caseId!==value.caseId),value]};
}
export function clearContextDeferral(ledger:ContextDeferralLedger,caseId?:string):ContextDeferralLedger {
 return {version:1,entries:ledger.entries.filter(entry=>entry.caseId!==caseId)};
}
