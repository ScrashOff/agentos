import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {pool,tx,lock,event,type DB} from './db.js';
import {config} from './config.js';
import {DomainError} from './contracts.js';
import {bridgeScript} from './bridge-path.js';
import {independentNativeSupervisorSql} from './native-supervision.js';
import {operatorRecoveryRequestSchema,operatorRecoveryCode,operatorRecoveryDigest,checkedOperatorIdentity,
 hasOperatorRecoveryIntent,type OperatorAuth,type OperatorRecoveryRequest} from './operator-recovery-contract.js';
const exec=promisify(execFile);
const controllerCandidate=fileURLToPath(new URL('../',import.meta.url)).replace(/[\\/]$/,'');
const refused=(code:string)=>new DomainError(code,'A tentativa ou sua autoridade mudou; confira o estado atual antes de repetir.',409);
async function observe(db:DB,taskId:string,request:OperatorRecoveryRequest,controller:string){
 return (await db.query(`SELECT r.id,r.fence,r.task_id,r.attempt,t.goal_id,t.instruction_revision_id,
  m.workspace,j.supervision->>'launchToken' launch_token,j.supervision->>'candidate' candidate,
  j.supervision->>'digest' digest,j.supervision->'owner' owner,
  encode(sha256(convert_to(jsonb_build_array(t.description,t.criteria,t.resources,t.allowed_accounts,
   t.required_memories,t.recovery,m.control,m.input_digests)::text,'UTF8')),'hex') contract_digest
  FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
  JOIN task_execution_metadata m ON m.task_id=t.id AND m.goal_id=t.goal_id AND m.version=1 AND m.execution_type='object'
  JOIN opencode_jobs j ON j.run_id=r.id JOIN settings s ON s.id='supervision:'||g.id
  LEFT JOIN settings a ON a.id='autonomy:'||g.project
  WHERE t.id=$1 AND r.id=$2 AND r.fence=$3 AND r.attempt=$4 AND t.attempt_count=r.attempt
   AND r.state='running' AND t.state='running' AND r.runtime='harness' AND m.control->>'kind'='harness'
   AND COALESCE(m.control->>'workforceLane','')<>'muse'
   AND g.state='active' AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
   AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
   AND j.state='running' AND ${independentNativeSupervisorSql()}
   AND NOT EXISTS(SELECT 1 FROM accounts account WHERE account.id=r.account_id AND account.bridge_id LIKE 'agentos:muse%')
   AND NOT EXISTS(SELECT 1 FROM runs other WHERE other.task_id=t.id AND other.id<>r.id
     AND other.state IN('queued','running','cancelling','reconciling'))
   AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text)
   AND NOT EXISTS(SELECT 1 FROM supervision_cases c WHERE c.task_id=t.id
     AND c.state IN('open','decided','delegated','waiting_evidence'))
   AND (SELECT count(*)=4 AND count(DISTINCT value->>'digest')=1 AND bool_and(value->>'digest' ~ '^[a-f0-9]{64}$')
    FROM settings WHERE id=ANY($6::text[]) AND value->>'candidate'=$5)
   AND (SELECT value->>'draining' FROM settings WHERE id='maintenance')='false'`,
  [taskId,request.runId,request.runFence,request.expectedAttempt,controller,
   ['agentos-workers','agentos-backlog','agentos-cycles','agentos-opencode'].map(s=>'worker-readiness:'+s)])).rows[0];
}
export async function inspectOperatorRecoveryIdentity(row:any){
 const {stdout}=await exec('ssh',['-T','-o','BatchMode=yes','-o','ConnectTimeout=8',config.bridgeHost,
  '/opt/deepseek-harness/node/bin/node',bridgeScript('inspect-operator-recovery.mjs'),row.id,row.workspace,row.fence,row.candidate],
  {timeout:20000,maxBuffer:32768,windowsHide:true});
 return JSON.parse(stdout);
}
function response(row:any,duplicate:boolean){const stopped=row.checkpoint.nativeStopReceipt;return {requestId:row.checkpoint.operatorRecoveryStop.request.requestId,
 runId:row.id,state:row.state,duplicate,stopConfirmed:stopped?.runFence===row.fence&&
  stopped?.process?.unit==='agentos-run-'+row.id&&stopped.process.stopped===true&&
  stopped.process.mainPid===0&&stopped.process.populated===false&&
  stopped.supervisor?.mainPid===0&&stopped.supervisor.cgroupEmpty===true&&
  ['inactive','failed'].includes(stopped.supervisor.activeState),
 recoveryPending:!['submitted','cancelled'].includes(row.state),leasesRetained:row.state==='reconciling',
 newAttemptQueued:false,code:operatorRecoveryCode};}
async function previous(db:DB,taskId:string,request:OperatorRecoveryRequest){
 const row=(await db.query('SELECT id,task_id,fence,state,checkpoint FROM runs WHERE id=$1 AND task_id=$2',[request.runId,taskId])).rows[0];
 const receipt=row?.checkpoint?.operatorRecoveryStop;
 if(!receipt)return null;
 if(row.fence!==request.runFence||receipt.requestDigest!==operatorRecoveryDigest(request)||
  !await hasOperatorRecoveryIntent(db,row))throw refused('OPERATOR_RECOVERY_REQUEST_CONFLICT');
 return response(row,true); // Historical receipt only; never a fresh authorization or retarget.
}
/** Explicit authenticated administration, never called by a model or timer. */
export async function requestOperatorRecovery(taskId:string,raw:unknown,auth:OperatorAuth,
 inspect=inspectOperatorRecoveryIdentity,expectedController=controllerCandidate){
 const request=operatorRecoveryRequestSchema.parse(raw);
 if(!['api-token','operator-session'].includes(auth?.channel))throw new DomainError('OPERATOR_REQUIRED','Operador autenticado obrigatório.',403);
 const recorded=await previous(pool,taskId,request);if(recorded)return recorded;
 if(!config.bridgeHost||!config.remoteExecution)throw refused('OPERATOR_RECOVERY_UNSUPPORTED');
 if(!/^\/opt\/agentos\/releases\/managed-[a-f0-9-]{36}$/.test(expectedController))throw refused('OPERATOR_RECOVERY_UNSUPPORTED');
 const observed=await observe(pool,taskId,request,expectedController);
 if(!observed){const replay=await previous(pool,taskId,request);if(replay)return replay;throw refused('OPERATOR_RECOVERY_STATE_CHANGED');}
 const frozen=(await pool.query(`SELECT instruction_snapshot->>'schema' schema,
  instruction_snapshot#>>'{task,id}' task_id,instruction_snapshot#>>'{task,goal_id}' goal_id,
  instruction_snapshot#>>'{task,execution,workspace}' workspace FROM runs WHERE id=$1 AND fence=$2`,[request.runId,request.runFence])).rows[0];
 const proof=checkedOperatorIdentity(await inspect(observed));
 return tx(async db=>{
  await lock(db);
  const replay=await previous(db,taskId,request);if(replay)return replay;
  const current=await observe(db,taskId,request,expectedController);
  if(!current||operatorRecoveryDigest(current)!==operatorRecoveryDigest(observed)||frozen?.schema!=='1'||
   frozen.task_id!==taskId||frozen.goal_id!==current.goal_id||frozen.workspace!==current.workspace||
   proof.runId!==current.id||proof.runFence!==current.fence||proof.workspace!==current.workspace||
   proof.owner.candidate!==current.candidate||proof.owner.mainPid!==Number(current.owner.pid)||
   proof.owner.controlGroup!==current.owner.cgroup)throw refused('OPERATOR_RECOVERY_STATE_CHANGED');
  checkedOperatorIdentity(proof);
  const receipt={version:1,taskId,goalId:current.goal_id,request,requestDigest:operatorRecoveryDigest(request),
   authChannel:auth.channel,requestedAt:new Date().toISOString(),launchToken:current.launch_token,
   candidate:current.candidate,candidateDigest:current.digest,contractDigest:current.contract_digest,
   identity:proof,identityDigest:operatorRecoveryDigest(proof),
   externalEvidence:{operatorSupplied:true,contextOnly:true,verifiedByServer:false},
   administrative:true,gameFailureClaimed:false};
  const changed=await db.query(`UPDATE runs SET state='reconciling',error_code=$3,
    checkpoint=COALESCE(checkpoint,'{}'::jsonb)||jsonb_build_object('operatorRecoveryStop',$4::jsonb)
    WHERE id=$1 AND fence=$2 AND state='running' RETURNING id,task_id,fence,state,checkpoint`,
   [current.id,current.fence,operatorRecoveryCode,JSON.stringify(receipt)]);
  if(!changed.rowCount)throw refused('OPERATOR_RECOVERY_STATE_CHANGED');
  await db.query(`UPDATE tasks SET state='reconciling',reason=$2,updated_at=clock_timestamp() WHERE id=$1`,
   [taskId,'Interrupção administrativa para recuperação solicitada; aguardando prova de encerramento.']);
  await event(db,operatorRecoveryCode,taskId,{runId:current.id,runFence:current.fence,requestId:request.requestId,
   requestDigest:receipt.requestDigest,identityDigest:receipt.identityDigest,receiptDigest:operatorRecoveryDigest(receipt),authChannel:auth.channel,
   reasonCode:request.reasonCode,evidenceRefs:request.evidenceRefs,externalEvidence:receipt.externalEvidence,
   administrative:true,gameFailureClaimed:false,leasesRetained:true,newAttemptQueued:false});
  return response(changed.rows[0],false);
 });
}
