import {createHash} from 'node:crypto';
import {z} from 'zod';
import type {DB} from './db.js';

export const operatorRecoveryCode='OPERATOR_RECOVERY_REQUESTED';
const hash=z.string().regex(/^[a-f0-9]{64}$/),workspace=z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/);
export const operatorRecoveryRequestSchema=z.object({
 requestId:z.uuid(),runId:z.uuid(),runFence:z.uuid(),expectedAttempt:z.number().int().positive(),
 intent:z.literal('recover_current_attempt'),operatorConfirmed:z.literal(true),reasonCode:z.literal('operator_requested_recovery'),
 evidenceRefs:z.array(z.object({kind:z.enum(['administrative_test_termination','operator_observation']),
  referenceId:z.uuid(),sha256:hash}).strict()).max(8).default([]),
}).strict();
export type OperatorRecoveryRequest=z.infer<typeof operatorRecoveryRequestSchema>;
export type OperatorAuth={channel:'api-token'|'operator-session'};
// JSONB reorders object keys. The persisted receipt and its event must keep the
// same digest after a round trip; arrays retain their semantic order.
const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):
 value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
export const operatorRecoveryDigest=(value:unknown)=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
export const operatorRecoveryNextAction='Reconheça a interrupção administrativa, sem classificá-la como falha espontânea do jogo. '
 +'Examine os resultados e artefatos preservados e a declaração contextual do operador; revise a hipótese e o teste antes de propor nova execução. '
 +'Se o teste já encerrou, não repita a espera pelo mesmo processo. Para nova verificação, defina condição observável de término e prazo explícito; '
 +'qualquer revisão, retomada e aceite continua sujeita ao contrato e às provas normais.';
export const operatorRecoveryIdentitySchema=z.object({version:z.literal(1),runId:z.uuid(),runFence:z.uuid(),workspace,
 checkedAt:z.string().datetime({offset:true}),state:z.literal('running'),runtime:z.literal('harness'),stateDigest:hash,
 process:z.object({unit:z.string(),mainPid:z.number().int().positive(),startTicks:z.string().regex(/^\d+$/),
  activeState:z.literal('active'),subState:z.literal('running'),controlGroup:z.string()}),
 owner:z.object({unit:z.string(),mainPid:z.number().int().positive(),startTicks:z.string().regex(/^\d+$/),
  activeState:z.literal('active'),subState:z.literal('running'),controlGroup:z.string(),candidate:z.string(),
  sourceHashes:z.record(z.string(),hash)}),
});
// Audited exact files of the legacy83ed owner and the admission3 service delta.
// New implementations require review; version/age labels are not compatibility.
export const operatorRecoveryCompatibleSources:Record<string,string[]>={
 'server/run-heartbeat.ts':['793565bafefe91d338da37b222aa3e8de34cd15931e15bacfc862753f9415aef'],
 'server/service.ts':['cfc3453014d454823c78c823da8236e7651807c8f6fdaf921b0fc6a2ad722c44','564d54e4d8505cda3d2c3016d03ff6728733cd2a6ee49d4ad89269f41599ad78'],
 'server/harness-supervision.ts':['53f5d66057046a8dbaf571aed16c6db42ae7b4841b9952f03f6e5e53389441f4'],
 'server/bridge.ts':['aa02c1960f55daace7749d71a2b7d5eff076777427d6fb5ae49a381c7ad54d90'],
 'server/runtime.ts':['bae0b93a8836eba7d66a4b22c360973c2000c35f780b3b87b5db3e1fcf6bbe97'],
 'server/native-supervision.ts':['8fd433538ed690b0e9e5118a695a91738a1b253c1dba98e3d5949cb0ae47b8d4',
  'a2004a734a9457073473e44401a30252d7a1d23922d184496c2016c94f5233f6'],
};
export function checkedOperatorIdentity(raw:unknown,now=Date.now()){
 const proof=operatorRecoveryIdentitySchema.parse(raw),age=now-Date.parse(proof.checkedAt);
 if(age< -5000||age>10000||proof.process.unit!==`agentos-run-${proof.runId}.service`||
  proof.process.controlGroup!==`/system.slice/${proof.process.unit}`||
  proof.owner.unit!==`agentos-native-supervisor-${proof.runId}.service`||
  proof.owner.controlGroup!==`/agentos_native_supervisors.slice/${proof.owner.unit}`||
  !/^\/opt\/agentos\/releases\/managed-[a-f0-9-]{36}$/.test(proof.owner.candidate)||
  Object.keys(proof.owner.sourceHashes).length!==Object.keys(operatorRecoveryCompatibleSources).length||
  Object.entries(operatorRecoveryCompatibleSources).some(([file,hashes])=>!hashes.includes(proof.owner.sourceHashes[file])))
  throw Error('OPERATOR_RECOVERY_IDENTITY_INVALID');
 return proof;
}
/** A checkpoint alone or a runtime/model error code never grants this origin. */
export async function hasOperatorRecoveryIntent(db:DB,run:{id:string;task_id:string;fence:string;checkpoint:any}){
 const receipt=run.checkpoint?.operatorRecoveryStop;
 if(receipt?.request?.runtime==='opencode'){
  const {hasOperatorOpenCodeRecoveryIntent}=await import('./operator-opencode-recovery-contract.js');
  return hasOperatorOpenCodeRecoveryIntent(db,run);
 }
 const request=operatorRecoveryRequestSchema.safeParse(receipt?.request);
 const proof=operatorRecoveryIdentitySchema.safeParse(receipt?.identity);
 if(receipt?.version!==1||!request.success||!proof.success||request.data.runId!==run.id||
  request.data.runFence!==run.fence||receipt.taskId!==run.task_id||proof.data.runId!==run.id||
  proof.data.runFence!==run.fence||receipt.requestDigest!==operatorRecoveryDigest(request.data)||
  receipt.identityDigest!==operatorRecoveryDigest(proof.data)||
  !['api-token','operator-session'].includes(receipt.authChannel)||
  !Number.isFinite(Date.parse(receipt.requestedAt??'')))return false;
 return !!(await db.query(`SELECT 1 FROM events WHERE kind=$1 AND entity_id=$2 AND payload->>'runId'=$3
  AND payload->>'runFence'=$4 AND payload->>'requestId'=$5 AND payload->>'requestDigest'=$6
  AND payload->>'identityDigest'=$7 AND payload->>'authChannel'=$8 AND payload->>'receiptDigest'=$9`,[operatorRecoveryCode,run.task_id,run.id,
   run.fence,request.data.requestId,receipt.requestDigest,receipt.identityDigest,receipt.authChannel,operatorRecoveryDigest(receipt)])).rowCount;
}
