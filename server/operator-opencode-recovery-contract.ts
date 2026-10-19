import {z} from 'zod';
import type {DB} from './db.js';
import {operatorRecoveryRequestSchema,operatorRecoveryDigest,operatorRecoveryCode} from './operator-recovery-contract.js';

/** Separate administrative contract. No runtime widening of Harness authority. */
export const operatorOpenCodeRecoveryRequestSchema=operatorRecoveryRequestSchema.extend({
 runtime:z.literal('opencode'),contextVersion:z.uuid(),planningRequestId:z.uuid().nullable(),
}).strict();
export type OperatorOpenCodeRecoveryRequest=z.infer<typeof operatorOpenCodeRecoveryRequestSchema>;
export const operatorOpenCodeRecoveryNextAction='O executor OpenCode foi encerrado por pedido administrativo. '
 +'Inspecione arquivos e recibos preservados; isso não é aceite nem falha espontânea do jogo. '
 +'Se for planejamento, não remonte os inputs históricos: a retomada aguarda transição canônica, '
 +'sob decisão supervisionada, que vincule request/sourceRevision atuais e novos inputs/helper antes de congelar nova tentativa. '
 +'Para esse planejamento, emita uma única ação revise com diagnóstico e instrução específicos para a sucessora, '
 +'sem sourceTaskId, lane, workKind ou route; após aceite e aplicação canônicos, o runtime cria uma request nova com o catálogo atual. '
 +'O helper atual usa response.selection.nextOffset; read retorna value ou query_child/reference com hash, '
 +'enquanto directory fornece identity. Leia referências necessárias; não repita polling da página zero. '
 +'Preserve request, contexto e tentativa antigos como auditoria; não os reescreva.';
const digest=z.string().regex(/^[a-f0-9]{64}$/);
const processIdentity=z.object({unit:z.string(),mainPid:z.number().int().positive(),startTicks:z.string().regex(/^\d+$/),
 activeState:z.literal('active'),subState:z.literal('running'),controlGroup:z.string()});
export const operatorOpenCodeRecoveryIdentitySchema=z.object({version:z.literal(1),runtime:z.literal('opencode'),
 runId:z.uuid(),runFence:z.uuid(),contextVersion:z.uuid(),workspace:z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/),
 state:z.literal('running'),stateDigest:digest,checkedAt:z.string().datetime({offset:true}),
 stateIdentityFields:z.object({runFence:z.boolean(),contextVersion:z.boolean()}),
 process:processIdentity,owner:processIdentity.extend({candidate:z.string(),sourceHashes:z.record(z.string(),digest)}),
});
// Exact audited owner versions only; filled from reviewed immutable owner bytes.
export const operatorOpenCodeRecoveryCompatibleSources:Record<string,string[]>={
 'scripts/opencode-batch.ts':['aeee63e352e965495642ae0bcc3454085ddd838ba3ad4363af2186ce9cba938e'],
 'server/run-heartbeat.ts':['793565bafefe91d338da37b222aa3e8de34cd15931e15bacfc862753f9415aef'],
 'server/service.ts':['b26c51733619ff71f9c17a55e8d05ae1a9274f2c1d587b1c10940a109f2dd946'],
 'server/native-supervision.ts':['a2004a734a9457073473e44401a30252d7a1d23922d184496c2016c94f5233f6'],
 'server/bridge-path.ts':['6c2823882895b5105290e776429c11c36d8c2b5b6906b30dd8ade91f714dbfb4'],
 'bridge/opencode-executor.mjs':['a087ec0f4b17922d921a0725a47f568bf63a2eb012b93f985f971a6873a7f83e'],
 'bridge/process-stop.mjs':['7216267adc64f10743d49d82c158658f8d4e5ebc3015c7378e4713a584724844'],
};
export function checkedOperatorOpenCodeIdentity(raw:unknown,now=Date.now()){
 const proof=operatorOpenCodeRecoveryIdentitySchema.parse(raw),age=now-Date.parse(proof.checkedAt);
 if(age< -5000||age>10000||proof.process.unit!==`agentos-run-${proof.runId}.service`||
  proof.process.controlGroup!==`/system.slice/${proof.process.unit}`||
  proof.owner.unit!==`agentos-native-supervisor-${proof.runId}.service`||
  proof.owner.controlGroup!==`/agentos_native_supervisors.slice/${proof.owner.unit}`||
  !/^\/opt\/agentos\/releases\/managed-[a-f0-9-]{36}$/.test(proof.owner.candidate)||
  !Object.keys(operatorOpenCodeRecoveryCompatibleSources).length||
  Object.keys(proof.owner.sourceHashes).length!==Object.keys(operatorOpenCodeRecoveryCompatibleSources).length||
  Object.entries(operatorOpenCodeRecoveryCompatibleSources).some(([file,hashes])=>!hashes.includes(proof.owner.sourceHashes[file])))
  throw Error('OPENCODE_RECOVERY_IDENTITY_INVALID');
 return proof;
}
/** Immutable intent + matching canonical event, never a model/runtime claim. */
export async function hasOperatorOpenCodeRecoveryIntent(db:DB,run:{id:string;task_id:string;fence:string;checkpoint:any}){
 const receipt=run.checkpoint?.operatorRecoveryStop;
 const request=operatorOpenCodeRecoveryRequestSchema.safeParse(receipt?.request);
 const proof=operatorOpenCodeRecoveryIdentitySchema.safeParse(receipt?.identity);
 if(receipt?.version!==1||!request.success||!proof.success||request.data.runId!==run.id||request.data.runFence!==run.fence||
  receipt.taskId!==run.task_id||proof.data.runId!==run.id||proof.data.runFence!==run.fence||
  proof.data.contextVersion!==request.data.contextVersion||receipt.requestDigest!==operatorRecoveryDigest(request.data)||
  receipt.identityDigest!==operatorRecoveryDigest(proof.data)||!['api-token','operator-session'].includes(receipt.authChannel)||
  !Number.isFinite(Date.parse(receipt.requestedAt??'')))return false;
 return !!(await db.query(`SELECT 1 FROM events WHERE kind=$1 AND entity_id=$2 AND payload->>'runId'=$3
  AND payload->>'runFence'=$4 AND payload->>'requestId'=$5 AND payload->>'requestDigest'=$6
  AND payload->>'identityDigest'=$7 AND payload->>'authChannel'=$8 AND payload->>'receiptDigest'=$9`,[
   operatorRecoveryCode,run.task_id,run.id,run.fence,request.data.requestId,receipt.requestDigest,
   receipt.identityDigest,receipt.authChannel,operatorRecoveryDigest(receipt)])).rowCount;
}
/** A new context UUID alone does not refresh embedded planner inputs. Until a
 * canonical request/context handoff exists, keep this exact planner stopped. */
export async function assertOperatorOpenCodePlannerContext(db:DB,task:any,run:any){
 if(task.execution?.kind==='opencode'&&task.execution.workKind==='planning'&&
  await hasOperatorOpenCodeRecoveryIntent(db,run))
  throw Object.assign(Error('OPENCODE_RECOVERY_PLANNER_CONTEXT_REQUIRED'),{code:'OPENCODE_RECOVERY_PLANNER_CONTEXT_REQUIRED'});
}
