import { randomUUID,createHash } from 'node:crypto';
import { pool,tx,lock,event,type DB } from './db.js';
import { assertStopped,inspectStopped } from './continuation.js';
import type { BacklogHandoff } from './backlog-handoffs.js';
import {readArtifact} from './service.js';
import {runtimeCandidateDigest} from './operation-progress.js';
import {accountBridge} from './bridge.js';
import {installDeliveryConsumptions,registerDeliveryConsumption} from './delivery-consumption.js';

const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):value && typeof value==='object'
  ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const digest=(value:any)=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
/** Runtime/account rotation and accumulated handoffs are not the task contract.
 * Instructions, permissions, candidate location and acceptance remain binding. */
export function handoffConsumerContract(task:any){
  const execution=task.execution??{};
  return digest({taskId:task.id,goalId:task.goal_id,description:task.description,criteria:task.criteria,
    covers:task.covers,stage:task.stage,agentId:task.agent_id,resources:task.resources,
    requiredMemories:task.required_memories,allowedAccounts:task.allowed_accounts,
    execution:Object.fromEntries(['kind','baseWorkspace','workspace','sourceWorkspace','readOnlySource','writePaths','profile']
      .map(key=>[key,execution[key]??null]))});
}

function inspectedCandidate(proof:any){
  const candidate=proof?.candidate;
  if(!candidate)return null;
  const bytes=Buffer.from(JSON.stringify(candidate));
  return runtimeCandidateDigest(bytes,createHash('sha256').update(bytes).digest('hex'));
}
const pathAllowed=(name:string)=>/^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*$/.test(name) &&
  !name.split('/').some(part=>['.','..','.git','assets','node_modules','evidence','data','rules','.agentos-snapshot.json',
    '.ssh','.aws','.codex','.claude'].includes(part) || part.startsWith('.env'));

/** Only an actual accepted patch and its matching versioned source hashes can
 * follow a stopped consumer into a newer attempt. This never applies a patch or
 * accepts the consumer; composition and independent QA still own those steps. */
async function repairCompatibility(db:DB,producer:any,proof:any){
  const candidateDigest=inspectedCandidate(proof);
  if(!candidateDigest)throw Error('HANDOFF_CANDIDATE_PROOF_REQUIRED');
  const refs=(await db.query(`SELECT a.id,a.name,a.digest,a.size FROM artifacts a JOIN runs r ON r.id=a.run_id
    WHERE r.task_id=$1 AND r.attempt=$2 AND r.state='submitted' AND a.name IN('implementation.patch','repair-report.json')`,
    [producer.id,producer.attempt_count])).rows;
  const patchRef=refs.find(r=>r.name==='implementation.patch'),reportRef=refs.find(r=>r.name==='repair-report.json');
  if(!patchRef || !reportRef)throw Error('HANDOFF_REPAIR_PROOF_REQUIRED');
  const [patch,report]=await Promise.all([readArtifact(patchRef.id),readArtifact(reportRef.id)]);
  if(patch.digest!==patchRef.digest || report.digest!==reportRef.digest || patch.bytes.length!==patchRef.size ||
    report.bytes.length!==reportRef.size)throw Error('HANDOFF_REPAIR_PROOF_CHANGED');
  let value:any;try{value=JSON.parse(report.bytes.toString());}catch{throw Error('HANDOFF_REPAIR_PROOF_INVALID');}
  const text=patch.bytes.toString('utf8'),names=new Set<string>();
  if(!Buffer.from(text).equals(patch.bytes) || /^(GIT binary patch|new file mode 120000|old mode|new mode|rename from |rename to |copy from |copy to )/m.test(text))
    throw Error('HANDOFF_REPAIR_PROOF_INVALID');
  for(const line of text.split('\n'))if(line.startsWith('--- ') || line.startsWith('+++ ')){
    const name=line.slice(4).split('\t')[0];if(name==='/dev/null')continue;
    if(!/^[ab]\//.test(name) || !pathAllowed(name.slice(2)))throw Error('HANDOFF_REPAIR_PROOF_INVALID');
    names.add(name.slice(2));
  }
  if(!names.size || names.size>128 || value.fixVerified!==true || !Array.isArray(value.sourceHashes) ||
    value.sourceHashes.length!==names.size)throw Error('HANDOFF_REPAIR_PROOF_INVALID');
  const hashes=new Map<string,{before:string|null;after:string|null}>();
  for(const entry of value.sourceHashes){
    if(!entry || !names.has(entry.path) || hashes.has(entry.path) || entry.before===entry.after ||
      [entry.before,entry.after].some(v=>v!==null && (typeof v!=='string' || !/^[a-f0-9]{64}$/.test(v))))
      throw Error('HANDOFF_REPAIR_PROOF_INVALID');
    hashes.set(entry.path,entry);
  }
  const files=new Map<string,string>(proof.candidate.files.map((file:any)=>[file.path,file.sha256]));
  const before=[...hashes].every(([name,h])=>(files.get(name)??null)===h.before);
  const after=[...hashes].every(([name,h])=>(files.get(name)??null)===h.after);
  if(!before && !after)throw Error('HANDOFF_PATCH_CONFLICT');
  const checked=await accountBridge.request('worker.checkComposition',{workspace:proof.workspace,composition:{version:1,patches:[{
    producerTaskId:producer.id,patch:{...patchRef,base64:patch.bytes.toString('base64')},
    report:{...reportRef,base64:report.bytes.toString('base64')},
  }]}},{timeout:45000});
  if(checked?.dryRun!==true || checked.workspace!==proof.workspace || checked.beforeDigest!==candidateDigest ||
    checked.afterDigest!==candidateDigest || checked.complete!==true || checked.receipts?.length!==1 ||
    !['compatible','already_applied'].includes(checked.receipts[0].status) ||
    checked.receipts[0].producerTaskId!==producer.id || checked.receipts[0].patchDigest!==patchRef.digest ||
    checked.receipts[0].reportDigest!==reportRef.digest)throw Error('HANDOFF_PATCH_CONFLICT');
  return {candidateDigest,producerAttempt:producer.attempt_count,patchArtifactId:patchRef.id,patchDigest:patchRef.digest,
    reportArtifactId:reportRef.id,reportDigest:reportRef.digest,status:after?'already_present':'preimages_match'};
}

export async function installDeferredHandoffs(db:DB=pool){
  await db.query(`CREATE TABLE IF NOT EXISTS deferred_handoffs(
    id uuid PRIMARY KEY,goal_id uuid NOT NULL REFERENCES goals(id),producer_task_id uuid NOT NULL UNIQUE REFERENCES tasks(id),
    consumer_task_id uuid NOT NULL REFERENCES tasks(id),source_task_id uuid NOT NULL REFERENCES tasks(id),
    source_proof jsonb NOT NULL,consumer_proof jsonb NOT NULL,handoff jsonb NOT NULL,state text NOT NULL DEFAULT 'waiting_result',
    receipt jsonb NOT NULL DEFAULT '{}',created_at timestamptz NOT NULL DEFAULT now(),applied_at timestamptz)`);
  await installDeliveryConsumptions(db);
}
export async function acceptedSnapshotProof(db:DB,task:any){
  const run=(await db.query(`SELECT r.id,r.attempt,r.fence FROM runs r WHERE r.task_id=$1 AND r.attempt=$2 AND r.state='submitted'
    AND (SELECT passed FROM acceptances a WHERE a.run_id=r.id ORDER BY created_at DESC LIMIT 1)=true
    AND NOT EXISTS(SELECT 1 FROM runs x WHERE x.task_id=r.task_id AND x.state IN('queued','running','cancelling','reconciling'))`,[task.id,task.attempt_count])).rows[0];
  if(!run || task.state!=='accepted')throw Error('SOURCE_NOT_CURRENT_ACCEPTED');
  const artifacts=(await db.query('SELECT id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[run.id])).rows;
  return {taskId:task.id,runId:run.id,fence:run.fence,attempt:run.attempt,workspace:task.execution.workspace,
    manifestDigest:createHash('sha256').update(JSON.stringify(artifacts)).digest('hex')};
}
export function sameSnapshotProof(a:any,b:any){
  return !!a && !!b && ['taskId','runId','fence','attempt','workspace','manifestDigest'].every(key=>a[key]===b[key]);
}
export async function stageDeferredHandoff(db:DB,goalId:string,producerId:string,source:any,consumer:any,handoff:BacklogHandoff){
  const sourceProof=await acceptedSnapshotProof(db,source);
  const run=(await db.query('SELECT id,fence,attempt FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[consumer.id])).rows[0];
  const dependencies=(await db.query('SELECT depends_on FROM dependencies WHERE task_id=$1 ORDER BY depends_on',[consumer.id])).rows.map(r=>r.depends_on);
  await db.query(`INSERT INTO deferred_handoffs(id,goal_id,producer_task_id,consumer_task_id,source_task_id,source_proof,consumer_proof,handoff)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,[randomUUID(),goalId,producerId,consumer.id,source.id,JSON.stringify(sourceProof),
      JSON.stringify({version:2,attempt:consumer.attempt_count,workspace:consumer.execution.workspace,runId:run?.id??null,fence:run?.fence??null,
        contractDigest:handoffConsumerContract(consumer),dependencies}),JSON.stringify(handoff)]);
  await event(db,'HANDOFF_STAGED',goalId,{producerTaskId:producerId,consumerTaskId:consumer.id,sourceProof});
}

/** Apply only after both actors really stopped. Callers inspect outside the
 * transaction, then recheck attempts, manifests and fences under admission lock. */
export async function applyDeferredHandoff(id:string,consumerStop:any,producerStop:any){
  // Filesystem and remote compatibility checks precede admission locking.
  // Every identity/artifact and fresh stop proof is checked again below.
  let preparedCompatibility:any,compatibilityError:string|undefined;
  const observed=(await pool.query(`SELECT h.consumer_proof,h.handoff,p.id,p.attempt_count,c.attempt_count consumer_attempt
    FROM deferred_handoffs h JOIN tasks p ON p.id=h.producer_task_id JOIN tasks c ON c.id=h.consumer_task_id
    JOIN goals g ON g.id=h.goal_id
    WHERE h.id=$1 AND h.state IN('waiting_result','staged','replanning_required') AND g.state='active' AND p.state='accepted'
      AND NOT EXISTS(SELECT 1 FROM settings a WHERE a.id='autonomy:'||g.project AND a.value->>'enabled'='false')
      AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.task_id=ANY(ARRAY[c.id,p.id]) AND r.state IN('queued','running','cancelling','reconciling'))`,[id])).rows[0];
  if(observed?.handoff.workKind==='repair' && observed.consumer_attempt!==observed.consumer_proof.attempt){
    try{preparedCompatibility=await repairCompatibility(pool,observed,consumerStop);}
    catch(e:any){compatibilityError=/^HANDOFF_[A-Z_]+$/.test(e.message??'')?e.message:'HANDOFF_REPAIR_PROOF_INVALID';}
  }
  return tx(async db=>{
    await lock(db);
    const handoff=(await db.query('SELECT * FROM deferred_handoffs WHERE id=$1 FOR UPDATE',[id])).rows[0];
    if(!handoff || (!['waiting_result','staged'].includes(handoff.state) &&
      !(handoff.state==='replanning_required' && handoff.receipt?.code==='CONSUMER_REVISION_CHANGED')))return false;
    const tasks=(await db.query('SELECT * FROM tasks WHERE id=ANY($1::uuid[]) FOR UPDATE',[[handoff.producer_task_id,handoff.consumer_task_id,handoff.source_task_id]])).rows;
    const consumer=tasks.find(t=>t.id===handoff.consumer_task_id),producer=tasks.find(t=>t.id===handoff.producer_task_id),source=tasks.find(t=>t.id===handoff.source_task_id);
    if(!consumer || !producer || !source || tasks.some(t=>t.goal_id!==handoff.goal_id))return false;
    if(!(await db.query(`SELECT 1 FROM goals g LEFT JOIN settings a ON a.id='autonomy:'||g.project
      WHERE g.id=$1 AND g.state='active' AND COALESCE(a.value->>'enabled','true')='true'`,[handoff.goal_id])).rowCount)return false;
    if((await db.query("SELECT 1 FROM runs WHERE task_id=ANY($1::uuid[]) AND state IN('queued','running','cancelling','reconciling')",[[consumer.id,producer.id]])).rowCount)return false;
    if(producer.state!=='accepted')return false;
    const old=handoff.consumer_proof;
    const latest=(await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[consumer.id])).rows[0];
    const reject=async(code:string)=>{
      const changed=await db.query(`UPDATE deferred_handoffs SET state='replanning_required',receipt=receipt||$2::jsonb
        WHERE id=$1 AND (state IS DISTINCT FROM 'replanning_required' OR receipt->>'code' IS DISTINCT FROM $3) RETURNING id`,[id,JSON.stringify({code}),code]);
      if(changed.rowCount)await event(db,'HANDOFF_REPLAN_REQUIRED',handoff.goal_id,{handoffId:id,consumerTaskId:consumer.id,code});return false;
    };
    if(['accepted','cancelled','superseded'].includes(consumer.state))return reject('CONSUMER_CLOSED');
    const revisionChanged=consumer.attempt_count!==old.attempt || consumer.execution.workspace!==old.workspace ||
      (latest?.id??null)!==old.runId || (latest?.fence??null)!==old.fence;
    if(consumer.execution.workspace!==old.workspace)return reject('CONSUMER_WORKSPACE_CHANGED');
    if(latest)assertStopped(consumerStop,latest,consumer);
    const producerRun=(await db.query('SELECT * FROM runs WHERE task_id=$1 AND attempt=$2',[producer.id,producer.attempt_count])).rows[0];
    assertStopped(producerStop,producerRun,producer);
    try{await acceptedSnapshotProof(db,producer);}catch{return reject('PRODUCER_ACCEPTANCE_CHANGED');}
    let proof;
    try{proof=await acceptedSnapshotProof(db,source);}catch{return reject('SOURCE_ACCEPTANCE_CHANGED');}
    if(!sameSnapshotProof(proof,handoff.source_proof))return reject('SOURCE_MANIFEST_CHANGED');
    let revalidation:any;
    if(revisionChanged){
      // Legacy rows can prove their old contract only through the immutable
      // original run snapshot, never by copying today's task into an old proof.
      const previous=old.runId?(await db.query('SELECT task_id,attempt,fence,instruction_snapshot FROM runs WHERE id=$1',[old.runId])).rows[0]:null;
      const frozen=previous?.task_id===consumer.id && previous.attempt===old.attempt && previous.fence===old.fence
        ?previous.instruction_snapshot?.task:null;
      const contract=old.contractDigest??(frozen?handoffConsumerContract(frozen):null);
      if(!contract)return reject('HANDOFF_CONTRACT_PROOF_REQUIRED');
      if(contract!==handoffConsumerContract(consumer))return reject('HANDOFF_CONTRACT_CHANGED');
      if(!latest || consumerStop?.workspace!==consumer.execution.workspace || !inspectedCandidate(consumerStop))
        return reject('HANDOFF_CANDIDATE_PROOF_REQUIRED');
      if(latest.state==='cancelled' || (await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",[latest.id])).rowCount)
        return reject('CONSUMER_CANCELLED');
      const dependencies=(await db.query('SELECT depends_on FROM dependencies WHERE task_id=$1',[consumer.id])).rows.map(r=>r.depends_on);
      if(Array.isArray(old.dependencies) && old.dependencies.some((dependency:string)=>!dependencies.includes(dependency)))
        return reject('HANDOFF_DEPENDENCIES_CHANGED');
      let compatibility:any={candidateDigest:inspectedCandidate(consumerStop),status:'diagnostic_evidence_only'};
      if(handoff.handoff.workKind==='repair'){
        if(compatibilityError)return reject(compatibilityError);
        if(!preparedCompatibility || preparedCompatibility.producerAttempt!==producer.attempt_count ||
          preparedCompatibility.candidateDigest!==inspectedCandidate(consumerStop))return reject('HANDOFF_REPAIR_PROOF_CHANGED');
        const refs=(await db.query('SELECT id,digest FROM artifacts WHERE run_id=$1 AND id=ANY($2::uuid[])',[producerRun.id,
          [preparedCompatibility.patchArtifactId,preparedCompatibility.reportArtifactId]])).rows;
        if(refs.length!==2 || !refs.some(r=>r.id===preparedCompatibility.patchArtifactId && r.digest===preparedCompatibility.patchDigest) ||
          !refs.some(r=>r.id===preparedCompatibility.reportArtifactId && r.digest===preparedCompatibility.reportDigest))return reject('HANDOFF_REPAIR_PROOF_CHANGED');
        compatibility=preparedCompatibility;
      }
      revalidation={version:1,previousConsumer:old,currentConsumer:{attempt:consumer.attempt_count,runId:latest.id,fence:latest.fence,
        workspace:consumer.execution.workspace,contractDigest:contract},compatibility,checkedAt:consumerStop.checkedAt};
    }
    const cycle=(await db.query(`WITH RECURSIVE parents(id) AS(SELECT depends_on FROM dependencies WHERE task_id=$1
      UNION SELECT d.depends_on FROM dependencies d JOIN parents p ON d.task_id=p.id) SELECT 1 FROM parents WHERE id=$2`,[producer.id,consumer.id])).rowCount;
    if(cycle)return reject('DEPENDENCY_CYCLE');
    const refs=consumer.execution.backlogHandoffs??[];
    if(!refs.some((r:any)=>r.taskId===producer.id))refs.push(handoff.handoff);
    await db.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2) ON CONFLICT DO NOTHING',[consumer.id,producer.id]);
    // Do not resume a functional failure implicitly. Its supervisor retains the
    // responsibility to revise/retry it; it now has the accepted delivery.
    await db.query("UPDATE tasks SET execution=jsonb_set(execution,'{backlogHandoffs}',$2::jsonb),updated_at=now() WHERE id=$1",[consumer.id,JSON.stringify(refs)]);
    await db.query("UPDATE deferred_handoffs SET state='applied',applied_at=now(),receipt=$2 WHERE id=$1",[id,JSON.stringify({sourceProof:proof,consumerRunId:latest?.id,producerRunId:producerRun.id,...(revalidation?{revalidation}:{})})]);
    await registerDeliveryConsumption(db,{...handoff,receipt:{consumerRunId:latest?.id??null}},consumer,producerRun);
    if(revalidation)await event(db,'HANDOFF_REVALIDATED',handoff.goal_id,{handoffId:id,producerTaskId:producer.id,consumerTaskId:consumer.id,
      previousRunId:old.runId,currentRunId:latest.id,candidateDigest:revalidation.compatibility.candidateDigest,status:revalidation.compatibility.status});
    await event(db,'HANDOFF_APPLIED',handoff.goal_id,{handoffId:id,producerTaskId:producer.id,consumerTaskId:consumer.id});return true;
  });
}
export async function reconcileDeferredHandoffs(inspect=inspectStopped){
  await pool.query(`UPDATE deferred_handoffs h SET state='staged' FROM tasks t
    WHERE h.producer_task_id=t.id AND t.state='accepted' AND h.state='waiting_result'`);
  const rows=(await pool.query(`SELECT h.*,c.execution consumer_execution,p.execution producer_execution,c.state consumer_state,
    p.state producer_state,cr.id consumer_run_id,pr.id producer_run_id FROM deferred_handoffs h
    JOIN tasks c ON c.id=h.consumer_task_id JOIN tasks p ON p.id=h.producer_task_id JOIN goals g ON g.id=h.goal_id
    LEFT JOIN runs cr ON cr.task_id=c.id AND cr.attempt=c.attempt_count
    LEFT JOIN runs pr ON pr.task_id=p.id AND pr.attempt=p.attempt_count
    WHERE (h.state IN('waiting_result','staged') OR (h.state='replanning_required' AND h.receipt->>'code'='CONSUMER_REVISION_CHANGED'))
    AND g.state='active' AND p.state='accepted'
    AND NOT EXISTS(SELECT 1 FROM settings a WHERE a.id='autonomy:'||g.project AND a.value->>'enabled'='false')
    AND (h.receipt->'inspection'->>'nextAt' IS NULL OR (h.receipt->'inspection'->>'nextAt')::timestamptz<=now())
    AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.task_id=ANY(ARRAY[c.id,p.id]) AND r.state IN('queued','running','cancelling','reconciling'))
    ORDER BY h.receipt->'inspection'->>'checkedAt' NULLS FIRST,h.created_at,h.id LIMIT 3`)).rows;
  // Remote stop/candidate checks are independent. Bound the wave to three;
  // admission remains serialized and each item owns a separate session lock.
  const results=await Promise.allSettled(rows.map(async row=>{
    const owner=await pool.connect();let owned=false;
    try{
      owned=(await owner.query('SELECT pg_try_advisory_lock(hashtextextended($1,4280093)) owned',[row.id])).rows[0].owned;
      if(!owned)return;
      // Record a fair retry window before remote I/O. Even an unconfirmable
      // stopped process cannot occupy the first positions forever.
      const claimed=await owner.query(`UPDATE deferred_handoffs SET receipt=jsonb_set(receipt,'{inspection}',
        jsonb_build_object('checkedAt',now(),'nextAt',now()+interval '60 seconds','code','CHECKING'))
        WHERE id=$1 AND (state IN('waiting_result','staged') OR (state='replanning_required' AND receipt->>'code'='CONSUMER_REVISION_CHANGED'))
        AND (receipt->'inspection'->>'nextAt' IS NULL OR (receipt->'inspection'->>'nextAt')::timestamptz<=now()) RETURNING id`,[row.id]);
      if(!claimed.rowCount)return;
      const proofs=await Promise.allSettled([
        row.consumer_run_id?inspect(row.consumer_run_id,row.consumer_execution.kind==='opencode'):Promise.resolve(null),
        inspect(row.producer_run_id,row.producer_execution.kind==='opencode')]);
      const failed=proofs.find((proof):proof is PromiseRejectedResult=>proof.status==='rejected');
      if(failed)throw failed.reason;
      const [consumerProof,producerProof]=proofs.map(proof=>(proof as PromiseFulfilledResult<any>).value);
      const applied=await applyDeferredHandoff(row.id,consumerProof,producerProof);
      if(!applied)await owner.query(`UPDATE deferred_handoffs SET receipt=jsonb_set(receipt,'{inspection,code}','"WAITING_STATE"')
        WHERE id=$1 AND receipt->'inspection'->>'code'='CHECKING'`,[row.id]);
    }catch(e:any){
      const value=e.code??e.message,code=/^[A-Z][A-Z0-9_]{2,80}$/.test(value??'')?value:'HANDOFF_INSPECTION_FAILED';
      // Keep retry metadata per handoff. A failed inspection cannot roll back
      // successful independent deliveries or disappear behind a healthy timer.
      await tx(async db=>{
        const changed=await db.query(`UPDATE deferred_handoffs SET receipt=jsonb_set(receipt,'{inspection,code}',to_jsonb($2::text))
          WHERE id=$1 AND state<>'applied' RETURNING goal_id,receipt->>'lastInspectionError' previous`,[row.id,code]);
        if(changed.rowCount && changed.rows[0].previous!==code){
          await db.query("UPDATE deferred_handoffs SET receipt=receipt||jsonb_build_object('lastInspectionError',$2::text) WHERE id=$1",[row.id,code]);
          await event(db,'HANDOFF_INSPECTION_FAILED',changed.rows[0].goal_id,{handoffId:row.id,code});
        }
      });
    }finally{
      if(owned)await owner.query('SELECT pg_advisory_unlock(hashtextextended($1,4280093))',[row.id]).catch(()=>{});
      owner.release();
    }
  }));
  // Connection/storage failures remain visible as controller failures, after
  // independent items have had their turn. Per-item inspection errors above
  // already have durable retry receipts and do not fail the whole wave.
  const failure=results.find((result):result is PromiseRejectedResult=>result.status==='rejected');
  if(failure)throw failure.reason;
}
