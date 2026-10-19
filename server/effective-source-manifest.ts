import type {DB} from './db.js';
import {DomainError} from './contracts.js';
import {deliveryDigest} from './delivery-supervision.js';
import {compositionTargetAuthority} from './composition-target-authority.js';
import {readDeliverySourceResolution,effectiveDeliverySource,readDeliverySourceIdentity,
  deliverySourceConflict,sourceIdentityAccepted,sourceIdentityFrozen} from './delivery-source-resolution.js';

const fail=(reason:string):never=>{throw Object.assign(new DomainError('INPUT_MANIFEST_CHANGED',
  'Effective repair sources could not be frozen: '+reason),{inputFailure:{phase:'effective_source_manifest',subcode:reason}});};
const names=new Set(['implementation.patch','repair-report.json']);
const pin=(a:any)=>({id:a.id,name:a.name,digest:a.digest,task_id:a.task_id,run_id:a.run_id});
const same=(a:any,b:any)=>deliveryDigest(a)===deliveryDigest(b);
const source=(taskId:string,runId:string,manifestDigest:string)=>({taskId,runId,manifestDigest});
const sourceKey=(taskId:string,runId:string)=>taskId+':'+runId;
const pair=(refs:any[],taskId:string,runId:string)=>{
  if(!Array.isArray(refs))return fail('REPAIR_REFS_MISSING');
  const selected=refs.filter(a=>names.has(a.name));
  if(selected.length!==2||new Set(selected.map(a=>a.name)).size!==2||
    selected.some(a=>a.run_id!==runId||typeof a.digest!=='string'||typeof a.id!=='string'))
    return fail('REPAIR_REFS_AMBIGUOUS');
  return selected.map(a=>({...a,task_id:taskId}));
};

/** Called under assembleContext's existing transaction/admission lock, after
 * checking the immutable run snapshot and fence. No artifact I/O, DDL, DAG or
 * acceptance mutation. Bound: 256 obligations, 1024 resolution events; sources
 * shared by obligations are validated once. A cap is refusal, never truncation.
 * Legacy contexts are not rewritten: only this next run's context is produced. */
export async function effectiveSourceManifest(db:DB,run:any,task:any,dependencies:any[]){
  if(task.stage!=='integrate'||task.execution?.readOnlySource||!task.execution?.sourceWorkspace)
    return {dependencyArtifacts:dependencies};
  if(!(await db.query("SELECT to_regclass('delivery_consumptions') present")).rows[0].present)
    return {dependencyArtifacts:dependencies};
  const rows=(await db.query(`SELECT d.* FROM delivery_consumptions d WHERE d.consumer_task_id=$1
    AND EXISTS(SELECT 1 FROM events e WHERE e.kind='DELIVERY_SOURCE_RESOLVED' AND e.entity_id=d.id::text)
    ORDER BY d.id LIMIT 257`,[task.id])).rows;
  if(rows.length>256)fail('OBLIGATION_BOUND');
  if(!rows.length)return {dependencyArtifacts:dependencies};
  const events=(await db.query(`SELECT e.id::text event_id,e.entity_id,e.payload,
    (history.origin_id=admission.origin_id AND history.number<=admission.number) before_admission,
    c.task_id case_task,c.goal_id case_goal,
    history.provenance decision_provenance,history.id revision_id,history.origin_id revision_origin_id,history.number revision_number,
    EXISTS(SELECT 1 FROM events a WHERE a.kind='SUPERVISION_APPLIED' AND a.entity_id=e.payload->>'consumerTaskId'
      AND a.payload->>'caseId'=e.payload->>'caseId' AND a.payload->>'decisionRunId'=e.payload->>'decisionRunId'
      AND a.payload->>'sourceRunId'=e.payload->>'consumerRunId' AND a.payload->'stopConfirmed'='true'::jsonb) applied_receipt,
    old.task_id prior_task,old.attempt prior_attempt,
    review.task_id review_task,review.state review_state,reviewer.goal_id review_goal,reviewer.stage review_stage,
    (SELECT passed FROM acceptances WHERE run_id=review.id ORDER BY created_at DESC,id DESC LIMIT 1) review_passed
    FROM events e JOIN runs r ON r.id=$2
    LEFT JOIN task_instruction_revisions admission ON admission.id::text=r.instruction_snapshot->>'revisionId'
      AND admission.task_id=r.task_id
    LEFT JOIN supervision_cases c ON c.id::text=e.payload->>'caseId'
    LEFT JOIN runs old ON old.id::text=e.payload->>'consumerRunId'
    LEFT JOIN runs review ON review.id::text=e.payload->>'decisionRunId'
    LEFT JOIN tasks reviewer ON reviewer.id=review.task_id
    LEFT JOIN LATERAL(SELECT id,origin_id,number,provenance FROM task_instruction_revisions v
      WHERE v.task_id=r.task_id AND v.goal_id=c.goal_id
       AND v.provenance->>'caseId'=e.payload->>'caseId'
       AND v.provenance->>'decisionRunId'=e.payload->>'decisionRunId'
       AND v.provenance->>'sourceRunId'=e.payload->>'consumerRunId'
       AND v.provenance->>'kind'='revise' ORDER BY v.number LIMIT 2)history ON true
    WHERE e.kind='DELIVERY_SOURCE_RESOLVED' AND e.entity_id=ANY($1::text[])
    ORDER BY e.id LIMIT 1025`,[rows.map(d=>d.id),run.id])).rows;
  if(events.length>1024)fail('RESOLUTION_BOUND');
  if(!events.length)return {dependencyArtifacts:dependencies};
  const dependencyRefs=(await db.query(`SELECT a.id,a.run_id,a.name,a.digest,a.size,t.id task_id
    FROM artifacts a JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id
    WHERE a.id=ANY($1::uuid[])`,[dependencies.map((a:any)=>a.id)])).rows;
  const dependencyById=new Map(dependencyRefs.map((a:any)=>[a.id,a]));
  dependencies=dependencies.map((ref:any)=>{
    const actual=dependencyById.get(ref.id);
    if(!actual||actual.name!==ref.name||actual.digest!==ref.digest||actual.task_id!==ref.task_id)
      fail('DEPENDENCY_ARTIFACT_CHANGED');
    return {...ref,run_id:actual.run_id,size:actual.size};
  });
  if(run.instruction_snapshot?.schema!==1||run.instruction_snapshot.task?.id!==task.id||
    run.instruction_snapshot.task?.goal_id!==task.goal_id)fail('SNAPSHOT_REQUIRED');
  const authority=(await db.query(`SELECT r.fence,r.attempt,r.state,r.instruction_snapshot,t.attempt_count,
    (SELECT id FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1) latest_id
    FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1 AND r.task_id=$2`,[run.id,task.id])).rows[0];
  if(!authority||authority.fence!==run.fence||authority.latest_id!==run.id||
    authority.attempt!==authority.attempt_count||!['queued','running'].includes(authority.state)||
    !same(authority.instruction_snapshot,run.instruction_snapshot))fail('RUN_CHANGED');
  const contract=deliveryDigest({criteria:task.criteria,covers:task.covers,stage:task.stage,
    baseWorkspace:task.execution?.baseWorkspace??null,sourceWorkspace:task.execution?.sourceWorkspace??null,
    readOnlySource:task.execution?.readOnlySource??false,writePaths:task.execution?.writePaths??null});
  const deps=new Set((await db.query('SELECT depends_on FROM dependencies WHERE task_id=$1',[task.id])).rows.map(x=>x.depends_on));
  const historicalTaskIds=[...new Set(events.map(e=>e.payload?.resolvedSource?.taskId).filter(id=>typeof id==='string'))];
  const siblings=(await db.query(`SELECT id,producer_task_id,producer_run_id FROM delivery_consumptions WHERE consumer_task_id=$1
    AND producer_task_id::text=ANY($2::text[]) ORDER BY id LIMIT 257`,[task.id,historicalTaskIds])).rows;
  if(siblings.length>256)fail('OBLIGATION_BOUND');
  const replacements=new Map<string,any>(),excluded=new Map<string,any>(),targets=new Map<string,string>();
  const bindings:any[]=[];
  for(const row of rows){
    const chain=events.filter(e=>e.entity_id===row.id);
    if(!chain.length)continue;
    if(row.goal_id!==task.goal_id||row.scope!==task.execution.baseWorkspace||row.consumer_contract_digest!==contract)
      fail('CONSUMER_CONTRACT_CHANGED');
    // Use the canonical binding reader; never treat an arbitrary event as a
    // replacement simply because its payload mentions this consumer.
    const resolution=await readDeliverySourceResolution(db,row);
    if(!resolution||resolution.eventId!==chain.at(-1).event_id)fail('RESOLUTION_CHANGED');
    if(chain.some((event:any)=>event.payload?.targetCandidate)&&!chain.at(-1)?.payload?.targetCandidate)
      fail('TARGET_CANDIDATE_CHANGED');
    if(!Array.isArray(row.binding.producerArtifacts)||!Array.isArray(row.binding.repairArtifacts)||
      deliveryDigest(row.binding.producerArtifacts)!==row.producer_manifest_digest||
      !row.binding.repairArtifacts.every((a:any)=>row.binding.producerArtifacts.some((b:any)=>same(a,b))))
      fail('ORIGINAL_MANIFEST_CHANGED');
    let prior=source(row.producer_task_id,row.producer_run_id,row.producer_manifest_digest);
    let priorRefs=pair(row.binding.repairArtifacts,prior.taskId,prior.runId),previousId:string|null=null;
    const seen=new Set([sourceKey(prior.taskId,prior.runId)]);
    const removed:any[]=[];
    for(const e of chain){
      const p=e.payload,replacement=p.replacement;
      if(!e.before_admission)fail('RESOLUTION_AFTER_ADMISSION');
      if(p.version!==1||p.obligationId!==row.id||p.consumerTaskId!==task.id||
        p.previousResolutionEventId!==previousId||!same(p.resolvedSource,prior)||
        p.original?.producerTaskId!==row.producer_task_id||p.original?.producerRunId!==row.producer_run_id||
        p.original?.producerManifestDigest!==row.producer_manifest_digest||p.acceptance!==false||
        p.compositionRequired!==true||p.independentQARequired!==true)fail('CHAIN_CHANGED');
      if(p.targetCandidate){
        if(p.targetCandidate.consumerTaskId!==task.id||p.targetCandidate.consumerRunId!==p.consumerRunId||
          p.targetCandidate.workspace!==task.execution?.workspace||
          !/^[a-f0-9]{64}$/.test(p.targetCandidate.candidateDigest??'')||
          !/^[a-f0-9]{64}$/.test(p.targetCandidate.candidateArtifactDigest??''))fail('TARGET_CANDIDATE_CHANGED');
      }
      if(e.case_task!==task.id||e.case_goal!==task.goal_id||!e.revision_id||e.applied_receipt!==true||
        e.prior_task!==task.id||e.prior_attempt>=authority.attempt||
        e.decision_provenance?.sourceAttempt!==e.prior_attempt||
        e.review_state!=='submitted'||e.review_stage!=='coordinate'||e.review_goal!==task.goal_id||e.review_passed!==true)
        fail('DECISION_UNPROVEN');
      const actions=e.decision_provenance?.decisionSnapshot?.actions;
      if(!Array.isArray(actions)||actions.length!==1||actions[0].kind!=='revise'||
        actions[0].sourceTaskId!==replacement?.producerTaskId||
        actions[0].deliveryResolution?.obligationId!==row.id||actions[0].deliveryResolution?.generation!==p.generation||
        actions[0].deliveryResolution?.expectedSourceRunId!==replacement?.producerRunId||
        p.key!==deliveryDigest({obligationId:row.id,generation:p.generation,decisionRunId:p.decisionRunId}))
        fail('DECISION_BINDING_CHANGED');
      if(!replacement||seen.has(sourceKey(replacement.producerTaskId,replacement.producerRunId))||!deps.has(prior.taskId)||!deps.has(replacement.producerTaskId))
        fail('CYCLE_OR_DEPENDENCY_CHANGED');
      if(deliveryDigest(replacement.producerArtifacts)!==replacement.producerManifestDigest)
        fail('REPLACEMENT_MANIFEST_CHANGED');
      const nextRefs=pair(replacement.repairArtifacts,replacement.producerTaskId,replacement.producerRunId);
      if(!nextRefs.every(a=>replacement.producerArtifacts.some((b:any)=>same({...a,task_id:undefined},{...b,task_id:undefined}))))
        fail('REPLACEMENT_REFS_CHANGED');
      removed.push(...priorRefs);seen.add(sourceKey(replacement.producerTaskId,replacement.producerRunId));
      prior=source(replacement.producerTaskId,replacement.producerRunId,replacement.producerManifestDigest);
      priorRefs=nextRefs;previousId=e.event_id;
    }
    const terminal=effectiveDeliverySource(row,resolution),key=sourceKey(prior.taskId,prior.runId);
    if(!replacements.has(key)){
      const identity=await readDeliverySourceIdentity(db,prior.taskId);
      const refs=(await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[prior.runId])).rows;
      if(!sourceIdentityAccepted(identity)||!sourceIdentityFrozen(identity)||identity.goal_id!==task.goal_id||
        identity.scope!==row.scope||identity.work_kind!=='repair'||identity.contract_digest!==resolution.replacement.contractDigest||
        deliverySourceConflict(terminal,identity,refs))fail('EFFECTIVE_SOURCE_CHANGED');
      if(!priorRefs.every(a=>dependencies.some(b=>same(pin(a),pin(b)))))fail('EFFECTIVE_INPUT_MISSING');
      replacements.set(key,{taskId:prior.taskId,runId:prior.runId,manifestDigest:prior.manifestDigest,
        contractDigest:identity.contract_digest,artifacts:priorRefs.map(pin)});
    }else if(replacements.get(key).manifestDigest!==prior.manifestDigest||
      replacements.get(key).contractDigest!==resolution.replacement.contractDigest)fail('EFFECTIVE_SOURCE_AMBIGUOUS');
    for(const old of removed){
      const oldKey=sourceKey(old.task_id,old.run_id),target=targets.get(oldKey);
      if(target&&target!==key)fail('REPLACEMENT_AMBIGUOUS');
      targets.set(oldKey,key);
      if(dependencies.some(a=>a.task_id===old.task_id&&a.name===old.name&&!same(pin(a),pin(old))&&
        !priorRefs.some(ref=>same(pin(a),pin(ref)))))fail('HISTORICAL_SOURCE_CHANGED');
      // All obligations that consume this original source must agree. A
      // replacement in one obligation cannot silently settle another one.
      for(const sibling of siblings.filter(d=>d.producer_task_id===old.task_id&&d.producer_run_id===old.run_id&&d.id!==row.id)){
        const last=events.filter(e=>e.entity_id===sibling.id).at(-1)?.payload?.replacement;
        if(!last||sourceKey(last.producerTaskId,last.producerRunId)!==key)fail('OBLIGATION_AMBIGUOUS');
      }
      excluded.set(old.id,pin(old));
    }
    const last=chain.at(-1);
    bindings.push({obligationId:row.id,eventIds:chain.map(e=>e.event_id),resolutionEventId:resolution.eventId,
      resolutionAuthority:{version:1,obligationId:row.id,eventId:last.event_id,caseId:last.payload.caseId,
        decisionRunId:last.payload.decisionRunId,consumerRunId:last.payload.consumerRunId,consumerAttempt:last.prior_attempt,
        revisionId:last.revision_id,revisionOriginId:last.revision_origin_id,revisionNumber:last.revision_number},
      resolutionDigest:deliveryDigest(resolution),original:{taskId:row.producer_task_id,runId:row.producer_run_id,
        manifestDigest:row.producer_manifest_digest,artifacts:row.binding.producerArtifacts},
      targetCandidate:resolution.targetCandidate??null,
      effectiveSource:replacements.get(key)});
  }
  // A cross-obligation A→B plus B→C is not authority to silently select C for
  // A. Require an explicit resolution of each obligation, rather than infer it.
  for(const value of replacements.values())if(targets.has(sourceKey(value.taskId,value.runId)))fail('CROSS_OBLIGATION_CHAIN');
  if(dependencies.some(a=>excluded.has(a.id)&&!same(pin(a),excluded.get(a.id))))fail('HISTORICAL_INPUT_CHANGED');
  const dependencyArtifacts=dependencies.filter(a=>!excluded.has(a.id));
  const historicalArtifacts=[...excluded.values()];
  const explicitHistoricalInputs=(task.execution?.inputs??[]).filter((a:any)=>excluded.has(a.id));
  if(explicitHistoricalInputs.some((a:any)=>a.name!==excluded.get(a.id).name||a.digest!==excluded.get(a.id).digest))
    fail('EXPLICIT_HISTORICAL_INPUT_CHANGED');
  const provenance:any={version:1,runId:run.id,runFence:run.fence,instructionSnapshotDigest:deliveryDigest(run.instruction_snapshot),
    bindings,historicalArtifacts,explicitHistoricalInputIds:explicitHistoricalInputs.map((a:any)=>a.id),
    acceptance:false,compositionRequired:true,independentQARequired:true};
  let targetProof:ReturnType<typeof compositionTargetAuthority>=null;
  try{targetProof=compositionTargetAuthority(bindings);}
  catch(error:any){fail(error?.code==='COMPOSITION_TARGET_CONFLICT'?'TARGET_CANDIDATE_AMBIGUOUS':'TARGET_CANDIDATE_AUTHORITY_UNPROVEN');}
  if(targetProof){
    // Keep every accepted patch already present in the frozen dependency set.
    // The explicit supervisor substitution removes only its validated old
    // refs; it must not silently drop an unrelated accepted repair.
    const groups=new Map<string,any[]>();
    for(const artifact of dependencyArtifacts.filter((a:any)=>['implementation.patch','repair-report.json'].includes(a.name))){
      if(typeof artifact.task_id!=='string'||typeof artifact.run_id!=='string')fail('COMPOSITION_ACTIVE_SOURCE_INCOMPLETE');
      const key=sourceKey(artifact.task_id,artifact.run_id),group=groups.get(key)??[];group.push(artifact);groups.set(key,group);
    }
    const active=[...groups.values()].map((group:any[])=>{
      const patches=group.filter(a=>a.name==='implementation.patch'),reports=group.filter(a=>a.name==='repair-report.json');
      if(patches.length!==1||reports.length!==1||group.some(a=>a.run_id!==patches[0].run_id)||
        !/^[a-f0-9]{64}$/.test(patches[0].digest??'')||!/^[a-f0-9]{64}$/.test(reports[0].digest??''))
        fail('COMPOSITION_ACTIVE_SOURCE_INCOMPLETE');
      const patch=patches[0],report=reports[0];
      return {producerTaskId:patch.task_id,producerRunId:patch.run_id,patchArtifactId:patch.id,patchDigest:patch.digest,
        reportArtifactId:report.id,reportDigest:report.digest};
    }).sort((a:any,b:any)=>a.producerTaskId.localeCompare(b.producerTaskId)||a.producerRunId.localeCompare(b.producerRunId));
    if(!active.length)fail('COMPOSITION_ACTIVE_SOURCE_INCOMPLETE');
    for(const item of active){
      const identity=await readDeliverySourceIdentity(db,item.producerTaskId);
      if(!sourceIdentityAccepted(identity)||!sourceIdentityFrozen(identity)||identity.goal_id!==task.goal_id||
        identity.run_id!==item.producerRunId||identity.scope!==task.execution?.baseWorkspace||identity.work_kind!=='repair')
        fail('COMPOSITION_ACTIVE_SOURCE_CHANGED');
    }
    const superseded=[...new Set(historicalArtifacts.filter((a:any)=>['implementation.patch','repair-report.json'].includes(a.name))
      .map((a:any)=>sourceKey(a.task_id,a.run_id)))].map(key=>{
        const group=historicalArtifacts.filter((a:any)=>sourceKey(a.task_id,a.run_id)===key);
        const patch=group.find((a:any)=>a.name==='implementation.patch'),report=group.find((a:any)=>a.name==='repair-report.json');
        if(!patch||!report||!patch.run_id||patch.run_id!==report.run_id)fail('COMPOSITION_SUPERSEDED_SOURCE_INCOMPLETE');
        return {producerTaskId:patch.task_id,producerRunId:patch.run_id,patchArtifactId:patch.id,patchDigest:patch.digest,
          reportArtifactId:report.id,reportDigest:report.digest};
      });
    const selection:any={version:1,authority:'supervisor-effective-source-manifest',consumerTaskId:task.id,
      consumerRunId:run.id,consumerRunFence:run.fence,workspace:task.execution.workspace,target:targetProof.target,
      ...(targetProof.targetAuthority?{targetAuthority:targetProof.targetAuthority}:{}),
      active,superseded,resolutionEventIds:bindings.flatMap((binding:any)=>binding.eventIds)};
    selection.digest=deliveryDigest(selection);
    provenance.compositionSelection=selection;
  }
  return {dependencyArtifacts,effectiveSources:{...provenance,digest:deliveryDigest(provenance)}};
}
