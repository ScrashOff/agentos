import {createHash} from 'node:crypto';
import {compositionTargetAuthority} from './composition-target-authority.js';

/** Only accepted dependency artifacts in this run's immutable manifest may
 * request composition. Recovery/operator inputs are evidence, not authority. */
const sha=(value:string)=>createHash('sha256').update(value).digest('hex');
const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'
  ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const digest=(value:any)=>sha(JSON.stringify(canonical(value)));
const validRef=(value:any)=>value&&typeof value.producerTaskId==='string'&&typeof value.producerRunId==='string'&&
  typeof value.patchArtifactId==='string'&&/^[a-f0-9]{64}$/.test(value.patchDigest)&&
  typeof value.reportArtifactId==='string'&&/^[a-f0-9]{64}$/.test(value.reportDigest);

export function patchComposition(task:any,manifest:any,inputs:any[]){
  if(task.execution?.readOnlySource || !task.execution?.sourceWorkspace || task.stage!=='integrate')return null;
  const refs=manifest.dependencyArtifacts??[],patches:any[]=[];
  const selection=manifest.effectiveSources?.compositionSelection;
  if(selection){
    const identity=manifest.runIdentity;
    const {digest:proofDigest,...body}=selection;
    if(selection.version!==1 || selection.authority!=='supervisor-effective-source-manifest' ||
      !identity || identity.taskId!==task.id || identity.goalId!==task.goal_id ||
      identity.runId!==selection.consumerRunId || identity.runFence!==selection.consumerRunFence ||
      selection.consumerTaskId!==task.id || selection.workspace!==task.execution.workspace ||
      !selection.target || typeof selection.target.consumerRunId!=='string' ||
      selection.target.workspace!==selection.workspace ||
      !/^[a-f0-9]{64}$/.test(selection.target.candidateDigest??'') ||
      typeof selection.target.candidateArtifactId!=='string' ||
      !/^[a-f0-9]{64}$/.test(selection.target.candidateArtifactDigest??'') ||
      !Array.isArray(selection.active) || !Array.isArray(selection.superseded) ||
      selection.active.some((x:any)=>!validRef(x)) || selection.superseded.some((x:any)=>!validRef(x)) ||
      digest(body)!==proofDigest)throw Error('COMPOSITION_SELECTION_INVALID');
    const bindings=manifest.effectiveSources.bindings;
    let targetProof:ReturnType<typeof compositionTargetAuthority>;
    try{targetProof=compositionTargetAuthority(bindings);}
    catch{throw Error('COMPOSITION_SELECTION_AUTHORITY_MISMATCH');}
    if(!targetProof||digest(selection.target)!==digest(targetProof.target)||
      digest(selection.targetAuthority??null)!==digest(targetProof.targetAuthority??null)||
      digest(selection.resolutionEventIds)!==digest(bindings.flatMap((binding:any)=>binding.eventIds??[])))
      throw Error('COMPOSITION_SELECTION_AUTHORITY_MISMATCH');
    const expectedActive=new Map<string,any>(),activeGroups=new Map<string,any[]>();
    for(const ref of refs.filter((r:any)=>['implementation.patch','repair-report.json'].includes(r.name))){
      if(typeof ref.task_id!=='string'||typeof ref.run_id!=='string')throw Error('COMPOSITION_SELECTION_ACTIVE_SET');
      const key=ref.task_id+':'+ref.run_id,group=activeGroups.get(key)??[];group.push(ref);activeGroups.set(key,group);
    }
    for(const [key,artifacts] of activeGroups){
      const patches=artifacts.filter((a:any)=>a.name==='implementation.patch'),reports=artifacts.filter((a:any)=>a.name==='repair-report.json');
      if(patches.length!==1||reports.length!==1)throw Error('COMPOSITION_SELECTION_ACTIVE_SET');
      const patch=patches[0],report=reports[0];
      expectedActive.set(key,{producerTaskId:patch.task_id,producerRunId:patch.run_id,
        patchArtifactId:patch.id,patchDigest:patch.digest,reportArtifactId:report.id,reportDigest:report.digest});
    }
    if(!expectedActive.size)throw Error('COMPOSITION_SELECTION_ACTIVE_SET');
    const listedActive=new Map(selection.active.map((item:any)=>[item.producerTaskId+':'+item.producerRunId,item]));
    if(selection.superseded.some((old:any)=>refs.some((r:any)=>r.task_id===old.producerTaskId&&
      (r.id===old.patchArtifactId||r.id===old.reportArtifactId))))throw Error('COMPOSITION_SELECTION_SUPERSEDED_ACTIVE');
    if(digest([...listedActive.values()].sort((a:any,b:any)=>a.producerTaskId.localeCompare(b.producerTaskId)))!==
      digest([...expectedActive.values()].sort((a:any,b:any)=>a.producerTaskId.localeCompare(b.producerTaskId))))
      throw Error('COMPOSITION_SELECTION_ACTIVE_SET');
    const activeIds=new Set<string>();
    for(const item of selection.active){
      const key=item.producerTaskId+':'+item.producerRunId;
      if(activeIds.has(key)||selection.superseded.some((old:any)=>old.producerTaskId+':'+old.producerRunId===key))
        throw Error('COMPOSITION_SELECTION_AMBIGUOUS');
      activeIds.add(key);
      const patch=refs.find((r:any)=>r.task_id===item.producerTaskId&&r.run_id===item.producerRunId&&
        r.name==='implementation.patch'&&r.id===item.patchArtifactId&&r.digest===item.patchDigest);
      const report=refs.find((r:any)=>r.task_id===item.producerTaskId&&r.run_id===item.producerRunId&&
        r.name==='repair-report.json'&&r.id===item.reportArtifactId&&r.digest===item.reportDigest);
      if(!patch||!report)throw Error('COMPOSITION_SELECTION_SOURCE_MISSING');
      const bind=(ref:any)=>inputs.find(i=>i.id===ref.id&&i.digest===ref.digest&&i.name===ref.name);
      const p=bind(patch),r=bind(report);if(!p||!r)throw Error('COMPOSITION_INPUT_BINDING');
      patches.push({producerTaskId:item.producerTaskId,patch:{id:p.id,name:p.name,digest:p.digest},
        report:{id:r.id,name:r.name,digest:r.digest}});
    }
    const expectedSuperseded=new Map<string,any>(),historyGroups=new Map<string,any[]>();
    for(const ref of (manifest.effectiveSources.historicalArtifacts??[]).filter((r:any)=>['implementation.patch','repair-report.json'].includes(r.name))){
      const key=ref.task_id+':'+ref.run_id,group=historyGroups.get(key)??[];group.push(ref);historyGroups.set(key,group);
    }
    for(const [key,group] of historyGroups){
      const patches=group.filter((a:any)=>a.name==='implementation.patch'),reports=group.filter((a:any)=>a.name==='repair-report.json');
      if(patches.length!==1||reports.length!==1)throw Error('COMPOSITION_SELECTION_HISTORY_MISSING');
      const patch=patches[0],report=reports[0];
      expectedSuperseded.set(key,{producerTaskId:patch.task_id,producerRunId:patch.run_id,patchArtifactId:patch.id,
        patchDigest:patch.digest,reportArtifactId:report.id,reportDigest:report.digest});
    }
    const listedSuperseded=new Map(selection.superseded.map((item:any)=>[item.producerTaskId+':'+item.producerRunId,item]));
    if(digest([...listedSuperseded.values()].sort((a:any,b:any)=>a.producerTaskId.localeCompare(b.producerTaskId)||a.producerRunId.localeCompare(b.producerRunId)))!==
      digest([...expectedSuperseded.values()].sort((a:any,b:any)=>a.producerTaskId.localeCompare(b.producerTaskId)||a.producerRunId.localeCompare(b.producerRunId))))
      throw Error('COMPOSITION_SELECTION_HISTORY_MISSING');
    for(const old of selection.superseded){
      if(activeIds.has(old.producerTaskId+':'+old.producerRunId)||refs.some((r:any)=>r.task_id===old.producerTaskId&&
        (r.id===old.patchArtifactId||r.id===old.reportArtifactId)))throw Error('COMPOSITION_SELECTION_SUPERSEDED_ACTIVE');
      if(!manifest.effectiveSources.historicalArtifacts?.some((r:any)=>r.id===old.patchArtifactId&&r.digest===old.patchDigest&&
        r.name==='implementation.patch'&&r.task_id===old.producerTaskId) ||
        !manifest.effectiveSources.historicalArtifacts?.some((r:any)=>r.id===old.reportArtifactId&&r.digest===old.reportDigest&&
        r.name==='repair-report.json'&&r.task_id===old.producerTaskId))throw Error('COMPOSITION_SELECTION_HISTORY_MISSING');
    }
    return patches.length?{version:2,expectedCandidateDigest:selection.target.candidateDigest,
      selection:{...body,digest:proofDigest},patches}:null;
  }
  const tasks=[...new Set(refs.filter((r:any)=>r.name==='implementation.patch').map((r:any)=>r.task_id))];
  for(const taskId of tasks){
    if(typeof taskId!=='string')continue;
    const patch=refs.find((r:any)=>r.task_id===taskId && r.name==='implementation.patch');
    const report=refs.find((r:any)=>r.task_id===taskId && r.name==='repair-report.json');
    if(!patch || !report)continue;
    const bind=(ref:any)=>inputs.find(i=>i.id===ref.id && i.digest===ref.digest && i.name===ref.name);
    const p=bind(patch),r=bind(report);if(!p || !r)throw Error('COMPOSITION_INPUT_BINDING');
    const reference=(i:any)=>({id:i.id,name:i.name,digest:i.digest});
    patches.push({producerTaskId:taskId,patch:reference(p),report:reference(r)});
  }
  return patches.length?{version:1,patches}:null;
}
