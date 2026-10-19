import { describe, expect, it } from 'vitest';
import { buildRetryEffectEvidence } from '../server/retry-effect-evidence.js';
import { readFileSync } from 'node:fs';

const mwProjection = JSON.parse(readFileSync(new URL('./fixtures/mw-supervision-memory-projection.json', import.meta.url), 'utf8'));

const h=(char:string)=>char.repeat(64);
const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
function run(attempt:number, overrides:any={}) {
  const failed=overrides.failed??['menu_rules'];
  const criteria=overrides.criteria??[{id:'menu_rules',description:'preserve canonical rules content'},
    {id:'native_boot',description:'native scene loads'}];
  return {
    id:id(attempt),attempt,state:'submitted',
    instructionSnapshot:{task:{criteria,execution:{baseWorkspace:'game/source',sourceWorkspace:'game/source',
      workspace:'game/work',readOnlySource:false}},executionInputs:[{id:id(50),name:'rules.json',digest:h('a'),size:31}]},
    manifest:{dependencyArtifacts:[{id:id(51),name:'accepted.patch',digest:h('b'),size:42,task_id:id(60),run_id:id(61)}],
      recoveryArtifacts:[],recoveryArtifactAliases:[],selected:[],supervisionMemory:{memories:[]}},
    acceptancePassed:false,
    checks:criteria.map((criterion:any)=>({id:criterion.id,passed:!failed.includes(criterion.id)})),
    changeDiffs:[{digest:h(overrides.diff??'c'),size:5518270}],
    ...overrides,
  };
}
const context={stage:'integrate',workKind:'repair',supervisedRevise:true};

describe('read-only retry-effect evidence',()=>{
  it('surfaces exact same change, frozen inputs, original criteria, and failed set',()=>{
    const result=buildRetryEffectEvidence(run(14),run(13),context);
    expect(result).toMatchObject({
      status:'same_change_same_inputs_same_failed_criteria',sameChangeDiff:true,sameFrozenInputs:true,
      sameCriteria:true,sameFailedCriteria:true,priorFailedCriteria:['menu_rules'],currentFailedCriteria:['menu_rules'],
      acceptanceRemainsFalse:true,decisionRequired:true,
    });
  });

  it('does not call changed test outcomes no progress when the patch and inputs stayed the same',()=>{
    const result=buildRetryEffectEvidence(run(14,{failed:['native_boot']}),run(13),context);
    expect(result?.status).toBe('same_change_same_inputs_failure_set_changed');
    expect(result?.sameFailedCriteria).toBe(false);
    expect(result?.currentFailedCriteria).toEqual(['native_boot']);
  });

  it('distinguishes implementation, input, and contract changes',()=>{
    expect(buildRetryEffectEvidence(run(14,{diff:'d'}),run(13),context)?.status).toBe('change_diff_changed');
    const changedInputs=run(14);
    changedInputs.instructionSnapshot.executionInputs[0].digest=h('e');
    expect(buildRetryEffectEvidence(changedInputs,run(13),context)?.status).toBe('material_inputs_changed');
    const changedMemory=run(14);
    changedMemory.manifest.selected=[{id:id(52),version:1,hash:h('f')}];
    expect(buildRetryEffectEvidence(changedMemory,run(13),context)?.status).toBe('same_change_same_material_inputs_memory_context_changed_same_failed_criteria');
    const changedCriteria=run(14,{criteria:[{id:'menu_rules',description:'changed contract'}]});
    expect(buildRetryEffectEvidence(changedCriteria,run(13),context)?.status).toBe('criteria_snapshot_changed');
  });

  it('projects real MW supervision-memory metadata and keeps patch identity separate from a changed memory context',()=>{
    const byAttempt=new Map(mwProjection.attempts.map((entry:any)=>[entry.attempt,entry]));
    const run196=byAttempt.get(196) as any, run197=byAttempt.get(197) as any;
    expect(run196.selected).toHaveLength(15);
    expect(run197.selected).toEqual(run196.selected);
    expect(run196.supervisionmemories).toBeNull();
    expect(run197.supervisionmemories).toBeNull();

    const before=byAttempt.get(142) as any, after=byAttempt.get(143) as any;
    expect(before.supervisionmemories).toBeNull();
    expect(after.supervisionmemories).toHaveLength(6);
    expect(after.supervisionmemories.every((memory:any)=>memory.id&&memory.version&&memory.hash&&memory.source)).toBe(true);
    expect(JSON.stringify(mwProjection)).not.toContain('\"content\":');

    const previous=run(142), current=run(143);
    previous.manifest.selected=before.selected;
    current.manifest.selected=after.selected;
    previous.manifest.supervisionMemory={memories:before.supervisionmemories??[]};
    current.manifest.supervisionMemory={memories:after.supervisionmemories??[]};
    previous.changeDiffs=[{digest:h('c'),size:100}];
    current.changeDiffs=[{digest:h('c'),size:100}];
    const result=buildRetryEffectEvidence(current,previous,context);
    expect(result).toMatchObject({sameCorePatch:true,sameMaterialInputs:true,sameControlMemoryContext:false,
      sameSelectedMemoryContext:false,
      sameFrozenInputs:false,status:'same_change_same_material_inputs_memory_context_changed_same_failed_criteria',
      acceptanceRemainsFalse:true,decisionRequired:true});
  });

  it('refuses to classify diagnostics, non-revise runs, acceptance, gaps, or nonconsecutive attempts',()=>{
    expect(buildRetryEffectEvidence(run(14),run(13),{...context,workKind:'diagnostic'})).toBeNull();
    expect(buildRetryEffectEvidence(run(14),run(13),{...context,workKind:'test'})).toBeNull();
    expect(buildRetryEffectEvidence(run(14),run(13),{...context,supervisedRevise:false})).toBeNull();
    expect(buildRetryEffectEvidence(run(14,{acceptancePassed:true}),run(13),context)).toBeNull();
    expect(buildRetryEffectEvidence(run(14,{changeDiffs:[]}),run(13),context)).toBeNull();
    expect(buildRetryEffectEvidence(run(15),run(13),context)).toBeNull();
    const inconsistentMemory=run(14);
    inconsistentMemory.manifest.supervisionMemory={memories:[{id:id(53),version:1,hash:h('f')}]};
    expect(buildRetryEffectEvidence(inconsistentMemory,run(13),context)).toBeNull();
  });
});
