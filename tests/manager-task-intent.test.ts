import { createHash } from 'node:crypto';
import { expect, it } from 'vitest';
import { projectManagerTaskContracts, projectManagerTaskIntent, managerTaskInstructionReference } from '../server/manager-task-intent.js';
import { focusManagerTasks } from '../server/manager-focus.js';

it('projects substantive current consumer requirements instead of a generic passed:true gate', () => {
  const description =
    'Validate paid20-cycle desktop/mobile, bonus resume and wallet reconciliation.\n\nREQUIREMENTS: ' +
    JSON.stringify([
      {
        id: 'game',
        description:
          'Native boot, real controls, isolated sessions and complete bonus followed by next spin; preserve original rules and fixed90% RTP.',
      },
    ]) +
    '\n\nCONSTRAINTS: Historical two-browser ceiling, later superseded.\n\nSUPERVISOR: Check the first resource consumption boundary without repeating the full RTP suite.';
  const original = { description, covers: ['game'], instruction_revision_id: 'revision-2' };
  const intent = projectManagerTaskIntent(original, 1000);
  const projected = JSON.parse(intent.text);
  expect(projected.requirements[0].description).toContain('complete bonus followed by next spin');
  expect(projected.instruction).toContain('paid20-cycle');
  expect(intent.descriptionHash).toBe(createHash('sha256').update(description).digest('hex'));
  expect(intent.revisionId).toBe('revision-2');
  expect(intent.dataOnly).toBe(true);
  expect(intent.text).not.toContain('Historical two-browser');
  expect(intent.omittedPaths).toContain('/otherInstructionSections');
  expect(JSON.stringify(intent).length).toBeLessThanOrEqual(1000);
  expect(original.description).toBe(description);
});

it('preserves an explicit malformed-requirement omission without inventing a requirement', () => {
  const intent = projectManagerTaskIntent(
    { description: 'Investigate native callback.\nREQUIREMENTS: [malformed]', covers: ['game'] },
    1000,
  );
  expect(JSON.parse(intent.text).requirements).toEqual([]);
  expect(intent.truncated).toBe(true);
  expect(intent.omittedPaths).toContain('/requirements');
});

it('bounds long instructions and filters unrelated requirements and assigned credentials', () => {
  const description =
    'Investigate the first native failure. API_KEY=private_fixture ' +
    'trace '.repeat(3000) +
    '\nREQUIREMENTS: ' +
    JSON.stringify([
      { id: 'game', description: 'Preserve genuine bytes and native acceptance.' },
      { id: 'other', description: 'Unrelated game context.' },
    ]);
  const intent = projectManagerTaskIntent({ description, covers: ['game'] }, 800);
  expect(JSON.parse(intent.text).requirements).toEqual([
    { id: 'game', description: 'Preserve genuine bytes and native acceptance.' },
  ]);
  expect(intent.text).not.toContain('private_fixture');
  expect(intent.text).not.toContain('Unrelated game context');
  expect(intent.truncated).toBe(true);
  expect(JSON.stringify(intent).length).toBeLessThanOrEqual(800);
});

function task(n:number,dependencies:string[]=[],state='failed') {
  return {id:`task-${n}`,state,dependencies,execution:{baseWorkspace:'project/focal'},
    criteria:[{id:`criterion-${n}`,kind:'json',path:'proof.json',expected:{passed:true,contract:`full-${n}`}}],
    covers:[`criterion-${n}`],instruction_revision_id:`revision-${n}`,
    description:`Original instruction ${n}. ${'Keep material source and independent validation. '.repeat(90)}\nREQUIREMENTS: `+
      JSON.stringify([{id:`criterion-${n}`,description:`Exact requirement ${n}. ${'Do not remove. '.repeat(100)}`}])};
}
const focus={taskId:'task-1',scope:'project/focal',contextMode:'delivery_graph'};

it('keeps all three focal contracts exact and stable while discovery rotates through one, six and twelve consumers',()=>{
  const tasks=[task(1,['task-2']),task(2),task(3,['task-1']),
    ...Array.from({length:12},(_,i)=>task(10+i))],original=structuredClone(tasks);
  const windows=[[{taskId:'task-1'}],tasks.slice(3,9).map(t=>({taskId:t.id})),tasks.slice(3).map(t=>({taskId:t.id}))];
  const maps=windows.map(consumers=>projectManagerTaskContracts(tasks,consumers,focus));
  for(const t of tasks.slice(0,3)){
    const contract=maps[0].get(t.id)!;
    expect(contract.criteria).toEqual(t.criteria);
    expect(contract.intent.descriptionHash).toBe(createHash('sha256').update(t.description).digest('hex'));
    expect(contract.intent.revisionId).toBe(t.instruction_revision_id);
    expect(contract.intent.truncated).toBe(true);
    expect(JSON.stringify(contract.intent).length).toBeLessThanOrEqual(1000);
    for(const map of maps)expect(map.get(t.id)).toEqual(contract);
  }
  expect(maps[0].has('task-10')).toBe(false);
  expect(tasks).toEqual(original);
});

it('uses the existing graph and its aggregate excerpt target for thirteen current contracts',()=>{
  const tasks=[task(1,Array.from({length:12},(_,i)=>`task-${i+2}`)),...Array.from({length:12},(_,i)=>task(i+2)),task(50)];
  const expected=focusManagerTasks(tasks.map(t=>({...t,baseWorkspace:t.execution.baseWorkspace})),focus)
    .filter(t=>!t.selection?.detailOmitted).map(t=>t.id);
  const first=projectManagerTaskContracts(tasks,[{taskId:'task-50'}],focus);
  const second=projectManagerTaskContracts(tasks,tasks.map(t=>({taskId:t.id})),focus);
  expect(expected).toHaveLength(13);
  expect(expected.reduce((size,id)=>size+JSON.stringify(first.get(id)!.intent).length,0)).toBeLessThanOrEqual(12000);
  for(const id of expected){expect(first.get(id)).toEqual(second.get(id));expect(first.get(id)!.criteria).toEqual(tasks.find(t=>t.id===id)!.criteria);}
  const closed=structuredClone(tasks);closed[1].state='accepted';
  const changed=projectManagerTaskContracts(closed,[],focus);
  expect(changed.has('task-2')).toBe(false);
  expect(changed.get('task-1')!.criteria).toEqual(tasks[0].criteria);
});

it('preserves generic no-scope focus semantics and keeps no-focus discovery behavior',()=>{
  const tasks=[task(1),{...task(2),execution:{baseWorkspace:'project/other'}},task(3,[],'accepted')];
  const generic=projectManagerTaskContracts(tasks,[],{taskId:'task-1'});
  expect([...generic.keys()]).toEqual(['task-1','task-2']);
  for(const t of tasks.slice(0,2))expect(generic.get(t.id)!.criteria).toEqual(t.criteria);
  const noFocus=projectManagerTaskContracts(tasks,[{taskId:'task-2'}],null);
  expect([...noFocus.keys()]).toEqual(['task-2']);
  expect(noFocus.get('task-2')!.intent).toEqual(projectManagerTaskIntent(tasks[1],12000));
});

it('keeps minimum provenance and exact criteria when a huge focal graph cannot meet its excerpt target',()=>{
  const tasks=Array.from({length:100},(_,i)=>task(i+1));
  const contracts=projectManagerTaskContracts(tasks,[],{taskId:'task-1'});
  // The target cannot erase mandatory hashes/revisions. Final review admission,
  // not this excerpt helper, enforces the hard context limit.
  expect([...contracts.values()].reduce((n,value)=>n+JSON.stringify(value.intent).length,0)).toBeGreaterThan(12000);
  for(const t of tasks){expect(contracts.get(t.id)!.criteria).toEqual(t.criteria);expect(contracts.get(t.id)!.intent.revisionId).toBe(t.instruction_revision_id);}
});

it('links omitted current constraints to the exact full instruction without assuming they are historical',()=>{
 const original={id:'11111111-1111-4111-8111-111111111111',attempt_count:3,instruction_revision_id:'revision-current',
  description:'Implement settlement.\nCONSTRAINTS: Mandatory non-JSON wallet accounting boundary.\n'+
   'Current substantive conditions. '.repeat(200),covers:[]};
 const before=structuredClone(original),intent=projectManagerTaskIntent(original);
 expect(intent).toMatchObject({representation:'instruction_excerpt',truncated:true,
  recovery:{kind:'task',taskId:original.id,expectedAttempt:3,offset:0,
   snapshotDigest:createHash('sha256').update(original.description).digest('hex')}});
 expect(intent.omittedPaths).toContain('/otherInstructionSections');
 expect(managerTaskInstructionReference(original)).toMatchObject({request:intent.recovery,
  totalCharacters:original.description.length,revisionId:'revision-current'});
 expect(JSON.stringify(intent).length).toBeLessThanOrEqual(1000);
 expect(original).toEqual(before);
 const changed=managerTaskInstructionReference({...original,description:original.description+'New requirement.'});
 expect(changed.digest).not.toBe(intent.descriptionHash);
 // A partially observed fixture/task must not invent an attempt or actionable recovery identity.
 expect(managerTaskInstructionReference({description:original.description})).not.toHaveProperty('request');
});
