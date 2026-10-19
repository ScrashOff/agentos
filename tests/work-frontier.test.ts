import { beforeAll, afterAll, it, expect } from 'vitest';
import { randomUUID } from 'node:crypto';
import { installTaskExecutionMetadata } from '../server/task-execution-metadata.js';
import { installRunCriteriaMetadata } from '../server/run-criteria-metadata.js';
import { pool,migrate } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { completeRun,validateTask,writeArtifact,goalDetail } from '../server/service.js';
import { projectWorkFrontier,objectiveFrontierNodes,occurrenceFrontier,readObjectiveFrontier,
  compactWorkFrontier,readObjectiveFrontierTasks,type FrontierNode } from '../server/work-frontier.js';
import { createOccurrence,occurrenceDetail } from '../server/cycles/store.js';

const boundary={kind:'objective' as const,id:'documents'};
const node=(id='report',extra:Partial<FrontierNode>={}):FrontierNode=>({
  id,title:'Relatório de pesquisa',scope:'research/source',state:'ready',dependencies:[],
  criteria:[{id:'sources',label:'Fontes verificáveis'}],accepted:false,live:false,...extra});

it('retains current accepted reviews as dependencies without counting them as final deliveries',()=>{
 const criteria=[{id:'review',label:'Independent review',name:'review.md'}];
 const tasks=[{id:'review',title:'Review',stage:'review',state:'accepted',attempt_count:1,criteria},
   {id:'delivery',title:'Final document',stage:'verify',state:'ready',attempt_count:0,criteria:[{id:'final',label:'Final document'}]}];
 const runs=[{id:'review-run',task_id:'review',attempt:1,state:'submitted',accepted_criteria:criteria}];
 const acceptance={run_id:'review-run',passed:true,checks:[{id:'review',passed:true}],created_at:'2026-10-07'};
 const dependencies=[{task_id:'delivery',depends_on:'review'}];
 const frontier=projectWorkFrontier(boundary,objectiveFrontierNodes(tasks,runs,[acceptance],dependencies));
 expect(frontier.entries.find(e=>e.taskId==='delivery')).toMatchObject({state:'ready',blockedBy:[]});
 expect(frontier.entries.find(e=>e.taskId==='review')).toMatchObject({state:'accepted',auxiliary:true});
 expect(frontier.counts).toMatchObject({total:1,accepted:0});expect(frontier.complete).toBe(false);
 const rejected=projectWorkFrontier(boundary,objectiveFrontierNodes(tasks,runs,[{...acceptance,passed:false}],dependencies));
 expect(rejected.entries.find(e=>e.taskId==='delivery')?.blockedBy).toEqual(['review']);
 const changed=projectWorkFrontier(boundary,objectiveFrontierNodes([{...tasks[0],criteria:[{...criteria[0],name:'changed.md'}]},tasks[1]],runs,[acceptance],dependencies));
 expect(changed.entries.find(e=>e.taskId==='delivery')?.blockedBy).toEqual(['review']);
});

it('tracks research, documentation and operations without requiring game or code artifacts',()=>{
  const initial=projectWorkFrontier(boundary,[node()]);
  expect(initial.counts).toMatchObject({accepted:0,total:1,ready:1});
  expect(initial.next).toBe('dispatch_ready');
  const final=projectWorkFrontier(boundary,[node('report',{state:'accepted',accepted:true,receipt:'sources-hash'})]);
  expect(final.complete).toBe(true);
  expect(final.counts.accepted).toBe(1);
});
it('activity, repeated attempts and accepted diagnostics do not create final delivery progress',()=>{
  const first=projectWorkFrontier(boundary,[node()]);
  const next=projectWorkFrontier(boundary,[node('report',{state:'running',live:true,attempt:20}),
    node('investigation',{state:'accepted',accepted:true,auxiliary:true,receipt:'diagnosis'})]);
  expect(next.deliveryFingerprint).toBe(first.deliveryFingerprint);
  expect(next.counts.accepted).toBe(0);
  expect(next.next).toBe('prepare_independent_work');
});
it('a live or old acceptance is not a current accepted criterion',()=>{
  const tasks=[{id:'report',title:'Report',state:'accepted',attempt_count:2,criteria:[{id:'sources',label:'Sources'}]}];
  const runs=[{id:'old',task_id:'report',state:'submitted',attempt:1}];
  const acceptances=[{id:'acceptance',run_id:'old',passed:true,checks:[{id:'sources',passed:true}],created_at:'2026-10-06'}];
  expect(projectWorkFrontier(boundary,objectiveFrontierNodes(tasks,runs,acceptances,[])).complete).toBe(false);
  runs.push({id:'current',task_id:'report',state:'running',attempt:2});
  expect(projectWorkFrontier(boundary,objectiveFrontierNodes(tasks,runs,acceptances,[])).counts.executing).toBe(1);
});
it('exposes missing dependencies and unplanned requirements instead of claiming no work',()=>{
  const result=projectWorkFrontier(boundary,[node('report',{dependencies:['missing-source']})],
    [{id:'delivery',description:'Publish a verified report'}]);
  expect(result.next).toBe('planning_gap');
  expect(result.entries[0].blockedBy).toEqual(['missing-source']);
  expect(result.rootBlockerTaskIds).toEqual(['report']);
  expect(result.requirements[0].state).toBe('unplanned');
});
it('keeps preparation and acceptance distinct in a dependency chain',()=>{
  const nodes=[node('analysis',{state:'accepted',accepted:true}),
    node('review',{stage:'verify',dependencies:['analysis'],covers:['sources']})];
  const result=projectWorkFrontier(boundary,nodes,[{id:'sources',description:'Reviewed sources'}]);
  expect(result.counts).toMatchObject({total:1,accepted:0,ready:1});
  expect(result.requirements[0].state).toBe('pending');
});
it('isolates recurring occurrences and does not carry a previous run acceptance forward',()=>{
  const occurrence={id:'run-1',version:1};
  const steps=[{id:'step-1',occurrence_id:'run-1',key:'report',state:'completed',definition:{title:'Daily report',dependsOn:[],criteria:[{id:'report',label:'Actual report'}]}}];
  const attempts=[{id:'attempt-1',step_id:'step-1',number:1,state:'completed',receipt:{recorded:true}}];
  const first=occurrenceFrontier(occurrence,steps,attempts,[]);
  expect(first.complete).toBe(true);
  expect(()=>occurrenceFrontier({id:'run-2',version:1},steps,attempts,[])).toThrow('FRONTIER_SCOPE');
  const second=occurrenceFrontier({id:'run-2',version:1},[{...steps[0],id:'step-2',occurrence_id:'run-2',state:'queued'}],[],[]);
  expect(second.complete).toBe(false);
  expect(second.counts.ready).toBe(1);
  expect(second.deliveryFingerprint).not.toBe(first.deliveryFingerprint);
});
it('uses artifact hashes rather than acceptance-row IDs for delivery identity',()=>{
  const tasks=[{id:'report',title:'Report',state:'accepted',attempt_count:1,criteria:[{id:'sources',label:'Sources',name:'sources.json'}]}];
  const runs=[{id:'run',task_id:'report',state:'submitted',attempt:1}];
  const acceptance={id:'row-1',run_id:'run',passed:true,checks:[{id:'sources',passed:true}],created_at:'2026-10-06'};
  const artifacts=[{run_id:'run',name:'sources.json',digest:'actual-hash'}];
  const first=projectWorkFrontier(boundary,objectiveFrontierNodes(tasks,runs,[acceptance],[],artifacts));
  const next=projectWorkFrontier(boundary,objectiveFrontierNodes(tasks,runs,[{...acceptance,id:'row-2'}],[],artifacts));
  expect(next.deliveryFingerprint).toBe(first.deliveryFingerprint);
});
it('does not carry an acceptance into changed criteria or an added obligation',()=>{
  const criteria=[{id:'sources',label:'Sources',name:'sources.json'}];
  const task={id:'report',title:'Report',state:'accepted',attempt_count:1,criteria};
  const run={id:'run',task_id:'report',state:'submitted',attempt:1,accepted_criteria:criteria};
  const acceptance={run_id:'run',passed:true,checks:[{id:'sources',passed:true}],created_at:'2026-10-06'};
  const changed={...task,criteria:[{...criteria[0],name:'new-sources.json'}]};
  expect(objectiveFrontierNodes([changed],[run],[acceptance],[])[0].accepted).toBe(false);
  const added={...task,criteria:[...criteria,{id:'review',label:'Review',name:'review.json'}]};
  expect(objectiveFrontierNodes([added],[{...run,accepted_criteria:undefined}],[acceptance],[])[0].accepted).toBe(false);
});
it('bounds the manager projection while preserving counts and explicit recovery of omitted work',()=>{
  const nodes=Array.from({length:1000},(_,i)=>node('task-'+i,{title:'X'.repeat(10000),
    reason:'Y'.repeat(10000),criteria:[{id:'sources',label:'Z'.repeat(10000)}]}));
  const full=projectWorkFrontier(boundary,nodes),compact=compactWorkFrontier(full);
  expect(JSON.stringify(compact).length).toBeLessThanOrEqual(6000);
  expect(compact.counts).toEqual(full.counts);
  expect(compact.selection.omittedEntries).toBeGreaterThan(0);
  expect(compact.selection.recovery).toMatchObject({taskEntity:'tasks',criterionField:'criteria'});
  expect(JSON.stringify(compact)).not.toContain('X'.repeat(100));
});

beforeAll(async()=>{await migrate();await installTaskExecutionMetadata();await installRunCriteriaMetadata();await seedRegistry();});
afterAll(async()=>{await pool.end();});
it('exposes a real non-code document acceptance through the API model and database frontier',async()=>{
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Research report','Long research work','frontier-research',$2)",[goal,'research-'+goal]);
  await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count) VALUES($1,$2,'report','Report','Verified citations','research.web','running',$3,1)",
    [task,goal,JSON.stringify([{id:'sources',label:'Sources preserved',type:'artifact',name:'sources.json'}])]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime) VALUES($1,$2,1,$3,1,'running','harness')",[run,task,fence]);
  await writeArtifact(run,fence,'sources.json',JSON.stringify({sources:['https://example.org/document']}));
  await completeRun(run,fence,'Research fixture delivery',{});
  await validateTask(task);
  const db=await readObjectiveFrontier(pool,goal),api=(await goalDetail(goal)).frontier;
  expect(db.complete).toBe(true);
  expect(api.counts).toEqual(db.counts);
  expect(api.deliveryFingerprint).toBe(db.deliveryFingerprint);
  // Current accepted criteria remain authoritative after limiting the expensive
  // frozen-snapshot projection to actual acceptance candidates.
  await pool.query("UPDATE tasks SET criteria=jsonb_set(criteria,'{0,name}','\"changed-sources.json\"') WHERE id=$1",[task]);
  const changed=await readObjectiveFrontier(pool,goal);
  expect(changed.complete).toBe(false);
  expect(changed.counts.accepted).toBe(0);
});
it('creates two recurring database occurrences with separate work and evidence',async()=>{
  const id=randomUUID();
  const definition={name:'Daily operations report',project:'frontier-ops',trigger:{kind:'manual'},steps:[
    {key:'report',title:'Operations report',kind:'agent',instructions:'Preserve sources',workspace:'frontier-ops/source',dependsOn:[],criteria:[{id:'report',label:'Report evidence',type:'artifact',name:'report.json'}]}]};
  await pool.query("INSERT INTO cycles(id,name,project,definition,request_key) VALUES($1,'Daily operations report','frontier-ops',$2,$3)",[id,JSON.stringify(definition),'recurring-'+id]);
  const cycle=(await pool.query('SELECT * FROM cycles WHERE id=$1',[id])).rows[0];
  const first=await createOccurrence(pool,cycle,'day-1'),second=await createOccurrence(pool,cycle,'day-2');
  const one=await occurrenceDetail(first.id),two=await occurrenceDetail(second.id);
  expect(one.frontier.boundary.id).not.toBe(two.frontier.boundary.id);
  expect(one.frontier.entries[0].taskId).not.toBe(two.frontier.entries[0].taskId);
  expect(one.frontier.complete).toBe(false);
  expect(two.frontier.counts).toMatchObject({ready:1,accepted:0,total:1});
});

it('preserves the current-or-uncertain frontier and the exact frozen acceptance contract',async()=>{
  const goal=randomUUID(),accepted=randomUUID(),uncertain=randomUUID(),coordinator=randomUUID();
  const criteria=[{id:'evidence',label:'Exact original criterion',type:'artifact',name:'evidence.json'}];
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Frozen frontier','Frozen frontier','frontier-frozen',$1::text)`,[goal]);
  for(const [id,state,stage] of [[accepted,'accepted','work'],[uncertain,'ready','work'],[coordinator,'accepted','coordinate']])
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
      VALUES($1::uuid,$2,$1::text,'Frozen contract','Original description','research.web',$3,$4,$5,2)`,
    [id,goal,state,stage,JSON.stringify(criteria)]);
  const current=randomUUID(),olderLive=randomUUID();
  for(const [id,task,attempt,state] of [[randomUUID(),accepted,1,'submitted'],[current,accepted,2,'submitted'],
    [olderLive,uncertain,1,'reconciling'],[randomUUID(),uncertain,2,'failed'],[randomUUID(),coordinator,2,'submitted']])
    await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,agent_version,runtime)
      VALUES($1,$2,$3,$4,$5,1,'harness')`,[id,task,attempt,state,randomUUID()]);
  const acceptance={id:randomUUID(),task_id:accepted,run_id:current,passed:true,
    checks:[{id:'evidence',passed:true}],created_at:new Date().toISOString()};
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,true,$4,'frozen-frontier-fixture',$5)`,
  [acceptance.id,accepted,current,JSON.stringify(acceptance.checks),acceptance.created_at]);
  const legacy=(await pool.query(`SELECT r.id,r.task_id,r.attempt,r.state,
    CASE WHEN t.state='accepted' AND r.state='submitted' AND r.attempt=t.attempt_count
      THEN r.instruction_snapshot->'task'->'criteria' END accepted_criteria
    FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 AND t.stage IS DISTINCT FROM 'coordinate'
    AND (r.attempt=t.attempt_count OR r.state IN('queued','running','cancelling','reconciling'))`,[goal])).rows;
  expect(legacy).toHaveLength(3);
  expect(legacy.find(r=>r.id===olderLive)?.accepted_criteria).toBeNull();
  expect(legacy.find(r=>r.id===current)?.accepted_criteria).toEqual(criteria);
  const tasks=await readObjectiveFrontierTasks(pool,goal);
  const expected=projectWorkFrontier({kind:'objective',id:goal},objectiveFrontierNodes(tasks,legacy,[acceptance],[]));
  const actual=await readObjectiveFrontier(pool,goal);
  expect(actual).toEqual(expected);
  expect(actual.counts.accepted).toBe(1);
  expect(actual.entries.find(e=>e.taskId===uncertain)?.state).toBe('executing');
  await pool.query(`UPDATE tasks SET criteria=jsonb_set(criteria,'{0,name}','"different.json"') WHERE id=$1`,[accepted]);
  expect((await readObjectiveFrontier(pool,goal)).counts.accepted).toBe(0);
  expect((await pool.query('SELECT criteria FROM run_criteria_metadata WHERE run_id=$1',[current])).rows[0].criteria).toEqual(criteria);
});

it('refuses a missing frozen projection instead of treating it as a legacy null criterion',async()=>{
  const goal=randomUUID(),task=randomUUID(),run=randomUUID();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1::uuid,'Missing criteria','Missing criteria','frontier-missing',$1::text)`,[goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count)
    VALUES($1::uuid,$2,$1::text,'Missing criteria','Original contract','research.web','accepted','[]',1)`,[task,goal]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,agent_version,runtime)
    VALUES($1,$2,1,'submitted',$3,1,'harness')`,[run,task,randomUUID()]);
  await pool.query('DELETE FROM run_criteria_metadata WHERE run_id=$1',[run]);
  try { await expect(readObjectiveFrontier(pool,goal)).rejects.toThrow('RUN_CRITERIA_METADATA_INCOMPLETE'); }
  finally { await installRunCriteriaMetadata(); }
});
