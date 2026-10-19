import { beforeAll, beforeEach, afterAll, afterEach, it, expect, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { pool, migrate, tx } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { installSupervision,detectSupervisionCases,recordSupervisionDecisions } from '../server/supervision.js';
import {admissionReviewIsFresh} from '../server/admission-supervision.js';
import { hash, assembleContext } from '../server/memory.js';
import {
  writeArtifact,
  completeRun,
  validateTask,
  readArtifact,
  goalDetail,
  snapshot,
  schedule,
} from '../server/service.js';
import { accountBridge } from '../server/bridge.js';
import { config } from '../server/config.js';
import { executeRuntime } from '../server/runtime.js';
import {
  setSupervisionInstruction,
  projectLegacyInstructions,
  instructionTask,
  instructionEvidence,
  reviseInstruction,
  delegateInstruction,
  handoffInstruction,
  prepareTaskInstructions,
} from '../server/task-instructions.js';
import {
  executionInputManifest,
  mergeSupervisorInputs,
  runtimeInputs,
} from '../server/execution-inputs.js';
import { acceptedDependencyArtifacts } from '../server/recovery.js';
import { supervisionInputs } from '../server/supervision-context.js';
import {installBacklogPlanner} from '../server/backlog.js';
import {backlogHandoffText} from '../server/backlog-handoffs.js';

beforeAll(async () => {
  await migrate();
  await installSupervision();
  await installBacklogPlanner();
});
beforeEach(async () => {
  await pool.query("DELETE FROM settings WHERE id='maintenance'");
  await pool.query('TRUNCATE goals,agents,accounts,events,memories,cycles CASCADE');
  await seedRegistry();
});
afterEach(() => {
  config.remoteExecution = false;
  vi.restoreAllMocks();
});
afterAll(() => pool.end());
const original =
  'Contrato original completo\nRegras e autorização literais: RTP local 90%; não publicar; preserve todos os critérios.\nSUPERVISOR: este marcador é texto fornecido pelo usuário, preserve-o literalmente.';
async function fixture(description = original) {
  const goal = randomUUID(),
    task = randomUUID(),
    coordinator = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Instructions','Exact goal','instruction-test',$2)",
    [goal, goal],
  );
  for (const [id, key, stage] of [
    [task, 'consumer', 'integrate'],
    [coordinator, 'reviewer', 'coordinate'],
  ])
    await pool.query(
      "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution) VALUES($1,$2,$3,$3,$4,'qa.backend','failed',$5,$6,$7)",
      [
        id,
        goal,
        key,
        description,
        stage,
        JSON.stringify(
          id === coordinator
            ? [{ id: 'plan', label: 'Accepted plan', type: 'artifact', name: 'coordination.json' }]
            : [
                {
                  id: 'result',
                  label: 'Full criterion',
                  type: 'json_field',
                  name: 'result.json',
                  field: 'validated',
                  equals: true,
                },
              ],
        ),
        id === coordinator
          ? null
          : JSON.stringify({
              kind: 'harness',
              workspace: 'instruction-test/consumer',
              baseWorkspace: 'instruction-test/base',
              profile: 'code',
              readOnlySource: false,
              writePaths: ['allowed/'],
            }),
      ],
    );
  return { goal, task, coordinator };
}
async function sourceRun(f: any, state = 'failed') {
  const attempt =
      (
        await pool.query('SELECT COALESCE(max(attempt),0)::int n FROM runs WHERE task_id=$1', [
          f.task,
        ])
      ).rows[0].n + 1,
    id = randomUUID(),
    fence = randomUUID();
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,$3,$4,'harness',1,$5)",
    [id, f.task, attempt, fence, state],
  );
  await pool.query('UPDATE tasks SET attempt_count=$2 WHERE id=$1', [f.task, attempt]);
  return (await pool.query('SELECT * FROM runs WHERE id=$1', [id])).rows[0];
}
async function reviewedCase(f: any, label = 'Current', kind = 'revise', backlog?:any) {
  const source = await sourceRun(f),
    id = randomUUID(),
    review = randomUUID(),
    fence = randomUUID();
  const attempt =
    (
      await pool.query('SELECT COALESCE(max(attempt),0)::int n FROM runs WHERE task_id=$1', [
        f.coordinator,
      ])
    ).rows[0].n + 1;
  const action = {
    caseId: id,
    kind,
    diagnosis: label + ' diagnosis, preserved literally.',
    instruction:
      label +
      ' instruction: execute this discriminating test and preserve original scope and requirements.',
  };
  const actions = kind === 'delegate' ? [action, { ...action, kind: 'revise' }] : [action];
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,$3,$4,'harness',1,'running')",
    [review, f.coordinator, attempt, fence],
  );
  await pool.query("UPDATE tasks SET state='running',attempt_count=$2 WHERE id=$1", [
    f.coordinator,
    attempt,
  ]);
  const artifact = await writeArtifact(
    review,
    fence,
    'coordination.json',
    JSON.stringify({ supervision: { actions },...(backlog?{backlog}:{}) }),
    'application/json',
  );
  await completeRun(review, fence, 'Accepted scoped review', {});
  await validateTask(f.coordinator);
  await pool.query(
    "INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,decision_run_id,applied_attempt) VALUES($1,$2,$3,$4,$5,'Review','[]',$6,$7,$8)",
    [
      id,
      f.goal,
      f.task,
      source.id,
      kind === 'delegate' ? 'delegated' : 'applied',
      JSON.stringify({ actions }),
      review,
      source.attempt,
    ],
  );
  return { id, source, action, actions, review, artifact };
}
const taskRow = async (f: any) =>
  (await pool.query('SELECT * FROM tasks WHERE id=$1', [f.task])).rows[0];
async function legacyBacklogFixture(includeModern=false){
 const f=await fixture(),child=randomUUID();
 const proposal={coverageKey:'handoff-regression',deliverToTaskId:f.task,lane:'muse',workKind:'diagnostic'} as const;
 const modernProposal={...proposal,coverageKey:'modern-handoff'};
 const c=await reviewedCase(f,'With backlog','revise',{reason:'Required complementary evidence',tasks:[proposal,...(includeModern?[modernProposal]:[])]});
 const revision=await apply(f,c);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
 VALUES($1,$2,'child','Complementary evidence','Execute complementary evidence','qa.backend','accepted','[]',
 '{"kind":"harness","workforceLane":"muse","workKind":"diagnostic"}')`,[child,f.goal]);
 await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[f.task,child]);
 await pool.query('INSERT INTO workforce_backlog_items VALUES($1,$2,$3)',[f.goal,proposal.coverageKey,child]);
 const handoff={taskId:child,plannerRunId:c.review,...proposal};
 delete (handoff as any).deliverToTaskId;
 await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('BACKLOG_TASK_CREATED',$1,$2)",
 [f.goal,JSON.stringify({taskId:child,sourceRunId:c.review,consumerTaskId:f.task,coverageKey:proposal.coverageKey,lane:'muse'})]);
 const description=revision.rendered+backlogHandoffText(handoff);
 await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[f.task,description]);
 let modern:any;
 if(includeModern){
  const modernId=randomUUID();modern={taskId:modernId,plannerRunId:c.review,coverageKey:modernProposal.coverageKey,lane:'muse',workKind:'diagnostic'};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
   VALUES($1,$2,'modern-child','Modern complementary evidence','Modern evidence','qa.backend','accepted','[]',
   '{"kind":"harness","workforceLane":"muse","workKind":"diagnostic"}')`,[modernId,f.goal]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.task,modernId]);
  await pool.query('INSERT INTO workforce_backlog_items VALUES($1,$2,$3)',[f.goal,modernProposal.coverageKey,modernId]);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('BACKLOG_TASK_CREATED',$1,$2)",
   [f.goal,JSON.stringify({taskId:modernId,sourceRunId:c.review,consumerTaskId:f.task,coverageKey:modernProposal.coverageKey,lane:'muse'})]);
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{backlogHandoffs}',$2::jsonb) WHERE id=$1",[f.task,JSON.stringify([modern])]);
 }
 return {f,c,child,revision,handoff,description,modern};
}
async function apply(
  f: any,
  c: any,
  text = reviseInstruction(c.action),
  kind: 'revise' | 'delegate' | 'delivery' = 'revise',
) {
  return tx((db) => setSupervisionInstruction(db, f.task, c.id, kind, text, c.source.id));
}

async function admissionFixture(){
 const f=await fixture(),old=await reviewedCase(f),revision=await apply(f,old);
 const c=await reviewedCase(f,'Forwarded'),latest=c.source,caseId=c.id;
 await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[latest.id]);
 await pool.query("UPDATE supervision_cases SET state='forwarded',reason='Old forwarded QA' WHERE id=$1",[caseId]);
 await pool.query('INSERT INTO settings VALUES($1,$2)', ['supervision:'+f.goal,JSON.stringify({enabled:true})]);
 await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
 await pool.query("UPDATE tasks SET state='recovery_blocked',execution=jsonb_set(execution,'{continuation}','{\"state\":\"needs_action\"}'::jsonb) WHERE id=$1",[f.task]);
 const blocked=(await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('RUN_ADMISSION_BLOCKED',$1,$2) RETURNING id,created_at",
  [f.task,JSON.stringify({code:'INSTRUCTION_CASE_CHANGED',goalId:f.goal,instructionRevisionId:revision.id,preserved:true})])).rows[0];
 return {f,c,latest,caseId,revision,blocked};
}

it('reopens a closed case after rejected admission without another failed run, preserves history and rejects stale decisions',async()=>{
 const x=await admissionFixture(),before=await taskRow(x.f);
 // Both timestamps appear identical to JavaScript, but PostgreSQL must still
 // reject a review that preceded the admission event by a fraction of a ms.
 await pool.query("UPDATE runs SET created_at=date_trunc('milliseconds',created_at) WHERE id=$1",[x.c.review]);
 await pool.query("UPDATE events SET created_at=(SELECT created_at+interval '0.1 milliseconds' FROM runs WHERE id=$2) WHERE id=$1",[x.blocked.id,x.c.review]);
 const runs=(await pool.query('SELECT id,instruction_snapshot FROM runs WHERE task_id=$1 ORDER BY attempt',[x.f.task])).rows;
 expect(await detectSupervisionCases()).toContain(x.caseId);
 let reopened=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[x.caseId])).rows[0];
 expect(reopened.state).toBe('open');
 expect(reopened.evidence).toContainEqual(expect.objectContaining({admissionEventId:String(x.blocked.id),code:'INSTRUCTION_CASE_CHANGED'}));
 const preserved=(await pool.query("SELECT payload FROM events WHERE kind='ADMISSION_SUPERVISION_REQUESTED' AND entity_id=$1",[x.f.task])).rows[0].payload;
 expect(preserved.previousCase.state).toBe('forwarded');
 expect(preserved.previousCase.decisionRunId).toBe(x.c.review);
 await recordSupervisionDecisions();
 reopened=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[x.caseId])).rows[0];
 expect(reopened.state).toBe('open');
 expect(await detectSupervisionCases()).toEqual([]);
 const after=await taskRow(x.f);
 expect([after.description,after.criteria,after.instruction_revision_id,after.attempt_count])
  .toEqual([before.description,before.criteria,before.instruction_revision_id,before.attempt_count]);
 expect((await pool.query('SELECT id,instruction_snapshot FROM runs WHERE task_id=$1 ORDER BY attempt',[x.f.task])).rows).toEqual(runs);
 await expect(sourceRun(x.f)).rejects.toThrow('INSTRUCTION_CASE_CHANGED');
});

it('does not reopen admission cases for a live or cancelled execution, disabled autonomy, or a different revision',async()=>{
 for(const block of ['live','cancelled','disabled','different_revision']){
  const x=await admissionFixture();
  if(block==='live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[x.latest.id]);
  if(block==='cancelled')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[x.f.task,JSON.stringify({runId:x.latest.id})]);
  if(block==='disabled')await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',['autonomy:instruction-test',JSON.stringify({enabled:false})]);
  if(block==='different_revision')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{instructionRevisionId}',to_jsonb($2::text)) WHERE id=$1",[x.blocked.id,randomUUID()]);
  await detectSupervisionCases();
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[x.caseId])).rows[0].state).toBe('forwarded');
  expect((await pool.query("SELECT 1 FROM events WHERE kind='ADMISSION_SUPERVISION_REQUESTED' AND entity_id=$1",[x.f.task])).rowCount).toBe(0);
  await pool.query('DELETE FROM settings WHERE id=$1',['autonomy:instruction-test']);
 }
});

it('retains admission supervision when resource or dependency reconciliation changes the task state',async()=>{
 for(const state of ['blocked_dependencies','waiting_resource']){
  const x=await admissionFixture();
  await pool.query('UPDATE tasks SET state=$2 WHERE id=$1',[x.f.task,state]);
  expect(await detectSupervisionCases()).toContain(x.caseId);
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[x.caseId])).rows[0].state).toBe('open');
  expect((await taskRow(x.f)).state).toBe('recovery_blocked');
  expect((await taskRow(x.f)).instruction_revision_id).toBe(x.revision.id);
 }
});

it('reconciles a proven legacy suffix alongside modern typed handoffs without altering either contract',async()=>{
 const x=await legacyBacklogFixture(true),before=await taskRow(x.f);
 await tx(db=>prepareTaskInstructions(db,x.f.task));
 const after=await taskRow(x.f);
 expect(after.description).toBe(x.revision.rendered);
 expect(after.execution.backlogHandoffs).toEqual([x.modern,x.handoff]);
 expect(after.criteria).toEqual(before.criteria);
 expect(after.instruction_revision_id).toBe(x.revision.id);
 expect((await pool.query("SELECT content FROM contexts WHERE manifest->>'kind'='legacy_backlog_description' AND manifest->>'taskId'=$1",[x.f.task])).rows[0].content).toBe(x.description);
 const run=await sourceRun(x.f);
 expect(run.instruction_snapshot.task.execution.backlogHandoffs).toEqual([x.modern,x.handoff]);
});

it('requires a fresh review for every admission event and reconciles only proven legacy handoffs before a new instruction',async()=>{
 expect(admissionReviewIsFresh([{kind:'RUN_ADMISSION_BLOCKED',at:'2026-10-07T00:10:00Z'}],'2026-10-07T00:09:59Z')).toBe(false);
 expect(admissionReviewIsFresh([{kind:'RUN_ADMISSION_BLOCKED',at:'invalid'}],new Date())).toBe(false);
 expect(admissionReviewIsFresh([{kind:'RUN_ADMISSION_BLOCKED',at:'2026-10-07T00:10:00Z'}],'2026-10-07T00:10:01Z')).toBe(true);
 const x=await legacyBacklogFixture();
 const latest=(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[x.f.task])).rows[0];
 const id=x.c.id,review=randomUUID(),fence=randomUUID(),action={...x.c.action,caseId:id,instruction:'Fresh instruction, preserving original contract'};
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'running')",[review,x.f.coordinator,fence]);
 await pool.query("UPDATE tasks SET state='running',attempt_count=2 WHERE id=$1",[x.f.coordinator]);
 await writeArtifact(review,fence,'coordination.json',JSON.stringify({supervision:{actions:[action]}}),'application/json');
 await completeRun(review,fence,'Fresh review',{});await validateTask(x.f.coordinator);
 await pool.query("UPDATE supervision_cases SET decision=$2,decision_run_id=$3,applied_attempt=$4 WHERE id=$1",
  [id,JSON.stringify({actions:[action]}),review,latest.attempt]);
 // Reuse the same stopped source in the real application; the historical
 // closed case keeps its unique source_run_id and immutable revision.
 const next=await tx(db=>setSupervisionInstruction(db,x.f.task,id,'revise',reviseInstruction(action),latest.id));
 expect(next.predecessor).toBe(x.revision.id);
 expect((await taskRow(x.f)).execution.backlogHandoffs).toEqual([x.handoff]);
 expect((await pool.query('SELECT rendered FROM task_instruction_revisions WHERE id=$1',[x.revision.id])).rows[0].rendered).toBe(x.revision.rendered);
});

it('repairs only a proven controller suffix and freezes complementary delivery context without changing the original contract',async()=>{
 const {f,c,revision,handoff,description}=await legacyBacklogFixture();
 const originalRun=(await pool.query('SELECT * FROM runs WHERE id=$1',[c.source.id])).rows[0];
 await tx(db=>prepareTaskInstructions(db,f.task));
 const t=await taskRow(f);
 expect(t.description).toBe(revision.rendered);
 expect(t.instruction_revision_id).toBe(revision.id);
 expect(t.execution.backlogHandoffs).toEqual([handoff]);
 expect((await pool.query('SELECT * FROM task_instruction_revisions WHERE id=$1',[revision.id])).rows[0]).toEqual(revision);
 expect((await pool.query('SELECT * FROM runs WHERE id=$1',[c.source.id])).rows[0]).toEqual(originalRun);
 const archived=(await pool.query("SELECT content FROM contexts WHERE run_id=$1 AND manifest->>'kind'='legacy_backlog_description'",[c.source.id])).rows[0];
 expect(archived.content).toBe(description);
 const run=await sourceRun(f);
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{backlogHandoffs}','[]') WHERE id=$1",[f.task]);
 const context=await assembleContext(run,await taskRow(f),[]);
 expect(context.content).toContain('BACKLOG_DELIVERY_CONTEXT_V1');
 expect(context.content).toContain('handoff-regression');
 expect((context.manifest as any).backlogHandoffs.references).toEqual([handoff]);
 expect((context.manifest as any).taskInstructions.descriptionHash).toBe(hash(revision.rendered));
});
it.each(['unknown-suffix','unaccepted-planner','live-attempt'])('preserves a legacy mismatch when provenance is %s',async(reason)=>{
 const {f,c,revision,description}=await legacyBacklogFixture();
 if(reason==='unknown-suffix')await pool.query("UPDATE tasks SET description=description||' Additional unknown instruction' WHERE id=$1",[f.task]);
 if(reason==='unaccepted-planner')await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[c.review]);
 if(reason==='live-attempt'){
  await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[f.task,revision.rendered]);
  await sourceRun(f,'running');
  await pool.query('UPDATE tasks SET description=$2 WHERE id=$1',[f.task,description]);
 }
 const before=await taskRow(f);
 await tx(db=>prepareTaskInstructions(db,f.task));
 expect(await taskRow(f)).toEqual(before);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='BACKLOG_INSTRUCTIONS_RECONCILED'")).rows[0].n).toBe(0);
});
it('isolates an unverified task admission so healthy work is queued while the integrity guard remains enforced',async()=>{
 const f=await fixture(),c=await reviewedCase(f),revision=await apply(f,c);
 config.bridgeHost='test-bridge';config.remoteExecution=true;
 try{
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,bridge_id,enabled,allow_unknown,max_concurrency)
   VALUES('guard-test','Codex fixture','codex','gpt-6.1-sol','["gpt-6.1-sol"]','fixture',true,true,4)`);
  await pool.query("UPDATE tasks SET state='ready',account_id='guard-test',model='gpt-6.1-sol',priority=CASE WHEN id=$1 THEN 99 ELSE 1 END WHERE goal_id=$2",[f.task,f.goal]);
  await pool.query("UPDATE tasks SET description=description||' Unverified change' WHERE id=$1",[f.task]);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
  const queued=await schedule();
  expect(queued).toHaveLength(1);
  expect((await pool.query('SELECT task_id FROM runs WHERE id=$1',[queued[0]])).rows[0].task_id).toBe(f.coordinator);
  expect((await taskRow(f)).state).toBe('recovery_blocked');
  expect((await taskRow(f)).instruction_revision_id).toBe(revision.id);
  expect((await pool.query("SELECT payload->>'code' code FROM events WHERE kind='RUN_ADMISSION_BLOCKED' AND entity_id=$1",[f.task])).rows[0].code).toBe('TASK_INSTRUCTIONS_CHANGED');
  await expect(sourceRun(f)).rejects.toThrow('TASK_INSTRUCTIONS_CHANGED');
 }finally{config.bridgeHost='';config.remoteExecution=false}
});

it('preserves exact original requirements, authorization, criteria and literal user markers with hashes', async () => {
  const f = await fixture(),
    before = await taskRow(f),
    c = await reviewedCase(f);
  const revision = await apply(f, c);
  const after = await taskRow(f),
    origin = (await pool.query('SELECT * FROM task_instruction_origins WHERE task_id=$1', [f.task]))
      .rows[0];
  expect(origin.baseline).toBe(original);
  expect(origin.original).toBe(original);
  expect(origin.original_hash).toBe(hash(original));
  expect(origin.baseline_hash).toBe(hash(original));
  expect(after.description.startsWith(original)).toBe(true);
  expect(after.criteria).toEqual(before.criteria);
  expect(after.execution).toEqual(before.execution);
  expect(revision.instruction_hash).toBe(hash(reviseInstruction(c.action)));
  expect(revision.provenance).toMatchObject({
    caseId: c.id,
    artifactId: c.artifact.id,
    decisionRunId: c.review,
  });
});
it('keeps only the latest case directive active over twelve revisions without growing the original contract', async () => {
  const f = await fixture();
  let last: any;
  for (let i = 0; i < 12; i++) {
    const c = await reviewedCase(f, 'Hypothesis ' + i);
    last = await apply(f, c);
  }
  const t = await taskRow(f);
  expect(t.description).toContain('Hypothesis 11 instruction');
  expect(t.description).not.toContain('Hypothesis 0 instruction');
  expect(t.description.length).toBeLessThan(original.length + 500);
  expect(
    (
      await pool.query('SELECT count(*)::int n FROM task_instruction_revisions WHERE task_id=$1', [
        f.task,
      ])
    ).rows[0].n,
  ).toBe(12);
  const run = await sourceRun(f),
    evidence = await tx((db) => instructionEvidence(db, run, instructionTask(run, t)));
  const recovered = await readArtifact(evidence.artifact!.id),
    archive = JSON.parse(recovered.bytes.toString());
  expect(recovered.digest).toBe(hash(recovered.bytes));
  expect(archive.baseline).toBe(original);
  expect(archive.revisions).toHaveLength(12);
  expect(archive.revisions[0].instruction).toContain('Hypothesis 0 instruction');
  expect(evidence.manifest.revisionId).toBe(last.id);
});
it('versions delegate then accepted delivery while retaining that case hypothesis and replacing superseded cases', async () => {
  const f = await fixture(),
    old = await reviewedCase(f, 'Superseded');
  await apply(f, old);
  const c = await reviewedCase(f, 'Current repair', 'delegate'),
    child = randomUUID();
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria) VALUES($1,$2,'bound-child','Repair','Authorized repair','qa.backend','accepted','[]')",
    [child, f.goal],
  );
  await pool.query(
    'UPDATE supervision_cases SET support_task_id=$2,support_task_ids=$3 WHERE id=$1',
    [c.id, child, JSON.stringify([child])],
  );
  const directive = delegateInstruction(['bound-child'], c.actions);
  await apply(f, c, directive, 'delegate');
  await pool.query("UPDATE supervision_cases SET state='applied' WHERE id=$1", [c.id]);
  const current = await apply(f, c, directive + '\n' + handoffInstruction, 'delivery');
  expect(current.number).toBe(3);
  expect((await taskRow(f)).description).toContain('Current repair instruction');
  expect((await taskRow(f)).description).not.toContain('Superseded instruction');
  const run = await sourceRun(f),
    evidence = await tx((db) =>
      instructionEvidence(db, run, instructionTask(run, awaitedTaskPlaceholderWithGoal(f))),
    );
  const archive = JSON.parse((await readArtifact(evidence.artifact!.id)).bytes.toString());
  expect(archive.revisions.map((r: any) => r.provenance.kind)).toEqual([
    'revise',
    'delegate',
    'delivery',
  ]);
});
it('freezes description, criteria, scope and case revision at admission even if mutable task fields subsequently change', async () => {
  const f = await fixture(),
    c = await reviewedCase(f);
  await apply(f, c);
  const run = await sourceRun(f),
    frozen = instructionTask(run, await taskRow(f));
  await pool.query(
    'UPDATE tasks SET description=\'Different task\',criteria=\'[]\',execution=\'{"kind":"harness","workspace":"other/unsafe","readOnlySource":false}\' WHERE id=$1',
    [f.task],
  );
  const t = instructionTask(run, await taskRow(f));
  expect(t.description).toBe(frozen.description);
  expect(t.criteria).toEqual(frozen.criteria);
  expect(t.execution).toEqual(frozen.execution);
  const evidence = await tx((db) => instructionEvidence(db, run, t));
  expect(evidence.manifest.descriptionHash).toBe(hash(frozen.description));
  await expect(sourceRun(f)).rejects.toThrow('TASK_INSTRUCTIONS_CHANGED');
});
it('never backfills or derives a new contract for an existing pre-feature run', async () => {
  const f = await fixture();
  await pool.query('ALTER TABLE runs DISABLE TRIGGER agentos_instruction_snapshot');
  let run: any;
  try {
    run = await sourceRun(f);
  } finally {
    await pool.query('ALTER TABLE runs ENABLE TRIGGER agentos_instruction_snapshot');
  }
  const before = (await pool.query('SELECT * FROM runs WHERE id=$1', [run.id])).rows[0];
  expect(() => instructionTask(run, awaitedTaskPlaceholder())).toThrow('Contrato');
  await expect(assembleContext(run, await taskRow(f), [])).rejects.toMatchObject({
    code: 'INSTRUCTION_SNAPSHOT_MISSING',
  });
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [run.id])).rows[0]).toEqual(before);
  expect(
    (await pool.query('SELECT count(*)::int n FROM contexts WHERE run_id=$1', [run.id])).rows[0].n,
  ).toBe(0);
});
function awaitedTaskPlaceholder() {
  return {};
}
it('rejects snapshot and immutable history mutation', async () => {
  const f = await fixture(),
    c = await reviewedCase(f),
    revision = await apply(f, c),
    run = await sourceRun(f);
  await expect(
    pool.query("UPDATE runs SET instruction_snapshot='{}' WHERE id=$1", [run.id]),
  ).rejects.toThrow('INSTRUCTION_SNAPSHOT_IMMUTABLE');
  await expect(
    pool.query("UPDATE task_instruction_revisions SET instruction='Changed' WHERE id=$1", [
      revision.id,
    ]),
  ).rejects.toThrow('INSTRUCTION_HISTORY_IMMUTABLE');
  await expect(
    pool.query('DELETE FROM task_instruction_origins WHERE task_id=$1', [f.task]),
  ).rejects.toThrow('INSTRUCTION_HISTORY_IMMUTABLE');
});
it('projects legacy only by exact ordered replay of accepted case/event appenders and keeps false markers intact', async () => {
  const f = await fixture();
  let text = original;
  for (let i = 0; i < 3; i++) {
    const c = await reviewedCase(f, 'Historic ' + i);
    text += reviseInstruction(c.action);
    await pool.query('UPDATE tasks SET description=$2 WHERE id=$1', [f.task, text]);
    await pool.query(
      "INSERT INTO events(kind,entity_id,payload) VALUES('SUPERVISION_APPLIED',$1,$2)",
      [
        f.task,
        JSON.stringify({
          caseId: c.id,
          sourceRunId: c.source.id,
          decisionRunId: c.review,
          stopConfirmed: true,
        }),
      ],
    );
  }
  const projected = await projectLegacyInstructions(pool, await taskRow(f));
  expect(projected.original).toBe(original);
  expect(projected.proof.removedCount).toBe(3);
  expect(projected.proof.verified).toBe(true);
  const baseline = text,
    c = await reviewedCase(f, 'New authoritative');
  await apply(f, c);
  const origin = (
    await pool.query('SELECT * FROM task_instruction_origins WHERE task_id=$1', [f.task])
  ).rows[0];
  expect(origin.baseline).toBe(baseline);
  expect(origin.baseline_hash).toBe(hash(baseline));
  expect(origin.original).toBe(original);
  expect((await taskRow(f)).description).not.toContain('Historic 0 instruction');
});
it.each(['missing-event', 'text-mismatch', 'unaccepted-decision'])(
  'keeps legacy integral and reports a fail-closed reason for %s',
  async (mode) => {
    const f = await fixture(),
      c = await reviewedCase(f, 'Legacy');
    const full =
      original +
      reviseInstruction(c.action) +
      (mode === 'text-mismatch' ? '\nUser suffix must remain' : '');
    await pool.query('UPDATE tasks SET description=$2 WHERE id=$1', [f.task, full]);
    if (mode !== 'missing-event')
      await pool.query(
        "INSERT INTO events(kind,entity_id,payload) VALUES('SUPERVISION_APPLIED',$1,$2)",
        [
          f.task,
          JSON.stringify({ caseId: c.id, sourceRunId: c.source.id, decisionRunId: c.review }),
        ],
      );
    if (mode === 'unaccepted-decision')
      await pool.query('DELETE FROM acceptances WHERE run_id=$1', [c.review]);
    const projected = await projectLegacyInstructions(pool, await taskRow(f));
    expect(projected.original).toBe(full);
    expect(projected.proof.removedCount).toBe(0);
    if (mode !== 'missing-event') expect(projected.proof.verified).toBe(false);
    if (mode === 'missing-event') expect(projected.proof.operationCount).toBe(0);
    const next = await reviewedCase(f, 'Current');
    await apply(f, next);
    const run = await sourceRun(f);
    const evidence = await tx((db) =>
      instructionEvidence(db, run, instructionTask(run, awaitedTaskPlaceholderWithGoal(f))),
    );
    expect(evidence.manifest.originalSource).toBe('preserved_baseline');
    expect(evidence.manifest.legacy.removedCount).toBe(0);
  },
);
function awaitedTaskPlaceholderWithGoal(f: any) {
  return { goal_id: f.goal };
}
it.each([
  'foreign-goal',
  'wrong-source',
  'live-run',
  'missing-artifact',
  'corrupt-artifact',
  'open-case',
])(
  'rejects unsafe revision scope/provenance for %s without changing the contract',
  async (mode) => {
    const f = await fixture(),
      c = await reviewedCase(f),
      before = await taskRow(f);
    if (mode === 'foreign-goal')
      await pool.query('UPDATE supervision_cases SET task_id=$2 WHERE id=$1', [
        c.id,
        f.coordinator,
      ]);
    if (mode === 'wrong-source') c.source.id = randomUUID();
    if (mode === 'live-run')
      await pool.query("UPDATE runs SET state='running' WHERE id=$1", [c.source.id]);
    if (mode === 'missing-artifact')
      await pool.query('DELETE FROM artifacts WHERE id=$1', [c.artifact.id]);
    if (mode === 'corrupt-artifact')
      await pool.query("UPDATE artifacts SET digest=repeat('0',64) WHERE id=$1", [c.artifact.id]);
    if (mode === 'open-case')
      await pool.query("UPDATE supervision_cases SET state='open' WHERE id=$1", [c.id]);
    await expect(apply(f, c)).rejects.toThrow('Contrato');
    expect((await taskRow(f)).description).toBe(before.description);
    expect(
      (await pool.query('SELECT count(*)::int n FROM task_instruction_revisions')).rows[0].n,
    ).toBe(0);
  },
);
it('serializes duplicate updates and admission with one immutable revision/fence', async () => {
  const f = await fixture(),
    c = await reviewedCase(f);
  const [a, b] = await Promise.all([apply(f, c), apply(f, c)]);
  expect(a.id).toBe(b.id);
  const run = await sourceRun(f);
  expect(run.instruction_snapshot.revisionId).toBe(a.id);
  await expect(
    pool.query('UPDATE runs SET fence=$2,instruction_snapshot=$3 WHERE id=$1', [
      run.id,
      randomUUID(),
      JSON.stringify({}),
    ]),
  ).rejects.toThrow('INSTRUCTION_SNAPSHOT_IMMUTABLE');
  expect((await pool.query('SELECT fence FROM runs WHERE id=$1', [run.id])).rows[0].fence).toBe(
    run.fence,
  );
});
it('delivers original and historical evidence as a real hashed input while keeping an existing context unchanged', async () => {
  const f = await fixture(),
    c = await reviewedCase(f, 'First');
  await apply(f, c);
  const run = await sourceRun(f);
  const ctx = await assembleContext(run, await taskRow(f), []),
    before = (await pool.query('SELECT * FROM contexts WHERE id=$1', [ctx.id])).rows[0];
  const next = await reviewedCase(f, 'Second');
  await apply(f, next);
  expect((await pool.query('SELECT * FROM contexts WHERE id=$1', [ctx.id])).rows[0]).toEqual(
    before,
  );
  const ref = (ctx.manifest as any).instructionArtifacts[0],
    evidence = await readArtifact(ref.id),
    archive = JSON.parse(evidence.bytes.toString());
  expect(evidence.digest).toBe(ref.digest);
  expect(archive.original).toBe(original);
  expect(archive.revisions).toHaveLength(1);
  expect(instructionTask(run, await taskRow(f), ctx.manifest).description).toContain(
    'First instruction',
  );
});
it.each(['harness', 'analytical'])(
  'sends the same frozen contract and verified archive to the %s adapter',
  async (kind) => {
    const f = await fixture(),
      c = await reviewedCase(f);
    await apply(f, c);
    if (kind === 'analytical')
      await pool.query('UPDATE tasks SET execution=NULL,instruction_revision_id=NULL WHERE id=$1', [
        f.task,
      ]);
    // Analytical routing belongs to the admission snapshot; instruction revision is
    // still selected through the unchanged rendered contract.
    if (kind === 'analytical')
      await pool.query('UPDATE tasks SET instruction_revision_id=$2 WHERE id=$1', [
        f.task,
        (await pool.query('SELECT id FROM task_instruction_revisions WHERE task_id=$1', [f.task]))
          .rows[0].id,
      ]);
    const run = await sourceRun(f),
      task = await taskRow(f),
      context = await assembleContext(run, task, []);
    const request = vi.spyOn(accountBridge, 'request').mockResolvedValue(
      kind === 'harness'
        ? { summary: 'Stopped delivery', artifacts: [], evidence: { stopped: true } }
        : {
            message: {
              content: [
                { type: 'text', text: '{"summary":"Scoped analytical result","artifacts":[]}' },
              ],
            },
            usage: {},
          },
    );
    config.remoteExecution = kind === 'harness';
    await executeRuntime({
      run,
      task: { ...task, description: 'Mutated live description' },
      account: {
        id: 'test',
        bridge_id: 'private-bridge-reference',
        provider: 'codex',
        model: 'test',
      },
      context,
      signal: new AbortController().signal,
      onEvent: async () => {},
    });
    const [method, payload] = request.mock.calls[0] as any;
    const manifest = (context.manifest as any).taskInstructions;
    if (kind === 'harness') {
      expect(method).toBe('worker.execute');
      expect(payload.prompt).toContain(task.description);
      expect(payload.prompt).not.toContain('Mutated live description');
      const input = payload.inputs.find((i: any) => i.name === 'task-instructions-history.json');
      expect(hash(Buffer.from(input.base64, 'base64'))).toBe(manifest.history.digest);
    } else {
      expect(method).toBe('generate');
      const message = JSON.parse(payload.messages[0].content);
      expect(message.description).toBe(task.description);
      expect(message.instructionManifest).toEqual(manifest);
      const input = message.dependencies.find(
        (i: any) => i.name === 'task-instructions-history.json',
      );
      expect(hash(input.content)).toBe(manifest.history.digest);
    }
  },
);
it('rejects forged adapter manifests and retains the existing 180k payload guard', async () => {
  const f = await fixture(),
    c = await reviewedCase(f);
  await apply(f, c);
  await pool.query('UPDATE tasks SET execution=NULL WHERE id=$1', [f.task]);
  const run = await sourceRun(f),
    task = await taskRow(f),
    context = await assembleContext(run, task, []);
  expect(() =>
    instructionTask(run, task, {
      taskInstructions: {
        ...(context.manifest as any).taskInstructions,
        descriptionHash: '0'.repeat(64),
      },
    }),
  ).toThrow('Contrato');
  const request = vi.spyOn(accountBridge, 'request');
  await expect(
    executeRuntime({
      run,
      task,
      account: { bridge_id: 'test' },
      context: { ...context, content: 'X'.repeat(180000) },
      signal: new AbortController().signal,
      onEvent: async () => {},
    }),
  ).rejects.toMatchObject({ code: 'CONTEXT_TOO_LARGE' });
  expect(request).not.toHaveBeenCalled();
});

it('prepares a verified legacy directive in the admission transaction without changing prior runs', async () => {
  const f = await fixture(),
    c = await reviewedCase(f, 'Legacy current'),
    full = original + reviseInstruction(c.action);
  await pool.query('UPDATE tasks SET description=$2,execution=execution||$3::jsonb WHERE id=$1', [
    f.task,
    full,
    JSON.stringify({ supervision: { caseId: c.id, sourceRunId: c.source.id } }),
  ]);
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('SUPERVISION_APPLIED',$1,$2)",
    [f.task, JSON.stringify({ caseId: c.id, sourceRunId: c.source.id, decisionRunId: c.review })],
  );
  const laterRetry = await sourceRun(f);
  const before = (await pool.query('SELECT * FROM runs WHERE id=$1', [c.source.id])).rows[0];
  await tx((db) => prepareTaskInstructions(db, f.task));
  const after = await taskRow(f),
    run = await sourceRun(f);
  expect(after.description.length).toBeLessThan(full.length + 200);
  const origin = (
    await pool.query('SELECT * FROM task_instruction_origins WHERE task_id=$1', [f.task])
  ).rows[0];
  expect(origin.original).toBe(original);
  expect(origin.baseline).toBe(full);
  expect(origin.proof.removedCount).toBe(1);
  expect(run.instruction_snapshot.task.description).toBe(after.description);
  expect(run.attempt).toBe(laterRetry.attempt + 1);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [c.source.id])).rows[0]).toEqual(
    before,
  );
});
it('replays accepted historical event decisions even when that case decision later changes', async () => {
  const f = await fixture(),
    c = await reviewedCase(f, 'Historical'),
    full = original + reviseInstruction(c.action);
  await pool.query('UPDATE tasks SET description=$2 WHERE id=$1', [f.task, full]);
  await pool.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('SUPERVISION_APPLIED',$1,$2)",
    [f.task, JSON.stringify({ caseId: c.id, sourceRunId: c.source.id, decisionRunId: c.review })],
  );
  await pool.query(
    "UPDATE supervision_cases SET decision='{}',decision_run_id=NULL,state='open' WHERE id=$1",
    [c.id],
  );
  const result = await projectLegacyInstructions(pool, await taskRow(f));
  expect(result.original).toBe(original);
  expect(result.proof.removedCount).toBe(1);
});
it('rejects authority changes before admission but keeps an already admitted run contract unchanged', async () => {
  const f = await fixture(),
    c = await reviewedCase(f);
  await apply(f, c);
  const run = await sourceRun(f),
    frozen = run.instruction_snapshot;
  await pool.query(
    'UPDATE supervision_cases SET decision=decision||\'{"external":true}\'::jsonb WHERE id=$1',
    [c.id],
  );
  await expect(sourceRun(f)).rejects.toThrow('INSTRUCTION_CASE_CHANGED');
  expect(instructionTask(run, await taskRow(f)).description).toBe(frozen.task.description);
});

const operatorInput = (text = 'Exact operator proof') => ({
  id: randomUUID(),
  name: 'operator-proof.txt',
  digest: hash(text),
  base64: Buffer.from(text).toString('base64'),
});
it('freezes operator input provenance at admission, delivers real bytes, and never invents artifact acceptance', async () => {
  const f = await fixture(),
    input = operatorInput();
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",
    [f.task, JSON.stringify([input])],
  );
  const run = await sourceRun(f),
    task = await taskRow(f),
    context = await assembleContext(run, task, []);
  expect(run.instruction_snapshot.executionInputs).toEqual(executionInputManifest([input]));
  expect((context.manifest as any).executionInputs).toEqual(
    run.instruction_snapshot.executionInputs,
  );
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{inputs}','[]') WHERE id=$1", [
    f.task,
  ]);
  const delivered = await runtimeInputs(
    instructionTask(run, await taskRow(f)),
    context.manifest,
    readArtifact,
  );
  expect(delivered).toEqual([input]);
  expect(Buffer.from(delivered[0].base64, 'base64').toString()).toBe('Exact operator proof');
  expect((await pool.query('SELECT 1 FROM artifacts WHERE id=$1', [input.id])).rowCount).toBe(0);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1', [run.id])).rowCount).toBe(
    0,
  );
});
it.each(['digest', 'path', 'collision'])(
  'rejects %s in operator inputs at database admission and adapter transport',
  async (mode) => {
    const f = await fixture(),
      input = operatorInput(),
      inputs = [input];
    if (mode === 'digest') input.digest = '0'.repeat(64);
    if (mode === 'path') input.name = '../outside.txt';
    if (mode === 'collision') inputs.push({ ...input, name: 'different.txt' });
    expect(() => mergeSupervisorInputs(inputs)).toThrow();
    await pool.query(
      "UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",
      [f.task, JSON.stringify(inputs)],
    );
    await expect(sourceRun(f)).rejects.toThrow(/SUPERVISION_INPUT/);
  },
);
it('checks the aggregate input limit across operator evidence and artifacts while preserving dedup', async () => {
  const inputs = [0, 1, 2].map(() => operatorInput('x'.repeat(8 * 1024 * 1024)));
  expect(mergeSupervisorInputs(inputs, [inputs[0]])).toHaveLength(3);
  const extra = operatorInput('extra');
  await expect(
    runtimeInputs(
      { execution: { inputs } },
      { dependencyArtifacts: [{ id: extra.id, name: extra.name, digest: extra.digest }] },
      async () => ({ ...extra, bytes: Buffer.from('extra') }),
    ),
  ).rejects.toMatchObject({ code: 'SUPERVISION_INPUTS_TOO_LARGE' });
});
it('fails closed on operator/artifact identity collisions without overwriting the old artifact', async () => {
  const f = await fixture(),
    c = await reviewedCase(f),
    input = { ...operatorInput(), id: c.artifact.id };
  const before = await readArtifact(c.artifact.id);
  await expect(
    runtimeInputs(
      { execution: { inputs: [input] } },
      { dependencyArtifacts: [c.artifact] },
      readArtifact,
    ),
  ).rejects.toMatchObject({ code: 'SUPERVISION_INPUT_COLLISION' });
  expect((await readArtifact(c.artifact.id)).bytes).toEqual(before.bytes);
});
it('keeps the internal instruction history as the owning run input and excludes it from accepted outputs', async () => {
  const f = await fixture(),
    c = await reviewedCase(f);
  await apply(f, c);
  const run = await sourceRun(f, 'running'),
    task = await taskRow(f),
    context = await assembleContext(run, task, []);
  const patch = await writeArtifact(run.id, run.fence, 'changes.patch', 'Exact accepted delta');
  await writeArtifact(run.id, run.fence, 'result.json', '{"validated":true}');
  await completeRun(run.id, run.fence, 'Scoped accepted output', {});
  await validateTask(f.task);
  const child = randomUUID();
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution) VALUES($1,$2,'downstream','downstream','Original downstream','qa.backend','pending','[]',$3)",
    [child, f.goal, JSON.stringify(task.execution)],
  );
  await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)', [child, f.task]);
  const downstream = await acceptedDependencyArtifacts(child);
  expect(downstream.map((a: any) => a.id)).toContain(patch.id);
  expect(downstream.some((a: any) => a.name === 'task-instructions-history.json')).toBe(false);
  const source = await taskRow(f),
    target = (await pool.query('SELECT * FROM tasks WHERE id=$1', [child])).rows[0];
  const delivered = await supervisionInputs(pool, source, target);
  expect(delivered.inputs.some((a: any) => a.name === 'task-instructions-history.json')).toBe(
    false,
  );
  expect(
    (await runtimeInputs(task, context.manifest, readArtifact)).some(
      (a: any) => a.name === 'task-instructions-history.json',
    ),
  ).toBe(true);
});
it('keeps a binary operator image as binary evidence in the analytical adapter', async () => {
  const f = await fixture(),
    bytes = Buffer.from([137, 80, 78, 71, 0, 255]),
    input = {
      id: randomUUID(),
      name: 'native-proof.png',
      digest: hash(bytes),
      base64: bytes.toString('base64'),
    };
  await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1', [
    f.task,
    JSON.stringify({ inputs: [input] }),
  ]);
  const run = await sourceRun(f),
    task = await taskRow(f),
    context = await assembleContext(run, task, []);
  const request = vi.spyOn(accountBridge, 'request').mockResolvedValue({
    message: {
      content: [{ type: 'text', text: '{"summary":"Binary limitation","artifacts":[]}' }],
    },
  });
  await executeRuntime({
    run,
    task,
    context,
    account: { bridge_id: 'test' },
    signal: new AbortController().signal,
    onEvent: async () => {},
  });
  const message = JSON.parse((request.mock.calls[0][1] as any).messages[0].content);
  expect(message.dependencies[0].mime).toBe('application/octet-stream');
  expect(message.dependencies[0].content).toContain('Binary artifact');
  expect(message.dependencies[0].digest).toBe(input.digest);
});
it('rejects coercible non-string input paths and identities', () => {
  const input = operatorInput();
  expect(() => mergeSupervisorInputs([{ ...input, id: [input.id] }])).toThrow();
  expect(() => mergeSupervisorInputs([{ ...input, name: [input.name] }])).toThrow();
});
it('removes only a verified recent suffix while keeping two unknown appends and literal user markers intact', async () => {
  const f = await fixture();
  let full = original,
    prefix = '';
  for (let i = 0; i < 4; i++) {
    const c = await reviewedCase(f, 'History ' + i);
    full += reviseInstruction(c.action);
    await pool.query(
      "INSERT INTO events(kind,entity_id,payload) VALUES('SUPERVISION_APPLIED',$1,$2)",
      [
        f.task,
        JSON.stringify({
          caseId: c.id,
          sourceRunId: c.source.id,
          decisionRunId: i < 2 ? null : c.review,
        }),
      ],
    );
    if (i === 1) prefix = full;
  }
  await pool.query('UPDATE tasks SET description=$2 WHERE id=$1', [f.task, full]);
  const projected = await projectLegacyInstructions(pool, await taskRow(f));
  expect(projected.original).toBe(prefix);
  expect(projected.original).toContain('texto fornecido pelo usuário');
  expect(projected.proof).toMatchObject({
    partial: true,
    removedCount: 2,
    projection: 'verified_suffix',
    verified: false,
    retainedUnknownReason: 'decision_or_event_unverified',
  });
  const c = await reviewedCase(f, 'Current'),
    revision = await apply(f, c),
    run = await sourceRun(f);
  const evidence = await tx((db) =>
    instructionEvidence(db, run, instructionTask(run, awaitedTaskPlaceholderWithGoal(f))),
  );
  const archive = JSON.parse((await readArtifact(evidence.artifact!.id)).bytes.toString());
  expect(archive.baseline).toBe(full);
  expect(archive.original).toBe(prefix);
  expect(evidence.manifest.originalSource).toBe('preserved_prefix');
  expect(revision.rendered).not.toContain('History 3 instruction');
});
it('serializes case changes with admission and leaves the admitted snapshot immutable after the writer resumes', async () => {
  const f = await fixture(),
    c = await reviewedCase(f);
  await apply(f, c);
  const admission = await pool.connect(),
    writer = await pool.connect();
  let pending: Promise<any> | undefined;
  try {
    await admission.query('BEGIN');
    const run = (
      await admission.query(
        "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'failed') RETURNING *",
        [randomUUID(), f.task, randomUUID()],
      )
    ).rows[0];
    const pid = (await writer.query('SELECT pg_backend_pid() pid')).rows[0].pid;
    pending = writer.query(
      'UPDATE supervision_cases SET decision=decision||\'{"newEvidenceDecision":true}\'::jsonb WHERE id=$1',
      [c.id],
    );
    let waiting = false;
    for (let i = 0; i < 50 && !waiting; i++) {
      waiting = !!(
        await pool.query("SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock'", [
          pid,
        ])
      ).rowCount;
      if (!waiting) await new Promise((resolve) => setTimeout(resolve, 5));
    }
    expect(waiting).toBe(true);
    await admission.query('COMMIT');
    await pending;
    pending = undefined;
    expect(
      (await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1', [run.id])).rows[0]
        .instruction_snapshot,
    ).toEqual(run.instruction_snapshot);
    expect(instructionTask(run, await taskRow(f)).description).toBe(
      run.instruction_snapshot.task.description,
    );
  } finally {
    await admission.query('ROLLBACK');
    await pending;
    admission.release();
    writer.release();
  }
});
it('keeps required memory IDs frozen when mutable task metadata changes after admission', async () => {
  const f = await fixture(),
    ids = [randomUUID()];
  await pool.query('UPDATE tasks SET required_memories=$2 WHERE id=$1', [
    f.task,
    JSON.stringify(ids),
  ]);
  const run = await sourceRun(f);
  await pool.query("UPDATE tasks SET required_memories='[]' WHERE id=$1", [f.task]);
  expect(instructionTask(run, await taskRow(f)).required_memories).toEqual(ids);
});
it('keeps historical run state in goalDetail without shipping large snapshots or operator base64', async () => {
  const f = await fixture('Original long requirement. '.repeat(5000)),
    input = operatorInput('binary evidence '.repeat(12000));
  await pool.query(
    "UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",
    [f.task, JSON.stringify([input])],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,summary) SELECT gen_random_uuid(),$1,n,gen_random_uuid(),'harness',1,'failed','Historical result '||n FROM generate_series(1,20)n",
    [f.task],
  );
  const detail = await goalDetail(f.goal),
    raw = (await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt', [f.task])).rows;
  expect(detail.runs).toHaveLength(20);
  for (const run of detail.runs) {
    const { fence, instruction_snapshot, ...prior } = raw.find((r) => r.id === run.id);
    expect(run).toEqual(prior);
  }
  const serialized = JSON.stringify(detail);
  expect(serialized).not.toContain('instruction_snapshot');
  expect(serialized).not.toContain(input.base64);
  const global = await snapshot();
  expect(global.runs.filter((run) => run.task_id === f.task)).toHaveLength(20);
  expect(JSON.stringify(global)).not.toContain('instruction_snapshot');
  expect(JSON.stringify(global)).not.toContain(input.base64);
  expect(serialized.length).toBeLessThan(400000);
  expect(detail.tasks.find((t: any) => t.id === f.task).execution.inputs[0]).toMatchObject({
    id: input.id,
    digest: input.digest,
    name: input.name,
  });
});
