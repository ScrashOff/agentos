import { expect, it } from 'vitest';
import {
  affectedTasks,
  pendingExecutionDependencies,
  taskAttention,
  latestObservation,
  retainExecutionEvents,
} from '../web/src/task-attention';
import {taskFlow} from '../web/src/operation-state';

const now = Date.parse('2026-10-05T19:00:00Z');
const task = {
  id: 'one',
  title: 'Protocolo OpenCode',
  state: 'failed',
  execution: { kind: 'opencode' },
};
const run = {
  id: 'run',
  task_id: 'one',
  attempt: 1,
  state: 'failed',
  created_at: '2026-10-05T18:00:00Z',
};
it('keeps never-started recovery tasks visible without treating two absent run IDs as matching', () => {
  const data = { tasks: [], runs: [], events: [], settings: {
    'continuation-supervisor': { state: 'running', checkedAt: new Date(now).toISOString() },
  } };
  for (const state of ['recovery_blocked', 'failed', 'retry_scheduled', 'waiting_resource', 'recovering_artifacts']) {
    const pending = { ...task, state, attempt_count: 0 };
    expect(() => taskAttention(pending, data, now)).not.toThrow();
    expect(taskAttention(pending, data, now)?.kind).not.toBe('automatic');
    expect(taskAttention({ ...pending, execution: { continuation: { state: 'supervising' } } }, data, now)?.kind).not.toBe('automatic');
  }
  expect(taskAttention({ ...task, state: 'recovery_blocked' }, data, now)?.title).toBe('Correção automática esgotada');
});
it('keeps the root quota cause visible through dependencies without making it a human approval',()=>{
  const parent={...task,execution:{continuation:{runId:run.id,state:'needs_action',message:'Old retry diagnosis'}}};
  const child={id:'child',state:'blocked_failed',title:'QA'};
  const data={tasks:[parent,child],runs:[{...run,error_code:'QUOTA_EXCEEDED'}],
    dependencies:[{task_id:child.id,depends_on:parent.id}]};
  expect(taskAttention(child,data,now)).toMatchObject({kind:'watch',causes:[parent],
    detail:'Protocolo OpenCode: Aguardando rota com cota'});
  expect(taskFlow(parent,data,now)).toMatchObject({action:false,kind:'waiting'});
});

it('an authoritative dependency hold supersedes an old verification action while its parent is supervised',()=>{
  const parent={...task,state:'recovery_blocked',execution:{continuation:{runId:run.id,state:'supervising'}}};
  const qa={id:'qa',title:'Independent verification',state:'blocked_dependencies',
    execution:{readOnlySource:true,continuation:{runId:'qa-run',state:'needs_action',message:'Old implementation routing failed'}}};
  const data={tasks:[parent,qa],dependencies:[{task_id:qa.id,depends_on:parent.id}],
    runs:[run,{...run,id:'qa-run',task_id:qa.id,state:'submitted'}],
    settings:{'continuation-supervisor':{state:'running',checkedAt:new Date(now).toISOString()}}};
  expect(pendingExecutionDependencies(qa,data)).toEqual([parent]);
  expect(taskFlow(qa,data,now)).toMatchObject({kind:'waiting',action:false});
  expect(taskAttention(qa,data,now)).toMatchObject({kind:'automatic',causes:[parent]});
  expect(taskFlow(parent,data,now)).toMatchObject({kind:'repair',action:false});
});
it('dependency waiting does not conceal the current attempt account refusal',()=>{
  const parent={...task,state:'recovery_blocked'};
  const qa={id:'qa',state:'blocked_dependencies',execution:{continuation:{runId:'qa-run',state:'needs_action',message:'Authentication refused'}}};
  const data={tasks:[parent,qa],dependencies:[{task_id:qa.id,depends_on:parent.id}],
    runs:[run,{...run,id:'qa-run',task_id:qa.id,error_code:'AUTH_REQUIRED'}]};
  expect(pendingExecutionDependencies(qa,data)).toEqual([]);
  expect(taskFlow(qa,data,now)).toMatchObject({kind:'action',action:true});
});
it('keeps an explicit current recovery queued until the scheduler actually holds it for dependencies',()=>{
  const parent={id:'parent',title:'Earlier step',state:'blocked_dependencies'};
  const recovery={...task,execution:{continuation:{runId:run.id,state:'waiting_slot',message:'Repair queued'}}};
  const data={tasks:[parent,recovery],runs:[run],dependencies:[{task_id:task.id,depends_on:parent.id}],
    settings:{'continuation-supervisor':{state:'running',checkedAt:new Date(now).toISOString()}}};
  expect(pendingExecutionDependencies(recovery,data)).toEqual([]);
  expect(taskFlow(recovery,data,now)).toMatchObject({kind:'repair',label:'Correção na fila',action:false});
  expect(pendingExecutionDependencies({...recovery,state:'blocked_dependencies'},data)).toEqual([parent]);
  expect(pendingExecutionDependencies({...recovery,execution:{}},data)).toEqual([parent]);
});

it('explains quota and auth failures even when the event has left the global window', () => {
  for (const [code, title] of [
    ['AUTH_REQUIRED', 'Autenticação recusada'],
    ['QUOTA_EXCEEDED', 'Aguardando rota com cota'],
  ]) {
    expect(
      taskAttention(task, { runs: [{ ...run, error_code: code }], events: [] }, now),
    ).toMatchObject({ title, code, kind: code === 'AUTH_REQUIRED' ? 'blocked' : 'watch' });
  }
});
it('traces a blocked chain to its root including another objective and avoids dependency cycles', () => {
  const child = { id: 'two', state: 'blocked_failed', title: 'Integração' };
  const end = { id: 'three', state: 'blocked_failed', title: 'QA' };
  const dependencies = [
    { task_id: 'two', depends_on: 'one' },
    { task_id: 'three', depends_on: 'two' },
  ];
  const data = {
    tasks: [task, child, end],
    dependencies,
    runs: [{ ...run, error_code: 'AUTH_REQUIRED' }],
  };
  expect(taskAttention(end, data, now)).toMatchObject({
    causes: [task],
    detail: 'Protocolo OpenCode: Autenticação recusada',
  });
  expect(affectedTasks('one', data).map((t) => t.id)).toEqual(['two', 'three']);
  dependencies.push({ task_id: 'two', depends_on: 'three' });
  expect(taskAttention(end, data, now)?.causes).toEqual([task]);
});
it('does not carry a prior auth failure into a new attempt or an accepted task', () => {
  const active = { ...task, state: 'running' };
  const data = {
    runs: [
      { ...run, error_code: 'AUTH_REQUIRED' },
      { ...run, id: 'run2', attempt: 2, state: 'running', started_at: new Date(now).toISOString() },
    ],
  };
  expect(taskAttention(active, data, now)).toBeNull();
  expect(taskAttention({ ...task, state: 'accepted' }, data, now)).toBeNull();
});
it('keeps uncertainty visible without implying human approval or safe retry', () => {
  const result = taskAttention({ ...task, state: 'reconciling' }, { runs: [run] }, now)!;
  expect(result.owner).toBe('Verificação do executor');
  expect(result.nextStep).toContain('Nenhuma aprovação sua');
  expect(result.nextStep).toContain('Conferir o processo remoto');
});
it('explains missing inputs separately from automatic recovery and approvals',()=>{
 const waiting={...task,state:'recovery_blocked',execution:{continuation:{runId:run.id,state:'waiting_evidence',message:'Original captured atlas bytes'}}};
 const alert=taskAttention(waiting,{runs:[run]},now)!;
 expect(alert).toMatchObject({title:'Aguardando insumos',detail:'Original captured atlas bytes',kind:'watch',owner:'Aguarda novos insumos'});
 expect(alert.nextStep).toContain('Novas evidências');
 expect(taskAttention({...waiting,state:'accepted'},{runs:[run]},now)).toBeNull();
});
it('distinguishes a missing first observation from an overdue or failed live observation', () => {
  const active = { ...task, state: 'running' };
  const current = { ...run, state: 'running', started_at: new Date(now - 120000).toISOString() };
  expect(taskAttention(active, { runs: [current] }, now)).toMatchObject({
    kind: 'watch',
    title: 'Sem observação do executor',
  });
  const observation = {
    observedAt: new Date(now).toISOString(),
    process: { available: true, activeState: 'active' },
    evidence: [
      {
        name: 'test.log',
        modifiedAt: new Date(now).toISOString(),
        preview: 'Error: socket closed',
      },
    ],
  };
  const result = taskAttention(
    active,
    {
      runs: [current],
      events: [
        {
          id: 1,
          created_at: new Date(now).toISOString(),
          kind: 'EXECUTION_OBSERVED',
          payload: { runId: current.id, observation },
        },
      ],
    },
    now,
  )!;
  expect(result.kind).toBe('watch');
  expect(result.detail).toContain('Error: socket closed');
});
it('shows the failed current acceptance criteria and separates pending validation from a failure', () => {
  const submitted = { ...task, state: 'submitted' };
  expect(taskAttention(submitted, { runs: [run] }, now)?.kind).toBe('automatic');
  const data = {
    runs: [run],
    acceptances: [
      {
        run_id: run.id,
        created_at: new Date(now).toISOString(),
        checks: [{ passed: false, label: 'Teste de retomada', detail: 'Sessão perdeu o saldo' }],
      },
    ],
  };
  expect(taskAttention(submitted, data, now)?.detail).toBe(
    'Teste de retomada: Sessão perdeu o saldo',
  );
});
it('preserves the last observation after 100 newer events and ignores older replay samples', () => {
  const observation = {
    id: 2,
    kind: 'EXECUTION_OBSERVED',
    created_at: '2026-10-05T18:01:00Z',
    payload: { runId: 'run', observation: { toolCalls: 2 } },
  };
  const busy = Array.from({ length: 120 }, (_, i) => ({
    id: 3 + i,
    kind: 'MODEL_REQUEST',
    created_at: '2026-10-05T18:02:00Z',
  }));
  const retained = retainExecutionEvents([observation], busy);
  expect(retained).toHaveLength(101);
  const replay = {
    ...observation,
    id: 1,
    created_at: '2026-10-05T18:00:00Z',
    payload: { runId: 'run', observation: { toolCalls: 1 } },
  };
  expect(latestObservation(run, retainExecutionEvents(retained, [replay]))?.toolCalls).toBe(2);
});
it('names running prerequisites without claiming acceptance or hiding an unconfirmed executor', () => {
  const submitted = {
    ...task,
    state: 'submitted',
    execution: { continuation: { runId: run.id, state: 'needs_action' } },
  };
  const parent = { id: 'parent', title: 'Native styles repair', state: 'running' };
  const data = {
    tasks: [submitted, parent],
    dependencies: [{ task_id: task.id, depends_on: parent.id }],
    runs: [{ ...run, state: 'submitted' }],
    settings: {
      'continuation-supervisor': { state: 'running', checkedAt: new Date(now).toISOString() },
    },
  };
  expect(taskAttention(submitted, data, now)).toMatchObject({
    title: 'Aguardando etapas anteriores',
    detail: parent.title,
    kind: 'automatic',
    causes: [parent],
  });
  expect(taskAttention(submitted, { ...data, settings: {} }, now)).toMatchObject({ kind: 'watch' });
  expect(taskAttention({ ...submitted, state: 'reconciling' }, data, now)).toMatchObject({
    title: 'Encerramento ainda não confirmado',
  });
  expect(
    taskAttention(
      submitted,
      { ...data, tasks: [submitted, { ...parent, state: 'accepted' }] },
      now,
    ),
  ).toMatchObject({ title: 'Encaminhamento técnico pendente', kind: 'watch' });
});

it('keeps the last model call and tool event even when another agent floods the timeline', () => {
  const previous = ['MODEL_REQUEST', 'OPENCODE_TOOL_EVENT', 'MUSE_TOOL_EVENT'].map((kind, i) => ({
    id: i + 1,
    kind,
    created_at: '2026-10-05T18:00:00Z',
    payload: { runId: 'slow-run' },
  }));
  const recent = Array.from({ length: 120 }, (_, i) => ({
    id: i + 4,
    kind: 'MODEL_REQUEST',
    created_at: '2026-10-05T18:01:00Z',
    payload: { runId: 'busy-run' },
  }));
  const kept = retainExecutionEvents(previous, recent);
  expect(kept.filter((e) => e.payload.runId === 'slow-run')).toHaveLength(3);
});
