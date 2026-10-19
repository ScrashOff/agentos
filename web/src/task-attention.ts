import { executionStatus, observedProblems, modelActivity } from './execution-status';
import {humanRecoveryRequest} from '../../shared/recovery';

type Row = Record<string, any>;
export type TaskAttention = {
  title: string;
  detail: string;
  nextStep: string;
  owner: string;
  kind: 'blocked' | 'watch' | 'automatic';
  code?: string;
  causes: Row[];
};
const active = new Set(['running', 'cancelling', 'reconciling']);
const terminal = new Set(['accepted', 'cancelled', 'superseded']);
export function currentRun(task: Row, runs: Row[] = []) {
  return runs
    .filter((r) => r.task_id === task.id)
    .sort(
      (a, b) =>
        (b.attempt ?? 0) - (a.attempt ?? 0) || Date.parse(b.created_at) - Date.parse(a.created_at),
    )[0];
}
// Report a real prerequisite wait; keep failed/auth-dependent chains and an
// unconfirmed current execution under their existing attention handling.
export function pendingExecutionDependencies(task: Row, data: Row) {
  const run = currentRun(task, data.runs);
  if (
    !['submitted', 'failed', 'recovery_blocked', 'blocked_dependencies'].includes(task.state) ||
    ['queued', 'running', 'cancelling', 'reconciling'].includes(run?.state) ||
    /AUTH|QUOTA/.test(run?.error_code ?? '')
  )
    return [];
  const parents = (data.dependencies ?? [])
    .filter((d: Row) => d.task_id === task.id)
    .map((d: Row) => data.tasks?.find((t: Row) => t.id === d.depends_on));
  if (parents.some((t: Row | undefined) => !t)) return [];
  const pending = parents.filter((t: Row) => t.state !== 'accepted');
  // The authoritative dependency hold precedes a continuation notice left by
  // an earlier verification attempt. That attempt cannot request action while
  // its implementation is still awaiting repair/acceptance. Its parent keeps
  // its own current attention, including genuine account/input blockers.
  if (task.state === 'blocked_dependencies' && pending.length) return pending;
  // An explicit recovery of the current attempt keeps its own state unless
  // the scheduler has actually placed the task on a dependency hold.
  const continuation = task.execution?.continuation;
  if (continuation && run && continuation.runId === run.id &&
      ['supervising', 'waiting_slot', 'collecting', 'waiting_evidence'].includes(continuation.state))
    return [];
  const executing = new Set([
    'running',
    'queued',
    'ready',
    'blocked_dependencies',
    'waiting_resource',
    'retry_scheduled',
  ]);
  return pending.length && pending.every((t: Row) => executing.has(t.state)) ? pending : [];
}
export function latestObservation(run: Row, events: Row[] = []) {
  return events
    .filter((e) => e.kind === 'EXECUTION_OBSERVED' && e.payload?.runId === run.id)
    .sort(
      (a, b) => Date.parse(b.created_at) - Date.parse(a.created_at) || Number(b.id) - Number(a.id),
    )[0]?.payload.observation;
}
// Keep the latest observation/model/tool event per run when other agents fill the
// global 100-event window. Older SSE replay must never overwrite a newer sample.
export function retainExecutionEvents(previous: Row[], incoming: Row[]): Row[] {
  const sorted = [
    ...new Map([...previous, ...incoming].map((e) => [String(e.id), e])).values(),
  ].sort(
    (a, b) => Date.parse(b.created_at) - Date.parse(a.created_at) || Number(b.id) - Number(a.id),
  );
  const seen = new Set<string>();
  const retainedKinds = new Set([
    'EXECUTION_OBSERVED',
    'MODEL_REQUEST',
    'MODEL_RESPONSE',
    'MODEL_ERROR',
    'OPENCODE_TOOL_EVENT',
    'HARNESS_TOOL_EVENT',
    'MUSE_TOOL_EVENT',
    'MUSE_STOPPED',
    'RUNTIME_STARTED',
  ]);
  return sorted.filter((event, index) => {
    if (!retainedKinds.has(event.kind) || !event.payload?.runId) return index < 100;
    const key = event.payload.runId + ':' + event.kind;
    if (seen.has(key)) return index < 100;
    seen.add(key);
    return true;
  });
}
export function affectedTasks(taskId: string, data: Row): Row[] {
  const seen = new Set([taskId]);
  const result: Row[] = [];
  const visit = (id: string) => {
    for (const dependency of data.dependencies ?? []) {
      if (dependency.depends_on !== id || seen.has(dependency.task_id)) continue;
      seen.add(dependency.task_id);
      const task = data.tasks?.find((t: Row) => t.id === dependency.task_id);
      if (task && !terminal.has(task.state)) {
        result.push(task);
        visit(task.id);
      }
    }
  };
  visit(taskId);
  return result;
}
export function taskAttention(
  task: Row,
  data: Row,
  now = Date.now(),
  seen = new Set<string>(),
): TaskAttention | null {
  if (terminal.has(task.state) || seen.has(task.id)) return null;
  seen = new Set(seen).add(task.id);
  const run = currentRun(task, data.runs);
  const code = run?.error_code;
  const result = (
    title: string,
    detail: string,
    nextStep: string,
    owner = 'Ação necessária',
    kind: TaskAttention['kind'] = 'blocked',
    causes: Row[] = [],
  ): TaskAttention => ({ title, detail, nextStep, owner, kind, code, causes });
  const continuation = task.execution?.continuation;
  const supervisor = data.settings?.['continuation-supervisor'];
  if(run?.state==='failed' && /^(QUOTA_EXCEEDED|RATE_LIMIT|RATE_LIMITED)$/.test(code??''))
    return result('Aguardando rota com cota',task.reason || run.summary || 'O provedor recusou esta tentativa.',
      'O agendador procura uma rota autorizada ou aguarda disponibilidade; esta falha não exige aprovação humana.',
      'Agendador','watch');
  if (task.state === 'recovery_blocked' && continuation && run && continuation.runId === run.id && continuation.state === 'waiting_evidence')
    return result('Aguardando insumos', continuation.message,
      'Confira os insumos indicados nos detalhes. Novas evidências fazem o supervisor reavaliar esta etapa; as outras frentes continuam.',
      'Aguarda novos insumos', 'watch');
  const prerequisites = pendingExecutionDependencies(task, data);
  if (prerequisites.length)
    return result(
      'Aguardando etapas anteriores',
      prerequisites.map((t: Row) => t.title ?? t.id).join('; '),
      'Acompanhar as etapas indicadas. Esta tarefa ainda depende do aceite delas.',
      'Depende de outra tarefa',
      supervisor?.state === 'running' && now - Date.parse(supervisor.checkedAt) < 90000
        ? 'automatic'
        : 'watch',
      prerequisites,
    );
  if(['failed','submitted','recovery_blocked','waiting_configuration'].includes(task.state) &&
      ['failed','submitted'].includes(run?.state) && continuation && run && continuation.runId===run.id &&
      continuation.state==='needs_action' && !humanRecoveryRequest(continuation,code))
    return result('Encaminhamento técnico pendente',continuation.message,
      'O sistema deve encaminhar este bloqueio ao supervisor ou reconciliador. Nenhuma autorização sua está pendente.',
      continuation.owner==='controller'?'Controlador':'Supervisor','watch');
  if (
    [
      'failed',
      'recovery_blocked',
      'reconciling',
      'retry_scheduled',
      'waiting_resource',
      'recovering_artifacts',
    ].includes(task.state) &&
    continuation && run && continuation.runId === run.id &&
    continuation.state !== 'needs_action' &&
    supervisor?.state === 'running' &&
    now - Date.parse(supervisor.checkedAt) < 90000
  )
    return result(
      continuation.state === 'supervising'
        ? 'Supervisor resolvendo o bloqueio'
        : continuation.state === 'waiting_slot'
          ? 'Correção na fila deste jogo'
          : 'Recuperação automática',
      continuation.message,
      'A retomada está sob responsabilidade do supervisor. Nenhuma aprovação sua está pendente.',
      'O sistema está cuidando',
      'automatic',
    );
  if (task.state === 'reconciling')
    return result(
      'Encerramento ainda não confirmado',
      task.reason ||
        'O controlador perdeu a confirmação do executor. Isso não comprova que ele continua trabalhando.',
      'Conferir o processo remoto e os arquivos preservados antes de liberar uma nova tentativa. Nenhuma aprovação sua está pendente.',
      'Verificação do executor',
    );
  if (task.state === 'blocked_failed') {
    const causes = new Map<string, Row>();
    let requiresIntervention = false;
    for (const dependency of data.dependencies ?? []) {
      if (dependency.task_id !== task.id) continue;
      const parent = data.tasks?.find((t: Row) => t.id === dependency.depends_on);
      if (!parent) continue;
      const alert = taskAttention(parent, data, now, seen);
      if (!alert) continue;
      requiresIntervention ||= alert.kind === 'blocked';
      for (const root of alert.causes.length ? alert.causes : [parent]) causes.set(root.id, root);
    }
    if (causes.size)
      return result(
        'Bloqueada por outra etapa',
        [...causes.values()]
          .map(
            (t) => `${t.title}: ${taskAttention(t, data, now, seen)?.title ?? t.reason ?? t.state}`,
          )
          .join('; '),
        'Resolver a causa abaixo. Esta etapa será liberada após o aceite das dependências.',
        'Depende de outra tarefa',
        requiresIntervention ? 'blocked' : 'watch',
        [...causes.values()],
      );
    return result(
      'Dependência sem aceite',
      task.reason || 'Uma etapa anterior falhou.',
      'Consultar as dependências da tarefa para identificar a etapa que precisa de correção.',
      'Depende de outra tarefa',
    );
  }
  // A previous attempt's error must never leak into a running/retried attempt.
  if (
    ['failed', 'waiting_configuration', 'waiting_quota', 'recovery_blocked'].includes(task.state)
  ) {
    if (code === 'AUTH_REQUIRED')
      return result(
        'Autenticação recusada',
        run?.summary || task.reason || 'O provedor recusou a autenticação desta tentativa.',
        'Verificar a conta em Contas e cotas; reconectar ou selecionar outra conta autorizada para a retomada.',
      );
    if (code === 'QUOTA_EXCEEDED' || task.state === 'waiting_quota')
      return result(
        'Limite de uso do provedor',
        task.reason || run?.summary || 'Não há cota disponível na rota selecionada.',
        task.state === 'waiting_quota'
          ? 'O agendador aguarda uma rota elegível. Confira a disponibilidade em Contas e cotas.'
          : 'Conferir a cota e retomar em outra conta autorizada, ou aguardar a renovação. Esta tentativa já encerrou.',
        task.state === 'waiting_quota' ? 'Aguarda disponibilidade' : 'Ação necessária',
      );
    if (task.state === 'recovery_blocked')
      return result(
        'Correção automática esgotada',
        task.reason || 'As tentativas disponíveis não produziram uma entrega aceita.',
        'Revisar os erros e decompor a correção em um novo plano antes de continuar.',
      );
    if (task.state === 'waiting_configuration')
      return result(
        'Configuração de execução pendente',
        task.reason || 'A rota ou o runtime ainda não está pronto.',
        'Conferir conta, modelo e runtime configurados para esta tarefa.',
      );
    return result(
      'Tentativa falhou',
      run?.summary || task.reason || 'O executor terminou sem uma entrega válida.',
      task.execution?.kind === 'opencode'
        ? 'Conferir os arquivos preservados e preparar a retomada pelo executor OpenCode.'
        : 'Consultar o erro e as evidências desta tentativa para corrigir a causa antes da retomada.',
    );
  }
  if (task.state === 'submitted') {
    const acceptance = (data.acceptances ?? [])
      .filter((a: Row) => a.run_id === run?.id)
      .sort((a: Row, b: Row) => Date.parse(b.created_at) - Date.parse(a.created_at))[0];
    const failed = acceptance?.checks?.filter((c: Row) => !c.passed) ?? [];
    if (failed.length)
      return result(
        'Entrega não passou nos critérios',
        failed.map((c: Row) => `${c.label}: ${c.detail ?? 'Não atendido'}`).join('; '),
        task.recovery?.mode === 'manual'
          ? 'Revisar as evidências e definir a correção dos critérios pendentes.'
          : 'Conferir os critérios abaixo e acompanhar o encaminhamento para correção. Entrega ainda não aceita.',
        'Validação da entrega',
      );
    return result(
      'Validação pendente',
      task.reason || 'O executor entregou o resultado; o aceite ainda não foi registrado.',
      'Aguardar a conferência dos critérios. Se houver revisão humana prevista, ela aparece no detalhe da tarefa.',
      'Validação da entrega',
      'automatic',
    );
  }
  if (!active.has(task.state) || !run) return null;
  const observation = latestObservation(run, data.events);
  // Allow the first observation to arrive; absence after the grace period is explicit.
  if (!observation && (!run.started_at || now - Date.parse(run.started_at) < 90000)) return null;
  const status = executionStatus(observation, run, now, modelActivity(run, data.events));
  if (['unknown', 'warning'].includes(status.level))
    return result(
      status.title,
      status.detail,
      'Conferir a última atividade e o processo remoto. Este aviso não é um pedido de aprovação nem confirma falha da tarefa.',
      'Acompanhar execução',
      'watch',
    );
  const problem = observedProblems(observation, run)[0];
  if (problem)
    return result(
      problem.title,
      `${problem.source}: ${problem.line}`,
      'Acompanhar a correção pelo agente e o próximo resultado. O erro no log ainda não é o resultado final.',
      'Acompanhar execução',
      'watch',
    );
  return null;
}
