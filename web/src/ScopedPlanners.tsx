import { useEffect, useState } from 'react';

type Scope = {
  scope: string; label: string;
  planner: { taskId: string | null; state: string; occupied: boolean; model: string | null; account: string | null };
  nextDelivery: { taskId: string; title: string; state: string; stage: string } | null;
  blocker: { reason: string; code: string | null; taskId: string | null; nextAction: string | null } | null;
  workers: { running: number; ready: number; queued: number; confirming: number };
  lastAdvance: { taskId: string; title: string; at: string; kind: string } | null;
  details: { sourceRevision: string | null; planningUpdatedAt: string | null; canonicalRemaining: number };
};
type Panel = { goalId: string; goalState: string; maxActive: number; observedAt: string; scopes: Scope[] };
const labels: Record<string, string> = {
  idle: 'Sob demanda', pending: 'Na fila', assigned: 'Na fila', queued: 'Na fila', running: 'Planejando',
  cancelling: 'Confirmando parada', reconciling: 'Confirmando execução', preparing: 'Preparando trabalho',
  applied: 'Plano aplicado', blocked_external: 'Insumo pendente', replanning_required: 'Replanejamento',
  awaiting_supervision: 'Com o gestor', superseded: 'Fonte atualizada',
};
function age(at: string) {
  const seconds = Math.max(0, Math.floor((Date.now() - Date.parse(at)) / 1000));
  return seconds < 60 ? 'agora' : seconds < 3600 ? `há ${Math.floor(seconds / 60)} min` : `há ${Math.floor(seconds / 3600)} h`;
}

/** An independent read never blocks the objective view. Poll only after the
 * previous request finishes; leaving this goal aborts the request and timer. */
export function ScopedPlanners({ goalId, inspect }: { goalId: string; inspect: (id: string) => void }) {
  const [panel, setPanel] = useState<Panel | null>(null);
  const [failed, setFailed] = useState(false);
  const [refresh, setRefresh] = useState(0);
  useEffect(() => {
    let disposed = false, timer: ReturnType<typeof setTimeout> | undefined;
    let controller: AbortController | undefined;
    setPanel(null); setFailed(false);
    const poll = async () => {
      controller = new AbortController();
      const timeout = setTimeout(() => controller?.abort(), 12000);
      try {
        const response = await fetch(`/api/panel/goals/${encodeURIComponent(goalId)}/planners`, { signal: controller.signal });
        if (!response.ok) throw Error('PLANNER_PANEL_UNAVAILABLE');
        const value = await response.json();
        if (value.goalId !== goalId || !Array.isArray(value.scopes)) throw Error('PLANNER_PANEL_INVALID');
        if (!disposed) { setPanel(value); setFailed(false); }
      } catch { if (!disposed) setFailed(true); }
      finally {
        clearTimeout(timeout);
        if (!disposed) timer = setTimeout(() => void poll(), 20000);
      }
    };
    void poll();
    return () => { disposed = true; clearTimeout(timer); controller?.abort(); };
  }, [goalId, refresh]);
  const stale = !!panel && Date.now() - Date.parse(panel.observedAt) > 90000;
  return (
    <section className="scoped-planners" aria-label="Planejamento por escopo">
      <header>
        <strong>Planejamento por escopo</strong>
        {panel && <span>{panel.scopes.filter(scope => scope.planner.state === 'running').length} planejando · limite {panel.maxActive}</span>}
      </header>
      {(failed || stale) && <p className="planner-read-warning" role="status">
        {panel ? 'Dados anteriores. Atualização indisponível.' : 'Não foi possível consultar os planejadores.'}
        <button className="text-button" onClick={() => setRefresh(value => value + 1)}>Atualizar</button>
      </p>}
      {!panel && !failed && <p className="planner-empty" role="status">Consultando planejadores…</p>}
      {panel && !panel.scopes.length && <p className="planner-empty">Nenhum escopo de planejamento registrado.</p>}
      {panel && <div className="scoped-planner-grid">{panel.scopes.map(scope => (
        <article className="scoped-planner-card" key={scope.scope} aria-label={'Planejamento: ' + scope.label}>
          <div className="scoped-planner-title">
            <h3>{scope.label}</h3>
            <span className={'flow-badge ' + (scope.planner.state === 'running' ? 'running' : '')}>
              {panel.goalState === 'paused' && !scope.planner.occupied ? 'Pausado' : labels[scope.planner.state] ?? scope.planner.state}
            </span>
          </div>
          <p className="planner-owner">Responsável: {scope.planner.taskId ? scope.planner.model ?? 'Planejador do escopo' : 'Planejador sob demanda'}</p>
          <p className="planner-next"><span>Próxima entrega</span><strong>{scope.nextDelivery?.title ?? 'Sem entrega pendente registrada'}</strong></p>
          {scope.blocker && <p className="planner-blocker">{scope.blocker.reason}</p>}
          <p className="planner-capacity">{scope.workers.running} workers executando · {scope.workers.ready} tarefas prontas
            {!!scope.workers.confirming && ` · ${scope.workers.confirming} em confirmação`}</p>
          <p className="planner-advance">{scope.lastAdvance
            ? `${scope.lastAdvance.kind === 'auxiliary' ? 'Evidência auxiliar aceita' : 'Etapa da entrega aceita'} ${age(scope.lastAdvance.at)}`
            : 'Sem avanço validado registrado'}</p>
          <details>
            <summary>Ver decisão e evidências</summary>
            <div className="planner-details">
              {scope.planner.account && <p>Conta: {scope.planner.account}</p>}
              {scope.blocker?.nextAction && <p>Encaminhamento: {scope.blocker.nextAction}</p>}
              {scope.blocker?.code && <small>Registro: {scope.blocker.code}</small>}
              <p>{scope.workers.queued} workers na fila · {scope.details.canonicalRemaining} etapas da entrega pendentes</p>
              {scope.lastAdvance && <button className="text-button" onClick={() => inspect(scope.lastAdvance!.taskId)}>Ver aceite: {scope.lastAdvance.title}</button>}
              {scope.nextDelivery && <button className="text-button" onClick={() => inspect(scope.nextDelivery!.taskId)}>Ver próxima entrega</button>}
              {scope.planner.taskId && <button className="text-button" onClick={() => inspect(scope.planner.taskId!)}>Ver planejador</button>}
              {scope.details.planningUpdatedAt && <small>Planejamento atualizado {age(scope.details.planningUpdatedAt)}</small>}
              <small>Escopo: {scope.scope}</small>
            </div>
          </details>
        </article>
      ))}</div>}
      {panel && <small className="planner-observed">Consulta {age(panel.observedAt)} · avanço exige aceite; atividade do executor não comprova entrega.</small>}
    </section>
  );
}
