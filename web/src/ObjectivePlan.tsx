import React from 'react';
type Row = Record<string, any>;
export function ObjectivePlan({ detail }: { detail: Row }) {
  if (!detail.objective) return null;
  const o = detail.objective,
    run = detail.planning?.[0],
    revision = detail.revisions?.[0],
    plan = revision?.plan;
  const states: Row = {
    planning: 'Preparando o plano',
    replanning: 'Revisando a abordagem',
    planned: 'Plano pronto para consulta',
    executing: 'Plano em execução',
    waiting_external: 'É necessária uma informação ou decisão',
    waiting_quota: 'Aguardando conta para planejar',
    waiting_predecessor: 'Próxima leva · aguarda o aceite da anterior',
  };
  return (
    <section className="objective-plan" aria-label="Planejamento automático">
      <div className="section-caption">
        <h2>
          {detail.goal.state === 'completed'
            ? 'Objetivo concluído'
            : (states[o.status] ?? o.status)}
        </h2>
        <span>
          {o.mode === 'plan' ? 'Somente planejamento' : 'Planejamento e execução'} · até{' '}
          {o.request.requestedCapacity} frentes
        </span>
      </div>
      {run?.state === 'waiting_quota' && <p className="goal-notice">{run.error}</p>}
      {o.reason && <p className="goal-notice">{o.reason}</p>}
      {run?.state === 'running' && (
        <p>
          O Planner está definindo entregas e dependências. {run.model} · o pedido está salvo e
          continua após fechar esta tela.
        </p>
      )}
      {plan && (
        <>
          <p>{plan.summary}</p>
          {plan.incremental && (
            <p className="goal-notice">
              Entregas incrementais · {plan.incremental.completedRequirementIds.length} de {plan.requirements.length} requisitos verificados em lotes anteriores.
              {' '}Lote atual: {plan.requirements.find((r: Row) => r.id === plan.incremental.selectedRequirementId)?.description}
              {' '}Próximo responsável: {run?.state === 'running' || run?.state === 'queued' ? 'planejador' : 'execução e verificação independente'}.
              {plan.incremental.pendingRequirementIds.length > 0 && ' O próximo lote será preparado automaticamente após o aceite deste; o objetivo permanece aberto.'}
            </p>
          )}
          <div className="plan-requirements">
            <strong>O que precisa ser comprovado</strong>
            <ul>
              {plan.requirements.map((r: Row) => (
                <li key={r.id}>{r.description}</li>
              ))}
            </ul>
          </div>
          <details open={o.mode === 'plan'}>
            <summary>
              Revisão {revision.revision} · {plan.tasks.length} etapas planejadas
            </summary>
            <div className="plan-task-list">
              {plan.tasks.map((t: Row) => (
                <article key={t.key}>
                  <strong>{t.title}</strong>
                  <span>
                    {t.agentId} ·{' '}
                    {t.stage === 'verify'
                      ? 'Validação'
                      : t.stage === 'integrate'
                        ? 'Integração'
                        : 'Execução'}
                  </span>
                  <small>
                    {t.dependencies.length
                      ? 'Após: ' + t.dependencies.join(', ')
                      : 'Pode começar após preparar o workspace'}
                  </small>
                  <p>{t.description}</p>
                </article>
              ))}
            </div>
          </details>
          {plan.approachChange && (
            <p>
              <strong>Mudança de abordagem: </strong>
              {plan.approachChange}
            </p>
          )}
        </>
      )}
      {run?.context?.manifest && (
        <small>
          Contexto: {run.context.manifest.selected.length} memórias ·{' '}
          {run.context.manifest.agents.length} especializações candidatas · seletor{' '}
          {run.context.manifest.selector}
          {run.context.manifest.fallbackReason ? ' · ' + run.context.manifest.fallbackReason : ''}
        </small>
      )}
      {detail.revisions?.length > 1 && (
        <details>
          <summary>Histórico de {detail.revisions.length} revisões</summary>
          {detail.revisions.map((r: Row) => (
            <p key={r.id}>
              Revisão {r.revision}: {r.plan.approachChange || r.plan.summary}
            </p>
          ))}
        </details>
      )}
      {detail.planningAttempts?.length > 0 && (
        <details>
          <summary>Tentativas de planejamento e consumo</summary>
          {detail.planningAttempts.map((a: Row) => (
            <p key={a.planning_run_id + ':' + a.attempt}>
              Tentativa {a.attempt}: {a.error ?? (a.has_output ? 'Proposta recebida' : 'Aguardando resultado')} ·{' '}
              {a.usage?.totalTokens ?? '—'} tokens
            </p>
          ))}
        </details>
      )}
    </section>
  );
}
