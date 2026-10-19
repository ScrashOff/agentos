import React, { useEffect, useState } from 'react';
import { Activity, AlertTriangle, Check, Clock, Terminal } from 'lucide-react';
import {
  executionStatus,
  historicalEstimate,
  actionSummary,
  observedProblems,
  modelActivity,
} from './execution-status';
import './execution.css';
import { latestObservation } from './task-attention';
type Row = Record<string, any>;
const stamp = (value: string) => new Date(value).toLocaleString('pt-BR');
const duration = (value: number) => {
  const s = Math.max(0, Math.floor(value / 1000));
  return s < 60
    ? `${s}s`
    : s < 3600
      ? `${Math.floor(s / 60)}min ${s % 60}s`
      : `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}min`;
};

export function ExecutionDetails({
  task,
  run,
  data,
  compact = false,
}: {
  task: Row;
  run: Row;
  data: Row;
  compact?: boolean;
}) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  const observation = latestObservation(run, data.events);
  const model = modelActivity(run, data.events);
  const status = executionStatus(observation, run, now, model);
  const problems = observedProblems(observation, run);
  const estimate = run.finished_at
    ? task.state === 'accepted'
      ? { title: 'Etapa aceita', detail: 'Os critérios desta etapa foram atendidos.' }
      : {
          title: 'Sem previsão de conclusão',
          detail:
            'A tentativa encerrou, mas a tarefa continua sem aceite. É preciso resolver as pendências antes de estimar o término.',
        }
    : historicalEstimate(task, run, data, now);
  const actions: Row[] = observation?.actions ?? [];
  const current = ['running', 'cancelling'].includes(run.state)
    ? actions.findLast((a) => a.state === 'running')
    : undefined;
  const lastResult = [...actions].reverse().find((a) => a.finishedAt);
  const elapsed = run.started_at
    ? new Date(run.finished_at ?? now).getTime() - new Date(run.started_at).getTime()
    : null;
  const evidence: Row[] = observation?.evidence ?? [];
  const currentEvidence = evidence.filter(
    (e) =>
      !run.started_at || new Date(e.modifiedAt).getTime() >= new Date(run.started_at).getTime(),
  );
  const priorEvidence = evidence.filter((e) => !currentEvidence.includes(e));
  const logs = currentEvidence.filter((e) => e.preview !== null);
  const files = observation?.files ?? [];
  const card = (
    <>
      <div className={'execution-signal ' + status.level}>
        {status.level === 'warning' || status.level === 'unknown' ? (
          <AlertTriangle size={18} />
        ) : (
          <Activity size={18} />
        )}
        <div>
          <strong>{status.title}</strong>
          <p>{status.detail}</p>
        </div>
        {observation?.observedAt && (
          <small title={stamp(observation.observedAt)}>
            Consultado há {duration(now - new Date(observation.observedAt).getTime())}
          </small>
        )}
      </div>
      {model.pending && (
        <p className="model-wait">
          Chamada {model.requests ?? ''} enviada há {duration(now - Date.parse(model.requestedAt))}.
          {typeof model.contextBytes === 'number' &&
            ` Contexto enviado: ${Math.round(model.contextBytes / 1024)} KB.`}{' '}
          Aguardando a resposta para executar o próximo passo.
        </p>
      )}
      {!observation && (
        <p className="inspection-note">
          Nenhuma observação desta tentativa disponível no histórico carregado. Isso não significa
          que nenhuma ferramenta foi executada.
        </p>
      )}
      {problems.map((problem: Row) => (
        <div className="execution-problem" key={problem.source}>
          <strong>{problem.title}</strong>
          <p>{problem.line}</p>
          <small>
            {problem.source} · {stamp(problem.at)}. Ocorrência do log, não o resultado final da
            tarefa.
          </small>
        </div>
      ))}
      {run.finished_at && run.summary && (
        <div className="execution-outcome">
          <h4>Relato da tentativa encerrada</h4>
          <p>{run.summary}</p>
        </div>
      )}
      <div className="execution-numbers">
        <div>
          <small>Tempo desta tentativa</small>
          <strong>{elapsed === null ? 'Não iniciou' : duration(elapsed)}</strong>
        </div>
        <div>
          <small>
            {model.museTool ? 'Última atividade de ferramenta' : 'Último retorno de ferramenta'}
          </small>
          <strong>
            {lastResult
              ? `${duration(now - new Date(lastResult.finishedAt).getTime())} atrás`
              : model.museTool
                ? `${duration(now - Date.parse(model.museTool.created_at))} atrás`
                : 'Ainda sem retorno'}
          </strong>
        </div>
        <div>
          <small>Previsão desta etapa</small>
          <strong>{estimate.title}</strong>
          <p>{estimate.detail}</p>
        </div>
      </div>
      {current && (
        <div className="current-command">
          <span>
            <Terminal size={15} />
            {actionSummary(current)}
          </span>
          <small>Sem retorno há {duration(now - new Date(current.startedAt).getTime())}</small>
          <pre>{current.command}</pre>
        </div>
      )}
      {!current && lastResult && (
        <div className="last-result">
          <strong>Último resultado · {actionSummary(lastResult)}</strong>
          <pre>
            {lastResult.output ||
              (observation?.runtime === 'opencode'
                ? 'Saída da ferramenta não incluída nesta observação.'
                : 'A ferramenta retornou sem texto.')}
          </pre>
        </div>
      )}
      {!!logs.length && (
        <div className="live-output">
          <h4>Saída parcial dos testes e programas</h4>
          <p>Arquivos lidos no workspace. Podem mudar e ainda não são evidências aceitas.</p>
          {logs.slice(0, compact ? 2 : 8).map((file, index) => (
            <details key={file.name} open={index === 0}>
              <summary>
                {file.name}
                <small>Atualizado {stamp(file.modifiedAt)}</small>
              </summary>
              <pre>{file.preview || 'Arquivo ainda vazio.'}</pre>
            </details>
          ))}
        </div>
      )}
      <div className="work-evidence">
        <h4>O que já mudou</h4>
        <p>
          {files.length} arquivos novos ou alterados em relação à base do workspace. Inclui
          alterações preservadas de tentativas anteriores.
        </p>
        {!!files.length && (
          <div className="changed-files">
            {files.map((file: Row) => (
              <span key={file.path}>
                <code>{file.path}</code>
                <small>
                  {file.status === 'new' ? 'Novo arquivo' : `+${file.added} / −${file.removed}`}
                </small>
              </span>
            ))}
          </div>
        )}
        {!!currentEvidence.length && <p>{currentEvidence.map((e) => e.name).join(' · ')}</p>}
        {!!priorEvidence.length && !compact && (
          <details>
            <summary>{priorEvidence.length} arquivos anteriores ao início desta tentativa</summary>
            {priorEvidence.map((e) => (
              <p key={e.name}>
                {e.name} · {stamp(e.modifiedAt)} · não comprova o trabalho atual
              </p>
            ))}
          </details>
        )}
      </div>
      {!compact && (
        <>
          <div className="execution-machine">
            <h4>Executor remoto</h4>
            <p>
              {observation?.process?.available
                ? `Estado: ${observation.process.activeState} / ${observation.process.subState}. Memória: ${Math.round((observation.process.memoryBytes ?? 0) / 1024 / 1024)} MB. Processos: ${(observation.process.processNames ?? []).join(', ') || 'sem nomes disponíveis'}.`
                : 'Sem observação recente do processo remoto.'}
            </p>
          </div>
          <div className="action-history">
            <h4>Histórico de comandos e resultados</h4>
            {model.museTool && (
              <p>
                {model.museTool.payload?.toolEvents ?? 1} eventos de ferramentas do Muse
                registrados. Última operação: {model.museTool.payload?.operation ?? 'ferramenta'} ·{' '}
                {stamp(model.museTool.created_at)}. Esses eventos não confirmam o resultado da
                tarefa.
              </p>
            )}
            {(!model.museTool || actions.length > 0) && (
              <p>
                {observation?.toolCalls ?? 0} chamadas encontradas no registro
                {observation?.traceTruncated ? ' parcial' : ''}; exibindo as últimas{' '}
                {actions.length}. Código de saída zero confirma o término do comando, não o aceite
                do jogo.
              </p>
            )}
            {[...actions].reverse().map((action, index) => (
              <details key={action.id} open={index === 0}>
                <summary>
                  <span>
                    {action.state === 'running' ? (
                      <Clock size={15} />
                    ) : action.state === 'error' ? (
                      <AlertTriangle size={15} />
                    ) : (
                      <Check size={15} />
                    )}
                    {actionSummary(action)}
                  </span>
                  <small>
                    {action.startedAt ? stamp(action.startedAt) : 'Sem horário'} ·{' '}
                    {action.state === 'running'
                      ? 'sem retorno'
                      : `retornou${action.exitCode !== null ? ` · saída ${action.exitCode}` : ''}`}
                  </small>
                </summary>
                <h5>Comando</h5>
                <pre>{action.command}</pre>
                <h5>Resultado</h5>
                <pre>
                  {action.output ||
                    (observation?.runtime === 'opencode' && action.state !== 'running'
                      ? 'Saída da ferramenta não incluída nesta observação.'
                      : 'Ainda sem resultado.')}
                </pre>
              </details>
            ))}
            {!actions.length && (
              <p>
                {model.museTool
                  ? 'A saída detalhada dos comandos não está incluída nesses eventos.'
                  : 'Nenhum comando disponível para esta tentativa.'}
              </p>
            )}
          </div>
        </>
      )}
    </>
  );
  return (
    <section
      className={'execution-details ' + (compact ? 'compact' : '')}
      aria-label="Detalhes ao vivo da execução"
    >
      {card}
    </section>
  );
}
