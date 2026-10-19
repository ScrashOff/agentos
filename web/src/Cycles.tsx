import React, { useEffect, useState } from 'react';
import {
  Plus,
  Play,
  Pause,
  ArrowLeft,
  RefreshCw,
  Check,
  Clock,
  AlertCircle,
  ChevronRight,
  Settings,
  Trash2,
} from 'lucide-react';
import './cycles.css';
type Row = Record<string, any>;
async function api(url: string, method = 'GET', body?: unknown) {
  const r = await fetch('/api' + url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const d = await r.json();
  if (!r.ok) throw Error(d.message ?? 'Falha na operação');
  return d;
}
const labels: Row = {
  draft: 'Rascunho',
  active: 'Vigiando',
  paused: 'Pausado',
  queued: 'Na fila',
  running: 'Executando',
  waiting: 'Aguardando',
  recovering: 'Recuperando',
  retry: 'Retomada agendada',
  attention: 'Precisa de você',
  reconciling: 'Confirmando executor',
  completed: 'Concluído',
  cancelled: 'Cancelado',
  cancelling: 'Encerrando',
  failed: 'Falhou',
};
const date = (v: any) => (v ? new Date(v).toLocaleString('pt-BR') : '—');
const emptyStep = (index: number): Row => ({
  key: 'etapa-' + index,
  title: 'Nova etapa',
  kind: 'agent',
  instructions: '',
  dependsOn: [],
  seconds: 60,
  maxAttempts: 3,
  readOnly: false,
  parameters: {},
  criteria: [{ id: 'report', label: 'Relatório da execução', type: 'artifact', name: 'report.md' }],
});
export function Cycles({ onManager }: { onManager: (message: string) => void }) {
  const [cycles, setCycles] = useState<Row[]>([]),
    [selected, setSelected] = useState<string | null>(
      new URLSearchParams(location.hash.slice(1)).get('cycle'),
    ),
    [runId, setRunId] = useState<string | null>(
      new URLSearchParams(location.hash.slice(1)).get('cycleRun'),
    ),
    [detail, setDetail] = useState<Row | null>(null),
    [run, setRun] = useState<Row | null>(null),
    [error, setError] = useState(''),
    [busy, setBusy] = useState(false),
    [editing, setEditing] = useState<Row | null>(null),
    [showNew, setShowNew] = useState(false),
    [health, setHealth] = useState<Row | null>(null);
  async function refresh() {
    try {
      const list = await api('/cycles');
      setCycles(list.cycles);
      setHealth(list.health);
      if (selected) setDetail(await api('/cycles/' + selected));
      if (runId) setRun(await api('/cycle-runs/' + runId));
      setError('');
    } catch (e: any) {
      setError(e.message);
    }
  }
  useEffect(() => {
    void refresh();
    const t = setInterval(() => void refresh(), 4000);
    return () => clearInterval(t);
  }, [selected, runId]);
  function open(id: string | null, execution: string | null = null) {
    setSelected(id);
    setRunId(execution);
    setRun(null);
    setDetail(null);
    history.replaceState(
      null,
      '',
      id ? '#cycle=' + id + (execution ? '&cycleRun=' + execution : '') : '#cycles',
    );
  }
  async function action(fn: () => Promise<any>) {
    setBusy(true);
    try {
      const r = await fn();
      await refresh();
      return r;
    } catch (e: any) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }
  const current = detail?.cycle;
  return (
    <div className="cycles-space">
      <div className="cycle-heading">
        <div>
          {selected ? (
            <button className="cycle-back" onClick={() => (runId ? open(selected) : open(null))}>
              <ArrowLeft size={16} />
              {runId ? 'Voltar ao ciclo' : 'Todos os ciclos'}
            </button>
          ) : (
            <span className="cycle-eyebrow">OPERAÇÕES CONTÍNUAS</span>
          )}
          <h2>{runId ? (run?.occurrence.title ?? 'Execução') : (current?.name ?? 'Ciclos')}</h2>
          <p>
            {runId
              ? 'Etapas, resultados e próximos passos desta execução.'
              : (current?.definition.description ??
                'Rotinas com espaço, fluxo e histórico próprios.')}
          </p>
        </div>
        <div className="cycle-buttons">
          <button
            className="button"
            disabled={busy}
            onClick={() => void refresh()}
            aria-label="Atualizar ciclos"
          >
            <RefreshCw size={15} />
          </button>
          {!selected && (
            <button className="button primary" onClick={() => setShowNew(true)}>
              <Plus size={16} />
              Novo ciclo
            </button>
          )}
          {current && !runId && (
            <>
              <button className="button" onClick={() => setEditing(current)}>
                <Settings size={15} />
                Editar fluxo
              </button>
              <button
                className="button"
                disabled={busy}
                onClick={() =>
                  void action(() =>
                    api('/cycles/' + selected + '/state', 'POST', {
                      version: current.version,
                      state: current.state === 'active' ? 'paused' : 'active',
                    }),
                  )
                }
              >
                {current.state === 'active' ? <Pause size={15} /> : <Play size={15} />}{' '}
                {current.state === 'active' ? 'Pausar' : 'Ativar'}
              </button>
              <button
                className="button primary"
                disabled={busy}
                onClick={() =>
                  void action(async () => {
                    const r = await api('/cycles/' + selected + '/run', 'POST', {
                      requestKey: crypto.randomUUID(),
                    });
                    open(selected, r.id);
                  })
                }
              >
                <Play size={15} />
                Executar agora
              </button>
            </>
          )}
        </div>
      </div>
      {error && (
        <div className="cycle-alert" role="alert">
          {error}
        </div>
      )}
      {!health || Date.now() - Date.parse(health.checkedAt) > 15000 ? (
        <div className="cycle-alert">
          <AlertCircle size={16} /> Sem confirmação recente do motor de ciclos.
        </div>
      ) : null}
      {!selected && (
        <div className="cycle-grid">
          {cycles.map((c) => (
            <button key={c.id} className="cycle-card" onClick={() => open(c.id)}>
              <div className="cycle-card-top">
                <span
                  className={
                    'cycle-badge ' +
                    (c.last_error
                      ? 'attention'
                      : c.attention_count
                        ? 'attention'
                        : c.active_count
                          ? 'running'
                          : c.state)
                  }
                >
                  {c.last_error
                    ? 'Conexão indisponível'
                    : c.attention_count
                      ? 'Precisa de você'
                      : c.active_count
                        ? 'Executando'
                        : labels[c.state]}
                </span>
                <ChevronRight size={18} />
              </div>
              <h3>{c.name}</h3>
              <p>{c.definition.description || c.project}</p>
              <div className="cycle-card-count">
                <strong>{c.active_count}</strong> execuções em aberto
              </div>
              <div className="cycle-card-footer">
                <span>
                  Última consulta válida
                  <br />
                  <b>{date(c.last_success_at)}</b>
                </span>
                <span>
                  Próxima consulta
                  <br />
                  <b>{c.state === 'active' ? date(c.next_check_at) : 'Pausado'}</b>
                </span>
              </div>
            </button>
          ))}
          {!cycles.length && (
            <div className="cycle-empty">
              <Clock size={32} />
              <h3>Crie sua primeira operação contínua</h3>
              <p>
                Defina um gatilho e as etapas que devem acontecer. Cada execução terá seu próprio
                acompanhamento.
              </p>
              <button className="button primary" onClick={() => setShowNew(true)}>
                Criar ciclo
              </button>
            </div>
          )}
        </div>
      )}
      {current && !runId && (
        <>
          <div className="cycle-flow">
            {current.definition.steps.map((s: Row, i: number) => (
              <div key={s.key}>
                <span>{i + 1}</span>
                <b>{s.title}</b>
                <small>
                  {
                    { agent: 'Agente', action: 'Ação', wait: 'Espera', review: 'Decisão' }[
                      s.kind as string
                    ]
                  }
                </small>
              </div>
            ))}
          </div>
          <div className="cycle-section-heading">
            <h3>Execuções</h3>
            <button
              className="button"
              onClick={() =>
                onManager(
                  `Quero trabalhar no ciclo ${current.name}, ID ${current.id}. Consulte o fluxo e as execuções antes de agir.`,
                )
              }
            >
              Conversar com o gestor
            </button>
          </div>
          <div className="cycle-run-list">
            {detail?.occurrences.map((o: Row) => (
              <button key={o.id} onClick={() => open(selected, o.id)}>
                <span className={'cycle-badge ' + o.state}>{labels[o.state] ?? o.state}</span>
                <div>
                  <b>{o.title}</b>
                  <small>{o.reason || 'Versão ' + o.version + ' · ' + date(o.created_at)}</small>
                </div>
                <ChevronRight size={18} />
              </button>
            ))}
            {!detail?.occurrences.length && (
              <p className="cycle-empty">
                Nenhuma execução ainda. Ative o ciclo ou execute uma vez.
              </p>
            )}
          </div>
        </>
      )}
      {run && (
        <>
          <div className="cycle-run-status">
            <span className={'cycle-badge ' + run.occurrence.state}>
              {labels[run.occurrence.state]}
            </span>
            <strong>
              {run.steps.filter((s: Row) => s.state === 'completed').length}/{run.steps.length}{' '}
              etapas concluídas
            </strong>
            <span>{run.occurrence.reason}</span>
            <button
              className="button"
              onClick={() =>
                onManager(
                  `Acompanhe e me ajude com a execução de ciclo ${run.occurrence.id}. Consulte etapas, tentativas e evidências.`,
                )
              }
            >
              Pedir ao gestor
            </button>
            {!['completed', 'cancelled'].includes(run.occurrence.state) && (
              <button
                className="button"
                onClick={() =>
                  void action(() =>
                    api('/cycle-runs/' + runId + '/command', 'POST', {
                      command: run.occurrence.state === 'paused' ? 'resume' : 'pause',
                    }),
                  )
                }
              >
                {run.occurrence.state === 'paused' ? 'Retomar' : 'Pausar novos passos'}
              </button>
            )}
            {!['completed', 'cancelled', 'cancelling'].includes(run.occurrence.state) && (
              <button
                className="button"
                disabled={busy}
                onClick={() =>
                  void action(() =>
                    api('/cycle-runs/' + runId + '/command', 'POST', { command: 'cancel' }),
                  )
                }
              >
                Encerrar execução
              </button>
            )}
          </div>
          <div className="cycle-step-list">
            {run.steps.map((s: Row, i: number) => {
              const attempts = run.attempts.filter((a: Row) => a.step_id === s.id);
              const latest = attempts.at(-1);
              const events = run.events.filter((e: Row) => e.step_id === s.id);
              const activity = events.at(-1);
              return (
                <article key={s.id} className={'cycle-step ' + s.state}>
                  <div className="cycle-step-title">
                    <span className="cycle-step-number">
                      {s.state === 'completed' ? <Check size={16} /> : i + 1}
                    </span>
                    <div>
                      <h3>{s.definition.title}</h3>
                      <p>
                        {s.reason || s.output?.summary || s.definition.instructions.slice(0, 110)}
                      </p>
                    </div>
                    <span className={'cycle-badge ' + s.state}>{labels[s.state] ?? s.state}</span>
                  </div>
                  {s.state === 'waiting' && <p>Até {date(s.next_at)}</p>}
                  {latest && (
                    <div className="cycle-step-meta">
                      <span>{latest.model || 'Ação registrada'}</span>
                      <span>Tentativa {latest.number}</span>
                      <span>Última atividade: {date(activity?.created_at)}</span>
                    </div>
                  )}
                  {s.state === 'attention' && (
                    <div className="cycle-attention">
                      <b>
                        {s.definition.kind === 'review'
                          ? 'Sua decisão é necessária'
                          : 'A etapa precisa de ajuste'}
                      </b>
                      <p>{s.reason}</p>
                      <button
                        className="button"
                        onClick={() =>
                          void action(async () => {
                            const note = window.prompt(
                              s.definition.kind === 'review'
                                ? 'Registre a decisão e o escopo aprovado:'
                                : 'O que foi corrigido ou deve mudar nesta retomada?',
                            );
                            if (note && note.trim().length >= 5)
                              await api('/cycle-steps/' + s.id + '/resolve', 'POST', {
                                decision: s.definition.kind === 'review' ? 'approve' : 'retry',
                                note,
                              });
                          })
                        }
                      >
                        {s.definition.kind === 'review'
                          ? 'Registrar aprovação'
                          : 'Retomar com orientação'}
                      </button>
                    </div>
                  )}
                  <details>
                    <summary>Detalhes, tentativas e arquivos</summary>
                    <p className="cycle-instructions">{s.definition.instructions}</p>
                    {s.output && (
                      <details>
                        <summary>Resultado estruturado</summary>
                        <pre>{JSON.stringify(s.output, null, 2)}</pre>
                      </details>
                    )}
                    {attempts.map((a: Row) => (
                      <div className="cycle-attempt" key={a.id}>
                        <b>
                          Tentativa {a.number} · {labels[a.state] ?? a.state}
                        </b>
                        <p>{a.summary}</p>
                        <small>
                          {date(a.started_at)} · {a.error_code ?? ''}
                        </small>
                        <div>
                          {run.artifacts
                            .filter((f: Row) => f.attempt_id === a.id)
                            .map((f: Row) => (
                              <a
                                key={f.id}
                                className="button"
                                href={'/api/cycle-artifacts/' + f.id}
                              >
                                {f.name}
                              </a>
                            ))}
                        </div>
                      </div>
                    ))}
                    <details>
                      <summary>Atividade técnica ({events.length})</summary>
                      <pre>
                        {events
                          .map(
                            (e: Row) =>
                              date(e.created_at) + ' ' + e.kind + ' ' + JSON.stringify(e.payload),
                          )
                          .join('\n')}
                      </pre>
                    </details>
                  </details>
                </article>
              );
            })}
          </div>
        </>
      )}
      {(showNew || editing) && (
        <CycleEditor
          initial={editing}
          close={() => {
            setShowNew(false);
            setEditing(null);
          }}
          saved={(id) => {
            setShowNew(false);
            setEditing(null);
            open(id);
            void refresh();
          }}
        />
      )}
    </div>
  );
}
function CycleEditor({
  initial,
  close,
  saved,
}: {
  initial: Row | null;
  close: () => void;
  saved: (id: string) => void;
}) {
  const [value, setValue] = useState<Row>(
    initial?.definition ?? {
      name: '',
      description: '',
      project: 'agentos',
      trigger: { kind: 'manual', seconds: 60, includeExisting: false },
      concurrency: 1,
      steps: [emptyStep(1)],
      allowedActions: [],
      allowedAccounts: [],
    },
  );
  const [error, setError] = useState(''),
    [busy, setBusy] = useState(false),
    [workspaces, setWorkspaces] = useState<Row[]>([]),
    [actions, setActions] = useState<Row[]>([]),
    [accounts, setAccounts] = useState<Row[]>([]);
  const [parameterErrors, setParameterErrors] = useState<Record<string, boolean>>({});
  useEffect(() => {
    void api('/workspaces')
      .then(setWorkspaces)
      .catch(() => {});
    void api('/cycles/actions')
      .then(setActions)
      .catch(() => {});
    void api('/connections')
      .then((d) => setAccounts(d.accounts ?? []))
      .catch(() => {});
  }, []);
  const set = (key: string, v: any) => setValue((prev) => ({ ...prev, [key]: v }));
  const step = (i: number, key: string, v: any) =>
    setValue((prev) => ({
      ...prev,
      steps: prev.steps.map((s: Row, n: number) => (n === i ? { ...s, [key]: v } : s)),
    }));
  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (Object.keys(parameterErrors).length) return;
    setBusy(true);
    try {
      const definition = {
        ...value,
        allowedActions: [
          ...new Set(value.steps.filter((s: Row) => s.kind === 'action').map((s: Row) => s.action)),
        ],
      };
      const r = await api(
        initial ? '/cycles/' + initial.id : '/cycles',
        initial ? 'PUT' : 'POST',
        initial
          ? { version: initial.version, definition }
          : { requestKey: crypto.randomUUID(), definition },
      );
      saved(r.id);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="cycle-modal">
      <form onSubmit={submit}>
        <div className="cycle-section-heading">
          <h2>{initial ? 'Editar fluxo' : 'Novo ciclo'}</h2>
          <button type="button" className="button" onClick={close}>
            Fechar
          </button>
        </div>
        {error && <div className="cycle-alert">{error}</div>}
        <label>
          Nome
          <input required value={value.name} onChange={(e) => set('name', e.target.value)} />
        </label>
        <label>
          O que este ciclo deve fazer?
          <textarea
            value={value.description}
            onChange={(e) => set('description', e.target.value)}
          />
        </label>
        <div className="cycle-fields">
          <label>
            Projeto
            <input
              required
              value={value.project}
              onChange={(e) => set('project', e.target.value)}
            />
          </label>
          <label>
            Gatilho
            <select
              value={value.trigger.kind}
              onChange={(e) =>
                set('trigger', {
                  ...value.trigger,
                  kind: e.target.value,
                  ...(e.target.value === 'bitrix'
                    ? {
                        groupId: value.trigger.groupId ?? 217,
                        stageId: value.trigger.stageId ?? 5655,
                      }
                    : {}),
                })
              }
            >
              <option value="manual">Execução manual</option>
              <option value="interval">A cada intervalo</option>
              <option value="bitrix">Entrada no funil Bitrix</option>
            </select>
          </label>
          {value.trigger.kind !== 'manual' && (
            <label>
              Intervalo em segundos
              <input
                type="number"
                min="30"
                value={value.trigger.seconds}
                onChange={(e) =>
                  set('trigger', { ...value.trigger, seconds: Number(e.target.value) })
                }
              />
            </label>
          )}
          <label>
            Agentes simultâneos
            <input
              type="number"
              min="1"
              max="10"
              value={value.concurrency}
              onChange={(e) => set('concurrency', Number(e.target.value))}
            />
          </label>
        </div>
        {value.trigger.kind === 'bitrix' && (
          <div className="cycle-fields">
            <label>
              Grupo
              <input
                type="number"
                value={value.trigger.groupId ?? 217}
                onChange={(e) =>
                  set('trigger', { ...value.trigger, groupId: Number(e.target.value) })
                }
              />
            </label>
            <label>
              Etapa
              <input
                type="number"
                value={value.trigger.stageId ?? 5655}
                onChange={(e) =>
                  set('trigger', { ...value.trigger, stageId: Number(e.target.value) })
                }
              />
            </label>
            <label>
              <input
                type="checkbox"
                checked={value.trigger.includeExisting}
                onChange={(e) =>
                  set('trigger', { ...value.trigger, includeExisting: e.target.checked })
                }
              />
              Incluir cards já existentes ao ativar
            </label>
          </div>
        )}
        <h3>Etapas do ciclo</h3>
        {value.steps.map((s: Row, i: number) => (
          <fieldset key={s.key}>
            <legend>Etapa {i + 1}</legend>
            <div className="cycle-fields">
              <label>
                Nome
                <input
                  required
                  value={s.title}
                  onChange={(e) => step(i, 'title', e.target.value)}
                />
              </label>
              <label>
                Tipo
                <select
                  value={s.kind}
                  onChange={(e) =>
                    setValue((prev) => ({
                      ...prev,
                      steps: prev.steps.map((x: Row, n: number) =>
                        n === i
                          ? {
                              ...x,
                              kind: e.target.value,
                              action: undefined,
                              criteria: e.target.value === 'agent' ? emptyStep(1).criteria : [],
                            }
                          : x,
                      ),
                    }))
                  }
                >
                  <option value="agent">Agente: implementar, investigar ou testar</option>
                  <option value="action">Executar ação cadastrada</option>
                  <option value="wait">Aguardar período</option>
                  <option value="review">Decisão explícita</option>
                </select>
              </label>
              <button
                type="button"
                className="button"
                disabled={value.steps.length === 1}
                onClick={() =>
                  set(
                    'steps',
                    value.steps
                      .filter((_: any, n: number) => n !== i)
                      .map((x: Row) => ({
                        ...x,
                        dependsOn: x.dependsOn.filter((k: string) => k !== s.key),
                      })),
                  )
                }
                aria-label="Remover etapa"
              >
                <Trash2 size={15} />
              </button>
            </div>
            <label>
              Instruções
              <textarea
                value={s.instructions}
                onChange={(e) => step(i, 'instructions', e.target.value)}
              />
            </label>
            {s.kind === 'agent' && (
              <>
                <div className="cycle-fields">
                  <label>
                    Workspace
                    <select
                      required
                      value={s.workspace ?? ''}
                      onChange={(e) => step(i, 'workspace', e.target.value)}
                    >
                      <option value="">Selecione</option>
                      {workspaces
                        .filter((w) => (w.id ?? '').startsWith(value.project + '/'))
                        .map((w) => (
                          <option key={w.id} value={w.id}>
                            {w.id}
                          </option>
                        ))}
                    </select>
                  </label>
                  <label>
                    Conta
                    <select
                      value={s.accountId ?? ''}
                      onChange={(e) => step(i, 'accountId', e.target.value || undefined)}
                    >
                      <option value="">Automática</option>
                      {accounts.map((a) => (
                        <option key={a.id} value={a.id}>
                          {a.label}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <label>
                  Modelo (vazio usa seleção automática)
                  <input
                    value={s.model ?? ''}
                    onChange={(e) => step(i, 'model', e.target.value || undefined)}
                    list={'models-' + i}
                  />
                  <datalist id={'models-' + i}>
                    {[
                      ...new Set(
                        accounts
                          .filter((a) => !s.accountId || a.id === s.accountId)
                          .flatMap((a) => a.models ?? []),
                      ),
                    ].map((m) => (
                      <option key={String(m)} value={String(m)} />
                    ))}
                  </datalist>
                </label>
                <label>
                  Arquivo de entrega
                  <input
                    value={s.criteria[0]?.name ?? 'report.md'}
                    onChange={(e) =>
                      step(i, 'criteria', [
                        {
                          id: 'report',
                          label: 'Entrega da etapa',
                          type: 'artifact',
                          name: e.target.value,
                        },
                      ])
                    }
                  />
                </label>
              </>
            )}
            {s.kind === 'action' && (
              <>
                <label>
                  Ação
                  <select
                    required
                    value={s.action ?? ''}
                    onChange={(e) => step(i, 'action', e.target.value)}
                  >
                    <option value="">Selecione</option>
                    {actions.map((a) => (
                      <option key={a.id} value={a.id}>
                        {a.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Parâmetros da ação (JSON)
                  <textarea
                    defaultValue={JSON.stringify(s.parameters ?? {}, null, 2)}
                    onChange={(e) => {
                      try {
                        const parsed = JSON.parse(e.target.value);
                        if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object')
                          throw Error();
                        step(i, 'parameters', parsed);
                        setParameterErrors((prev) => {
                          const next = { ...prev };
                          delete next[s.key];
                          return next;
                        });
                        setError('');
                      } catch {
                        setParameterErrors((prev) => ({ ...prev, [s.key]: true }));
                        setError('Parâmetros precisam ser um objeto JSON válido.');
                      }
                    }}
                  />
                </label>
              </>
            )}
            {s.kind === 'wait' && (
              <label>
                Duração em segundos
                <input
                  type="number"
                  min="1"
                  value={s.seconds}
                  onChange={(e) => step(i, 'seconds', Number(e.target.value))}
                />
              </label>
            )}
            <div className="cycle-dependencies">
              <span>Começar depois de:</span>
              {value.steps
                .filter((x: Row) => x.key !== s.key)
                .map((x: Row) => (
                  <label key={x.key}>
                    <input
                      type="checkbox"
                      checked={s.dependsOn.includes(x.key)}
                      onChange={(e) =>
                        step(
                          i,
                          'dependsOn',
                          e.target.checked
                            ? [...s.dependsOn, x.key]
                            : s.dependsOn.filter((k: string) => k !== x.key),
                        )
                      }
                    />
                    {x.title}
                  </label>
                ))}
            </div>
          </fieldset>
        ))}
        <div className="cycle-section-heading">
          <button
            type="button"
            className="button"
            onClick={() =>
              set('steps', [
                ...value.steps,
                { ...emptyStep(Date.now()), dependsOn: [value.steps.at(-1).key] },
              ])
            }
          >
            <Plus size={15} />
            Adicionar etapa
          </button>
          <button
            type="submit"
            disabled={busy || Object.keys(parameterErrors).length > 0}
            className="button primary"
          >
            {busy ? 'Salvando…' : 'Salvar ciclo'}
          </button>
        </div>
        <p className="cycle-help">
          Salvar cria uma versão do fluxo. Execuções já iniciadas preservam sua versão.
        </p>
      </form>
    </div>
  );
}
