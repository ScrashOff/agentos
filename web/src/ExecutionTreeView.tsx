import React, { useState, useEffect, useMemo } from 'react';
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Clock,
  Code2,
  Cpu,
  ExternalLink,
  FileCode,
  FileText,
  Filter,
  FolderGit2,
  Layers,
  Maximize2,
  Minimize2,
  Play,
  RefreshCw,
  Search,
  ShieldCheck,
  Sparkles,
  Terminal,
  Wrench,
  X,
} from 'lucide-react';
import './execution-tree.css';
import { taskAttention, latestObservation } from './task-attention';

type Row = Record<string, any>;

interface ExecutionTreeViewProps {
  detail: Row;
  data: Row;
  inspect: (taskId: string) => void;
}

interface TreeNode {
  id: string;
  task: Row;
  role: 'coordinator' | 'specialist' | 'worker' | 'validator';
  level: number;
  children: TreeNode[];
  latestRun?: Row;
  writePaths: string[];
  totalTokens: number;
  hasAttention: boolean;
  attentionReason?: string;
  lastEventText?: string;
}

const activeStates = new Set(['running', 'cancelling', 'reconciling']);

const stateLabels: Record<string, string> = {
  running: 'Em execução',
  accepted: 'Aceita',
  submitted: 'Aguardando validação',
  queued: 'Na fila',
  ready: 'Pronta para despacho',
  blocked_dependencies: 'Aguardando dependência',
  blocked_failed: 'Dependência falhou',
  waiting_quota: 'Aguardando cota',
  waiting_resource: 'Aguardando recurso',
  waiting_configuration: 'Configuração pendente',
  retry_scheduled: 'Retomada agendada',
  reconciling: 'Sem confirmação',
  cancelling: 'Cancelando',
  cancelled: 'Cancelada',
  failed: 'Falhou',
  recovering_artifacts: 'Recuperando artefatos',
  recovery_blocked: 'Correção requer plano',
};

function formatTokens(n: number): string {
  if (!n) return '0';
  if (n < 1000) return `${n}`;
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}k`;
  return `${(n / 1_000_000).toFixed(2)}M`;
}

function formatDuration(startedAt?: string, finishedAt?: string): string {
  if (!startedAt) return '—';
  const start = new Date(startedAt).getTime();
  const end = finishedAt ? new Date(finishedAt).getTime() : Date.now();
  const diffSec = Math.max(0, Math.floor((end - start) / 1000));
  if (diffSec < 60) return `${diffSec}s`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ${diffSec % 60}s`;
  return `${Math.floor(diffSec / 3600)}h ${Math.floor((diffSec % 3600) / 60)}m`;
}

function resolveRole(task: Row): 'coordinator' | 'specialist' | 'worker' | 'validator' {
  if (task.agent_role) {
    const r = task.agent_role.toLowerCase();
    if (r.includes('coord') || r.includes('architect')) return 'coordinator';
    if (r.includes('spec') || r.includes('lead')) return 'specialist';
    if (r.includes('valida') || r.includes('qa') || r.includes('test')) return 'validator';
    return 'worker';
  }
  const title = (task.title || '').toLowerCase();
  if (title.startsWith('coord') || title.startsWith('arquiteto') || title.includes('planejador')) return 'coordinator';
  if (title.startsWith('especialista') || title.includes('specialist') || title.includes('backend') || title.includes('frontend')) return 'specialist';
  if (title.startsWith('valida') || title.includes('qa') || title.includes('test') || title.includes('linter')) return 'validator';
  return 'worker';
}

function resolveWritePaths(task: Row): string[] {
  const execution = task.execution || {};
  if (Array.isArray(execution.writePaths) && execution.writePaths.length > 0) {
    return execution.writePaths;
  }
  if (Array.isArray(execution.paths) && execution.paths.length > 0) {
    return execution.paths;
  }
  if (execution.workspace) {
    return [execution.workspace.split('/').pop() || execution.workspace];
  }
  return [];
}

export function ExecutionTreeView({ detail, data, inspect }: ExecutionTreeViewProps) {
  const { tasks = [], dependencies = [], runs = [], acceptances = [], goal } = detail;
  const [filterRole, setFilterRole] = useState<string>('all');
  const [filterState, setFilterState] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [inspectedNode, setInspectedNode] = useState<TreeNode | null>(null);
  const [drawerTab, setDrawerTab] = useState<'overview' | 'prompt' | 'files' | 'logs' | 'criteria'>('overview');
  const [now, setNow] = useState(Date.now());

  // Live timer tick for active runtimes
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  // Compute Tree Nodes
  const { treeRoots, allNodeIds, metrics } = useMemo(() => {
    const runsByTask = new Map<string, Row[]>();
    for (const r of runs) {
      const list = runsByTask.get(r.task_id) || [];
      list.push(r);
      runsByTask.set(r.task_id, list);
    }

    // Build raw nodes
    const nodeMap = new Map<string, TreeNode>();
    let runningCount = 0;
    let acceptedCount = 0;
    let attentionCount = 0;
    let globalTokens = 0;

    for (const task of tasks) {
      const taskRuns = (runsByTask.get(task.id) || []).sort(
        (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      );
      const latestRun = taskRuns[0];
      const role = resolveRole(task);
      const writePaths = resolveWritePaths(task);
      const alert = taskAttention(task, data);

      let nodeTokens = 0;
      for (const r of taskRuns) {
        if (r.usage) {
          nodeTokens += (r.usage.prompt_tokens || 0) + (r.usage.completion_tokens || 0);
        }
      }
      globalTokens += nodeTokens;

      if (task.state === 'running') runningCount++;
      if (task.state === 'accepted') acceptedCount++;
      if (alert !== null) attentionCount++;

      // Event snippet
      const latestEvent = data.events?.find(
        (e: Row) =>
          (latestRun && (e.payload?.runId === latestRun.id || e.entity_id === latestRun.id)) ||
          e.entity_id === task.id
      );

      let lastEventText = '';
      if (latestEvent) {
        const p = latestEvent.payload || {};
        if (latestEvent.kind === 'OPENCODE_TOOL_EVENT') lastEventText = `OpenCode · ${p.tool || 'tool'}${p.detail ? ` (${p.detail})` : ''}`;
        else if (latestEvent.kind === 'MODEL_REQUEST') lastEventText = 'Consultando modelo LLM...';
        else if (latestEvent.kind === 'MODEL_RESPONSE') lastEventText = 'Resposta recebida do modelo';
        else if (latestEvent.kind === 'RUN_STARTED') lastEventText = 'Sessão do agente iniciada';
      }

      nodeMap.set(task.id, {
        id: task.id,
        task,
        role,
        level: task.agent_level ?? (role === 'coordinator' ? 0 : role === 'specialist' ? 1 : 2),
        children: [],
        latestRun,
        writePaths,
        totalTokens: nodeTokens,
        hasAttention: alert !== null,
        attentionReason: alert?.detail,
        lastEventText,
      });
    }

    // Parent-child relationships
    const roots: TreeNode[] = [];
    const childIds = new Set<string>();

    // Pass 1: explicit parent_task_id
    for (const node of nodeMap.values()) {
      const parentId = node.task.parent_task_id;
      if (parentId && nodeMap.has(parentId) && parentId !== node.id) {
        nodeMap.get(parentId)!.children.push(node);
        childIds.add(node.id);
      }
    }

    // Pass 2: dependency inference for unparented nodes
    const depMap = new Map<string, string[]>();
    for (const d of dependencies) {
      const list = depMap.get(d.task_id) || [];
      list.push(d.depends_on);
      depMap.set(d.task_id, list);
    }

    for (const node of nodeMap.values()) {
      if (!childIds.has(node.id) && !node.task.parent_task_id) {
        const parents = depMap.get(node.id);
        if (parents && parents.length > 0) {
          const directParentId = parents[0];
          const directParent = nodeMap.get(directParentId);
          // Only nest if parent is higher in hierarchy
          if (directParent && directParent.level < node.level && directParent.id !== node.id) {
            directParent.children.push(node);
            childIds.add(node.id);
          }
        }
      }
    }

    // Root nodes are those not claimed as children
    for (const node of nodeMap.values()) {
      if (!childIds.has(node.id)) {
        roots.push(node);
      }
    }

    // Sort roots by role (coordinators first), then level, then title
    const roleWeight = { coordinator: 0, specialist: 1, worker: 2, validator: 3 };
    roots.sort((a, b) => roleWeight[a.role] - roleWeight[b.role] || a.task.title.localeCompare(b.task.title));

    const allIds = new Set(nodeMap.keys());
    return {
      treeRoots: roots,
      allNodeIds: allIds,
      metrics: {
        total: tasks.length,
        running: runningCount,
        accepted: acceptedCount,
        attention: attentionCount,
        tokens: globalTokens,
      },
    };
  }, [tasks, dependencies, runs, data, acceptances]);

  // Keep inspectedNode in sync if tasks change
  useEffect(() => {
    if (inspectedNode) {
      const freshTask = tasks.find((t: Row) => t.id === inspectedNode.id);
      if (freshTask) {
        setInspectedNode((prev) => (prev ? { ...prev, task: freshTask } : null));
      }
    }
  }, [tasks]);

  // Expand all by default on first load
  useEffect(() => {
    if (allNodeIds.size > 0 && expandedNodes.size === 0) {
      setExpandedNodes(new Set(allNodeIds));
    }
  }, [allNodeIds]);

  const toggleExpand = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setExpandedNodes((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const expandAll = () => setExpandedNodes(new Set(allNodeIds));
  const collapseAll = () => setExpandedNodes(new Set());

  // Filter matching
  const matchesFilter = (node: TreeNode): boolean => {
    // Role filter
    if (filterRole !== 'all' && node.role !== filterRole) return false;

    // State filter
    if (filterState === 'running' && node.task.state !== 'running') return false;
    if (filterState === 'accepted' && node.task.state !== 'accepted') return false;
    if (filterState === 'waiting' && !['queued', 'ready', 'blocked_dependencies', 'waiting_quota', 'waiting_resource'].includes(node.task.state)) return false;
    if (filterState === 'attention' && !node.hasAttention) return false;

    // Search query
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      const matchTitle = (node.task.title || '').toLowerCase().includes(q);
      const matchKey = (node.task.key || '').toLowerCase().includes(q);
      const matchRole = node.role.toLowerCase().includes(q);
      const matchScope = node.writePaths.some((p) => p.toLowerCase().includes(q));
      const matchModel = ((node.latestRun?.model || node.task.model || '')).toLowerCase().includes(q);
      return matchTitle || matchKey || matchRole || matchScope || matchModel;
    }

    return true;
  };

  // Check if subtree has any matching node
  const subtreeHasMatch = (node: TreeNode): boolean => {
    if (matchesFilter(node)) return true;
    return node.children.some((child) => subtreeHasMatch(child));
  };

  // Render individual tree node
  const renderNode = (node: TreeNode, depth: number = 0) => {
    const isExpanded = expandedNodes.has(node.id);
    const hasChildren = node.children.length > 0;
    const isRunning = node.task.state === 'running';
    const isAccepted = node.task.state === 'accepted';
    const hasAttention = node.hasAttention;
    const isSelected = inspectedNode?.id === node.id;

    // Resolve model name
    const account = data.accounts?.find(
      (a: Row) => a.id === (node.latestRun?.account_id ?? node.task.account_id)
    );
    const modelName = node.latestRun?.model ?? node.task.model ?? account?.model ?? 'Auto LLM';

    // Role Icon
    const roleIcon = {
      coordinator: <Layers size={13} />,
      specialist: <Cpu size={13} />,
      worker: <Wrench size={13} />,
      validator: <ShieldCheck size={13} />,
    }[node.role];

    const roleName = {
      coordinator: 'Coordenador',
      specialist: 'Especialista',
      worker: 'Operário',
      validator: 'Validador QA',
    }[node.role];

    // Status class
    const statusClass = isRunning
      ? 'running'
      : isAccepted
        ? 'accepted'
        : hasAttention
          ? 'attention'
          : 'waiting';

    return (
      <div className="tree-node-wrapper" key={node.id}>
        <div
          className={`tree-node-card level-${Math.min(node.level, 3)} ${isSelected ? 'selected' : ''}`}
          onClick={() => {
            setInspectedNode(node);
            setDrawerTab('overview');
          }}
          title="Clique para abrir inspeção no painel lateral"
        >
          {/* Header Row */}
          <div className="node-card-header">
            <div className="node-identity">
              {hasChildren ? (
                <button
                  className="expand-toggle-btn"
                  onClick={(e) => toggleExpand(node.id, e)}
                  title={isExpanded ? 'Recolher subagentes' : 'Expandir subagentes'}
                >
                  {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                </button>
              ) : (
                <span style={{ width: 24 }} />
              )}

              <span className={`role-badge ${node.role}`}>
                {roleIcon}
                {roleName} (N{node.level})
              </span>

              <div className="node-title-group">
                <span className="node-title">{node.task.title}</span>
                {node.task.key && <span className="node-key">{node.task.key}</span>}
              </div>
            </div>

            <div className="node-status-area">
              <span className={`node-status-badge ${statusClass}`}>
                {isRunning && <span className="pulse-indicator" />}
                {isAccepted && <CheckCircle2 size={13} />}
                {hasAttention && <AlertTriangle size={13} />}
                {!isRunning && !isAccepted && !hasAttention && <Clock size={13} />}
                {stateLabels[node.task.state] || node.task.state}
              </span>
            </div>
          </div>

          {/* Body Row: Bounded write paths and active actions */}
          <div className="node-card-body">
            {node.writePaths.length > 0 && (
              <div className="node-scopes-wrapper">
                <span className="scope-tag-label">
                  <FileCode size={12} />
                  Escopo delimitado:
                </span>
                {node.writePaths.map((p, i) => (
                  <span className="scope-pill" key={i}>
                    {p}
                  </span>
                ))}
              </div>
            )}

            {isRunning && node.lastEventText && (
              <div className="node-live-action">
                <Activity size={13} className="live-action-icon" />
                <span>{node.lastEventText}</span>
              </div>
            )}
          </div>

          {/* Footer Row: Telemetry, Tokens, Duration and Inspect Button */}
          <div className="node-card-footer">
            <div className="node-meta-left">
              <span className="node-model-badge" title="Modelo de IA atribuído">
                ⚡ {modelName}
              </span>

              <span className="node-meta-item" title="Tokens consumidos por este agente">
                <Sparkles size={12} />
                {formatTokens(node.totalTokens)} tokens
              </span>

              <span className="node-meta-item" title="Tempo de execução">
                <Clock size={12} />
                {formatDuration(node.latestRun?.started_at, node.latestRun?.finished_at)}
              </span>

              {node.latestRun && (
                <span className="node-meta-item">
                  Tentativa {node.latestRun.attempt}
                </span>
              )}

              {hasChildren && (
                <span className="node-meta-item">
                  <FolderGit2 size={12} />
                  {node.children.length} {node.children.length === 1 ? 'subagente' : 'subagentes'}
                </span>
              )}
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
              <button
                className="action-btn-secondary"
                onClick={(e) => {
                  e.stopPropagation();
                  setInspectedNode(node);
                }}
              >
                Inspecionar
                <ChevronRight size={13} />
              </button>

              <button
                className="action-btn-secondary"
                onClick={(e) => {
                  e.stopPropagation();
                  inspect(node.id);
                }}
                title="Abrir em janela completa"
              >
                <ExternalLink size={12} />
              </button>
            </div>
          </div>
        </div>

        {/* Children Subtree */}
        {hasChildren && isExpanded && (
          <div className="tree-children-container">
            {node.children
              .filter((child) => subtreeHasMatch(child))
              .map((child) => renderNode(child, depth + 1))}
          </div>
        )}
      </div>
    );
  };

  const visibleRoots = treeRoots.filter((r) => subtreeHasMatch(r));

  // Compute percentage progress
  const progressPercent = metrics.total > 0
    ? Math.round((metrics.accepted / metrics.total) * 100)
    : 0;

  // Observation and files for lateral drawer
  const drawerObservation = inspectedNode?.latestRun
    ? latestObservation(inspectedNode.latestRun, data.events)
    : null;
  const drawerFiles = drawerObservation?.files || [];
  const drawerActions = drawerObservation?.actions || [];
  const drawerAcceptance = inspectedNode?.latestRun
    ? acceptances.find((a: Row) => a.run_id === inspectedNode.latestRun?.id)
    : null;
  const drawerArtifacts = inspectedNode?.latestRun
    ? data.artifacts?.filter((a: Row) => a.run_id === inspectedNode.latestRun?.id) || []
    : [];

  return (
    <div className="execution-tree-container">
      {/* Goal Master Banner */}
      {goal && (
        <div className="tree-goal-banner">
          <div className="goal-banner-top">
            <div className="goal-banner-meta">
              <span className="goal-project-pill">{goal.project || 'AgentOS'}</span>
              <span className={`goal-status-badge ${goal.state === 'paused' ? 'paused' : 'active'}`}>
                {goal.state === 'paused' ? 'Pausado' : goal.state === 'completed' ? 'Concluído' : 'Operação Ativa'}
              </span>
            </div>
            <span style={{ fontSize: '0.78rem', color: '#94a3b8' }}>
              Meta ID: <code>{goal.id.slice(0, 13)}...</code>
            </span>
          </div>

          <h3 className="goal-banner-title">{goal.title}</h3>

          <div className="goal-banner-progress-area">
            <div className="goal-progress-stats">
              <span>Progresso da Decomposição Hierárquica</span>
              <strong>{metrics.accepted} de {metrics.total} etapas aceitas ({progressPercent}%)</strong>
            </div>
            <div className="goal-progress-bar-wrap">
              <div
                className="goal-progress-bar-fill"
                style={{ width: `${progressPercent}%` }}
              />
            </div>
          </div>
        </div>
      )}

      {/* Telemetry Dashboard Header */}
      <div className="tree-dashboard">
        <div className="tree-metric-card">
          <div className="metric-icon-box purple">
            <Layers size={22} />
          </div>
          <div className="metric-data">
            <span className="metric-label">Escala de Agentes</span>
            <span className="metric-value">{metrics.total} / 30</span>
            <span className="metric-sub">Capacidade concorrente ativa</span>
          </div>
        </div>

        <div className="tree-metric-card">
          <div className="metric-icon-box blue">
            <Activity size={22} />
          </div>
          <div className="metric-data">
            <span className="metric-label">Em Execução Simultânea</span>
            <span className="metric-value">{metrics.running} ativos</span>
            <span className="metric-sub">Workers processando tarefas</span>
          </div>
        </div>

        <div className="tree-metric-card">
          <div className="metric-icon-box green">
            <CheckCircle2 size={22} />
          </div>
          <div className="metric-data">
            <span className="metric-label">Etapas Aceitas</span>
            <span className="metric-value">{metrics.accepted} validadas</span>
            <span className="metric-sub">Critérios e testes aprovados</span>
          </div>
        </div>

        <div className="tree-metric-card">
          <div className="metric-icon-box cyan">
            <Sparkles size={22} />
          </div>
          <div className="metric-data">
            <span className="metric-label">Consumo de Tokens</span>
            <span className="metric-value">{formatTokens(metrics.tokens)}</span>
            <span className="metric-sub">Verboo / OpenAI LLMs</span>
          </div>
        </div>
      </div>

      {/* Controls & Search Toolbar */}
      <div className="tree-controls-bar">
        <div className="tree-search-wrapper">
          <Search size={15} />
          <input
            type="text"
            className="tree-search-input"
            placeholder="Buscar agente, arquivo ou modelo..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>

        <div className="tree-filter-pills">
          <span style={{ fontSize: '0.78rem', color: '#8190a7', marginRight: 4 }}>Papel:</span>
          {[
            ['all', 'Todos'],
            ['coordinator', 'Coordenadores'],
            ['specialist', 'Especialistas'],
            ['worker', 'Operários'],
            ['validator', 'Validadores'],
          ].map(([k, l]) => (
            <button
              key={k}
              className={`filter-pill ${filterRole === k ? 'active' : ''}`}
              onClick={() => setFilterRole(k)}
            >
              {l}
            </button>
          ))}
        </div>

        <div className="tree-filter-pills">
          <span style={{ fontSize: '0.78rem', color: '#8190a7', marginRight: 4 }}>Status:</span>
          {[
            ['all', 'Todos'],
            ['running', 'Em execução'],
            ['waiting', 'Na fila / Espera'],
            ['accepted', 'Aceitas'],
            ['attention', 'Atenção'],
          ].map(([k, l]) => (
            <button
              key={k}
              className={`filter-pill ${filterState === k ? 'active' : ''}`}
              onClick={() => setFilterState(k)}
            >
              {l}
            </button>
          ))}
        </div>

        <div className="tree-view-actions">
          <button className="action-btn-secondary" onClick={expandAll} title="Expandir toda a árvore">
            <Maximize2 size={13} />
            Expandir
          </button>
          <button className="action-btn-secondary" onClick={collapseAll} title="Recolher toda a árvore">
            <Minimize2 size={13} />
            Recolher
          </button>
        </div>
      </div>

      {/* Hierarchical Execution Tree */}
      <div className="execution-tree">
        {visibleRoots.length > 0 ? (
          visibleRoots.map((root) => renderNode(root))
        ) : (
          <div className="tree-empty-state">
            <AlertCircle size={32} color="#8190a7" />
            <h4>Nenhum agente ou tarefa corresponde aos filtros selecionados</h4>
            <p>Ajuste os filtros de papel, status ou o termo de busca para visualizar as tarefas.</p>
          </div>
        )}
      </div>

      {/* Lateral Sliding Inspection Drawer */}
      {inspectedNode && (
        <>
          <div
            className="tree-drawer-backdrop"
            onClick={() => setInspectedNode(null)}
          />

          <aside className="tree-drawer" role="dialog" aria-modal="true">
            <div className="drawer-header">
              <div className="drawer-header-left">
                <div className="drawer-header-meta">
                  <span className={`role-badge ${inspectedNode.role}`}>
                    {inspectedNode.role.toUpperCase()} (N{inspectedNode.level})
                  </span>
                  <span className={`node-status-badge ${inspectedNode.task.state}`}>
                    {stateLabels[inspectedNode.task.state] || inspectedNode.task.state}
                  </span>
                  {inspectedNode.task.key && (
                    <span className="node-key">{inspectedNode.task.key}</span>
                  )}
                </div>
                <h3 className="drawer-title">{inspectedNode.task.title}</h3>
              </div>

              <button
                className="drawer-close-btn"
                onClick={() => setInspectedNode(null)}
                aria-label="Fechar painel"
              >
                <X size={18} />
              </button>
            </div>

            {/* Drawer Tabs */}
            <div className="drawer-tabs">
              {[
                ['overview', 'Visão Geral'],
                ['prompt', 'Prompt & Contrato'],
                ['files', `Arquivos & Diffs (${inspectedNode.writePaths.length})`],
                ['logs', `Logs ao Vivo (${drawerActions.length})`],
                ['criteria', `Critérios (${inspectedNode.task.criteria?.length || 0})`],
              ].map(([k, l]) => (
                <button
                  key={k}
                  className={`drawer-tab ${drawerTab === k ? 'active' : ''}`}
                  onClick={() => setDrawerTab(k as any)}
                >
                  {l}
                </button>
              ))}
            </div>

            {/* Drawer Body Content */}
            <div className="drawer-content">
              {drawerTab === 'overview' && (
                <>
                  <div className="drawer-section">
                    <h4>
                      <Sparkles size={15} color="#528fff" />
                      Telemetria do Agente
                    </h4>
                    <dl className="drawer-dl">
                      <div>
                        <dt>Modelo & Provedor</dt>
                        <dd>
                          {inspectedNode.latestRun?.model ?? inspectedNode.task.model ?? 'Verboo / OpenAI'}
                        </dd>
                      </div>
                      <div>
                        <dt>Conta Vinculada</dt>
                        <dd>
                          {data.accounts?.find(
                            (a: Row) =>
                              a.id ===
                              (inspectedNode.latestRun?.account_id ?? inspectedNode.task.account_id)
                          )?.label || 'Automática'}
                        </dd>
                      </div>
                      <div>
                        <dt>Tokens Consumidos</dt>
                        <dd>{formatTokens(inspectedNode.totalTokens)} tokens</dd>
                      </div>
                      <div>
                        <dt>Tempo Decorrido</dt>
                        <dd>
                          {formatDuration(
                            inspectedNode.latestRun?.started_at,
                            inspectedNode.latestRun?.finished_at
                          )}
                        </dd>
                      </div>
                      <div>
                        <dt>Tentativa Atual</dt>
                        <dd>{inspectedNode.latestRun?.attempt || 1}ª tentativa</dd>
                      </div>
                      <div>
                        <dt>Executor / Runtime</dt>
                        <dd>{inspectedNode.task.execution?.kind || 'OpenCode'}</dd>
                      </div>
                    </dl>
                  </div>

                  <div className="drawer-section">
                    <h4>
                      <FileCode size={15} color="#3bd6a4" />
                      Workspace & Bounded Scope
                    </h4>
                    <p style={{ margin: 0, fontSize: '0.8rem', color: '#94a3b8' }}>
                      Workspace Base: <code>{inspectedNode.task.execution?.workspace || './'}</code>
                    </p>
                    {inspectedNode.writePaths.length > 0 ? (
                      <div className="diff-files-list">
                        {inspectedNode.writePaths.map((path, idx) => (
                          <div className="diff-file-row" key={idx}>
                            <span className="diff-file-path">{path}</span>
                            <span className="diff-stat-add">Escopo Autorizado</span>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <p style={{ margin: 0, fontSize: '0.78rem', color: '#8190a7' }}>
                        Nenhum escopo restrito declarado; permissão para o diretório local do módulo.
                      </p>
                    )}
                  </div>

                  {inspectedNode.latestRun?.summary && (
                    <div className="drawer-section">
                      <h4>
                        <FileText size={15} color="#c084fc" />
                        Sumário da Tentativa
                      </h4>
                      <p style={{ margin: 0, fontSize: '0.82rem', color: '#cbd5e1', lineHeight: 1.55 }}>
                        {inspectedNode.latestRun.summary}
                      </p>
                    </div>
                  )}
                </>
              )}

              {drawerTab === 'prompt' && (
                <div className="drawer-section">
                  <h4>
                    <Code2 size={15} color="#528fff" />
                    Contrato de Instrução do Subagente
                  </h4>
                  <p style={{ margin: 0, fontSize: '0.78rem', color: '#94a3b8' }}>
                    Contexto delimitado seguindo as diretrizes Anthropic Claude Code para evitar poluição da janela de contexto.
                  </p>
                  <pre className="code-pre-box">{inspectedNode.task.description}</pre>
                </div>
              )}

              {drawerTab === 'files' && (
                <div className="drawer-section">
                  <h4>
                    <FileCode size={15} color="#3bd6a4" />
                    Arquivos Alterados & Evidências
                  </h4>
                  {drawerFiles.length > 0 ? (
                    <div className="diff-files-list">
                      {drawerFiles.map((f: Row, i: number) => (
                        <div className="diff-file-row" key={i}>
                          <span className="diff-file-path">{f.path}</span>
                          <div>
                            {f.added > 0 && <span className="diff-stat-add">+{f.added} </span>}
                            {f.removed > 0 && <span className="diff-stat-remove">−{f.removed}</span>}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p style={{ margin: 0, fontSize: '0.8rem', color: '#8190a7' }}>
                      Nenhum arquivo modificado registrado ainda nesta tentativa.
                    </p>
                  )}

                  {drawerArtifacts.length > 0 && (
                    <>
                      <h4 style={{ marginTop: '1rem' }}>Artefatos Preservados</h4>
                      <div className="diff-files-list">
                        {drawerArtifacts.map((a: Row) => (
                          <div className="diff-file-row" key={a.id}>
                            <a
                              href={`/api/artifacts/${a.id}`}
                              className="diff-file-path"
                              target="_blank"
                              rel="noreferrer"
                            >
                              📄 {a.name}
                            </a>
                            <span style={{ fontSize: '0.72rem', color: '#8190a7' }}>
                              {Math.ceil(a.size / 1024)} KB
                            </span>
                          </div>
                        ))}
                      </div>
                    </>
                  )}
                </div>
              )}

              {drawerTab === 'logs' && (
                <div className="drawer-section">
                  <h4>
                    <Terminal size={15} color="#528fff" />
                    Histórico de Ferramentas & Comandos
                  </h4>
                  {drawerActions.length > 0 ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                      {drawerActions.slice().reverse().map((act: Row, i: number) => (
                        <details key={act.id || i} open={i === 0}>
                          <summary style={{ cursor: 'pointer', fontSize: '0.8rem', color: '#cbd5e1' }}>
                            <strong>{act.tool || 'Comando'}</strong> ·{' '}
                            {act.finishedAt ? 'Concluído' : 'Em execução'}
                          </summary>
                          <pre className="code-pre-box" style={{ marginTop: '0.5rem' }}>
                            {act.output || act.command || 'Sem saída de texto'}
                          </pre>
                        </details>
                      ))}
                    </div>
                  ) : (
                    <p style={{ margin: 0, fontSize: '0.8rem', color: '#8190a7' }}>
                      Ainda sem comandos ou chamadas de ferramentas registradas nesta tentativa.
                    </p>
                  )}
                </div>
              )}

              {drawerTab === 'criteria' && (
                <div className="drawer-section">
                  <h4>
                    <ShieldCheck size={15} color="#f59e0b" />
                    Critérios de Aceite Obrigatórios
                  </h4>
                  <p style={{ margin: 0, fontSize: '0.78rem', color: '#94a3b8' }}>
                    A conclusão da tarefa só é aprovada após validação empírica de todos os critérios.
                  </p>
                  <ul className="criteria-list-check">
                    {(inspectedNode.task.criteria || []).map((c: Row) => {
                      const check = drawerAcceptance?.checks?.find((chk: Row) => chk.id === c.id);
                      const isPassed = check?.passed === true;
                      return (
                        <li
                          className={`criteria-item-check ${isPassed ? 'passed' : 'pending'}`}
                          key={c.id}
                        >
                          {isPassed ? <Check size={16} /> : <Clock size={16} />}
                          <div className="criteria-item-info">
                            <strong>{c.label}</strong>
                            <small>
                              {check?.detail || 'Aguardando verificação empírica antes do aceite.'}
                            </small>
                          </div>
                        </li>
                      );
                    })}
                  </ul>
                </div>
              )}
            </div>

            {/* Drawer Footer Actions */}
            <div className="drawer-footer">
              <button
                className="action-btn-secondary"
                onClick={() => {
                  const id = inspectedNode.id;
                  setInspectedNode(null);
                  inspect(id);
                }}
              >
                <ExternalLink size={13} />
                Abrir em Janela Completa
              </button>

              <button
                className="action-btn-secondary"
                onClick={() => setInspectedNode(null)}
              >
                Fechar
              </button>
            </div>
          </aside>
        </>
      )}
    </div>
  );
}
