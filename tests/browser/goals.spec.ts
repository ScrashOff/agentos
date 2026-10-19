import { expect, test, type Page } from '@playwright/test';

function fixture() {
  const now = new Date().toISOString();
  const goal = {
    id: 'progress-fixture',
    title: 'Piloto de interface — dados simulados',
    project: 'ui-only',
    state: 'active',
    created_at: now,
    description: 'Sem despacho de agentes.',
  };
  const gameNames = ['wild-bandito', 'mahjong-ways', 'lucky-neko'];
  const tasks = gameNames.flatMap((name, game) =>
    ['implement', 'validate', 'review'].map((phase, step) => ({
      id: `${name}-${phase}`,
      key: `${name}-${phase}`,
      goal_id: goal.id,
      title: `${name}: ${phase}`,
      state: game === 0 && step === 0 ? 'running' : 'blocked_dependencies',
      execution: { kind: 'harness', workspace: `ui-only/${name}` },
      account_id: 'account-test',
      model: 'current-model',
      agent_id: 'agent-test',
      attempt_count: game === 0 && step === 0 ? 2 : 0,
      created_at: now,
      updated_at: now,
      description: 'Contrato local simulado.',
      criteria: [
        { id: 'proof', type: 'artifact', name: 'proof.txt', label: 'Evidência obrigatória' },
      ],
    })),
  );
  const dependencies = tasks
    .slice(1)
    .map((task, i) => ({ task_id: task.id, depends_on: tasks[i].id }));
  const runs = [
    {
      id: 'run-current',
      task_id: tasks[0].id,
      attempt: 2,
      model: 'current-model',
      account_id: 'account-test',
      state: 'running',
      created_at: now,
      started_at: now,
      heartbeat_at: now,
    },
    {
      id: 'run-old',
      task_id: tasks[0].id,
      attempt: 1,
      model: 'previous-model',
      account_id: 'account-test',
      state: 'submitted',
      created_at: '2026-01-01T00:00:00Z',
      started_at: '2026-01-01T00:00:00Z',
      heartbeat_at: null,
    },
  ];
  const state = {
    goals: [
      goal,
      { ...goal, id: 'demo-only', title: 'Exemplo oculto', is_demo: true, state: 'completed' },
    ],
    // Deliberately out of order: presentation must use dependencies, not SQL/alphabetical order.
    tasks: [...tasks].reverse(),
    runs,
    agents: [{ id: 'agent-test', name: 'Agente simulado' }],
    accounts: [{ id: 'account-test', label: 'Conta de teste', model: 'default-model' }],
    events: [
      {
        id: '1',
        entity_id: tasks[0].id,
        kind: 'MODEL_REQUEST',
        created_at: now,
        payload: { runId: 'run-current', requests: 7 },
      },
    ],
    memories: [],
    contexts: [],
    artifacts: [
      {
        id: 'artifact-test',
        run_id: 'run-old',
        name: 'proof.txt',
        digest: 'abc'.repeat(22),
        size: 30,
      },
    ],
    integrations: {},
    settings: {},
    engine: { lastTick: now, error: null, active: 1 },
    recall: { synced: 0 },
    serverTime: now,
  };
  const detail = {
    goal,
    tasks: state.tasks,
    runs,
    dependencies,
    acceptances: [
      {
        run_id: 'run-old',
        passed: false,
        created_at: now,
        checks: [
          {
            id: 'proof',
            label: 'Evidência obrigatória',
            passed: false,
            detail: 'Arquivo ausente na tentativa antiga.',
          },
        ],
      },
    ],
  };
  return { state, detail, tasks };
}

test('never-started recovery tasks do not blank the objective directory or its details', async ({ page }) => {
  const data: any = fixture();
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  data.tasks[2].state = 'recovery_blocked';
  data.tasks[2].attempt_count = 0;
  data.state.settings['continuation-supervisor'] = {
    state: 'running', checkedAt: new Date().toISOString(),
  };
  await openFixture(page, data, false, false);
  await expect(page.getByRole('heading', { name: data.detail.goal.title, exact: true })).toBeVisible();
  await page.locator('nav').getByRole('button', { name: /^Objetivos e tarefas/ }).click();
  await expect(page.getByRole('heading', { name: 'Objetivos e tarefas', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: /Piloto de interface/ })).toBeVisible();
  expect(errors).toEqual([]);
});

test('objective directory reports final deliveries rather than treating auxiliary task counts as completion', async ({ page }) => {
  const data: any = fixture();
  data.state.goals[0].accepted_count = 2700;
  data.state.goals[0].task_count = 2834;
  data.state.settings['operation-progress:progress-fixture'] = {
    delivery: { verified: 0, integrated: 1, total: 3 },
  };
  await openFixture(page, data, false, false);
  await page.locator('nav').getByRole('button', { name: /^Objetivos e tarefas/ }).click();
  const card = page.locator('.operation-card').filter({ hasText: data.detail.goal.title });
  await expect(card).toContainText('0/3 entregas validadas');
  await expect(card).toContainText('1/3 integrações validadas');
  await expect(card).not.toContainText('2700/2834 etapas aceitas');
  await expect(card.getByLabel('Progresso das entregas finais').locator('i')).toHaveAttribute('style', 'width: 0%;');
});

test('objective details resolve while global state keeps refreshing and live events start at the snapshot cursor',async({page})=>{
  const data=fixture();let stateReads=0;let eventUrl='';
  await page.route('**/api/session',route=>route.fulfill({json:{authenticated:true}}));
  await page.route('**/api/panel/state',route=>{stateReads++;return route.fulfill({json:{...data.state,eventCursor:'240608',serverTime:new Date().toISOString()}});});
  await page.route('**/api/events?**',route=>{
    eventUrl=route.request().url();
    return route.fulfill({contentType:'text/event-stream',body:'id: 240609\ndata: '+JSON.stringify({id:'240609',kind:'MODEL_RESPONSE',payload:{runId:'run-current'}})+'\n\n'});
  });
  await page.route('**/api/panel/goals/progress-fixture',async route=>{
    await new Promise(resolve=>setTimeout(resolve,7500));
    await route.fulfill({json:data.detail});
  });
  await page.goto('/#goal=progress-fixture');
  await expect(page.getByRole('heading',{name:'Carregando o objetivo',exact:true})).toBeVisible();
  await expect(page.getByRole('heading',{name:data.detail.goal.title,exact:true})).toBeVisible({timeout:12000});
  expect(stateReads).toBeGreaterThan(1);
  expect(new URL(eventUrl).searchParams.get('after')).toBe('240608');
});

test('a failed objective read has a visible error and does not erase the workspace navigation',async({page})=>{
  const data=fixture();
  await page.route('**/api/session',route=>route.fulfill({json:{authenticated:true}}));
  await page.route('**/api/panel/state',route=>route.fulfill({json:{...data.state,eventCursor:'100'}}));
  await page.route('**/api/events?**',route=>route.fulfill({contentType:'text/event-stream',body:': fixture\n\n'}));
  await page.route('**/api/panel/goals/progress-fixture',route=>route.fulfill({status:503,json:{message:'Consulta temporariamente indisponível'}}));
  await page.goto('/#goal=progress-fixture');
  await expect(page.getByRole('heading',{name:'Não foi possível abrir o objetivo',exact:true})).toBeVisible();
  await expect(page.getByText('Consulta temporariamente indisponível',{exact:true})).toBeVisible();
  await expect(page.locator('nav').getByRole('button',{name:/^Objetivos e tarefas/})).toBeVisible();
});

test('automatic plan explains its requirements, roles, context and retry history', async ({
  page,
}) => {
  const data: any = fixture();
  data.detail.objective = {
    mode: 'execute',
    status: 'executing',
    request: { requestedCapacity: 2 },
  };
  data.detail.revisions = [
    {
      id: 'plan-one',
      revision: 1,
      plan: {
        summary: 'Duas frentes independentes com integração e QA.',
        requirements: [{ id: 'works', description: 'Os dois fluxos precisam passar no teste.' }],
        tasks: [
          {
            key: 'fix',
            title: 'Corrigir o fluxo',
            agentId: 'engineering.backend.node',
            stage: 'work',
            dependencies: [],
            description: 'Produzir código e evidência.',
          },
          {
            key: 'qa',
            title: 'Validar a candidata',
            agentId: 'qa.integration',
            stage: 'verify',
            dependencies: ['fix'],
            description: 'Comprovar sem alterar o código.',
          },
        ],
      },
    },
  ];
  data.detail.planning = [
    {
      state: 'completed',
      context: {
        manifest: {
          selected: [{ id: 'memory-one' }],
          agents: ['engineering.backend.node', 'qa.integration'],
          selector: 'laya',
        },
      },
    },
  ];
  data.detail.planningAttempts = [
    {
      planning_run_id: 'planning-one',
      attempt: 1,
      error: 'Faltava cobertura de um requisito',
      usage: { totalTokens: 100 },
    },
  ];
  await openFixture(page, data);
  const panel = page.getByRole('region', { name: 'Planejamento automático' });
  await expect(panel).toContainText('até 2 frentes');
  await expect(panel).toContainText('Os dois fluxos precisam passar no teste.');
  await panel.getByText('Revisão 1 · 2 etapas planejadas').click();
  await expect(panel).toContainText('engineering.backend.node');
  await expect(panel).toContainText('Após: fix');
  await expect(panel).toContainText('seletor laya');
  await panel.getByText('Tentativas de planejamento e consumo').click();
  await expect(panel).toContainText('Faltava cobertura de um requisito');
  await page.screenshot({ path: 'artifacts/screenshots/automatic-plan.png', fullPage: true });
});
async function openFixture(
  page: Page,
  data: ReturnType<typeof fixture>,
  direct = false,
  technical = true,
) {
  let sessionReady = false;
  await page.route('**/api/session', async (route) => {
    await new Promise((resolve) => setTimeout(resolve, 100));
    sessionReady = true;
    await route.fulfill({ json: { csrfToken: 'local-fixture' } });
  });
  await page.route('**/api/panel/state', (route) => route.fulfill({ json: {...data.state,eventCursor:'100'} }));
  await page.route('**/api/workspaces', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/chats', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/connections', (route) => route.fulfill({ json: { accounts: [] } }));
  await page.route('**/api/events?**', (route) =>
    route.fulfill({ contentType: 'text/event-stream', body: ': fixture\n\n' }),
  );
  await page.route('**/api/panel/goals/progress-fixture', (route) =>
    route.fulfill(
      sessionReady
        ? { json: data.detail }
        : { status: 401, json: { message: 'Inicie uma sessão local.' } },
    ),
  );
  await page.route('**/api/panel/goals/progress-fixture/planners', route => route.fulfill(
    (data as any).plannerPanelUnavailable ? { status: 503, json: { message: 'Fixture unavailable' } } :
      { json: (data as any).plannerPanel ?? { goalId: data.detail.goal.id, goalState: 'active', maxActive: 5,
        observedAt: new Date().toISOString(), scopes: [] } },
  ));
  await page.route('**/api/panel/tasks/**', route => {
    const id=route.request().url().split('/').at(-1)?.split('?')[0];
    const task=data.state.tasks.find(t=>t.id===id)!;
    return route.fulfill({json:{task,runs:data.state.runs.filter(r=>r.task_id===id),
      acceptances:data.detail.acceptances,artifacts:data.state.artifacts,page:{total:2,nextBefore:null}}});
  });
  await page.route('**/api/panel/collections/**', route => {
    const kind=route.request().url().split('/').at(-1)?.split('?')[0];
    return route.fulfill({json:{rows:kind==='runs'?data.state.runs:kind==='artifacts'?data.state.artifacts:data.state.contexts,
      page:{total:2,nextBefore:null}}});
  });
  await page.route('**/api/artifacts/artifact-test', (route) =>
    route.fulfill({
      contentType: 'text/plain',
      headers: { 'content-disposition': 'attachment; filename="proof.txt"' },
      body: 'Browser fixture evidence',
    }),
  );
  await page.goto(direct ? '/#goal=progress-fixture' : '/');
  if (direct) {
    await expect(
      page.getByRole('heading', { name: data.detail.goal.title, exact: true }),
    ).toBeVisible();
    if (technical)
      await page.getByText('Histórico, plano e detalhes técnicos', { exact: true }).click();
    return;
  }
  if (await page.getByRole('button', { name: 'Abrir menu' }).isVisible())
    await page.getByRole('button', { name: 'Abrir menu' }).click();
  await page
    .locator('nav')
    .getByRole('button', { name: 'Objetivos e tarefas', exact: false })
    .click();
  await expect(page.getByRole('button', { name: /Exemplo oculto/ })).not.toBeVisible();
  await page.getByRole('button', { name: /Piloto de interface/ }).click();
  if (technical)
    await page.getByText('Histórico, plano e detalhes técnicos', { exact: true }).click();
}

test('scope planners show ownership, deliverables and accepted progress with details on demand', async ({ page }) => {
  const data: any = fixture();
  data.state.settings['planning-demand:progress-fixture'] = { requests: [] };
  const alpha = { scope: 'ui-only/alpha', label: 'alpha',
    planner: { taskId: data.tasks[0].id, state: 'running', occupied: true, model: 'glm-5.3-flash', account: 'GLM test' },
    nextDelivery: { taskId: data.tasks[1].id, title: 'Integrar correções já validadas', state: 'blocked_dependencies', stage: 'integrate' },
    blocker: { reason: 'Falta comprovar consumo do patch.', code: 'PATCH_NOT_CONSUMED', taskId: data.tasks[1].id, nextAction: 'Conferir a pré-imagem preservada.' },
    workers: { running: 3, ready: 2, queued: 1, confirming: 1 },
    lastAdvance: { taskId: data.tasks[0].id, title: 'Diagnóstico aceito', at: new Date().toISOString(), kind: 'auxiliary' },
    details: { sourceRevision: 'a'.repeat(64), planningUpdatedAt: new Date().toISOString(), canonicalRemaining: 2 } };
  data.plannerPanel = { goalId: 'progress-fixture', goalState: 'active', maxActive: 5,
    observedAt: new Date().toISOString(), scopes: [alpha, { ...alpha, scope: 'ui-only/beta', label: 'beta',
      planner: { taskId: null, state: 'pending', occupied: false, model: null, account: null },
      lastAdvance: null, blocker: null, workers: { running: 0, ready: 1, queued: 0, confirming: 0 } }] };
  await openFixture(page, data, true, false);
  const board = page.getByRole('region', { name: 'Planejamento por escopo', exact: true });
  await expect(board).toContainText('1 planejando · limite 5');
  const card = board.getByRole('article', { name: 'Planejamento: alpha' });
  await expect(card).toContainText('Responsável: glm-5.3-flash');
  await expect(card).toContainText('Integrar correções já validadas');
  await expect(card).toContainText('3 workers executando · 2 tarefas prontas · 1 em confirmação');
  await expect(card).toContainText('Evidência auxiliar aceita');
  await expect(card).toContainText('Falta comprovar consumo do patch.');
  await expect(card.getByRole('button', { name: 'Ver planejador', exact: true })).not.toBeVisible();
  await expect(page.locator('.game-card')).toHaveCount(3);
  await expect(page.locator('.operation-kpis > div').first()).toContainText('0');
  await card.getByText('Ver decisão e evidências', { exact: true }).click();
  await expect(card).toContainText('Conferir a pré-imagem preservada.');
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(true);
  await card.getByRole('button', { name: 'Ver planejador', exact: true }).click();
  await expect(page.getByRole('dialog')).toContainText('run-current');
});

test('a failed planner read leaves the objective usable and offers a local retry', async ({ page }) => {
  const data: any = fixture();
  data.state.settings['planning-demand:progress-fixture'] = { requests: [] };
  data.plannerPanelUnavailable = true;
  await openFixture(page, data, true, false);
  const board = page.getByRole('region', { name: 'Planejamento por escopo', exact: true });
  await expect(board).toContainText('Não foi possível consultar os planejadores.');
  await expect(page.locator('.game-card')).toHaveCount(3);
  await expect(page.getByRole('heading', { name: data.detail.goal.title, exact: true })).toBeVisible();
  data.plannerPanelUnavailable = false;
  await board.getByRole('button', { name: 'Atualizar', exact: true }).click();
  await expect(board).toContainText('Nenhum escopo de planejamento registrado.');
  await expect(board).not.toContainText('Não foi possível consultar os planejadores.');
});

test('compact board separates automatic repair from user action and hides verbose history', async ({
  page,
}) => {
  const data: any = fixture();
  data.state.settings['continuation-supervisor'] = {
    state: 'running',
    checkedAt: new Date().toISOString(),
  };
  const failed = data.tasks.find((t: any) => t.id === 'mahjong-ways-implement');
  failed.state = 'failed';
  failed.execution.continuation = {
    state: 'waiting_slot',
    runId: 'run-failed',
    message: 'Correção na fila.',
  };
  data.state.runs.push({
    id: 'run-failed',
    task_id: failed.id,
    attempt: 1,
    state: 'failed',
    created_at: new Date().toISOString(),
  });
  await openFixture(page, data, true, false);
  await expect(page.locator('.game-card')).toHaveCount(3);
  await expect(page.locator('.operation-banner')).toContainText('Você não precisa agir agora');
  await expect(page.locator('.operation-kpis > div').nth(2)).toContainText('1');
  await expect(page.locator('.operation-kpis > div').nth(3)).toContainText('0');
  await expect(page.locator('.attention-panel')).toHaveCount(0);
  await expect(page.locator('.focus-card')).toHaveCount(0);
  await expect(page.locator('.operation-technical')).not.toHaveAttribute('open', '');
  await page
    .locator('.game-card')
    .filter({ hasText: 'Mahjong Ways' })
    .getByRole('button', { name: 'Ver etapas' })
    .click();
  await expect(page.locator('.game-stage-list')).toContainText('Correção na fila');
  await page
    .locator('.game-card')
    .filter({ hasText: 'Wild Bandito' })
    .getByRole('button', { name: 'Ver execução' })
    .click();
  await expect(page.getByRole('dialog')).toBeVisible();
});

test('recovered artifacts link the failed attempt to a collection without pretending another model ran', async ({
  page,
}) => {
  const data = fixture();
  data.tasks[0].state = 'accepted';
  Object.assign(data.detail.runs[0], {
    state: 'submitted',
    model: null,
    account_id: null,
    checkpoint: { kind: 'artifact_recovery', sourceRunId: 'run-old', modelInvoked: false },
  });
  Object.assign(data.detail.runs[1], { state: 'failed', error_code: 'ARTIFACT_PATH' });
  data.state.artifacts[0].run_id = 'run-current';
  data.detail.acceptances.push({
    run_id: 'run-current',
    passed: true,
    created_at: new Date().toISOString(),
    checks: [],
  });
  await openFixture(page, data, true, false);
  const card = page
    .locator('.game-card')
    .filter({ has: page.getByRole('heading', { name: 'Wild Bandito' }) });
  await card.getByRole('button', { name: 'Ver etapas' }).click();
  await card.getByRole('button', { name: /wild-bandito: implement/ }).click();
  const dialog = page.getByRole('dialog');
  await expect(dialog.locator('.attempt-detail').first()).toContainText('Coleta 2');
  await expect(dialog).toContainText('Automática · sem chamada ao modelo');
  await dialog.locator('.attempt-detail').nth(1).locator('summary').click();
  await expect(dialog).toContainText('Falhou · arquivos recuperados');
  await expect(dialog).toContainText('Arquivos disponíveis na coleta 2');
  await expect(dialog.getByRole('link', { name: /proof.txt/ })).toBeVisible();
});

test('compact board stays readable on a phone', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openFixture(page, fixture(), true, false);
  await expect(page.locator('.game-card')).toHaveCount(3);
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1),
  ).toBe(true);
  await page.screenshot({ path: 'artifacts/screenshots/compact-board-mobile.png', fullPage: true });
});

test('shows missing GLM and Muse backlog and delivery gates without counting diagnostics as game progress', async ({
  page,
}) => {
  const data: any = fixture();
  data.tasks[1].stage = 'integrate';
  data.tasks[2].stage = 'verify';
  data.tasks[4].stage = 'integrate';
  data.tasks[4].state = 'accepted';
  data.tasks[5].stage = 'verify';
  data.tasks[5].state = 'accepted';
  const diagnostics = Array.from({ length: 25 }, (_, n) => ({
    ...data.tasks[0],
    id: 'diagnostic-' + n,
    key: 'diagnostic-' + n,
    title: 'Diagnóstico aceito ' + n,
    state: 'accepted',
    stage: 'work',
    attempt_count: 1,
  }));
  data.detail.tasks.push(...diagnostics);
  data.state.settings['workforce:progress-fixture'] = {
    enabled: true,
    glmTarget: 12,
    glmRequestLimit: 6,
    museTarget: 6,
  };
  data.state.settings['workforce-health:progress-fixture'] = {
    observedAt: new Date().toISOString(),
    glm: { running: 1, target: 12, requestLimit: 6, ready: 0, blocked: 3 },
    muse: {
      running: 1,
      target: 6,
      ready: 0,
      blocked: 7,
      reason: 'Muse aguarda dependências; o gestor pode planejar trabalho independente.',
    },
    reason: 'Sem tarefas prontas; o gestor precisa replanejar os bloqueios.',
  };
  data.state.settings['operation-progress:progress-fixture'] = {
    stagnation: { detected: true },
    nextTransition: { reason: 'Definir nova hipótese e um teste discriminante.' },
    milestones: [
      {
        scope: 'ui-only/mahjong-ways',
        integration: { state: 'accepted', candidateDigest: 'integrated-hash' },
        verification: { state: 'accepted', candidateDigest: 'different-hash' },
        verificationMatchesCandidate: false,
      },
    ],
  };
  await openFixture(page, data, true, false);
  const pool = page.getByRole('region', { name: 'Reposição das sessões GLM' });
  await expect(pool).toContainText('GLM: 1/12 sessões em execução');
  await expect(pool).toContainText('0 tarefas prontas · 3 bloqueadas');
  const muse = page.getByRole('region', { name: 'Reposição das sessões Muse' });
  await expect(muse).toContainText('Muse: 1/6 sessões em execução');
  await expect(muse).toContainText('0 tarefas prontas · 7 bloqueadas');
  await expect(muse).toContainText('Muse aguarda dependências');
  await expect(muse).not.toContainText('limite de 6 chamadas');
  await expect(page.getByRole('region', { name: 'Avanço da entrega' })).toContainText(
    'Definir nova hipótese',
  );
  const card = page.locator('.game-card').filter({
    has: page.getByRole('heading', { name: 'Wild Bandito', exact: true }),
  });
  await expect(card).toContainText('Integração: pendente · QA final: pendente');
  await expect(card).not.toContainText('etapas aceitas');
  await expect(
    page.locator('.game-card').filter({
      has: page.getByRole('heading', { name: 'Mahjong Ways', exact: true }),
    }),
  ).toContainText('Integração: aceita · QA final: pendente');
  await card.getByRole('button', { name: 'Ver etapas' }).click();
  await expect(card).toContainText('25/26 entregas complementares aceitas');
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(
    true,
  );
  await page.screenshot({
    path: 'artifacts/screenshots/framework-process-mobile.png',
    fullPage: true,
  });
});

test('ready work does not imply admission during a lane update', async ({ page }) => {
  const data: any = fixture();
  data.state.settings['workforce:progress-fixture'] = {
    enabled: true,
    glmTarget: 12,
    museTarget: 6,
  };
  data.state.settings['workforce-health:progress-fixture'] = {
    observedAt: new Date().toISOString(),
    reason: 'Muse pode repor uma sessão.',
    glm: {
      running: 1,
      target: 12,
      ready: 2,
      blocked: 0,
      admission: { allowed: false, service: 'agentos-opencode', code: 'WORKER_ROLLOUT' },
      reason: 'GLM aguarda atualização do executor; as duas tarefas prontas estão preservadas.',
    },
    muse: {
      running: 1,
      target: 6,
      ready: 1,
      blocked: 0,
      admission: { allowed: true, service: 'agentos-workers' },
      reason: 'Muse pode repor uma sessão.',
    },
  };
  await openFixture(page, data, true, false);
  const glm = page.getByRole('region', { name: 'Reposição das sessões GLM' });
  await expect(glm).toContainText('2 tarefas prontas');
  await expect(glm).toContainText('GLM aguarda atualização do executor');
  await expect(glm).not.toContainText('Muse pode repor uma sessão');
  await expect(page.getByRole('region', { name: 'Reposição das sessões Muse' })).toContainText(
    'Muse pode repor uma sessão',
  );
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(
    true,
  );
});

test('delivery card shows the technical owner and next action with evidence details on demand',async({page})=>{
  const data:any=fixture();
  data.detail.deliveryFlow={observedAt:new Date().toISOString(),cases:[{task_id:data.tasks[0].id,state:'decided',total:1,since:new Date().toISOString()}],consumptions:[],accepted:[]};
  await openFixture(page,data,true,false);
  const card=page.getByLabel('Encaminhamento de Wild Bandito',{exact:true});
  await expect(card).toContainText('Responsável: Supervisão');
  await expect(card).toContainText('Decisão registrada; efeito ainda pendente.');
  await expect(card).toContainText('Aplicar a decisão e conferir a retomada.');
  await expect(card.getByText('Candidata atual ainda sem identificação confirmada.')).not.toBeVisible();
  await card.getByText('Ver pendência e candidata',{exact:true}).click();
  await expect(card.getByText('Candidata atual ainda sem identificação confirmada.')).toBeVisible();
  await page.setViewportSize({width:390,height:844});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  await page.screenshot({path:'artifacts/screenshots/delivery-owner-mobile.png',fullPage:true});
});

test('periodic manager stays compact and exposes actual decisions on demand', async ({ page }) => {
  const data: any = fixture();
  data.state.settings['management:progress-fixture'] = { enabled: true, intervalSeconds: 120 };
  data.state.settings['management-state:progress-fixture'] = {
    reviewedAt: new Date().toISOString(),
    summary: 'Integração priorizada pelo gestor.',
    capacityReason: 'Duas validações aguardam o patch do carregamento.',
    receipts: [{ code: 'APPLIED', reason: 'Prioridade ajustada para liberar o QA.' }],
  };
  await openFixture(page, data, true, false);
  await expect(page.locator('.operation-manager summary')).toContainText(
    'Gestor automático · a cada 2 min',
  );
  await expect(page.getByText('Integração priorizada pelo gestor.')).not.toBeVisible();
  await page.locator('.operation-manager summary').click();
  await expect(page.getByText('Integração priorizada pelo gestor.')).toBeVisible();
  await expect(page.locator('.operation-manager')).toContainText(
    'Aplicado — Prioridade ajustada para liberar o QA.',
  );
  await page.setViewportSize({ width: 390, height: 844 });
  await expect
    .poll(() => page.locator('.sidebar').evaluate((el) => el.getBoundingClientRect().right))
    .toBeLessThanOrEqual(1);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(
    true,
  );
  await page.screenshot({
    path: 'artifacts/screenshots/periodic-manager-mobile.png',
    fullPage: true,
  });
});

test('workforce shows actual execution separately from its target without adding game cards', async ({
  page,
}) => {
  const data: any = fixture();
  data.state.settings['workforce:progress-fixture'] = {
    enabled: true,
    glmTarget: 12,
    glmRequestLimit: 6,
    lunaTarget: 2,
  };
  data.state.settings['continuation-supervisor'] = {
    state: 'running',
    checkedAt: new Date().toISOString(),
  };
  data.detail.runs[0].model = 'glm-5.3-flash';
  data.detail.runs[0].runtime = 'opencode';
  const coordinator = {
    ...data.tasks[0],
    id: 'coordinate',
    title: 'Coordenar entregas',
    stage: 'coordinate',
    execution: {},
    state: 'running',
    model: 'gpt-6-sol',
  };
  const globalReview = {
    ...coordinator,
    id: 'review-global',
    title: 'Revisão geral',
    stage: 'review',
    state: 'queued',
    model: 'claude-opus',
  };
  data.detail.tasks.push(coordinator, globalReview);
  data.state.runs.push(
    {
      ...data.detail.runs[0],
      id: 'coord-run',
      task_id: coordinator.id,
      model: 'gpt-6-sol',
      runtime: 'harness',
    },
    {
      ...data.detail.runs[0],
      id: 'review-run',
      task_id: globalReview.id,
      model: 'claude-opus',
      runtime: 'harness',
      state: 'queued',
      account_id: 'agy',
    },
  );
  data.state.accounts.push({ id: 'agy', label: 'AGY teste', provider: 'agy' });
  for (const [index, state] of ['running', 'queued'].entries()) {
    const worker = {
      ...data.tasks[0],
      id: 'luna-' + index,
      title: 'Validar jogo com Luna ' + (index + 1),
      stage: 'work',
      state,
      model: 'gpt-6-luna',
    };
    data.detail.tasks.push(worker);
    data.state.runs.push({
      ...data.detail.runs[0],
      id: 'luna-run-' + index,
      task_id: worker.id,
      model: 'gpt-6-luna',
      runtime: 'harness',
      state,
    });
  }
  await openFixture(page, data, true, false);
  const workforce = page.getByRole('region', { name: 'Equipe da operação', exact: true });
  await expect(workforce.locator('header')).toContainText('3 em execução · 2 na fila');
  const glm = workforce.locator('.workforce-group').filter({ hasText: 'GLM · OpenCode' });
  await expect(glm).toContainText('1 em execução');
  await expect(glm).toContainText('Meta: 12 sessões');
  await expect(glm).toContainText('Máx. 6 requisições simultâneas ao modelo');
  const luna = workforce.locator('.workforce-group').filter({ hasText: 'GPT-6 Luna · Codex' });
  await expect(luna).toContainText('1 em execução');
  await expect(luna).toContainText('Meta: 2 sessões · 1 na fila');
  await expect(luna).not.toContainText('requisições simultâneas');
  const sol = workforce.locator('.workforce-group').filter({ hasText: 'Sol · coordenação' });
  await expect(sol).toContainText('1 em execução');
  await expect(page.locator('.game-card')).toHaveCount(3);
  await expect(page.getByText('Frentes em cópias isoladas.', { exact: true })).toBeVisible();
  await expect(workforce.getByRole('button', { name: /Coordenar entregas/ })).not.toBeVisible();
  await page.screenshot({ path: 'artifacts/screenshots/workforce-desktop.png', fullPage: true });
  await workforce.getByText('Ver agentes', { exact: true }).click();
  await expect(workforce.getByRole('button', { name: /Coordenar entregas/ })).toBeVisible();
  await expect(workforce.getByRole('button', { name: /Validar jogo com Luna 1/ })).toContainText(
    'gpt-6-luna',
  );
  await page.setViewportSize({ width: 390, height: 844 });
  await expect
    .poll(() =>
      page.locator('.sidebar').evaluate((element) => element.getBoundingClientRect().right),
    )
    .toBeLessThanOrEqual(1);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(
    true,
  );
  await page.screenshot({ path: 'artifacts/screenshots/workforce-mobile.png', fullPage: true });
  await workforce.getByRole('button', { name: /Coordenar entregas/ }).click();
  await expect(page.getByRole('dialog')).toContainText('coord-run');
});

test('consolidated operation is archived and opens its canonical goal', async ({ page }) => {
  const data: any = fixture();
  data.state.goals.push({
    ...data.detail.goal,
    id: 'merged-fixture',
    title: 'OpenCode consolidado',
    state: 'cancelled',
    merged_into: data.detail.goal.id,
  });
  data.state.settings['exclusive-workstreams:ui-only'] = { enabled: true };
  await openFixture(page, data, true);
  await expect(page.getByText('Uma execução por jogo.', { exact: true })).toBeVisible();
  await page
    .locator('nav')
    .getByRole('button', { name: 'Objetivos e tarefas', exact: false })
    .click();
  await expect(page.locator('.goal-directory > .operation-cards > .operation-card')).toHaveCount(1);
  await expect(page.getByRole('button', { name: /OpenCode consolidado/ })).not.toBeVisible();
  await page.getByText('Histórico de objetivos', { exact: false }).click();
  await page.getByRole('button', { name: /OpenCode consolidado/ }).click();
  await expect(page).toHaveURL(/#goal=progress-fixture$/);
  await expect(
    page.getByRole('heading', { name: data.detail.goal.title, exact: true }),
  ).toBeVisible();
});

test('progress distinguishes active work, dependencies, current attempt and old evidence', async ({
  page,
}) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  const data = fixture();
  await openFixture(page, data);
  await expect(
    page.getByText('Fila, banco e executores no servidor · acesso privado pela tailnet'),
  ).toBeVisible();
  await expect(page.locator('.delivery-metrics > div').nth(0)).toContainText('0 / 3');
  await expect(page.locator('.delivery-metrics > div').nth(1)).toContainText('1');
  await expect(page.locator('.delivery-metrics > div').nth(2)).toContainText('8');
  await expect(page.locator('.focus-card')).toHaveCount(1);
  await expect(page.locator('.focus-card')).toContainText('current-model');
  await expect(page.locator('.focus-card')).toContainText('Consultando o modelo · chamada 7');
  await expect(page.locator('.delivery-title h3')).toHaveText([
    'Wild Bandito',
    'Mahjong Ways',
    'Lucky Neko',
  ]);
  await expect(page.locator('.delivery-row').nth(1)).toContainText('Espera uma etapa anterior');
  await expect(page.locator('.delivery-row').nth(1)).toContainText('wild-bandito: review');
  await page.getByRole('button', { name: 'Ver tarefa e evidências' }).click();
  const dialog = page.getByRole('dialog');
  await expect(dialog).toContainText('Ainda não verificado nesta tentativa.');
  await expect(dialog.locator('.attempt-detail').first()).toContainText('current-model');
  await dialog.locator('.attempt-detail').nth(1).locator('summary').click();
  await expect(dialog.locator('.attempt-detail').nth(1)).toContainText('previous-model');
  await expect(dialog).toContainText('Arquivo ausente na tentativa antiga.');
  const downloadPromise = page.waitForEvent('download');
  await dialog.getByRole('link', { name: /proof.txt/ }).click();
  const download = await downloadPromise;
  expect(download.suggestedFilename()).toBe('proof.txt');
  expect(await download.failure()).toBeNull();
  await page.getByRole('button', { name: 'Fechar', exact: true }).click();
  // Exercise the automatic refresh while the objective stays open.
  data.tasks.slice(0, 3).forEach((task) => {
    task.state = 'accepted';
  });
  data.tasks[3].state = 'queued';
  data.detail.runs[0].state = 'submitted';
  await expect(page.locator('.delivery-metrics > div').nth(0)).toContainText('1 / 3', {
    timeout: 10000,
  });
  await expect(page.locator('.delivery-row').first()).toContainText('3/3 etapas aceitas');
  await page.reload();
  await expect(
    page.getByRole('heading', { name: data.detail.goal.title, exact: true }),
  ).toBeVisible();
  await page.getByText('Histórico, plano e detalhes técnicos', { exact: true }).click();
  await expect(page.locator('.delivery-metrics > div').nth(0)).toContainText('1 / 3');
  expect(errors).toEqual([]);
});

test('a direct objective link waits for local authentication in a fresh browser', async ({
  page,
}) => {
  await openFixture(page, fixture(), true);
  await expect(page.getByRole('alert')).toHaveCount(0);
  await expect(page.locator('.delivery-row')).toHaveCount(3);
});

test('shows actual commands, partial output, age of results and an honest missing ETA', async ({
  page,
}) => {
  const data = fixture();
  const old = new Date(Date.now() - 6 * 60000).toISOString();
  const now = new Date().toISOString();
  data.state.events.push({
    id: '2',
    kind: 'EXECUTION_OBSERVED',
    entity_id: data.tasks[0].id,
    created_at: now,
    payload: {
      runId: 'run-current',
      observation: {
        observedAt: now,
        toolCalls: 2,
        process: {
          available: true,
          activeState: 'active',
          subState: 'running',
          memoryBytes: 600000000,
          processNames: ['node', 'chrome'],
        },
        actions: [
          {
            id: 'call1',
            tool: 'bash',
            state: 'returned',
            command: 'node validate.mjs',
            startedAt: old,
            finishedAt: old,
            output: 'session isolation: true',
            exitCode: 0,
          },
          {
            id: 'call2',
            tool: 'bash',
            state: 'running',
            command: 'node evidence/browser.mjs',
            startedAt: old,
            finishedAt: null,
            output: '',
            exitCode: null,
          },
        ],
        evidence: [
          { name: 'browser.log', modifiedAt: now, preview: 'TICK 2: canvas width=0', size: 32 },
          {
            name: 'old-report.json',
            modifiedAt: '2026-01-01T00:00:00Z',
            preview: 'old success',
            size: 32,
          },
        ],
        files: [
          { path: 'server.mjs', added: 20, removed: 15 },
          { path: 'qa-protocol/new.test.mjs', status: 'new', added: null, removed: null },
        ],
      },
    },
  } as any);
  await openFixture(page, data, true);
  const live = page.getByRole('region', { name: 'Detalhes ao vivo da execução' });
  await expect(live).toContainText('Sem novo retorno há mais de 5 minutos');
  await expect(live).toContainText('node evidence/browser.mjs');
  await expect(live).toContainText('TICK 2: canvas width=0');
  await expect(live).toContainText('Sem previsão confiável');
  await expect(live).toContainText('server.mjs');
  await expect(live).toContainText('qa-protocol/new.test.mjs');
  await expect(live).toContainText('Novo arquivo');
  await expect(live.getByText('old success', { exact: true })).not.toBeVisible();
  await expect(page.getByText('O que falta nesta etapa')).toBeVisible();
  await page.getByRole('button', { name: 'Ver tarefa e evidências' }).click();
  const dialog = page.getByRole('dialog');
  await expect(dialog).toContainText('Histórico de comandos e resultados');
  await expect(dialog).toContainText('arquivos anteriores ao início');
  await dialog.getByText('Execução de testes e validações', { exact: true }).click();
  await expect(dialog).toContainText('session isolation: true');
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});

test('unaccepted submission and stale observations never appear as running or accepted', async ({
  page,
}) => {
  const data = fixture();
  data.tasks[0].state = 'submitted';
  data.detail.runs[0].state = 'submitted';
  data.detail.acceptances.push({
    run_id: 'run-current',
    passed: false,
    created_at: new Date().toISOString(),
    checks: [
      {
        id: 'proof',
        label: 'Evidência obrigatória',
        passed: false,
        detail: 'Não atende ao contrato.',
      },
    ],
  });
  data.state.serverTime = '2026-01-01T00:00:00Z';
  data.state.engine.lastTick = data.state.serverTime;
  await openFixture(page, data);
  await expect(page.getByText('Agendador sem confirmação recente')).toBeVisible();
  await expect(page.locator('.focus-card')).toContainText('Entrega sem aceite');
  await expect(page.locator('.focus-card')).toContainText('1 critérios ainda não atendidos');
  await expect(page.locator('.delivery-metrics > div').nth(0)).toContainText('0 / 3');
  await expect(page.locator('.delivery-metrics > div').nth(1).locator('strong')).toHaveText('0');
  // The rejected submission and all eight waiting descendants remain visible.
  await expect(page.locator('.delivery-metrics > div').nth(3).locator('strong')).toHaveText('9');
  await page.getByRole('button', { name: 'Em execução', exact: true }).click();
  await expect(page.getByText('Nenhuma entrega neste filtro.')).toBeVisible();
  await page.getByRole('button', { name: 'Pedem atenção', exact: true }).click();
  await expect(page.locator('.delivery-row')).toHaveCount(3);
  await expect(page.locator('.delivery-row').first()).toContainText('Entrega sem aceite');
});

test('shows automatic repair with the previous failure without counting it as an accepted game', async ({
  page,
}) => {
  const data = fixture();
  Object.assign(data.detail.runs[0], {
    checkpoint: {
      kind: 'automatic_recovery',
      strategy: 'replan_and_repair',
      previousSummary: 'Chromium encerrou antes do giro; evidência insuficiente.',
    },
  });
  await openFixture(page, data, true);
  const focus = page.locator('.focus-card');
  await expect(focus).toContainText('Replanejando e corrigindo');
  await expect(focus).toContainText('Chromium encerrou antes do giro');
  await expect(focus).toContainText('O aceite continua pendente de novos testes');
  await expect(page.locator('.delivery-metrics > div').nth(0)).toContainText('0 / 3');
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});

for (const width of [390, 820]) {
  test(`queue, delivery steps and task details remain usable at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 950 });
    const data = fixture();
    data.tasks[0].state = 'queued';
    data.detail.runs[0].state = 'queued';
    await openFixture(page, data);
    await expect(page.locator('.delivery-metrics > div').nth(1).locator('strong')).toHaveText('0');
    await expect(page.locator('.focus-card')).toHaveCount(0);
    await expect(
      page.getByText('Despachada para a fila; o executor ainda não iniciou.').first(),
    ).toBeVisible();
    const layout = await page.evaluate(() => ({
      width: innerWidth,
      pageWidth: document.documentElement.scrollWidth,
      outside: [...document.querySelectorAll('body *')]
        .filter((e) => e.getBoundingClientRect().right > innerWidth + 1)
        .map((e) => ({
          tag: e.tagName,
          class: e.className,
          right: e.getBoundingClientRect().right,
        })),
    }));
    expect(layout.pageWidth, JSON.stringify(layout)).toBeLessThanOrEqual(layout.width);
    await page
      .getByRole('button', { name: 'wild-bandito: implement — Na fila', exact: true })
      .click();
    await expect(page.getByRole('dialog')).toContainText('Workspace');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
      true,
    );
    await page.screenshot({
      path: `artifacts/screenshots/task-detail-${width}.png`,
      fullPage: true,
    });
    await page.getByRole('button', { name: 'Fechar', exact: true }).click();
    await page.screenshot({ path: `artifacts/screenshots/progress-${width}.png`, fullPage: true });
  });
}

test('attention count opens concrete reasons, and dependency links lead to the failed execution', async ({
  page,
}) => {
  const data: any = fixture();
  data.tasks[0].state = 'failed';
  data.detail.runs[0].state = 'failed';
  data.detail.runs[0].error_code = 'QUOTA_EXCEEDED';
  data.detail.runs[0].summary = 'O provedor informou limite de uso.';
  data.tasks[1].state = 'blocked_failed';
  await openFixture(page, data, true);
  const alerts = page.getByRole('region', { name: 'Avisos e bloqueios', exact: true });
  await page.getByRole('button', { name: /Ver \d+ avisos e bloqueios/ }).click();
  await expect(alerts).toBeFocused();
  await expect(alerts).toContainText('Aguardando rota com cota');
  await expect(alerts).toContainText('Conta de teste');
  await expect(alerts).toContainText('Próximo passo:');
  await alerts
    .getByRole('button', { name: 'Ver causa: wild-bandito: implement', exact: true })
    .first()
    .click();
  await expect(page.getByRole('dialog')).toContainText('QUOTA_EXCEEDED');
  await expect(page.getByRole('dialog')).toContainText('Aguardando rota com cota');
  await page.getByRole('button', { name: 'Fechar', exact: true }).click();
  data.tasks[0].state = 'accepted';
  data.tasks[1].state = 'ready';
  await expect(alerts).toContainText('0 bloqueios · 7 avisos de acompanhamento', {
    timeout: 10000,
  });
  await expect(alerts).not.toContainText('Aguardando rota com cota');
});

test('native OpenCode exposes uncertainty and command details without an approval prompt', async ({
  page,
}) => {
  const data: any = fixture();
  data.tasks[0].execution.kind = 'opencode';
  data.tasks[0].state = 'reconciling';
  data.detail.runs[0].state = 'reconciling';
  data.detail.runs[0].error_code = 'STOP_UNCONFIRMED';
  data.state.events.push({
    id: '2',
    kind: 'EXECUTION_OBSERVED',
    entity_id: data.tasks[0].id,
    created_at: new Date().toISOString(),
    payload: {
      runId: 'run-current',
      observation: {
        observedAt: new Date().toISOString(),
        process: { available: true, activeState: 'inactive' },
        actions: [
          {
            id: 'oc1',
            tool: 'bash',
            command: 'node --test qa-protocol/game.test.mjs',
            state: 'returned',
            output: '1 test passed',
            finishedAt: new Date().toISOString(),
          },
        ],
        evidence: [],
        files: [],
      },
    },
  });
  await openFixture(page, data, true);
  const alerts = page.getByRole('region', { name: 'Avisos e bloqueios', exact: true });
  await expect(alerts).toContainText('Nenhuma aprovação sua está pendente');
  await alerts.getByRole('button', { name: 'Ver motivo e execução' }).first().click();
  const dialog = page.getByRole('dialog');
  await expect(dialog).toContainText('OpenCode');
  await expect(dialog).toContainText('STOP_UNCONFIRMED');
  await expect(dialog).toContainText('1 test passed');
  await expect(dialog).toContainText('node --test qa-protocol/game.test.mjs');
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});

test('a quota failure with a legacy needs-action notice stays visible without demanding human approval',async({page})=>{
  const data:any=fixture();
  data.tasks[0].state='failed';
  data.state.runs[0].state='failed';
  data.state.runs[0].error_code='QUOTA_EXCEEDED';
  data.tasks[0].execution.continuation={runId:'run-current',state:'needs_action',message:'Old repeated failure diagnosis'};
  await openFixture(page,data,true);
  await expect(page.getByText('Precisa de você',{exact:true})).toHaveCount(0);
  await expect(page.locator('.operation-actions')).toHaveCount(0);
  await expect(page.getByRole('region',{name:'Avisos e bloqueios',exact:true})).toContainText('Aguardando rota com cota');
});

test('a recurring blocker assigned to Sol is automatic supervision, not human approval', async ({
  page,
}) => {
  const data: any = fixture();
  data.tasks[0].state = 'recovery_blocked';
  data.detail.runs[0].state = 'failed';
  data.tasks[0].execution.continuation = {
    runId: 'run-current',
    state: 'supervising',
    message: 'Sol está preparando uma correção focal.',
  };
  data.state.settings = {
    'continuation-supervisor': { state: 'running', checkedAt: new Date().toISOString() },
  };
  await openFixture(page, data, true);
  await expect(page.getByText('Em supervisão', { exact: true }).first()).toBeVisible();
  await expect(page.getByText('Supervisor resolvendo o bloqueio', { exact: true })).toBeVisible();
  await expect(page.getByText('Precisa de você', { exact: true })).toHaveCount(0);
});

test('a verification waiting for its supervised implementation does not repeat its old human-action notice',async({page})=>{
  const data:any=fixture(),now=new Date().toISOString();
  const parent=data.tasks[0],qa=data.tasks[1];
  parent.state='recovery_blocked';
  parent.execution.continuation={runId:'run-current',state:'supervising',message:'Assigned to the supervisor'};
  data.state.runs[0].state='submitted';
  qa.execution.readOnlySource=true;
  qa.execution.continuation={runId:'qa-current',state:'needs_action',message:'Old implementation routing failed'};
  data.state.runs.push({...data.state.runs[0],id:'qa-current',task_id:qa.id,attempt:1});
  data.state.settings['continuation-supervisor']={state:'running',checkedAt:now};
  await openFixture(page,data,true,false);
  await expect(page.getByText('Sua ação é necessária',{exact:true})).not.toBeVisible();
  await expect(page.locator('.operation-actions')).toHaveCount(0);
  await page.locator('.game-card').filter({has:page.getByRole('heading',{name:'Wild Bandito',exact:true})}).getByRole('button',{name:'Ver etapas',exact:true}).click();
  await page.locator('.game-stage-list').getByRole('button',{name:new RegExp(qa.title)}).click();
  await expect(page.getByRole('dialog')).toContainText('Aguardando etapas anteriores');
  await expect(page.getByText('Old implementation routing failed',{exact:true})).not.toBeVisible();
});

test('attention filter hides healthy siblings instead of displaying an entire matching game', async ({
  page,
}) => {
  const data: any = fixture();
  data.tasks[0].state = 'failed';
  data.detail.runs[0].state = 'failed';
  data.detail.runs[0].error_code = 'AUTH_REQUIRED';
  data.detail.dependencies = [];
  await openFixture(page, data, true);
  await page.getByRole('button', { name: 'Pedem atenção', exact: true }).click();
  await expect(page.locator('.delivery-step')).toHaveCount(1);
  await expect(page.locator('.delivery-step')).toContainText('implement');
  await expect(page.getByRole('region', { name: 'Avisos e bloqueios', exact: true })).toContainText(
    'Autenticação recusada',
  );
});

test('a dependency in another objective opens that objective execution evidence', async ({
  page,
}) => {
  const data: any = fixture();
  data.tasks[0].state = 'blocked_failed';
  const external = {
    ...data.tasks[0],
    id: 'external-task',
    goal_id: 'external-goal',
    title: 'Protocolo no OpenCode',
    state: 'failed',
    execution: { kind: 'opencode' },
  };
  data.state.tasks = [...data.state.tasks, external];
  const externalRun = {
    ...data.detail.runs[0],
    id: 'external-run',
    task_id: external.id,
    state: 'failed',
    error_code: 'AUTH_REQUIRED',
    summary: 'Conta externa precisa de conexão.',
  };
  data.state.runs = [...data.state.runs, externalRun];
  data.detail.dependencies.push({ task_id: data.tasks[0].id, depends_on: external.id });
  await page.route('**/api/panel/goals/external-goal', (route) =>
    route.fulfill({
      json: {
        goal: { ...data.detail.goal, id: 'external-goal' },
        tasks: [external],
        runs: [externalRun],
        dependencies: [],
        acceptances: [],
      },
    }),
  );
  await openFixture(page, data, true);
  await page
    .getByRole('button', { name: 'Ver causa: Protocolo no OpenCode', exact: true })
    .first()
    .click();
  await expect(page.getByRole('dialog')).toContainText('Conta externa precisa de conexão.');
  await expect(page.getByRole('dialog')).toContainText('external-run');
});

test('changing the objective URL in the same tab loads the requested objective', async ({
  page,
}) => {
  const data: any = fixture();
  await page.route('**/api/panel/goals/second-goal', (route) =>
    route.fulfill({
      json: {
        ...data.detail,
        goal: { ...data.detail.goal, id: 'second-goal', title: 'Segundo objetivo' },
      },
    }),
  );
  await openFixture(page, data, true);
  await page.goto('/#goal=second-goal');
  await expect(page.getByRole('heading', { name: 'Segundo objetivo', exact: true })).toBeVisible();
});

test('shows pending model latency instead of implying that tools are working', async ({ page }) => {
  const data: any = fixture();
  data.state.events[0].created_at = new Date(Date.now() - 120000).toISOString();
  data.state.events[0].payload.contextBytes = 180000;
  data.state.events.push({
    id: '2',
    kind: 'EXECUTION_OBSERVED',
    created_at: new Date().toISOString(),
    payload: {
      runId: 'run-current',
      observation: {
        observedAt: new Date().toISOString(),
        process: { available: true, activeState: 'active' },
        actions: [],
        evidence: [],
      },
    },
  });
  await openFixture(page, data, true);
  const live = page.getByRole('region', { name: 'Detalhes ao vivo da execução' });
  await expect(live).toContainText('Aguardando resposta do modelo');
  await expect(live).toContainText('Contexto enviado: 176 KB');
  await expect(live).not.toContainText('Entre chamadas de ferramentas');
});

test('uses observed tool activity when the recent event window has no tool event', async ({
  page,
}) => {
  const data: any = fixture();
  data.state.events = [
    {
      id: '2',
      kind: 'EXECUTION_OBSERVED',
      created_at: new Date().toISOString(),
      payload: {
        runId: 'run-current',
        observation: {
          observedAt: new Date().toISOString(),
          process: { available: true, activeState: 'active' },
          actions: [
            {
              id: 'tool-one',
              tool: 'bash',
              state: 'returned',
              finishedAt: new Date().toISOString(),
              output: 'test result',
            },
          ],
          evidence: [],
        },
      },
    },
  ];
  await openFixture(page, data, true);
  await expect(page.locator('.focus-activity')).toContainText('Última ferramenta: bash');
  await expect(page.locator('.focus-activity')).not.toContainText('Sem atividade');
});
