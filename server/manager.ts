import { z } from 'zod';
import { pool, tx, lock, event } from './db.js';
import { DomainError } from './contracts.js';
import {
  createCycle,
  updateCycle,
  setCycleState,
  listCycles,
  cycleDetail,
  occurrenceDetail,
  runCycle,
  commandOccurrence,
} from './cycles/store.js';
import { resolveCycleStep } from './cycles/engine.js';
import { availableActions } from './cycles/integrations.js';

export const managerTools = [
  {
    name: 'manage_cycles',
    description:
      'Administra o espaço independente de Ciclos. Nunca converta ciclos em objetivos. list/actions/get/get_run consultam; create/update recebem definitionJson com name,project,trigger:{kind:manual|interval|bitrix,seconds,groupId,stageId,includeExisting},concurrency,allowedActions,allowedAccounts,steps:[{key,title,kind:agent|action|wait|review,instructions,dependsOn,workspace,criteria,action,parameters,seconds}]. Agente exige workspace e criteria. Salvar não ativa o ciclo. run cria execução própria. activate/pause usam version. Retorne links #cycle=ID.',
    parameters: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: [
            'list',
            'actions',
            'get',
            'get_run',
            'create',
            'update',
            'activate',
            'pause',
            'run',
            'pause_run',
            'resume_run',
            'cancel_run',
            'retry_step',
          ],
        },
        id: { type: 'string' },
        version: { type: 'integer' },
        definitionJson: { type: 'string' },
        inputJson: { type: 'string' },
        note: { type: 'string' },
      },
      required: ['action'],
    },
  },
  {
    name: 'manage_operation',
    description:
      'Ajusta um objetivo existente: pause/resume preservam tarefas; prioritize altera prioridade da tarefa; workforce configura o teto Muse e GLM de sessões deste objetivo; supervision com enabled ativa/desativa a revisão automática a cada dois minutos pelo coordenador configurado. Não muda limite de seis requisições GLM. Nunca encerra ou aprova trabalho por conta própria.',
    parameters: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['pause', 'resume', 'prioritize', 'workforce', 'supervision'],
        },
        id: { type: 'string' },
        priority: { type: 'integer' },
        museTarget: { type: 'integer' },
        glmTarget: { type: 'integer' },
        plannerMaxActive: { type: 'integer', minimum: 1, maximum: 30,
          description: 'Teto de planners por escopo neste objetivo; compartilham as sessões GLM e respeitam o teto global.' },
        enabled: {
          type: 'boolean',
          description:
            'Ativa ou desativa a revisão automática pelo mesmo gestor Sol, a cada dois minutos.',
        },
      },
      required: ['action', 'id'],
    },
  },
  {
    name: 'develop_agentos',
    description:
      'Cria uma execução própria de desenvolvimento do AgentOS para implementar e testar uma mudança no app. O worker recebe cópia isolada do fonte; publicação ocorre por ação independente após validação. Use para pedidos explícitos de implementar, não para somente planejamento.',
    parameters: {
      type: 'object',
      properties: {
        request: { type: 'string' },
        accountId: { type: 'string' },
        model: { type: 'string' },
      },
      required: ['request'],
    },
  },
];
export async function managerTool(
  name: string,
  args: any,
  key: string,
  planningOnly = false,
): Promise<any> {
  const readOnly =
    name === 'manage_cycles' && ['list', 'actions', 'get', 'get_run'].includes(args.action);
  if (planningOnly && !readOnly)
    throw new DomainError(
      'PLANNING_ONLY',
      'Este pedido está em planejamento; nenhuma alteração foi aplicada.',
    );
  if (name === 'manage_cycles') {
    if (args.action === 'list') return listCycles();
    if (args.action === 'actions') return availableActions();
    if (args.action === 'create') return createCycle(JSON.parse(args.definitionJson), key);
    const id = z.uuid().parse(args.id);
    if (args.action === 'get') return cycleDetail(id);
    if (args.action === 'get_run') return occurrenceDetail(id);
    if (args.action === 'update')
      return updateCycle(id, z.number().int().parse(args.version), JSON.parse(args.definitionJson));
    if (args.action === 'activate' || args.action === 'pause')
      return setCycleState(
        id,
        z.number().int().parse(args.version),
        args.action === 'activate' ? 'active' : 'paused',
      );
    if (args.action === 'run')
      return runCycle(id, key, args.inputJson ? JSON.parse(args.inputJson) : {});
    if (['pause_run', 'resume_run', 'cancel_run'].includes(args.action))
      return commandOccurrence(id, args.action.split('_')[0]);
    if (args.action === 'retry_step')
      return resolveCycleStep(id, 'retry', z.string().min(5).parse(args.note));
  }
  if (name === 'manage_operation') {
    const id = z.uuid().parse(args.id);
    return tx(async (db) => {
      await lock(db);
      if (args.action === 'supervision') {
        const enabled = z.boolean().parse(args.enabled);
        const goal = (await db.query('SELECT id FROM goals WHERE id=$1', [id])).rows[0];
        const policy = (
          await db.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + id])
        ).rows[0]?.value;
        if (!goal || !policy?.enabled || !policy.coordinatorModel)
          throw new DomainError('WORKFORCE_MISSING', 'Objetivo sem gestor configurado.');
        const value = { enabled, intervalSeconds: 120, enabledAt: new Date().toISOString() };
        await db.query(
          'INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
          ['management:' + id, JSON.stringify(value)],
        );
        if (enabled)
          await db.query(
            'INSERT INTO settings VALUES($1,\'{"enabled":true}\') ON CONFLICT(id) DO UPDATE SET value=settings.value||excluded.value',
            ['supervision:' + id],
          );
        await event(db, 'MANAGER_AUTOMATION', id, value);
        return value;
      }
      if (args.action === 'pause' || args.action === 'resume') {
        const state = args.action === 'pause' ? 'paused' : 'active';
        const changed = (
          await db.query(
            "UPDATE goals SET state=$2 WHERE id=$1 AND state IN('active','paused') RETURNING id,state",
            [id, state],
          )
        ).rows[0];
        if (!changed) throw new DomainError('GOAL_STATE', 'Objetivo encerrado ou inexistente.');
        await event(db, 'MANAGER_GOAL_STATE', id, { state });
        return changed;
      }
      if (args.action === 'prioritize') {
        const priority = z.number().int().min(-100).max(100).parse(args.priority);
        const changed = (
          await db.query(
            "UPDATE tasks SET priority=$2 WHERE id=$1 AND state NOT IN('accepted','cancelled','superseded') RETURNING id,priority",
            [id, priority],
          )
        ).rows[0];
        if (!changed) throw new DomainError('TASK_STATE', 'Tarefa encerrada ou inexistente.');
        await event(db, 'MANAGER_TASK_PRIORITY', id, { priority });
        return changed;
      }
      if (args.action === 'workforce') {
        const patch = z
          .object({
            museTarget: z.number().int().min(0).max(6).optional(),
            glmTarget: z.number().int().min(1).max(30).optional(),
            plannerMaxActive: z.number().int().min(1).max(30).optional(),
          })
          .parse(args);
        const changed = (
          await db.query('UPDATE settings SET value=value||$2::jsonb WHERE id=$1 RETURNING value', [
            'workforce:' + id,
            JSON.stringify(patch),
          ])
        ).rows[0];
        if (!changed)
          throw new DomainError('WORKFORCE_MISSING', 'Objetivo sem política de equipe.');
        await event(db, 'MANAGER_WORKFORCE', id, patch);
        return changed.value;
      }
    });
  }
  if (name === 'develop_agentos') {
    const request = z.string().min(10).max(15000).parse(args.request);
    const sourceWorkspace =
      (
        await pool.query(
          "SELECT value->>'workspace' workspace FROM settings WHERE id='agentos-source-workspace'",
        )
      ).rows[0]?.workspace ?? 'agentos/source';
    const connection = {
      ...(args.accountId ? { accountId: args.accountId } : {}),
      ...(args.model ? { model: args.model } : {}),
    };
    const cycle = await createCycle(
      {
        name: request.slice(0, 100),
        description: 'Evolução do próprio AgentOS em cópia isolada: ' + request,
        project: 'agentos',
        trigger: { kind: 'manual' },
        allowedActions: ['agentos.validate', 'agentos.publish'],
        steps: [
          {
            key: 'implement',
            title: 'Implementar e testar mudança',
            kind: 'agent',
            workspace: sourceWorkspace,
            ...connection,
            instructions:
              request +
              '\nLeia AGENTS.md. Implemente no fonte isolado; execute testes relevantes e build. Não altere credenciais, guards ou serviços ativos. Entregue evidence/change-report.md e evidence/verification.json com passed:true somente quando testes reais passarem. A publicação é independente.',
            criteria: [
              {
                id: 'report',
                type: 'artifact',
                label: 'Relatório com arquivos e testes',
                name: 'change-report.md',
              },
              {
                id: 'verified',
                type: 'json_field',
                label: 'Validação executada',
                name: 'verification.json',
                field: 'passed',
                equals: true,
              },
            ],
          },
          {
            key: 'validate',
            title: 'Validar candidata isolada',
            kind: 'action',
            action: 'agentos.validate',
            dependsOn: ['implement'],
            repairStep: 'implement',
            criteria: [
              {
                id: 'checks',
                type: 'json_field',
                label: 'Build e suíte completos',
                name: 'verification.json',
                field: 'passed',
                equals: true,
              },
            ],
          },
          {
            key: 'publish',
            title: 'Publicar e conferir interface',
            kind: 'action',
            action: 'agentos.publish',
            dependsOn: ['validate'],
          },
        ],
      },
      key + ':definition',
    );
    const execution = await runCycle(cycle.id, key + ':run');
    return {
      cycleId: cycle.id,
      executionId: execution.id,
      link: '#cycle=' + cycle.id + '&cycleRun=' + execution.id,
      publication: 'Build e suíte isolados antes da publicação; executores ativos preservados.',
    };
  }
  throw new DomainError('MANAGER_ACTION', 'Ação de gestão não reconhecida.');
}
