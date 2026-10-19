import { readFile } from 'node:fs/promises';
import { DomainError } from '../contracts.js';
import { hash } from '../memory.js';

export function clean(value: any): any {
  if (Array.isArray(value)) return value.map(clean);
  if (value && typeof value === 'object')
    return Object.fromEntries(
      Object.entries(value).map(([k, v]) => [
        k,
        /^(authorization|password|secret|access_token|refresh_token|api_key|apiKey|webhook)$/i.test(
          k,
        )
          ? '[redacted]'
          : clean(v),
      ]),
    );
  return typeof value === 'string'
    ? value
        .replace(/https?:\/\/[^\s"'<>]+\/rest\/\d+\/[^/\s]+\//g, '[private-webhook]/')
        .replace(/([?&](?:auth|token|signature|access_token)=)[^&\s"'<>]+/gi, '$1[redacted]')
        .replace(/\bsk-[\w.-]{24,}/g, '[redacted]')
    : value;
}
export async function bitrixCall(method: string, params: any) {
  const allowed = [
    'tasks.task.list',
    'tasks.task.get',
    'im.dialog.messages.get',
    'disk.attachedObject.get',
    'task.commentitem.add',
    'tasks.task.update',
  ];
  if (!allowed.includes(method)) throw new DomainError('BITRIX_METHOD', 'Método fora do conector.');
  const key = process.env.AGENTOS_BITRIX_KEY;
  if (!key) throw new DomainError('BITRIX_AUTH', 'Configure a conexão Bitrix no servidor.');
  const response = await fetch(
    (process.env.AGENTOS_BITRIX_URL ?? 'https://bitrix.example.com').replace(/\/$/, '') +
      '/api/call',
    {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json' },
      body: JSON.stringify({ method, params }),
      signal: AbortSignal.timeout(30000),
      redirect: 'error',
    },
  );
  if (!response.ok)
    throw new DomainError(
      response.status === 429 ? 'RATE_LIMIT' : 'BITRIX_UNAVAILABLE',
      'Conexão Bitrix indisponível (HTTP ' + response.status + ').',
    );
  const envelope = (await response.json()) as any;
  if (envelope.status !== 'success')
    throw new DomainError('BITRIX_UNAVAILABLE', 'O proxy não confirmou a leitura solicitada.');
  const data = envelope.data?.result ?? envelope.data;
  if (data?.error)
    throw new DomainError('BITRIX_ACCESS', 'O Bitrix não permitiu a operação solicitada.');
  return data;
}
export async function bitrixSnapshot(id: number) {
  const data = await bitrixCall('tasks.task.get', {
    taskId: id,
    select: ['*', 'UF_TASK_WEBDAV_FILES', 'CHAT_ID'],
  });
  const task = data.task ?? data;
  const messages: any[] = [];
  if (task.chatId) {
    let last: number | undefined;
    for (let page = 0; page < 100; page++) {
      const data = await bitrixCall('im.dialog.messages.get', {
        DIALOG_ID: 'chat' + task.chatId,
        LIMIT: 100,
        ...(last ? { LAST_ID: last } : {}),
      });
      const batch = data.messages ?? [];
      if (!batch.length) break;
      messages.push(...batch);
      const next = Math.min(...batch.map((m: any) => Number(m.id)));
      if (last && next >= last)
        throw new DomainError('BITRIX_PAGINATION', 'Paginação do chat não avançou.');
      last = next;
      if (batch.length < 100) break;
      if (page === 99)
        throw new DomainError(
          'BITRIX_TOO_LARGE',
          'Chat excedeu a janela de coleta; leitura incompleta.',
        );
    }
  }
  const attachmentIds = task.ufTaskWebdavFiles ?? [];
  // Never pretend a linked attachment was read. Release gates inspect this flag.
  return clean({
    task,
    messages,
    attachmentIds,
    attachmentsRead: attachmentIds.length === 0,
    checkedAt: new Date().toISOString(),
  });
}
export async function listBitrix(groupId: number, stageId: number) {
  const rows: any[] = [];
  let start = 0;
  for (let page = 0; page < 100; page++) {
    const data = await bitrixCall('tasks.task.list', {
      filter: { GROUP_ID: groupId, STAGE_ID: stageId },
      select: ['ID', 'TITLE', 'DESCRIPTION', 'STAGE_ID', 'STATUS'],
      order: { ID: 'ASC' },
      start,
    });
    const batch = data.tasks ?? [];
    rows.push(...batch);
    if (batch.length < 50 && data.next === undefined) break;
    const next = Number(data.next ?? start + 50);
    if (next <= start) throw new DomainError('BITRIX_PAGINATION', 'Paginação não avançou.');
    start = next;
    if (page === 99)
      throw new DomainError(
        'BITRIX_TOO_LARGE',
        'Funil excedeu limite de coleta; consulta incompleta.',
      );
  }
  return rows.map((t) => ({
    id: Number(t.id ?? t.ID),
    title: String(t.title ?? t.TITLE),
    revision: hash(JSON.stringify([t.title ?? t.TITLE, t.description ?? t.DESCRIPTION])),
    stageId: Number(t.stageId ?? t.STAGE_ID),
  }));
}

export type ActionProfile = {
  label: string;
  command: string;
  args: string[];
  cwd?: string;
  effect: 'read' | 'write' | 'destructive';
  timeoutSeconds?: number;
  projects: string[];
};
export async function actionProfiles(): Promise<Record<string, ActionProfile>> {
  const file = process.env.AGENTOS_ACTION_PROFILES;
  if (!file) return {};
  return JSON.parse(await readFile(file, 'utf8'));
}
export async function availableActions() {
  return [
    { id: 'bitrix.inspect', label: 'Ler card completo e chat do Bitrix', effect: 'read' },
    ...Object.entries(await actionProfiles()).map(([id, p]) => ({
      id,
      label: p.label,
      effect: p.effect,
      projects: p.projects,
    })),
  ];
}
