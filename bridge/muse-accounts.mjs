// Muse subscription credentials remain in the isolated AgentOS account store.
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';

export const MUSE_BINARY = '/opt/agentos-muse/muse-bin-1.4.3-R5018.1';
const ROOT = '/var/lib/agentos-bridge/muse';
const cache = new Map();
const pending = new Map();
const failure = (code) => Object.assign(new Error(code), { code });

export function museAccountPaths(account) {
  const id = account.id?.match(/^agentos:muse:([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/)?.[1];
  if (!id) throw failure('MUSE_ACCOUNT_INVALID');
  const home = `${ROOT}/${id}`;
  if (account.home !== home || account.authPath !== `${home}/.config/muse/auth.json`)
    throw failure('MUSE_ACCOUNT_INVALID');
  return { home, authPath: account.authPath };
}

export async function museCredential(account) {
  const { authPath } = museAccountPaths(account);
  let meta;
  try {
    meta = JSON.parse(await readFile(authPath, 'utf8')).providers?.meta;
  } catch {
    throw failure('AUTH_REQUIRED');
  }
  if (
    !meta?.api_key ||
    typeof meta.api_key !== 'string' ||
    meta.api_base_url !== 'https://api.meta.ai/v1'
  )
    throw failure('AUTH_REQUIRED');
  return { key: meta.api_key, baseUrl: meta.api_base_url };
}

function environment(account) {
  const { home, authPath } = museAccountPaths(account);
  return {
    PATH: '/usr/local/bin:/usr/bin:/bin',
    LANG: 'C.UTF-8',
    HOME: home,
    XDG_CONFIG_HOME: `${home}/.config`,
    XDG_DATA_HOME: `${home}/.local/share`,
    XDG_CACHE_HOME: `${home}/.cache`,
    XDG_STATE_HOME: `${home}/.local/state`,
    MUSE_AUTH_PATH: authPath,
    MUSE_NO_AUTO_UPDATE: '1',
    BROWSER: '/bin/true',
    NO_COLOR: '1',
  };
}

export function museQuota(usage) {
  if (!usage || !Number.isFinite(usage.observedAtMs)) return { status: 'unknown', groups: [] };
  const windows = [];
  for (const [label, window] of [
    ['Janela atual', usage.window],
    ['Semanal', usage.weekly],
  ]) {
    // Do not infer remaining percentages from an undocumented shape.
    if (!window || !Number.isFinite(window.usedPercent) || window.usedPercent < 0) continue;
    windows.push({
      label,
      remainingPercent: Math.max(0, Math.min(100, 100 - window.usedPercent)),
      resetsAt: Number.isFinite(window.resetsAtMs)
        ? new Date(window.resetsAtMs).toISOString()
        : null,
    });
  }
  return {
    status: windows.length ? 'ok' : 'unknown',
    updated: new Date(usage.observedAtMs).toISOString(),
    groups: windows.length ? [{ label: 'Muse', windows }] : [],
    tier: usage.tier ?? null,
  };
}

export async function museSnapshot(account, { force = false } = {}) {
  museAccountPaths(account);
  const old = cache.get(account.id);
  if (!force && old && Date.now() - old.at < 60000) return old.value;
  if (pending.has(account.id)) return pending.get(account.id);
  const work = (async () => {
    await museCredential(account);
    const env = environment(account);
    await mkdir(`${account.home}/probe`, { recursive: true, mode: 0o700 });
    const child = spawn(
      MUSE_BINARY,
      ['serve', '--provider', 'meta', '--disable-shell', '--disable-write'],
      { cwd: `${account.home}/probe`, env, stdio: ['pipe', 'pipe', 'ignore'] },
    );
    const requests = new Map();
    let id = 0;
    const fail = () => {
      for (const p of requests.values()) p.reject(failure('MUSE_DISCOVERY_FAILED'));
      requests.clear();
    };
    child.once('error', fail);
    child.once('close', fail);
    child.stdin.on('error', fail);
    const lines = createInterface({ input: child.stdout });
    lines.on('line', (line) => {
      if (line.length > 2 * 1024 * 1024) return;
      try {
        const m = JSON.parse(line),
          p = requests.get(m.id);
        if (!p) return;
        requests.delete(m.id);
        m.error ? p.reject(failure('MUSE_DISCOVERY_FAILED')) : p.resolve(m.result);
      } catch {}
    });
    const rpc = (method, params = {}) =>
      new Promise((resolve, reject) => {
        requests.set(++id, { resolve, reject });
        child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
      });
    const timer = setTimeout(() => {
      fail();
      child.kill('SIGTERM');
    }, 18000);
    try {
      await rpc('initialize', { clientInfo: { name: 'agentos', version: '0.1.0' } });
      child.stdin.write(
        JSON.stringify({ jsonrpc: '2.0', method: 'initialized', params: {} }) + '\n',
      );
      const catalog = await rpc('model/list');
      if (catalog.source !== 'providerCatalog' || catalog.providerId !== 'meta')
        throw failure('MUSE_CATALOG_INVALID');
      const models = catalog.models
        .map((m) => m.modelId)
        .filter((m) => typeof m === 'string' && /^muse-/.test(m));
      if (!models.length) throw failure('MUSE_CATALOG_EMPTY');
      const usage = await rpc('usage/read');
      const value = {
        models,
        model: models.includes('muse-spark-1.3')
          ? 'muse-spark-1.3'
          : (models.find((m) => !m.includes('contributor')) ?? models[0]),
        quota: museQuota(usage.usage),
        authStatus: 'connected',
        connected: true,
      };
      cache.set(account.id, { at: Date.now(), value });
      return value;
    } finally {
      clearTimeout(timer);
      lines.close();
      child.stdin.end();
      const kill = setTimeout(() => child.kill('SIGKILL'), 1000);
      kill.unref();
      child.once('close', () => clearTimeout(kill));
    }
  })().finally(() => pending.delete(account.id));
  pending.set(account.id, work);
  return work;
}

export function parseMuseLoginNotice(output) {
  const clean = output.replace(/\x1b\[[0-9;]*[A-Za-z]/g, '');
  const links = clean.match(/https:\/\/[^\s<>"']+/g) ?? [];
  for (const link of links) {
    let url;
    try {
      url = new URL(link);
    } catch {
      continue;
    }
    if (url.hostname !== 'auth.meta.com' || url.username || url.password) continue;
    const userCode =
      url.searchParams.get('user_code') ??
      clean.match(/(?:Enter|Confirm)[^\n]*code[^\n]*\n\s*([A-Z0-9-]{4,20})/i)?.[1];
    return {
      type: 'auth_url',
      url: url.href,
      instructions: 'Entre na sua conta Meta para conectar o Muse Code.',
      ...(userCode ? { userCode } : {}),
    };
  }
  return null;
}

export async function museLogin(account, signal, notify) {
  museAccountPaths(account);
  await mkdir(path.dirname(account.authPath), { recursive: true, mode: 0o700 });
  const child = spawn(MUSE_BINARY, ['login'], {
    cwd: account.home,
    env: environment(account),
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '',
    lastUrl;
  const collect = (b) => {
    output = (output + b.toString()).slice(-24000);
    const notice = parseMuseLoginNotice(output);
    if (notice && notice.url !== lastUrl) {
      lastUrl = notice.url;
      notify(notice);
    }
  };
  child.stdout.on('data', collect);
  child.stderr.on('data', collect);
  let escalation;
  const abort = () => {
    child.kill('SIGTERM');
    escalation = setTimeout(() => child.kill('SIGKILL'), 1000);
    escalation.unref();
  };
  signal.addEventListener('abort', abort, { once: true });
  if (signal.aborted) abort();
  try {
    const code = await new Promise((resolve, reject) => {
      child.once('error', () => reject(failure('MUSE_LOGIN_START_FAILED')));
      child.once('close', resolve);
    });
    if (signal.aborted) throw failure('CANCELLED');
    if (code !== 0) throw failure('MUSE_LOGIN_FAILED');
    await museCredential(account);
    const meta = JSON.parse(await readFile(account.authPath, 'utf8')).providers.meta;
    const snapshot = await museSnapshot(account, { force: true });
    return {
      ...snapshot,
      identity: { email: meta.user_email ?? null, name: meta.user_full_name ?? null },
    };
  } finally {
    signal.removeEventListener('abort', abort);
    clearTimeout(escalation);
  }
}
