import { readModelPolicy } from './model-policy.mjs';
import { readFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const exec = promisify(execFile);
export async function contextService(method, params) {
  if (method === 'context.rank') {
    if (readModelPolicy()) throw Error('MODEL_POLICY_GLM_ONLY');
    if (
      typeof params.goal !== 'string' ||
      params.goal.length > 1200 ||
      !Array.isArray(params.blocks) ||
      params.blocks.length > 16
    )
      throw Error('CONTEXT_INVALID');
    const r = await fetch('http://127.0.0.1:3081/context-rank', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
      redirect: 'error',
      signal: AbortSignal.timeout(8000),
    });
    if (!r.ok) throw Error('LAYA_UNAVAILABLE');
    return r.json();
  }
  if (method === 'memory.search') {
    if (!/^[a-zA-Z0-9_-]{1,100}$/.test(params.project) || typeof params.query !== 'string')
      throw Error('MEMORY_SCOPE');
    // This reader opens the existing notes in SQLite read-only mode. It never invokes its writer.
    const { stdout } = await exec(
      '/usr/bin/python3',
      ['/opt/agentos-bridge/read-memory.py', params.project, params.query.slice(0, 1200)],
      { timeout: 8000, maxBuffer: 256 * 1024 },
    );
    return JSON.parse(stdout);
  }
  if (method === 'memory.push') {
    if (
      params.space !== 'agentos-development' ||
      !/^agentos-[a-f0-9-]{36}$/.test(params.id) ||
      params.scope !== 'private'
    )
      throw Error('MEMORY_SCOPE');
    const token = (await readFile('/etc/harness-memory/hub-token', 'utf8')).trim();
    const r = await fetch('https://recall.example.ts.net:8443/shared/push', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      redirect: 'error',
      signal: AbortSignal.timeout(10000),
      body: JSON.stringify(params),
    });
    if (!r.ok) throw Error('RECALL_HTTP_' + r.status);
    return r.json();
  }
  throw Error('METHOD_NOT_FOUND');
}
