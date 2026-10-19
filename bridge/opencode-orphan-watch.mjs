import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const helper = fileURLToPath(new URL('./opencode-orphans.py', import.meta.url));
async function request(action, runId, proof) {
  return new Promise((resolve) => {
    const child = spawn('python3', [helper, action, ...(action === 'inspect' ? [runId] : [])],
      { stdio: ['pipe', 'pipe', 'ignore'], windowsHide: true });
    let body = '', expired = false;
    const timer = setTimeout(() => { expired = true; child.kill(); }, 6000);
    child.stdin.on('error', () => {});
    child.stdout.on('data', (bytes) => { if (body.length < 8192) body += bytes; });
    child.on('error', () => { clearTimeout(timer); resolve(null); });
    child.on('close', (code) => {
      clearTimeout(timer);
      try { resolve(!expired && code === 0 && body.length <= 8192 ? JSON.parse(body) : null); }
      catch { resolve(null); }
    });
    child.stdin.end(proof ? JSON.stringify(proof) : '');
  });
}

export function createOrphanCheck({ runId, canRecover, emit = () => {}, inspect, recover }) {
  let previous;
  return async () => {
    if (!canRecover()) { previous = undefined; return; }
    const proof = await inspect(runId);
    if (!proof || !canRecover()) { previous = undefined; return; }
    const stable = previous && JSON.stringify(previous) === JSON.stringify(proof);
    previous = proof;
    if (!stable) return;
    const result = await recover(proof);
    previous = undefined;
    if (result?.action === 'terminated') emit(result.reason === 'EXPIRED_TOOL_TIMEOUT_CONFIRMED'
      ? 'OPENCODE_EXPIRED_TOOL_RECOVERED' : 'OPENCODE_ORPHAN_FIXTURE_RECOVERED', result);
  };
}

export function watchOrphanFixtures(runId, canRecover, emit) {
  let stopped = false, checking = false;
  const check = createOrphanCheck({ runId, canRecover: () => !stopped && canRecover(), emit,
    inspect: (id) => request('inspect', id), recover: (proof) => request('recover', runId, proof) });
  const timer = setInterval(async () => {
    if (checking || stopped) return;
    checking = true;
    try { await check(); } catch { /* No fallback signal on uncertain evidence. */ }
    finally { checking = false; }
  }, 15000);
  timer.unref();
  return () => { stopped = true; clearInterval(timer); };
}
