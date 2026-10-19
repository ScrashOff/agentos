// Private host-wide GLM request semaphore. A session consumes no slot between requests.
import net from 'node:net';
import path from 'node:path';
import { chmod, mkdir, readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

export const DEFAULT_GLM_GATE_SOCKET = '/run/agentos-glm-gate/gate.sock';
export const GLM_MAX_REQUESTS = 6;

// /proc field 22 distinguishes a still-running executor from a reused PID.
// Read failures other than ENOENT are not proof that an old executor stopped.
export async function legacyExecutorIsActive({ pid, startTime }) {
  if (!Number.isSafeInteger(pid) || pid < 1 || !/^\d+$/.test(String(startTime))) {
    throw new Error('GLM_LEGACY_IDENTITY_INVALID');
  }
  let text;
  try {
    text = await readFile(`/proc/${pid}/stat`, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
  const fields = text
    .slice(text.lastIndexOf(')') + 2)
    .trim()
    .split(/\s+/);
  if (!/^\d+$/.test(fields[19] || '')) throw new Error('GLM_LEGACY_STAT_INVALID');
  return fields[19] === String(startTime);
}

export async function createGlmRequestGate({
  socketPath = DEFAULT_GLM_GATE_SOCKET,
  maxConcurrent = GLM_MAX_REQUESTS,
  reservationsPath,
  legacyReservations = [],
  legacyActive = legacyExecutorIsActive,
  refreshMs = 1000,
} = {}) {
  if (
    !Number.isSafeInteger(maxConcurrent) ||
    maxConcurrent < 1 ||
    maxConcurrent > GLM_MAX_REQUESTS
  ) {
    throw new Error('GLM_GATE_LIMIT_INVALID');
  }
  const clients = new Set();
  const holders = new Set();
  const queue = [];
  let legacyIds = [];
  let legacyBlocked = false;
  let closed = false;
  let granted = 0;
  let released = 0;
  let cancelled = 0;
  let maxObserved = 0;
  let refreshChain = Promise.resolve();
  const snapshot = () => ({
    limit: maxConcurrent,
    active: holders.size,
    queued: queue.filter((entry) => !entry.closed).length,
    legacyReserved: legacyBlocked ? maxConcurrent : legacyIds.length,
    legacyRunIds: legacyIds,
    reservationState: legacyBlocked ? 'unconfirmed' : 'confirmed',
    effectiveActive: holders.size + (legacyBlocked ? maxConcurrent : legacyIds.length),
    maxObserved,
    granted,
    released,
    cancelled,
    closed,
  });
  const send = (entry, value) => {
    if (!entry.socket.destroyed) entry.socket.write(`${JSON.stringify(value)}\n`);
  };
  const drain = () => {
    if (closed || legacyBlocked) return;
    while (queue.length && holders.size + legacyIds.length < maxConcurrent) {
      const entry = queue.shift();
      if (entry.closed || entry.socket.destroyed) continue;
      entry.state = 'granted';
      holders.add(entry);
      granted += 1;
      maxObserved = Math.max(maxObserved, holders.size + legacyIds.length);
      send(entry, { type: 'granted', ...snapshot() });
    }
    queue.forEach((entry, index) => {
      if (!entry.closed) send(entry, { type: 'queued', position: index + 1, ...snapshot() });
    });
  };
  const refresh = () => {
    // A single chain prevents an older /proc read from overwriting a newer one.
    refreshChain = refreshChain.then(async () => {
      if (closed) return;
      try {
        let records = legacyReservations;
        if (reservationsPath) {
          const parsed = JSON.parse(await readFile(reservationsPath, 'utf8'));
          records = Array.isArray(parsed) ? parsed : parsed.reservations;
        }
        if (!Array.isArray(records)) throw new Error('GLM_LEGACY_CONFIG_INVALID');
        const ids = new Set();
        const identities = new Set();
        const active = [];
        for (const record of records) {
          if (
            !record ||
            typeof record.runId !== 'string' ||
            !record.runId ||
            ids.has(record.runId)
          ) {
            throw new Error('GLM_LEGACY_CONFIG_INVALID');
          }
          if (
            !Number.isSafeInteger(record.pid) ||
            record.pid < 1 ||
            !/^\d+$/.test(String(record.startTime))
          ) {
            throw new Error('GLM_LEGACY_IDENTITY_INVALID');
          }
          const identity = `${record.pid}:${record.startTime}`;
          if (identities.has(identity)) throw new Error('GLM_LEGACY_IDENTITY_DUPLICATED');
          ids.add(record.runId);
          identities.add(identity);
          if (await legacyActive(record)) active.push(record.runId);
        }
        legacyIds = active;
        legacyBlocked = false;
        maxObserved = Math.max(maxObserved, holders.size + legacyIds.length);
      } catch {
        // Never infer spare capacity from missing files or unreadable process state.
        legacyBlocked = true;
      }
      drain();
    });
    return refreshChain;
  };
  const remove = (entry) => {
    if (entry.closed) return;
    entry.closed = true;
    clients.delete(entry);
    if (holders.delete(entry)) released += 1;
    const index = queue.indexOf(entry);
    if (index !== -1) {
      queue.splice(index, 1);
      cancelled += 1;
    }
    void refresh();
  };
  await refresh();
  if (process.platform !== 'win32')
    await mkdir(path.dirname(socketPath), { recursive: true, mode: 0o700 });
  const server = net.createServer((socket) => {
    if (closed) return socket.destroy();
    const entry = { socket, state: 'new', closed: false, buffer: '' };
    clients.add(entry);
    socket.on('error', () => remove(entry));
    socket.on('close', () => remove(entry));
    socket.on('data', (chunk) => {
      entry.buffer += chunk.toString('utf8');
      if (entry.buffer.length > 8192) return socket.destroy();
      while (entry.buffer.includes('\n')) {
        const newline = entry.buffer.indexOf('\n');
        const line = entry.buffer.slice(0, newline);
        entry.buffer = entry.buffer.slice(newline + 1);
        let message;
        try {
          message = JSON.parse(line);
        } catch {
          return socket.destroy();
        }
        if (message?.type === 'release' && entry.state === 'granted') {
          remove(entry);
          socket.end();
        } else if (
          message?.type === 'acquire' &&
          entry.state === 'new' &&
          typeof message.runId === 'string' &&
          message.runId.length > 0 &&
          message.runId.length <= 200
        ) {
          entry.runId = message.runId;
          entry.state = 'queued';
          queue.push(entry);
          send(entry, { type: 'queued', position: queue.length, ...snapshot() });
          void refresh();
        } else if (message?.type === 'status' && entry.state === 'new') {
          entry.state = 'status';
          void refresh().then(() =>
            socket.end(`${JSON.stringify({ type: 'status', ...snapshot() })}\n`),
          );
        } else {
          socket.destroy();
        }
      }
    });
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, () => {
      server.off('error', reject);
      resolve();
    });
  });
  if (process.platform !== 'win32') await chmod(socketPath, 0o600);
  const timer = setInterval(
    () => {
      void refresh();
    },
    Math.max(10, refreshMs),
  );
  timer.unref();
  return {
    server,
    socketPath,
    async status() {
      await refresh();
      return snapshot();
    },
    async close() {
      if (closed) return;
      closed = true;
      clearInterval(timer);
      // Loss of this socket is a mandatory abort signal to all model gateways.
      for (const entry of clients) entry.socket.destroy();
      await new Promise((resolve) => server.close(resolve));
      await refreshChain;
    },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2);
  const value = (name) => {
    const i = args.indexOf(name);
    return i < 0 ? undefined : args[i + 1];
  };
  const gate = await createGlmRequestGate({
    socketPath: value('--socket') || process.env.AGENTOS_GLM_GATE_SOCKET || DEFAULT_GLM_GATE_SOCKET,
    reservationsPath: value('--reservations') || process.env.AGENTOS_GLM_LEGACY_RESERVATIONS,
    maxConcurrent: Number(value('--max') || GLM_MAX_REQUESTS),
  });
  process.stdout.write(
    `${JSON.stringify({ event: 'GLM_GATE_READY', ...(await gate.status()) })}\n`,
  );
  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    await gate.close();
  };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
}
