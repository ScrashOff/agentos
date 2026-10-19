import net from 'node:net';
import { DEFAULT_GLM_GATE_SOCKET } from './glm-request-gate.mjs';

const failure = (code) => Object.assign(new Error(code), { code });
const notify = (callback, value) => {
  try {
    callback?.(value);
  } catch {
    /* Observers cannot release a permit. */
  }
};

// Keep this slot until the complete provider response body is consumed/cancelled,
// not merely until fetch() resolves with response headers.
export function acquireGlmRequestSlot({
  runId,
  signal,
  onLost,
  onState,
  socketPath = process.env.AGENTOS_GLM_GATE_SOCKET || DEFAULT_GLM_GATE_SOCKET,
} = {}) {
  return new Promise((resolve, reject) => {
    if (typeof runId !== 'string' || !runId || runId.length > 200)
      return reject(failure('GLM_GATE_RUN_INVALID'));
    if (signal?.aborted) return reject(failure('GLM_REQUEST_ABORTED'));
    const socket = net.createConnection(socketPath);
    let buffer = '';
    let acquired = false;
    let finished = false;
    const cleanup = () => signal?.removeEventListener('abort', abort);
    const lost = (error) => {
      if (finished) return;
      finished = true;
      cleanup();
      socket.destroy();
      notify(onState, { state: 'lost', code: error.code });
      if (acquired) notify(onLost, error);
      else reject(error);
    };
    const abort = () => lost(failure('GLM_REQUEST_ABORTED'));
    const release = () => {
      if (finished) return;
      finished = true;
      cleanup();
      socket.end(`${JSON.stringify({ type: 'release' })}\n`);
      notify(onState, { state: 'released' });
    };
    signal?.addEventListener('abort', abort, { once: true });
    // Cover an abort racing with listener registration.
    if (signal?.aborted) abort();
    socket.once('connect', () => {
      if (!finished) socket.write(`${JSON.stringify({ type: 'acquire', runId })}\n`);
    });
    socket.on('error', () => lost(failure('GLM_GATE_UNAVAILABLE')));
    socket.on('end', () => lost(failure('GLM_GATE_LOST')));
    socket.on('close', () => lost(failure('GLM_GATE_LOST')));
    socket.on('data', (chunk) => {
      if (finished) return;
      buffer += chunk.toString('utf8');
      if (buffer.length > 65536) return lost(failure('GLM_GATE_PROTOCOL'));
      while (buffer.includes('\n')) {
        const newline = buffer.indexOf('\n');
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        let message;
        try {
          message = JSON.parse(line);
        } catch {
          return lost(failure('GLM_GATE_PROTOCOL'));
        }
        if (message?.type === 'queued' && !acquired) {
          notify(onState, { ...message, state: 'queued' });
        } else if (message?.type === 'granted' && !acquired) {
          notify(onState, { ...message, state: 'acquired' });
          // A synchronous observer may abort the owning request on admission.
          // Reject that acquisition rather than returning an already-released slot.
          if (!finished) {
            acquired = true;
            resolve({ release });
          }
        } else return lost(failure('GLM_GATE_PROTOCOL'));
      }
    });
  });
}

export function getGlmRequestGateStatus({
  socketPath = process.env.AGENTOS_GLM_GATE_SOCKET || DEFAULT_GLM_GATE_SOCKET,
  timeoutMs = 3000,
} = {}) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    let buffer = '';
    let done = false;
    const finish = (error, value) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      socket.destroy();
      if (error) reject(error);
      else resolve(value);
    };
    const timer = setTimeout(() => finish(failure('GLM_GATE_STATUS_TIMEOUT')), timeoutMs);
    socket.on('connect', () => socket.write(`${JSON.stringify({ type: 'status' })}\n`));
    socket.on('error', () => finish(failure('GLM_GATE_UNAVAILABLE')));
    socket.on('close', () => {
      if (!done) finish(failure('GLM_GATE_LOST'));
    });
    socket.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      if (buffer.length > 65536) return finish(failure('GLM_GATE_PROTOCOL'));
      if (buffer.includes('\n')) {
        try {
          const message = JSON.parse(buffer.slice(0, buffer.indexOf('\n')));
          if (message?.type !== 'status') throw new Error();
          finish(null, message);
        } catch {
          finish(failure('GLM_GATE_PROTOCOL'));
        }
      }
    });
  });
}
