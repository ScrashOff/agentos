// Watches real provider progress, not transport keepalives. No model content,
// credential or provider error message leaves this helper.
export const MODEL_IDLE_TIMEOUT_MS = 300_000;
const idleFailure = () => Object.assign(Error('MODEL_IDLE_TIMEOUT'), {code:'MODEL_IDLE_TIMEOUT'});
export const modelAbortFailure = signal => signal?.reason?.code === 'MODEL_IDLE_TIMEOUT'
  ? idleFailure() : Object.assign(Error('CANCELLED'), {code:'CANCELLED'});

export function semanticModelWatchdog(callerSignal, idleTimeoutMs = MODEL_IDLE_TIMEOUT_MS,
  timers = {setTimeout, clearTimeout}) {
  if (!Number.isFinite(idleTimeoutMs) || idleTimeoutMs <= 0)
    throw Object.assign(Error('INVALID_MODEL_IDLE_TIMEOUT'), {code:'INVALID_MODEL_IDLE_TIMEOUT'});
  const controller = new AbortController();
  let timer, disposed = false;
  const clear = () => { if (timer !== undefined) timers.clearTimeout(timer); timer = undefined; };
  const callerAborted = () => { clear(); controller.abort(callerSignal.reason); };
  const progress = () => {
    if (disposed || controller.signal.aborted) return;
    clear();
    timer = timers.setTimeout(() => { timer = undefined; controller.abort(idleFailure()); }, idleTimeoutMs);
    timer?.unref?.();
  };
  callerSignal?.addEventListener('abort', callerAborted, {once:true});
  if (callerSignal?.aborted) callerAborted();
  else progress(); // Includes fetch/headers and time until the first semantic delta.
  return {signal:controller.signal, progress,
    dispose() { disposed = true; clear(); callerSignal?.removeEventListener('abort', callerAborted); }};
}

// Cancellation is awaited before this generator settles: the outer gate must
// keep its permit while the response reader/transport is unwinding.
export async function* readAbortableModelBody(body, signal) {
  if (!body) throw Object.assign(Error('MODEL_INCOMPLETE_RESPONSE'), {code:'MODEL_INCOMPLETE_RESPONSE'});
  if (typeof body.getReader !== 'function') {
    // Compatibility with finite async iterables used by offline adapters/tests.
    for await (const part of body) {
      if (signal?.aborted) throw modelAbortFailure(signal);
      yield part;
    }
    if (signal?.aborted) throw modelAbortFailure(signal);
    return;
  }
  const reader = body.getReader();
  let ended = false, cancellation;
  const cancel = () => {
    cancellation ??= Promise.resolve().then(() => reader.cancel(signal?.reason)).catch(() => {});
    return cancellation;
  };
  const aborted = () => { void cancel(); };
  signal?.addEventListener('abort', aborted, {once:true});
  try {
    if (signal?.aborted) throw modelAbortFailure(signal);
    while (true) {
      const part = await reader.read();
      if (signal?.aborted) throw modelAbortFailure(signal);
      if (part.done) { ended = true; break; }
      yield part.value;
    }
  } finally {
    signal?.removeEventListener('abort', aborted);
    if (!ended || signal?.aborted) await cancel();
    if (cancellation) await cancellation;
    reader.releaseLock();
  }
}
