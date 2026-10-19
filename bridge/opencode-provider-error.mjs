// Only typed provider availability evidence leaves the trusted gateway.
// Never retain an upstream message, body, URL, credential or arbitrary header.
const quotaCodes = new Set([
  'insufficient_quota', 'credit_balance_exhausted',
  'organization_spend_limit_exceeded', 'project_spend_limit_exceeded',
  'organization_usage_limit_exceeded',
]);
const shortDay = '(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)';
const longDay = '(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)';
const month = '(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)';
const httpDates = [
  new RegExp(`^${shortDay}, \\d{2} ${month} \\d{4} \\d{2}:\\d{2}:\\d{2} GMT$`),
  new RegExp(`^${longDay}, \\d{2}-${month}-\\d{2} \\d{2}:\\d{2}:\\d{2} GMT$`),
  new RegExp(`^${shortDay} ${month} (?: \\d|\\d{2}) \\d{2}:\\d{2}:\\d{2} \\d{4}$`),
];

export function retryAfterSeconds(value, now = Date.now()) {
  if (typeof value !== 'string' || value.length > 80 || !Number.isFinite(now)) return null;
  const text = value.trim();
  let seconds;
  if (/^\d+$/.test(text)) seconds = Number(text);
  else if (httpDates.some(pattern => pattern.test(text))) {
    // HTTP asctime dates are UTC even though that obsolete format omits GMT.
    let normalized = text.endsWith('GMT') ? text : text + ' GMT';
    if (httpDates[1].test(text)) {
      const year = new Date(now).getUTCFullYear();
      let expanded = Math.floor(year / 100) * 100 + Number(text.match(/-(\d{2}) /)[1]);
      if (expanded > year + 50) expanded -= 100;
      normalized = text.replace(/-(\d{2}) /, `-${expanded} `);
    }
    const date = Date.parse(normalized);
    seconds = Math.max(0, Math.ceil((date - now) / 1000));
  } else return null;
  return Number.isSafeInteger(seconds) && seconds >= 0 &&
    Number.isSafeInteger(now + seconds * 1000) && now + seconds * 1000 <= 8.64e15
    ? seconds : null;
}

async function quotaBody(response, maxBytes, timeoutMs) {
  if (!response.body) return false;
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0, timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => { void reader.cancel().catch(() => {}); reject(Error('ERROR_BODY_TIMEOUT')); }, timeoutMs);
  });
  try {
    while (true) {
      const part = await Promise.race([reader.read(), timeout]);
      if (part.done) break;
      size += part.value.byteLength;
      if (size > maxBytes) return false;
      chunks.push(part.value);
    }
    const value = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    const error = value?.error;
    return error && typeof error === 'object' &&
      [error.code, error.type].some(code => typeof code === 'string' && quotaCodes.has(code));
  } catch { return false; }
  finally {
    clearTimeout(timer);
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export async function classifyProviderError(response, { now = Date.now(), maxBytes = 8192, timeoutMs = 1500 } = {}) {
  const status = response.status;
  const retry = retryAfterSeconds(response.headers.get('retry-after'), now);
  if (status === 429) {
    const quota = await quotaBody(response, maxBytes, timeoutMs);
    return { code: quota ? 'QUOTA_EXCEEDED' : 'RATE_LIMIT', status,
      category: quota ? 'quota' : 'rate_limit', retryAfterSeconds: retry };
  }
  await response.body?.cancel().catch(() => {});
  if (status === 402) return { code: 'QUOTA_EXCEEDED', status, category: 'payment', retryAfterSeconds: retry };
  if ([401, 403].includes(status)) return { code: 'AUTH_REQUIRED', status, category: 'authentication', retryAfterSeconds: null };
  return { code: 'MODEL_REQUEST_FAILED', status, category: 'request', retryAfterSeconds: null };
}

export function temporaryProviderReply(failure) {
  if (failure?.code !== 'RATE_LIMIT' || failure.status !== 429) throw Error('INVALID_PROVIDER_FAILURE');
  const retry = failure.retryAfterSeconds;
  return { status: 429, headers: { 'content-type': 'application/json', 'cache-control': 'no-store',
    ...(Number.isSafeInteger(retry) && retry >= 0 ? { 'retry-after': String(retry) } : {}) },
    body: JSON.stringify({ error: { message: 'RATE_LIMIT', type: 'rate_limit_error', code: 'rate_limit_error' } }) };
}

export function providerBackoff(failure, { runId, workspace, model }, now = Date.now()) {
  if (failure?.code !== 'RATE_LIMIT' || failure.status !== 429 ||
      !Number.isSafeInteger(failure.retryAfterSeconds) || failure.retryAfterSeconds < 0) return null;
  const until = now + failure.retryAfterSeconds * 1000;
  if (!Number.isSafeInteger(until) || until > 8.64e15) return null;
  return { version: 1, source: 'http_retry_after', runId, workspace, model,
    status: 429, category: 'rate_limit', observedAt: new Date(now).toISOString(), notBefore: new Date(until).toISOString() };
}

// The caller has already checked the real physical stop and immutable execution
// identity. This additional fence retains a provider wait across actor restart.
export function pendingProviderBackoff(state, identity, now = Date.now()) {
  const value = state?.providerBackoff;
  if (value == null) return null;
  const observed = Date.parse(value.observedAt), until = Date.parse(value.notBefore);
  if (value.version !== 1 || value.source !== 'http_retry_after' || value.status !== 429 ||
      value.category !== 'rate_limit' || value.runId !== identity.runId ||
      value.workspace !== identity.workspace || value.model !== identity.model ||
      state.runId !== identity.runId || state.workspace !== identity.workspace || state.model !== identity.model ||
      !Number.isSafeInteger(observed) || !Number.isSafeInteger(until) || until < observed || observed > now + 5000 ||
      new Date(observed).toISOString() !== value.observedAt || new Date(until).toISOString() !== value.notBefore)
    throw Error('INVALID_PROVIDER_BACKOFF_PROOF');
  return until > now ? { notBefore: value.notBefore, seconds: Math.ceil((until - now) / 1000) } : null;
}

export function isRateLimitClientError(item) {
  return item?.type === 'error' && item.error?.name === 'APIError' &&
    item.error?.data?.statusCode === 429;
}

export function mergeProviderBackoff(previous, incoming, now = Date.now()) {
  // Concurrent 429s can arrive out of order. An absent/shorter header is not
  // evidence revoking the longer minimum already received by this actor.
  if (previous && Date.parse(previous.notBefore) > now &&
      (!incoming || Date.parse(previous.notBefore) > Date.parse(incoming.notBefore))) return previous;
  return incoming;
}

export function successfulProviderRequest(failure, wait, requestStartedAt) {
  const newerFailure = failure && failure.observedAt >= requestStartedAt;
  return {
    failure: newerFailure ? failure : null,
    wait: wait && (newerFailure || Date.parse(wait.observedAt) >= requestStartedAt) ? wait : null,
  };
}

export function requestWithProviderBackoff(state, identity, request, now = Date.now()) {
  const waiting = pendingProviderBackoff(state, identity, now);
  // This fence runs before gate admission and network I/O, including when an
  // SDK caps/overflows its own timer or immediately replays the local request.
  if (waiting) return { code:'RATE_LIMIT',status:429,category:'rate_limit',
    retryAfterSeconds:waiting.seconds,localBackoff:true,observedAt:Date.parse(state.providerBackoff.observedAt) };
  return request();
}
