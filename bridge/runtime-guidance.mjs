// Host-only control socket. It is NEVER mounted into the worker namespace.
import { createServer, createConnection } from 'node:net';
import { constants } from 'node:fs';
import { open, rename, chmod, unlink } from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
import path from 'node:path';

const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const identityFields = ['goalId', 'taskId', 'runId', 'runFence', 'contextVersion'];
export function validateGuidanceClaim(raw,identity){
  if(!raw||Object.keys(raw).some(k=>!['version','source','sessionId','runId','runFence','actionId','messageId','turn'].includes(k))||
    raw.version!==1||raw.source!=='sdk-agent-inbox-claimed'||raw.sessionId!==identity.runId||raw.runId!==identity.runId||raw.runFence!==identity.runFence||
    !uuid.test(raw.actionId??'')||!uuid.test(raw.messageId??'')||!Number.isSafeInteger(raw.turn)||raw.turn<0)return null;
  return {...identity,version:1,source:raw.source,sessionId:raw.sessionId,actionId:raw.actionId,messageId:raw.messageId,turn:raw.turn};
}
const sha = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const fail = code => Object.assign(new Error(code), { code });
export function validateGuidanceIdentity(identity) {
  if (!identity || !identityFields.every(key => uuid.test(identity[key] ?? ''))) throw fail('GUIDANCE_IDENTITY_INVALID');
  return Object.fromEntries(identityFields.map(key => [key, identity[key]]));
}
export function validateGuidanceRequest(raw, identity) {
  validateGuidanceIdentity(identity);
  if (!raw || !identityFields.every(key => raw[key] === identity[key])) throw fail('GUIDANCE_IDENTITY_MISMATCH');
  if (!uuid.test(raw.actionId ?? '') || typeof raw.instruction !== 'string' ||
      !raw.instruction.trim() || raw.instruction.length > 2400 || !Array.isArray(raw.facts) || raw.facts.length > 8 ||
      raw.facts.some(f => !f || typeof f.evidenceRef !== 'string' || f.evidenceRef.length > 160 ||
        !f.evidenceRef || typeof f.statement !== 'string' || !f.statement.trim() || f.statement.length > 500))
    throw fail('GUIDANCE_REQUEST_INVALID');
  // This capability adds advisory context only. Authorization and tool policies
  // remain in the existing runtime; instruction text is not a security boundary.
  return { ...validateGuidanceIdentity(raw), actionId: raw.actionId, instruction: raw.instruction,
    facts: raw.facts.map(f => ({ evidenceRef: f.evidenceRef, statement: f.statement })) };
}

async function atomicJson(file, value) {
  const temporary = file + '.' + randomUUID() + '.tmp';
  const fd = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try { await fd.writeFile(JSON.stringify(value)); await fd.sync(); } finally { await fd.close(); }
  await rename(temporary, file);
  const parent = await open(path.dirname(file), constants.O_RDONLY | constants.O_DIRECTORY);
  try { await parent.sync(); } finally { await parent.close(); }
}

/** One owner serializes actions and persists a claim BEFORE dispatch. A crash
 * between claim and receipt is uncertain, never grounds for replay. */
export async function createRuntimeGuidanceServer({ directory, identity, dispatch, emit = () => {}, clock = () => new Date().toISOString() }) {
  identity = validateGuidanceIdentity(identity);
  const socketPath = path.join(directory, 'guidance.sock');
  let closed = false, chain = Promise.resolve(), closePromise;
  const pending = new Map(), observed = new Map();
  const file = id => path.join(directory, 'guidance-' + id + '.json');
  async function read(id) {
    if (!uuid.test(id ?? '')) throw fail('GUIDANCE_REQUEST_INVALID');
    try {
      const fd = await open(file(id), constants.O_RDONLY | constants.O_NOFOLLOW);
      try { return JSON.parse(await fd.readFile('utf8')); } finally { await fd.close(); }
    } catch (e) { if (e.code === 'ENOENT') return null; throw e; }
  }
  const serial = fn => {
    const result = chain.then(fn);
    chain = result.catch(() => {});
    return result;
  };
  async function consume(receipt) {
    const proof=observed.get(receipt?.messageId);
    if (!receipt?.messageId || receipt.state !== 'queued_next_step' || !proof || proof.actionId!==receipt.actionId) return receipt;
    const consumed = { ...receipt, state: 'consumed', consumedAt: clock(), consumptionProof:proof };
    await atomicJson(file(receipt.actionId), consumed);
    pending.delete(receipt.messageId);
    emit('RUNTIME_GUIDANCE_CONSUMED', { ...identity, actionId: receipt.actionId, messageId: receipt.messageId });
    return consumed;
  }
  async function handle(raw) {
    const request = validateGuidanceRequest(raw, identity);
    const previous = await read(request.actionId);
    if (previous) {
      if (previous.requestDigest !== sha(request)) throw fail('GUIDANCE_ACTION_CHANGED');
      return previous; // Including dispatching/uncertain: do not repeat an effect.
    }
    if (closed) throw fail('GUIDANCE_RUNTIME_CLOSED');
    const claim = { ...identity, actionId: request.actionId, requestDigest: sha(request), state: 'dispatching', proposedAt: clock() };
    await atomicJson(file(request.actionId), claim);
    emit('RUNTIME_GUIDANCE_PROPOSED', { ...identity, actionId: request.actionId, requestDigest: claim.requestDigest });
    let result;
    try { result = await dispatch(request); }
    catch {
      const uncertain = { ...claim, state: 'uncertain', reason: 'RPC_OUTCOME_UNCONFIRMED' };
      await atomicJson(file(request.actionId), uncertain);
      emit('RUNTIME_GUIDANCE_UNCONFIRMED', { ...identity, actionId: request.actionId });
      return uncertain;
    }
    if (!uuid.test(result?.messageId ?? '') || result?.status !== 'queued_next_step') {
      const uncertain = { ...claim, state: 'uncertain', reason: 'RPC_RECEIPT_INVALID' };
      await atomicJson(file(request.actionId), uncertain);
      return uncertain;
    }
    const receipt = { ...claim, state: 'queued_next_step', messageId: result.messageId, queuedAt: clock() };
    await atomicJson(file(request.actionId), receipt);
    pending.set(receipt.messageId, receipt.actionId);
    emit('RUNTIME_GUIDANCE_QUEUED', { ...identity, actionId: request.actionId, messageId: receipt.messageId });
    return consume(receipt);
  }
  const server = createServer(socket => {
    let bytes = 0, text = '', responded = false;
    socket.setTimeout(15000, () => socket.destroy());
    socket.on('error', () => {});
    const reply = value => { if (!responded) { responded = true; socket.end(JSON.stringify(value) + '\n'); } };
    socket.on('data', chunk => {
      if (responded) return;
      bytes += chunk.length;
      if (bytes > 16000) { reply({ error: 'GUIDANCE_FRAME_TOO_LARGE' }); return; }
      text += chunk.toString('utf8');
      if (!text.includes('\n')) return;
      let raw;
      try { raw = JSON.parse(text.split('\n')[0]); } catch { reply({ error: 'GUIDANCE_REQUEST_INVALID' }); return; }
      const operation = raw.operation === 'receipt'
        ? () => { validateGuidanceRequest({ ...raw, instruction: 'receipt', facts: [] }, identity); return read(raw.actionId); }
        : () => handle(raw);
      void serial(operation).then(result => reply({ result }), e => reply({ error: e.code ?? 'GUIDANCE_FAILED' }));
    });
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(socketPath, resolve); });
  await chmod(socketPath, 0o600);
  return {
    socketPath,
    handle: raw => serial(() => handle(raw)),
    // Only the dedicated typed SDK claim notification proves consumption.
    // Pending insertion, tool output and RPC enqueue ACK never suffice.
    async observe(messageIds,rawProof) {
      const proof=validateGuidanceClaim(rawProof,identity);
      if(!proof||!messageIds.includes(proof.messageId))return false;
      const owned=await read(proof.actionId);
      if(!owned||owned.actionId!==proof.actionId||!identityFields.every(k=>owned[k]===identity[k])||(owned.messageId&&owned.messageId!==proof.messageId))return false;
      const prior=observed.get(proof.messageId);if(prior&&prior.actionId!==proof.actionId)return false;
      observed.set(proof.messageId,proof);
      return serial(async () => {
        for (const [messageId, actionId] of pending) if (observed.has(messageId)) await consume(await read(actionId));
      });
    },
    readReceipt: id => serial(() => read(id)),
    close() {
      closed = true;
      return closePromise ??= (async()=>{
        await new Promise(resolve => server.close(resolve));
        await chain;
        await unlink(socketPath).catch(e => { if (e.code !== 'ENOENT') throw e; });
      })();
    },
  };
}

/** Private SSH bridge forwards to the version-pinned owning process, rather
 * than a new process opening the actor's stdin or mutating its prompt. */
export async function sendRuntimeGuidance(root, request, { timeoutMs = 12000 } = {}) {
  validateGuidanceIdentity(request);
  if (!uuid.test(request.actionId ?? '')) throw fail('GUIDANCE_REQUEST_INVALID');
  const socketPath = path.join(root, request.runId, 'guidance.sock');
  return new Promise((resolve, reject) => {
    const socket = createConnection(socketPath);
    let text = '', settled = false;
    const finish = (e, result) => { if (settled) return; settled = true; socket.destroy(); e ? reject(e) : resolve(result); };
    socket.setTimeout(timeoutMs, () => finish(fail('GUIDANCE_OUTCOME_UNCONFIRMED')));
    socket.on('error', e => finish(fail(e.code === 'ENOENT' || e.code === 'ECONNREFUSED' ? 'GUIDANCE_CHANNEL_UNAVAILABLE' : 'GUIDANCE_OUTCOME_UNCONFIRMED')));
    socket.on('connect', () => socket.write(JSON.stringify(request) + '\n'));
    socket.on('data', chunk => {
      text += chunk.toString('utf8');
      if (text.length > 16000) return finish(fail('GUIDANCE_RECEIPT_INVALID'));
      if (!text.includes('\n')) return;
      try { const value = JSON.parse(text.split('\n')[0]); finish(value.error ? fail(value.error) : null, value.result); }
      catch { finish(fail('GUIDANCE_RECEIPT_INVALID')); }
    });
    socket.on('end', () => { if (!settled) finish(fail('GUIDANCE_OUTCOME_UNCONFIRMED')); });
  });
}

// An owner normally closes its socket before its final delivery is collected.
// Its durable inbox-splice receipt remains readable after that physical stop.
export async function readRuntimeGuidanceReceipt(root, request) {
  const identity=validateGuidanceIdentity(request);
  if (!uuid.test(request.actionId ?? '')) throw fail('GUIDANCE_REQUEST_INVALID');
  const file=path.join(root,request.runId,'guidance-'+request.actionId+'.json');
  let fd;
  try { fd=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW); }
  catch (e) { if (e.code==='ENOENT') return null; throw e; }
  try {
    if ((await fd.stat()).size>16000) throw fail('GUIDANCE_RECEIPT_INVALID');
    const receipt=JSON.parse(await fd.readFile('utf8'));
    if (receipt.actionId!==request.actionId || !identityFields.every(k=>receipt[k]===identity[k])) throw fail('GUIDANCE_IDENTITY_MISMATCH');
    return receipt;
  } finally { await fd.close(); }
}
