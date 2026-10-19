import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile, symlink, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { createRuntimeGuidanceServer, sendRuntimeGuidance, readRuntimeGuidanceReceipt } from '../bridge/runtime-guidance.mjs';

const identity = () => Object.fromEntries(['goalId','taskId','runId','runFence','contextVersion'].map(k => [k, randomUUID()]));
const request = id => ({ ...id, actionId: randomUUID(), instruction: 'Read the discriminating failure and correct the scoped implementation.', facts: [{ evidenceRef: 'event:42', statement: 'The scoped test returned a nonzero exit.' }] });
const claim=(req,messageId)=>({version:1,source:'sdk-agent-inbox-claimed',sessionId:req.runId,runId:req.runId,runFence:req.runFence,actionId:req.actionId,messageId,turn:1});
async function fixture(t, dispatch) {
  const root = await mkdtemp(path.join(tmpdir(), 'guide-'));
  const id = identity();
  const directory = path.join(root, id.runId);
  const { mkdir } = await import('node:fs/promises'); await mkdir(directory, { mode: 0o700 });
  const events = [];
  const owner = await createRuntimeGuidanceServer({ directory, identity: id, dispatch, emit: (kind, payload) => events.push({ kind, payload }) });
  t.after(async () => { await owner.close(); await rm(root, { recursive: true, force: true }); });
  return { root, directory, id, owner, events };
}
test('a private socket queues an identity-bound next-step and only canonical claim proof consumes it', async t => {
  const messageId = randomUUID();
  const f = await fixture(t, async () => ({ messageId, status: 'queued_next_step' }));
  const req = request(f.id);
  const receipt = await sendRuntimeGuidance(f.root, req);
  assert.equal(receipt.state, 'queued_next_step');
  assert.equal((await stat(f.owner.socketPath)).mode & 0o777, 0o600);
  await f.owner.observe([randomUUID()]);
  assert.equal((await f.owner.readReceipt(req.actionId)).state, 'queued_next_step');
  await f.owner.observe([messageId]);
  assert.equal((await f.owner.readReceipt(req.actionId)).state,'queued_next_step');
  await f.owner.observe([messageId],claim(req,messageId));
  assert.equal((await sendRuntimeGuidance(f.root, { ...f.id, actionId: req.actionId, operation: 'receipt' })).state, 'consumed');
  assert.deepEqual(f.events.map(e => e.kind), ['RUNTIME_GUIDANCE_PROPOSED','RUNTIME_GUIDANCE_QUEUED','RUNTIME_GUIDANCE_CONSUMED']);
});
test('concurrent identical sends have exactly one dispatch and recover the same receipt', async t => {
  let calls = 0;
  const f = await fixture(t, async () => { calls++; await new Promise(r => setTimeout(r, 15)); return { messageId: randomUUID(), status: 'queued_next_step' }; });
  const req = request(f.id);
  const receipts = await Promise.all(Array.from({ length: 8 }, () => sendRuntimeGuidance(f.root, req)));
  assert.equal(calls, 1); assert.ok(receipts.every(r => r.messageId === receipts[0].messageId));
});
test('reused action id with changed instruction is rejected', async t => {
  const f = await fixture(t, async () => ({ messageId: randomUUID(), status: 'queued_next_step' }));
  const req = request(f.id); await f.owner.handle(req);
  await assert.rejects(f.owner.handle({ ...req, instruction: 'Different context.' }), { code: 'GUIDANCE_ACTION_CHANGED' });
});
test('old run fence or context cannot steer a different attempt', async t => {
  let calls = 0;
  const f = await fixture(t, async () => { calls++; return {}; });
  for (const key of ['runId','runFence','contextVersion','taskId','goalId'])
    await assert.rejects(f.owner.handle({ ...request(f.id), [key]: randomUUID() }), { code: 'GUIDANCE_IDENTITY_MISMATCH' });
  assert.equal(calls, 0);
});
test('ambiguous dispatch is persisted and never replayed as a success', async t => {
  let calls = 0;
  const f = await fixture(t, async () => { calls++; throw Error('connection lost after send'); });
  const req = request(f.id);
  assert.equal((await f.owner.handle(req)).state, 'uncertain');
  assert.equal((await f.owner.handle(req)).state, 'uncertain');
  assert.equal(calls, 1);
});
test('a crash window claim from a former owner prevents automatic replay', async t => {
  let calls = 0;
  const f = await fixture(t, async () => { calls++; return {}; });
  const req = request(f.id);
  const { createHash } = await import('node:crypto');
  const digest = createHash('sha256').update(JSON.stringify(req)).digest('hex');
  await writeFile(path.join(f.directory, 'guidance-' + req.actionId + '.json'), JSON.stringify({ ...f.id, actionId: req.actionId, requestDigest: digest, state: 'dispatching' }));
  assert.equal((await f.owner.handle(req)).state, 'dispatching'); assert.equal(calls, 0);
});
test('canonical claim preceding RPC acknowledgement is recovered, never mislabeled as effect verified', async t => {
  const messageId = randomUUID(); let owner,observation;
  const f = await fixture(t, async req => { observation=owner.observe([messageId],claim(req,messageId)); return { messageId, status: 'queued_next_step' }; });
  owner = f.owner;
  const receipt = await owner.handle(request(f.id));
  await observation;assert.equal((await owner.readReceipt(receipt.actionId)).state, 'consumed'); assert.equal(receipt.effectVerified, undefined);
});
test('journal symlink is refused and target is preserved', async t => {
  const f = await fixture(t, async () => ({}));
  const req = request(f.id), outside = path.join(f.root, 'outside');
  await writeFile(outside, 'preserve');
  await symlink(outside, path.join(f.directory, 'guidance-' + req.actionId + '.json'));
  await assert.rejects(f.owner.handle(req), { code: 'ELOOP' });
  assert.equal(await (await import('node:fs/promises')).readFile(outside, 'utf8'), 'preserve');
});
test('oversized advice and non UUID fence are refused before effects', async t => {
  const f = await fixture(t, async () => { throw Error('must not dispatch'); });
  await assert.rejects(f.owner.handle({ ...request(f.id), instruction: 'a'.repeat(2401) }), { code: 'GUIDANCE_REQUEST_INVALID' });
  await assert.rejects(f.owner.handle({ ...request(f.id), runFence: 1 }), { code: 'GUIDANCE_IDENTITY_MISMATCH' });
});
test('the durable consumed receipt survives socket shutdown for final-effect reconciliation', async t=>{
 const messageId=randomUUID();const f=await fixture(t,async()=>({messageId,status:'queued_next_step'}));
 const req=request(f.id);await f.owner.handle(req);await f.owner.observe([messageId],claim(req,messageId));
 await f.owner.close();
 const receipt=await readRuntimeGuidanceReceipt(f.root,req);assert.equal(receipt.state,'consumed');
 await assert.rejects(readRuntimeGuidanceReceipt(f.root,{...req,contextVersion:randomUUID()}),{code:'GUIDANCE_IDENTITY_MISMATCH'});
});
test('foreign, cancellation-shaped, missing and malformed claim provenance cannot consume',async t=>{
 const messageId=randomUUID(),f=await fixture(t,async()=>({messageId,status:'queued_next_step'})),req=request(f.id);await f.owner.handle(req);
 for(const proof of [null,{...claim(req,messageId),source:'agent/inbox/spliced'},{...claim(req,messageId),runFence:randomUUID()},{...claim(req,messageId),sessionId:randomUUID()},{...claim(req,messageId),actionId:randomUUID()},{...claim(req,messageId),outcome:'canceled'},{...claim(req,messageId),turn:-1}])await f.owner.observe([messageId],proof);
 assert.equal((await f.owner.readReceipt(req.actionId)).state,'queued_next_step');
 await f.owner.observe([messageId],claim(req,messageId));const saved=await f.owner.readReceipt(req.actionId);assert.equal(saved.state,'consumed');assert.equal(saved.consumptionProof.source,'sdk-agent-inbox-claimed');assert.equal(saved.consumptionProof.runFence,req.runFence);
});
test('legacy consumed journal remains historical evidence and is not upgraded to new provenance',async t=>{
 const messageId=randomUUID(),f=await fixture(t,async()=>({messageId,status:'queued_next_step'})),req=request(f.id);const queued=await f.owner.handle(req);
 await writeFile(path.join(f.directory,'guidance-'+req.actionId+'.json'),JSON.stringify({...queued,state:'consumed',consumedAt:new Date().toISOString()}));
 await f.owner.observe([messageId],claim(req,messageId));const saved=await f.owner.readReceipt(req.actionId);assert.equal(saved.state,'consumed');assert.equal(saved.consumptionProof,undefined);
});
