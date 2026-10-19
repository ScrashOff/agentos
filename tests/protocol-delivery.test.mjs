import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {protocolDelivery} from '../bridge/protocol-delivery.mjs';
import {collectArtifacts} from '../bridge/artifact-collector.mjs';

test('malformed final recovery includes the existing markdown and preserves failed tests', async () => {
  const root = await mkdtemp(path.join(tmpdir(), 'protocol-recovery-'));
  try {
    await mkdir(path.join(root, 'evidence'));
    const report = JSON.stringify({ suiteExecuted: true, exitCode: 1, command: 'node --test qa-protocol/neko.test.mjs', passed: false });
    await writeFile(path.join(root, 'evidence/protocol-report.json'), report);
    await writeFile(path.join(root, 'evidence/protocol-tests.log'), 'not ok - boot');
    await writeFile(path.join(root, 'evidence/protocol-report.md'), '8 cases failed; game not accepted.');
    const delivery = await protocolDelivery(root, 'qa-protocol/neko.test.mjs');
    const files = await collectArtifacts(delivery, root);
    assert.equal(files.length, 3);
    assert.equal(Buffer.from(files.find(a => a.name === 'protocol-report.json').base64, 'base64').toString(), report);
    assert.equal(Buffer.from(files.find(a => a.name === 'protocol-report.md').base64, 'base64').toString(), '8 cases failed; game not accepted.');
    await rm(path.join(root, 'evidence/protocol-report.md'));
    assert.equal((await protocolDelivery(root, 'qa-protocol/neko.test.mjs')).artifacts.length, 2);
    await writeFile(path.join(root, 'evidence/protocol-tests.log'), '');
    await assert.rejects(protocolDelivery(root, 'qa-protocol/neko.test.mjs'), /DELIVERY_NOT_RECOVERABLE/);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('native collection preserves existing repair output and negative verdicts',async()=>{
 const root=await mkdtemp(path.join(tmpdir(),'protocol-collection-'));
 try {
  await mkdir(path.join(root,'evidence'));await mkdir(path.join(root,'qa-protocol'));
  const file='qa-protocol/native.test.mjs';
  for(const [name,body] of Object.entries({
   'evidence/protocol-report.json':JSON.stringify({suiteExecuted:true,command:'node --test '+file,exitCode:1,passed:false}),
   'evidence/protocol-tests.log':'not ok 1 - genuine native failure',
   'evidence/repair-report.json':JSON.stringify({fixVerified:false,limitation:'Missing genuine module'}),
   'evidence/implementation.patch':'Actual partial patch',
   'evidence/repair-tests.log':'before: failed; after: still failed',
   [file]:'Actual executed test source',
  }))await writeFile(path.join(root,name),body);
  const value=await protocolDelivery(root,file);
  assert.deepEqual(value.artifacts.map(a=>a.name),['protocol-report.json','protocol-tests.log','native.test.mjs','implementation.patch','repair-report.json','repair-tests.log']);
  const artifacts=await collectArtifacts(value,root);
  assert.equal(JSON.parse(Buffer.from(artifacts.find(a=>a.name==='repair-report.json').base64,'base64').toString()).fixVerified,false);
  assert.equal(JSON.parse(Buffer.from(artifacts[0].base64,'base64').toString()).passed,false);
  assert.ok(!artifacts.some(a=>a.name==='protocol-report.md'));
 }finally{await rm(root,{recursive:true,force:true})}
});
