import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,mkdir,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {withProtocolArtifacts} from '../bridge/protocol-artifacts.mjs';
import {collectArtifacts} from '../bridge/artifact-collector.mjs';
test('collects real omitted protocol files without rewriting a negative report', async t=>{
  const root=await mkdtemp(path.join(os.tmpdir(),'agentos-protocol-manifest-'));
  t.after(()=>rm(root,{recursive:true,force:true}));
  await mkdir(path.join(root,'evidence'));
  const report=JSON.stringify({suiteExecuted:true,exitCode:1,command:'node qa-protocol/real.test.mjs',defect:'Real failed assertion'});
  await writeFile(path.join(root,'evidence','protocol-report.json'),report);
  await writeFile(path.join(root,'evidence','protocol-tests.log'),'not ok: real assertion\n');
  const original={summary:'Delivery only',artifacts:[]};
  const artifacts=await collectArtifacts(withProtocolArtifacts(original,'qa-protocol/real.test.mjs'),root);
  assert.equal(Buffer.from(artifacts[0].base64,'base64').toString(),report);
  assert.equal(original.artifacts.length,0);
  assert.equal(withProtocolArtifacts(withProtocolArtifacts(original,'qa-protocol/real.test.mjs'),'qa-protocol/real.test.mjs').artifacts.length,2);
});
test('does not invent missing evidence or silently replace a declared artifact',async t=>{
  const root=await mkdtemp(path.join(os.tmpdir(),'agentos-protocol-missing-'));
  t.after(()=>rm(root,{recursive:true,force:true}));
  await assert.rejects(collectArtifacts(withProtocolArtifacts({summary:'Missing',artifacts:[]},'qa-protocol/real.test.mjs'),root),e=>e.code==='ARTIFACT_MISSING');
  const declared={summary:'Other path',artifacts:[{name:'protocol-report.json',path:'evidence/declared.json'}]};
  assert.equal(withProtocolArtifacts(declared,'qa-protocol/real.test.mjs').artifacts[0].path,'evidence/declared.json');
  assert.equal(withProtocolArtifacts(declared,null),declared);
});
