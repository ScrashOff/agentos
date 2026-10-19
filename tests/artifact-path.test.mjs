import {test} from 'node:test';
import assert from 'node:assert/strict';
import {artifactPath} from '../bridge/artifact-path.mjs';
test('normalizes virtual workspace paths without accepting host or escaping paths',()=>{
 assert.equal(artifactPath('/workspace/evidence/tests.log'),'evidence/tests.log');
 assert.equal(artifactPath('evidence/tests.log'),'evidence/tests.log');
 for(const p of ['/etc/passwd','/workspace/../etc/passwd','../secret','C:\\secret','/workspace//secret','/workspace/./secret'])assert.throws(()=>artifactPath(p));
});
