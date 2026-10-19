import {writeFile} from 'node:fs/promises';
import assert from 'node:assert/strict';
export const name='readonly-profile-proof';
export const inject=['sdkAppStartup','tools','fs','systemPrompt','agentosReadonlyFsReady'];
export async function apply(ctx){
 // Loaded last after the real profile and addon, no provider/model invocation.
 const names=ctx.tools.schemas().map(t=>t.name);
 assert.ok(names.includes('bash'));assert.ok(names.includes('read'));
 for(const n of ['write','edit','read_image'])assert.ok(!names.includes(n));
 const execute=p=>ctx.tools.execute({name:'read',arguments:{file_path:p},callId:'readonly-proof',signal:new AbortController().signal});
 const good=await execute('fixture.txt');assert.equal(good.isError,false,JSON.stringify(good.error??good.content));
 const missing=await execute('missing.txt');assert.equal(missing.isError,true);assert.equal(missing.error?.info?.code,'FS_NOT_FOUND',JSON.stringify(missing.error??missing.content));
 for(const p of ['../outside.txt','/home/agent/outside.txt','escape.txt']){const result=await execute(p);assert.equal(result.isError,true);assert.equal(result.error?.info?.code,'FS_SANDBOX_DENIED');}
 await writeFile('/workspace/profile-proof.json',JSON.stringify({profile:'sdk-minimal',sdkAppStartup:true,registry:names,fixtureRead:true,missingCode:missing.error.info.code,lexicalAndRealpathEscapesRejected:true,modelInvoked:false,rawTelemetryPersisted:false}));
}
