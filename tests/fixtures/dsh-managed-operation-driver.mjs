// Deterministic SDK adapter for the real registered managed_operation tool.
// It never contacts a provider, reads an account, or accepts a task.
import assert from 'node:assert/strict';
import {readFile,readdir,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {LlmAdapter} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-llm/lib/index.js';

export const name='managed-operation-driver-fixture';
export const inject=['llm','tools','managedOperationToolReady','agentosReadonlyFsReady'];

export function apply(ctx){
 const names=ctx.tools.schemas().map(tool=>tool.name);
 for(const name of ['bash','read','managed_operation'])assert.ok(names.includes(name),name+' missing');
 for(const name of ['write','edit','read_image'])assert.ok(!names.includes(name),name+' must stay unavailable');
 const tool=ctx.tools.get('managed_operation');
 assert.equal(tool.isConcurrencySafe({}),false);
 assert.ok(tool.output?.schema,'actual SDK definition must declare its output schema');
 const mode=process.env.AGENTOS_OPERATION_FIXTURE_MODE;
 assert.ok(['success','negative'].includes(mode));

 class Scripted extends LlmAdapter {
  step=0;
  async resolveModel(provider,id){return {provider,id,name:id,context:{contextWindow:128000},defaultMaxTokens:1024};}
  async *stream(options){
   options.signal.throwIfAborted();
   const step=this.step++,launch=step%2===0,invocation=Math.floor(step/2)+1;
   assert.ok(invocation<=2,'fixture cannot launch additional model steps');
   let blocks;
   if(launch){
    const policy=JSON.parse(await readFile('/run/operation-policy.json','utf8'));
    assert.equal(policy.runId,process.env.AGENTOS_RUN_ID);
    assert.equal(policy.runFence,process.env.AGENTOS_RUN_FENCE);
    assert.equal(policy.sourceSnapshotDigest,createHash('sha256').update(await readFile('/workspace/.agentos-snapshot.json')).digest('hex'));
    await assert.rejects(writeFile('/run/operation-policy.json','MUST_NOT_WRITE'),error=>['EROFS','EACCES','EPERM'].includes(error.code));
    await assert.rejects(writeFile('/workspace/.agentos-snapshot.json','MUST_NOT_WRITE'),error=>['EROFS','EACCES','EPERM'].includes(error.code));
    blocks=[{type:'tool-call',id:'fixture_operation_'+invocation,name:'managed_operation',arguments:JSON.stringify({
     stageKey:'sdk-operation-'+mode,
     argv:[process.execPath,'/workspace/operation-fixture.mjs',mode],
     inputFiles:['/workspace/operation-input.txt'],
     cwd:'/workspace',deadlineMs:10000,
    })}];
   }else{
    const directories=await readdir('/workspace/evidence/managed-operations');
    assert.equal(directories.length,1,'same stage must preserve one operation identity');
    const receipt=JSON.parse(await readFile('/workspace/evidence/managed-operations/'+directories[0]+'/operation-report.json','utf8'));
    assert.equal(receipt.outcome,'exited');
    assert.equal(receipt.exitCode,mode==='negative'?3:0);
    assert.equal(receipt.physicalExitConfirmed,true);
    assert.equal(receipt.acceptance,false);
    assert.equal(receipt.verification,'unverified_observation');
    assert.equal(receipt.passed,mode==='negative'?false:undefined);
    assert.equal(await readFile('/workspace/evidence/operation-effect.txt','utf8'),'one execution\n');
    const messages=JSON.stringify(options.messages);
    assert.ok(messages.includes(receipt.operationId),'SDK must return the registered tool result to the model');
    assert.ok(messages.includes('unverified_observation'));
    await writeFile('/workspace/evidence/sdk-operation-proof.json',JSON.stringify({
     mode,modelSteps:this.step,invocations:invocation,operationId:receipt.operationId,
     exitCode:receipt.exitCode,physicalExitConfirmed:true,acceptance:false,
     fixedPolicyReadOnly:true,snapshotReadOnly:true,singleEffect:true,
    }));
    blocks=[{type:'text',text:'MANAGED_OPERATION_FIXTURE_COMPLETED_'+invocation}];
   }
   for(const [index,block] of blocks.entries()){
    yield {type:'block-start',index,blockType:block.type};
    yield {type:'block-end',index,block};
   }
   yield {type:'finish',reason:{kind:launch?'tool-calls':'stop'}};
  }
 }
 ctx.llm.registerAdapter(['managed-operation-scripted-fixture'],new Scripted());
}
