import Schema from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/schemastery/lib/index.mjs';
import {JsonRpcLineTransport} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-sdk-protocol/lib/index.js';
import {HarnessSdkJsonRpcServer} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-sdk-jsonrpc-server/lib/index.js';
import {createUserMessage} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-llm/lib/index.js';
import {SteeringControl} from './steering-control.mjs';
import {GuidanceWaitBroker} from './guidance-wait-broker.mjs';
import {ownedGuidanceClaim} from './guidance-claim.mjs';
// Named exports preserve the Loader plugin contract; no default export.
export const name='agentos-sdk-steering';
export const inject=['agents'];
export const Config=Schema.object({maxTokensAsSuccess:Schema.boolean().default(false),
  runId:Schema.string().required(),runFence:Schema.string().required(),guidanceDeadlineNs:Schema.string()});
export function apply(ctx,config){
  const transport=new JsonRpcLineTransport(config.input??process.stdin,config.output??process.stdout);
  const server=new HarnessSdkJsonRpcServer(ctx,transport,{maxTokensAsSuccess:config.maxTokensAsSuccess});
  let broker;
  const control=new SteeringControl({server,ctx,runId:config.runId,runFence:config.runFence,createUserMessage,onQueued:receipt=>broker?.publishQueued(receipt)});
  if(config.guidanceDeadlineNs!==undefined)broker=new GuidanceWaitBroker({runId:config.runId,runFence:config.runFence,deadlineNs:config.guidanceDeadlineNs,
    assertOwner:agent=>{if(control.closed||!control.initialized||!control.agent||agent!==control.agent||ctx.agents.get(config.runId)!==agent||String(agent.session.id)!==config.runId||agent.status!=='running')throw Error('GUIDANCE_WAIT_SESSION_MISMATCH');}});
  ctx.provide('guidanceWaitBroker',broker??Object.freeze({available:false}));
  const claimed=new Set();
  ctx.on('agent/inbox/claimed',payload=>{
    const proof=ownedGuidanceClaim(control,ctx.agents,payload);
    if(!proof||claimed.has(proof.messageId))return;
    claimed.add(proof.messageId);transport.notify('session.guidanceClaimed',proof);
  });
  const rootFiber=ctx.root.fiber;
  let exitTask;
  const disposeAndExit=()=>exitTask??=(async()=>{
    await Promise.allSettled([Promise.resolve().then(()=>transport.flush())]);
    await Promise.allSettled([Promise.resolve().then(()=>rootFiber.dispose())]);
    (config.exit??process.exit)(0);
  })();
  transport.onRequest(async(method,params)=>{
    if(method==='initialize')await ctx.get('loader')?.await();
    if(method==='shutdown')broker?.close();
    const result=await control.request(method,params);
    if(method==='shutdown')setImmediate(()=>void disposeAndExit());
    return result;
  });
  ctx.effect(()=>{
    transport.start();
    return async()=>{control.close();broker?.close();await server.shutdown();transport.close();};
  },'jsonrpc.serve');
}
