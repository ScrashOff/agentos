import {createHash} from 'node:crypto';
const uuid=/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
const fail=code=>{throw Object.assign(new Error(code),{code});};
/** Per-process admission only. Durable action ownership/reconciliation belongs to controller. */
export class SteeringControl {
  constructor({server,ctx,runId,runFence,createUserMessage,onQueued}) {
    if(!uuid.test(runId??'')||!uuid.test(runFence??''))fail('STEERING_RUN_IDENTITY_INVALID');
    Object.assign(this,{server,ctx,runId,runFence,createUserMessage,onQueued});
    this.closed=false;this.initialized=false;this.agent=null;this.actions=new Map();
  }
  async request(method,params) {
    if(method==='shutdown'){this.closed=true;return this.server.handleRequest(method,params);}
    if(this.closed)fail('STEERING_RUNTIME_CLOSED');
    if(method==='session/steer')return this.steer(params);
    if(method==='session/prompt' && params?.sessionId!==this.runId)fail('STEERING_SESSION_MISMATCH');
    const result=await this.server.handleRequest(method,params);
    if(method==='initialize' && !this.closed)this.initialized=true;
    if(method==='session/prompt' && !this.closed)this.agent=this.ctx.agents.get(this.runId);
    return result;
  }
  close(){this.closed=true;}
  steer(p) {
    if(this.closed)fail('STEERING_RUNTIME_CLOSED');
    if(!p||Object.keys(p).some(k=>!['sessionId','runId','runFence','actionId','contentBlocks'].includes(k))||
      !uuid.test(p.actionId??'')||!Array.isArray(p.contentBlocks)||p.contentBlocks.length!==1||
      p.contentBlocks[0]?.type!=='text'||typeof p.contentBlocks[0].text!=='string'||
      !p.contentBlocks[0].text.trim()||p.contentBlocks[0].text.length>12000||
      Object.keys(p.contentBlocks[0]).some(k=>!['type','text'].includes(k)))fail('STEERING_PARAMS_INVALID');
    if(p.sessionId!==this.runId||p.runId!==this.runId)fail('STEERING_SESSION_MISMATCH');
    if(p.runFence!==this.runFence)fail('STEERING_FENCE_MISMATCH');
    const bodyDigest=createHash('sha256').update(JSON.stringify(p.contentBlocks)).digest('hex');
    const prior=this.actions.get(p.actionId);
    if(prior){if(prior.bodyDigest!==bodyDigest)fail('STEERING_ACTION_CONFLICT');return prior.receipt;}
    if(!this.initialized||!this.agent)fail('STEERING_SESSION_NOT_READY');
    const agent=this.ctx.agents.get(this.runId);
    if(agent!==this.agent||String(agent.session.id)!==this.runId)fail('STEERING_SESSION_CLOSED');
    if(agent.status!=='running')fail('STEERING_TURN_CLOSED');
    const message=this.createUserMessage({content:p.contentBlocks,source:{kind:'user'}});
    // No await between live/closure validation, registry comparison and admission.
    // steer() in 0.1.6 redirects to next-turn while an aborted driver closes.
    // inject() is the public next-step API without wakeup or turn creation.
    agent.inject(message);
    const receipt=Object.freeze({runId:this.runId,runFence:this.runFence,sessionId:this.runId,
      actionId:p.actionId,messageId:message.id,status:'queued_next_step'});
    this.actions.set(p.actionId,{bodyDigest,receipt});
    // Notify only after validated injection and idempotency admission commit.
    // Notification failure cannot change the outcome of an already queued guide.
    try{this.onQueued?.(receipt);}catch{}
    return receipt;
  }
}
