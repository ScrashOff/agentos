const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
/** Notification only: never reads, removes, claims or reinjects SDK inbox input. */
export class GuidanceWaitBroker {
 constructor({runId,runFence,deadlineNs,clock=process.hrtime.bigint,assertOwner}){
  if(!uuid.test(runId??'')||!uuid.test(runFence??'')||!/^\d{1,20}$/.test(deadlineNs??''))throw Error('GUIDANCE_WAIT_IDENTITY_INVALID');
  this.runId=runId;this.runFence=runFence;this.deadline=BigInt(deadlineNs);this.clock=clock;this.assertOwner=assertOwner;
  const remaining=this.deadline-clock();if(remaining>600_000_000_000n)throw Error('GUIDANCE_WAIT_DEADLINE_INVALID');
  this.available=true;this.closed=false;this.receipts=new Map();this.notified=new Set();this.waiter=null;
 }
 publishQueued(receipt){
  if(this.closed||receipt?.runId!==this.runId||receipt.runFence!==this.runFence||receipt.sessionId!==this.runId||receipt.status!=='queued_next_step'||!uuid.test(receipt.actionId??'')||!uuid.test(receipt.messageId??''))return false;
  const prior=this.receipts.get(receipt.actionId);if(prior)return prior.messageId===receipt.messageId;
  // A notification bound is advisory only; overflowing it never rejects the guide.
  if(this.receipts.size>=128)return false;
  this.receipts.set(receipt.actionId,Object.freeze({status:'guidance_queued',actionId:receipt.actionId,messageId:receipt.messageId}));
  this.wake();return true;
 }
 next(){if(this.closed)return {status:'runtime_closed'};if(this.clock()>=this.deadline)return {status:'deadline'};
  for(const receipt of this.receipts.values())if(!this.notified.has(receipt.messageId))return receipt;
  return null;
 }
 wake(){const next=this.next();if(next&&this.waiter){if(next.messageId)this.notified.add(next.messageId);this.waiter.finish(next);}}
 wait(agent,signal){
  this.assertOwner(agent);signal.throwIfAborted();
  if(this.waiter)throw Error('GUIDANCE_WAIT_ALREADY_PENDING');
  const next=this.next();if(next){if(next.messageId)this.notified.add(next.messageId);return Promise.resolve(next);}
  return new Promise((resolve,reject)=>{
   const abort=()=>finish(null,signal.reason??new Error('GUIDANCE_WAIT_ABORTED'));
   const finish=(value,error)=>{clearTimeout(timer);signal.removeEventListener('abort',abort);if(this.waiter?.finish===finish)this.waiter=null;error?reject(error):resolve(value);};
   let timer;const tick=()=>{if(this.clock()<this.deadline){timer=setTimeout(tick,Math.max(1,Math.ceil(Number(this.deadline-this.clock())/1e6)));return;}this.wake();};
   timer=setTimeout(tick,Math.max(1,Math.ceil(Number(this.deadline-this.clock())/1e6)));
   this.waiter={finish};signal.addEventListener('abort',abort,{once:true});
   // Recheck after subscription: a preexisting queued ACK cannot be missed.
   this.wake();if(signal.aborted)abort();
  });
 }
 close(){this.closed=true;if(this.waiter)this.waiter.finish({status:'runtime_closed'});}
}
