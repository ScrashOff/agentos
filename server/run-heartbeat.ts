/** A failed observation does not prove cancellation or loss of the run fence.
 * Keep one observation in flight so database contention cannot exhaust the
 * actor's pool with a new heartbeat every three seconds. */
export class RunHeartbeat {
  private closed=false;
  private pending?:Promise<void>;
  constructor(private check:()=>Promise<{state:string}|null>,private confirmedStop:()=>void){}
  tick():Promise<void>{
    if(this.closed)return Promise.resolve();
    if(this.pending)return this.pending;
    this.pending=Promise.resolve().then(this.check).then(value=>{
      if(!this.closed && (!value || value.state==='cancelling'))this.confirmedStop();
    }).catch(()=>{
      // Retain ownership and retry the observation. A database error contains
      // no stop proof and must not cancel the remote execution.
    }).finally(()=>{this.pending=undefined;});
    return this.pending;
  }
  async close(){this.closed=true;await this.pending;}
}
