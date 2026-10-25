import type {PoolClient} from 'pg';
import {pool} from './db.js';

/** A notification is a scheduling hint, never a task result or dispatch receipt.
 * The existing periodic reconciliation remains the durable recovery path. */
export class PlanningWake {
  private client?:PoolClient;
  private pending?:Promise<boolean>;
  private retry?:ReturnType<typeof setTimeout>;
  private closed=false;
  private queued=false;
  constructor(private wake:()=>void,private connect=()=>pool.connect()){}
  private notify=(message:{channel:string})=>{
    if(this.closed||message.channel!=='agentos_planning_wake'||this.queued)return;
    this.queued=true;
    queueMicrotask(()=>{this.queued=false;if(!this.closed)this.wake();});
  };
  private disconnected=()=>{
    const client=this.client;this.client=undefined;
    if(client){client.off('notification',this.notify);client.off('error',this.disconnected);client.off('end',this.disconnected);client.release(true);}
    this.reconnect();
  };
  private reconnect(){
    if(!this.closed&&!this.retry)this.retry=setTimeout(()=>{this.retry=undefined;void this.start();},5000);
  }
  start():Promise<boolean>{
    if(this.closed)return Promise.resolve(false);
    if(this.client)return Promise.resolve(true);
    if(this.pending)return this.pending;
    this.pending=(async()=>{
      let client:PoolClient|undefined;
      try{
        client=await this.connect();
        if(this.closed){client.release();return false;}
        this.client=client;client.on('error',this.disconnected);client.on('end',this.disconnected);
        client.on('notification',this.notify);
        await client.query('LISTEN agentos_planning_wake');
        return this.client===client;
      }catch{
        if(this.client===client)this.disconnected();else this.reconnect();
        return false;
      }
    })().finally(()=>{this.pending=undefined;});
    return this.pending;
  }
  async close(){
    this.closed=true;clearTimeout(this.retry);await this.pending;
    const client=this.client;this.client=undefined;if(!client)return;
    client.off('notification',this.notify);client.off('error',this.disconnected);client.off('end',this.disconnected);
    try{await client.query('UNLISTEN agentos_planning_wake');client.release();}catch{client.release(true);}
  }
}
