import {beforeAll,afterAll,it,expect} from 'vitest';
import {pool,migrate,tx,event} from '../server/db.js';
import {installPlanningEvents} from '../server/planning-events.js';
beforeAll(async()=>{await migrate();await installPlanningEvents();});
afterAll(async()=>{await pool.end();});
it('notifies after committed delivery events and keeps payloads private; polling covers lost notifications',async()=>{
  const listener=await pool.connect();const notifications:string[]=[];
  const receive=(message:any)=>{if(message.channel==='agentos_planning_wake')notifications.push(message.payload);};
  listener.on('notification',receive);
  try{
    await listener.query('LISTEN agentos_planning_wake');
    let delivered!:()=>void;
    const seen=new Promise<void>(resolve=>{delivered=resolve;});
    listener.once('notification',()=>delivered());
    await tx(async db=>{
      await event(db,'TASK_ACCEPTED','test-delivery',{privateData:'never in notification'});
      await event(db,'VALIDATION_PENDING','test-delivery',{});
      expect(notifications).toHaveLength(0);
    });
    await seen;
    expect(notifications).toEqual(['']);
    await tx(async db=>{await event(db,'RUN_HEARTBEAT','test-delivery',{});});
    await expect(tx(async db=>{await event(db,'RUN_FAILED','test-delivery',{});throw Error('ROLLBACK');})).rejects.toThrow('ROLLBACK');
    await listener.query('SELECT pg_sleep(0.03)');
    expect(notifications).toEqual(['']);
  }finally{await listener.query('UNLISTEN agentos_planning_wake');listener.off('notification',receive);listener.release();}
});
