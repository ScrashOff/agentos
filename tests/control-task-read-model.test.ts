import {beforeAll,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {readManagementTasks} from '../server/management.js';
import {readObjectiveFrontierTasks,objectiveFrontierNodes,projectWorkFrontier} from '../server/work-frontier.js';
import {projectManagerTaskIntent} from '../server/manager-task-intent.js';

beforeAll(async()=>{await migrate();await seedRegistry();});
afterAll(async()=>{await pool.end();});

async function fixture(){
 const goal=randomUUID(),tasks=Array.from({length:24},()=>randomUUID());
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Long work','Control projection','control-read-model',$2)",[goal,goal]);
 const description='Initial instruction\nREQUIREMENTS: '+JSON.stringify([{id:'delivery',description:'Preserve the real evidence'}])+'\nCONSTRAINTS: '+ 'X'.repeat(12000)+'\nSUPERVISOR: Current authorized repair';
 const criteria=[{id:'proof',label:'Evidence',type:'artifact',name:'proof.json'}];
 for(const [i,id] of tasks.entries())await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,covers,execution,attempt_count)
 VALUES($1,$2,$3,'Work',$4,'qa.backend',$5,$6,$7,'["delivery"]',$8,1)`,[id,goal,'task-'+i,description,
 i===0?'accepted':i===1?'running':'failed',i===2?'verify':'work',JSON.stringify(criteria),JSON.stringify({kind:'harness',
 baseWorkspace:'control/source',workspace:'control/task-'+i,workKind:i===0?'repair':undefined,readOnlySource:i===2,
 context:'PRIVATE CONTEXT '+ 'Z'.repeat(512*1024),prompt:'PRIVATE PROMPT '+ 'P'.repeat(32000),
 continuationRoutes:[{accountId:'not-used',model:'test',until:'2099-01-01T00:00:00Z'}],
 verificationRecheck:i===2?{version:1,sourceTaskId:id,sourceAttempt:1,targetTaskId:tasks[0],targetAttempt:1,candidateDigest:'retained'}:undefined,
 backlogContext:{sourceTaskId:tasks[0],fullCapturedContext:'SECRET DATA '+ 'Q'.repeat(32000)}})]);
 await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[tasks[2],tasks[0]]);
 const run=randomUUID();
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime,summary,instruction_snapshot)
 VALUES($1,$2,1,$3,1,'submitted','harness',$4,$5)`,[run,tasks[0],randomUUID(),'S'.repeat(4000),JSON.stringify({task:{criteria}})]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,reviewer,passed,checks) VALUES($1,$2,$3,'control-test',true,$4)",[randomUUID(),tasks[0],run,JSON.stringify([{id:'proof',passed:true}])]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,agent_version,state,runtime)
 VALUES($1,$2,1,$3,1,'reconciling','harness')`,[randomUUID(),tasks[1],randomUUID()]);
 const immutable=(await pool.query('SELECT id,md5(execution::text) digest FROM tasks WHERE goal_id=$1 ORDER BY id',[goal])).rows;
 return {goal,tasks,run,description,immutable};
}

it('frontier reads no worker contexts while retaining exact acceptance, dependencies and uncertain actors',async()=>{
 const f=await fixture();
 const rows=await readObjectiveFrontierTasks(pool,f.goal);
 const full=(await pool.query('SELECT * FROM tasks WHERE goal_id=$1',[f.goal])).rows;
 const runs=(await pool.query('SELECT * FROM runs WHERE task_id=ANY($1::uuid[])',[f.tasks])).rows;
 const acceptances=(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows;
 const dependencies=(await pool.query('SELECT * FROM dependencies WHERE task_id=ANY($1::uuid[])',[f.tasks])).rows;
 const frontier=(tasks:any[])=>projectWorkFrontier({kind:'objective',id:f.goal},objectiveFrontierNodes(tasks.sort((a,b)=>a.id.localeCompare(b.id)),runs,acceptances,dependencies));
 expect(frontier(rows)).toEqual(frontier(full));
 expect(JSON.stringify(rows).length).toBeLessThan(24000);
 expect(JSON.stringify(rows)).not.toContain('PRIVATE CONTEXT');
 expect((await pool.query('SELECT id,md5(execution::text) digest FROM tasks WHERE goal_id=$1 ORDER BY id',[f.goal])).rows).toEqual(f.immutable);
});

it('manager retains exact intent, recheck, cooldown and source identities beside large immutable contexts',async()=>{
 const f=await fixture(),rows=await readManagementTasks(pool,f.goal);
 const source=rows.find(t=>t.id===f.tasks[0]),actor=rows.find(t=>t.id===f.tasks[1]),qa=rows.find(t=>t.id===f.tasks[2]);
 expect(source).toMatchObject({state:'accepted',last_run_id:f.run,last_run_state:'submitted',acceptance_passed:true});
 expect(actor.has_live_run).toBe(true);
 expect(qa.dependencies).toEqual([f.tasks[0]]);
 expect(qa.execution).toMatchObject({readOnlySource:true,verificationRecheck:{candidateDigest:'retained'},
 continuationRoutes:[{accountId:'not-used',model:'test',until:'2099-01-01T00:00:00Z'}],backlogContext:{sourceTaskId:f.tasks[0]}});
 expect(projectManagerTaskIntent(qa,1800)).toEqual(projectManagerTaskIntent({...qa,description:f.description},1800));
 expect(source.summary.length).toBe(700);
 expect(JSON.stringify(rows).length).toBeLessThan(400000);
 expect(JSON.stringify(rows)).not.toMatch(/PRIVATE CONTEXT|PRIVATE PROMPT|SECRET DATA/);
 expect((await pool.query('SELECT id,md5(execution::text) digest FROM tasks WHERE goal_id=$1 ORDER BY id',[f.goal])).rows).toEqual(f.immutable);
});
