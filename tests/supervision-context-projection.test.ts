import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {readSupervisionRepairPeers,supervisionInputs} from '../server/supervision-context.js';

let goalId:string,sequence=0;
const base='context-projection/game';
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();sequence=0;
  goalId=await goal();
});
afterAll(()=>pool.end());
async function goal(){const id=randomUUID();await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
  VALUES($1,'Context graph','Original objective','context-projection',$2)`,[id,id]);return id;}
async function task(options:any={}){
  const id=randomUUID(),execution=Object.hasOwn(options,'execution')?options.execution:{kind:'harness',baseWorkspace:base,
    workspace:'context-projection/'+id,workKind:options.workKind??'repair',contextSnapshot:options.payload??''};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,created_at)
    VALUES($1,$2,$3,$3,'Original instructions','qa.backend',$4,'work','[]',$5,$6,$7)`,
    [id,options.goalId??goalId,id,options.state??'accepted',execution===null?null:JSON.stringify(execution),
      options.attempt??1,new Date(Date.parse('2026-10-01T00:00:00Z')+sequence++*1000)]);
  return (await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
}
async function run(t:any,passed=true,attempt=t.attempt_count){
  const id=randomUUID();await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,$3,$4,'harness',1,'submitted')`,[id,t.id,attempt,randomUUID()]);
  await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer)
    VALUES($1,$2,$3,$4,'[]','fixture-proof')`,[randomUUID(),t.id,id,passed]);return id;
}
async function edge(child:any,parent:any){await pool.query('INSERT INTO dependencies VALUES($1,$2)',[child.id,parent.id]);}
async function artifact(runId:string,name:string,text:string){
  const id=randomUUID(),bytes=Buffer.from(text),digest=createHash('sha256').update(bytes).digest('hex');
  await mkdir(path.join(config.dataDir,'artifacts'),{recursive:true});
  await writeFile(path.join(config.dataDir,'artifacts',id),bytes,{flag:'wx'});
  await pool.query('INSERT INTO artifacts(id,run_id,name,digest,size,mime) VALUES($1,$2,$3,$4,$5,$6)',
    [id,runId,name,digest,bytes.length,'text/plain']);return {id,digest,text};
}

it('retains ordered unique accepted repair ancestors from both roots through diamonds and cycles',async()=>{
  const source=await task(),target=await task({state:'failed',workKind:'diagnostic'});
  const first=await task({payload:'Large immutable input '.repeat(40000)}),middle=await task({workKind:'diagnostic',state:'failed'});
  const second=await task(),sourcePeer=await task();
  const firstRun=await run(first),secondRun=await run(second),sourceRun=await run(sourcePeer);await run(source);
  await edge(target,source);await edge(target,first);await edge(target,middle);
  await edge(middle,second);await edge(second,first);await edge(first,second);await edge(source,sourcePeer);
  const before=(await pool.query('SELECT execution FROM tasks WHERE id=$1',[first.id])).rows[0].execution;
  const peers=await readSupervisionRepairPeers(pool,source,target);
  expect(peers).toEqual([{id:first.id,title:first.title,run_id:firstRun},
    {id:second.id,title:second.title,run_id:secondRun},{id:sourcePeer.id,title:sourcePeer.title,run_id:sourceRun}]);
  expect(JSON.stringify(peers).length).toBeLessThan(1000);
  expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[first.id])).rows[0].execution).toEqual(before);
});

it.each(['workspace','goal','null','array'])('does not cross an ineligible %s ancestor and reenter the target scope',async(kind)=>{
  const source=await task(),target=await task({state:'failed'}),hidden=await task();await run(hidden);
  const foreignGoal=kind==='goal'?await goal():goalId;
  const execution=kind==='null'?null:kind==='array'?[]:{kind:'harness',baseWorkspace:kind==='workspace'?'context-projection/other':base,workKind:'diagnostic'};
  const barrier=await task({execution,goalId:foreignGoal});
  await edge(target,barrier);await edge(barrier,hidden);
  expect(await readSupervisionRepairPeers(pool,source,target)).toEqual([]);
});

it('uses the current attempt and accepted repair identity without treating old successes or diagnostics as repair deliveries',async()=>{
  const source=await task(),target=await task({state:'failed'}),valid=await task();
  const rejected=await task({attempt:2}),noRun=await task(),notAccepted=await task({state:'failed'}),diagnostic=await task({workKind:'diagnostic'});
  const validRun=await run(valid);await run(rejected,true,1);await run(rejected,false,2);await run(notAccepted);await run(diagnostic);
  for(const peer of [source,valid,rejected,noRun,notAccepted,diagnostic])await edge(target,peer);
  expect(await readSupervisionRepairPeers(pool,source,target)).toEqual([{id:valid.id,title:valid.title,run_id:validRun}]);
});

it('preserves real artifact bytes, roles and rejected evidence when constructing the supervision context',async()=>{
  const source=await task({workKind:'diagnostic'}),target=await task({state:'failed'}),repair=await task();
  const sourceRun=await run(source),repairRun=await run(repair),targetRun=await run(target,false);
  const original=await artifact(sourceRun,'module.json','{"source":"accepted original"}');
  const patch=await artifact(repairRun,'implementation.patch','accepted patch bytes\n');
  const rejected=await artifact(targetRun,'verification.json','{"passed":false}');
  await artifact(repairRun,'unrelated.txt','not a repair artifact');
  await edge(target,repair);
  const result=await supervisionInputs(pool,source,target);
  expect(result.omitted).toEqual([]);
  expect(result.inputs.map((i:any)=>[i.id,i.digest,Buffer.from(i.base64,'base64').toString()])).toEqual([
    [original.id,original.digest,original.text],[patch.id,patch.digest,patch.text],[rejected.id,rejected.digest,rejected.text]]);
  expect(result.provenance.map((p:any)=>[p.artifactId,p.taskId,p.runId,p.role])).toEqual([
    [original.id,source.id,sourceRun,'accepted_source'],[patch.id,repair.id,repairRun,'accepted_repair'],
    [rejected.id,target.id,targetRun,'rejected_evidence']]);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[target.id])).rows[0].state).toBe('failed');
});
