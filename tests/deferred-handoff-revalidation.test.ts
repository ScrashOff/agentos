import {beforeAll,beforeEach,afterAll,afterEach,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,migrate,tx} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {completeRun,writeArtifact,validateTask,failRun} from '../server/service.js';
import {installDeferredHandoffs,stageDeferredHandoff,applyDeferredHandoff,reconcileDeferredHandoffs} from '../server/deferred-handoffs.js';
import {accountBridge} from '../server/bridge.js';
const sha=(text:string)=>createHash('sha256').update(text).digest('hex');
const before='original\n',after='fixed\n';
beforeAll(async()=>{await migrate();await installDeferredHandoffs();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();await pool.query("DELETE FROM settings WHERE id='autonomy:handoff-test'");});
afterEach(()=>vi.restoreAllMocks());afterAll(async()=>{await pool.end();});
const task=async(id:string)=>(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
async function run(id:string,attempt=1){
  const runId=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=$2 WHERE id=$1",[id,attempt]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,$3,$4,'harness',1,'running')",[runId,id,attempt,fence]);
  return {id:runId,fence};
}
function proof(runId:string,workspace:string,bytes=before){
  const files=[{path:'src/app.js',sha256:sha(bytes)}];
  return {runId,workspace,checkedAt:new Date().toISOString(),
    process:{unit:'agentos-run-'+runId,stopped:true,mainPid:0,populated:false,activeState:'inactive'},
    candidate:{files,candidateDigest:sha(JSON.stringify(files.map(f=>[f.path,f.sha256]))),baseDigest:sha('baseline')}};
}
async function fixture(kind:'repair'|'diagnostic'='repair'){
  const goal=randomUUID(),source=randomUUID(),consumer=randomUUID(),producer=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Handoff fixture','Preserve original delivery contract','handoff-test',$2)",[goal,goal]);
  for(const [id,stage,workKind,criteria] of [[source,'work','diagnostic',[]],[consumer,'integrate',undefined,[{id:'real',label:'Final verification',type:'artifact',name:'final.json'}]],[producer,'work',kind,[]]] as const){
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
      VALUES($1::uuid,$2,$1::text,'Scoped delivery','Preserve criteria and permissions','engineering.integration','ready',$3,$4,$5)`,
      [id,goal,stage,JSON.stringify(criteria),JSON.stringify({kind:'harness',workspace:'test/o-'+id,baseWorkspace:'test/base',workKind})]);
  }
  const sourceRun=await run(source);await completeRun(sourceRun.id,sourceRun.fence,'Accepted baseline',{});await validateTask(source);
  const consumerRun=await run(consumer);await failRun(consumerRun.id,consumerRun.fence,'RESULT_FORMAT','Envelope missing');
  const producerRun=await run(producer);
  if(kind==='repair'){
    await writeArtifact(producerRun.id,producerRun.fence,'implementation.patch','--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-original\n+fixed\n');
    await writeArtifact(producerRun.id,producerRun.fence,'repair-report.json',JSON.stringify({fixVerified:true,sourceHashes:[{path:'src/app.js',before:sha(before),after:sha(after)}]}));
  }
  await completeRun(producerRun.id,producerRun.fence,'Accepted auxiliary result',{});await validateTask(producer);
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  await tx(async db=>stageDeferredHandoff(db,goal,producer,await task(source),await task(consumer),
    {taskId:producer,plannerRunId:sourceRun.id,coverageKey:'delivery-'+producer,lane:'glm',workKind:kind}));
  const handoff=(await pool.query('SELECT * FROM deferred_handoffs WHERE producer_task_id=$1',[producer])).rows[0];
  const next=await run(consumer,2);await failRun(next.id,next.fence,'RESULT_FORMAT','Later envelope failure');
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  const consumerProof=proof(next.id,'test/o-'+consumer),producerProof=proof(producerRun.id,'test/o-'+producer);
  vi.spyOn(accountBridge,'request').mockImplementation(async(method,params:any)=>{
    expect(method).toBe('worker.checkComposition');
    // Remote filesystem work is outside the global scheduler transaction.
    await tx(async db=>{await db.query("SET LOCAL lock_timeout='500ms'");await db.query('SELECT pg_advisory_xact_lock(4280001)');});
    const p=params.composition.patches[0];return {dryRun:true,workspace:params.workspace,
      beforeDigest:consumerProof.candidate.candidateDigest,afterDigest:consumerProof.candidate.candidateDigest,complete:true,
      receipts:[{producerTaskId:producer,status:'compatible',patchDigest:p.patch.digest,reportDigest:p.report.digest}]};
  });
  return {goal,source,consumer,producer,handoff,sourceRun,producerRun,consumerRun,next,consumerProof,producerProof};
}
it('revalidates a newer stopped consumer using exact contract, source and checked patch preimages without accepting or retrying it',async()=>{
  const f=await fixture();
  expect(await applyDeferredHandoff(f.handoff.id,f.consumerProof,f.producerProof)).toBe(true);
  const row=(await pool.query('SELECT * FROM deferred_handoffs WHERE id=$1',[f.handoff.id])).rows[0];
  expect(row).toMatchObject({state:'applied',consumer_proof:{runId:f.consumerRun.id},receipt:{revalidation:{currentConsumer:{runId:f.next.id},compatibility:{status:'preimages_match'}}}});
  expect((await task(f.consumer)).state).toBe('failed');
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[f.consumer])).rowCount).toBe(2);
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2',[f.consumer,f.producer])).rowCount).toBe(1);
  expect(await applyDeferredHandoff(f.handoff.id,f.consumerProof,f.producerProof)).toBe(false);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='HANDOFF_REVALIDATED'")).rowCount).toBe(1);
});
it.each(['contract','candidate','source','stop','cancel','dependency'] as const)('refuses changed %s while preserving rejected evidence and the consumer',async(change)=>{
  const f=await fixture();
  if(change==='contract')await pool.query("UPDATE tasks SET description='Different authorized work' WHERE id=$1",[f.consumer]);
  if(change==='candidate')f.consumerProof=proof(f.next.id,'test/o-'+f.consumer,'conflicting change\n');
  if(change==='source')await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[f.source]);
  if(change==='stop')f.consumerProof.checkedAt='2000-01-01T00:00:00.000Z';
  if(change==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.consumer,JSON.stringify({runId:f.next.id})]);
  if(change==='dependency')await pool.query("UPDATE deferred_handoffs SET consumer_proof=consumer_proof||$2::jsonb WHERE id=$1",[f.handoff.id,JSON.stringify({dependencies:[f.source]})]);
  if(change==='stop')await expect(applyDeferredHandoff(f.handoff.id,f.consumerProof,f.producerProof)).rejects.toThrow('STOP_UNCONFIRMED');
  else expect(await applyDeferredHandoff(f.handoff.id,f.consumerProof,f.producerProof)).toBe(false);
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1 AND depends_on=$2',[f.consumer,f.producer])).rowCount).toBe(0);
  expect((await task(f.consumer)).state).toBe('failed');
});
it('reconciles a legacy revision rejection only from its immutable old run contract',async()=>{
  const f=await fixture();
  await pool.query(`UPDATE deferred_handoffs SET consumer_proof=consumer_proof-'contractDigest'-'version'-'dependencies',
    state='replanning_required',receipt='{"code":"CONSUMER_REVISION_CHANGED"}' WHERE id=$1`,[f.handoff.id]);
  expect(await applyDeferredHandoff(f.handoff.id,f.consumerProof,f.producerProof)).toBe(true);
});
it('rejects a patch that the real dry-run checker refused despite matching declared preimages',async()=>{
  const f=await fixture();vi.mocked(accountBridge.request).mockResolvedValueOnce({dryRun:true,complete:false,receipts:[{status:'conflict'}]});
  expect(await applyDeferredHandoff(f.handoff.id,f.consumerProof,f.producerProof)).toBe(false);
  expect((await pool.query('SELECT receipt FROM deferred_handoffs WHERE id=$1',[f.handoff.id])).rows[0].receipt.code).toBe('HANDOFF_PATCH_CONFLICT');
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1',[f.consumer])).rowCount).toBe(0);
});
it('does not attach a repair when another attempt starts during the read-only compatibility check',async()=>{
  const f=await fixture(),original=vi.mocked(accountBridge.request).getMockImplementation()!;
  vi.mocked(accountBridge.request).mockImplementationOnce(async(method,params,options)=>{
    const result=await original(method,params,options);await run(f.consumer,3);return result;
  });
  expect(await applyDeferredHandoff(f.handoff.id,f.consumerProof,f.producerProof)).toBe(false);
  expect((await task(f.consumer)).state).toBe('running');
  expect((await pool.query('SELECT 1 FROM dependencies WHERE task_id=$1',[f.consumer])).rowCount).toBe(0);
});
it('does not substitute the current contract for a missing legacy proof',async()=>{
  const f=await fixture();
  await pool.query(`UPDATE deferred_handoffs SET consumer_proof=$2,state='replanning_required',receipt='{"code":"CONSUMER_REVISION_CHANGED"}' WHERE id=$1`,
    [f.handoff.id,JSON.stringify({attempt:0,workspace:'test/o-'+f.consumer,runId:null,fence:null})]);
  expect(await applyDeferredHandoff(f.handoff.id,f.consumerProof,f.producerProof)).toBe(false);
  expect((await pool.query('SELECT receipt FROM deferred_handoffs WHERE id=$1',[f.handoff.id])).rows[0].receipt.code).toBe('HANDOFF_CONTRACT_PROOF_REQUIRED');
});
it('gives the fourth delivery a turn after three unconfirmed actors and deduplicates inspection errors',async()=>{
  const f=await fixture('diagnostic');const blocked=new Set<string>();
  for(let i=0;i<3;i++){
    const id=randomUUID();await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
      SELECT $1::uuid,goal_id,$1::text,title,description,agent_id,'ready',stage,criteria,jsonb_set(execution,'{workspace}',to_jsonb($2::text)) FROM tasks WHERE id=$3`,[id,'test/o-'+id,f.producer]);
    const r=await run(id);await completeRun(r.id,r.fence,'Accepted diagnostic',{});await validateTask(id);blocked.add(r.id);
    await tx(async db=>stageDeferredHandoff(db,f.goal,id,await task(f.source),await task(f.consumer),
      {taskId:id,plannerRunId:f.sourceRun.id,coverageKey:'blocked-'+id,lane:'glm',workKind:'diagnostic'}));
  }
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[f.goal]);
  await pool.query("UPDATE deferred_handoffs SET created_at=now()-interval '1 hour' WHERE id<>$1",[f.handoff.id]);
  const inspect=vi.fn(async(runId:string)=>{if(blocked.has(runId))throw Error('STOP_UNCONFIRMED');
    return runId===f.next.id?f.consumerProof:f.producerProof;});
  await reconcileDeferredHandoffs(inspect);
  expect((await pool.query("SELECT count(*)::int n FROM deferred_handoffs WHERE receipt->'inspection'->>'code'='STOP_UNCONFIRMED'")).rows[0].n).toBe(3);
  await reconcileDeferredHandoffs(inspect);
  expect((await pool.query('SELECT state FROM deferred_handoffs WHERE id=$1',[f.handoff.id])).rows[0].state).toBe('applied');
  await pool.query("UPDATE deferred_handoffs SET receipt=jsonb_set(receipt,'{inspection,nextAt}','\"2000-01-01T00:00:00Z\"') WHERE state<>'applied'");
  await reconcileDeferredHandoffs(inspect);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='HANDOFF_INSPECTION_FAILED'")).rows[0].n).toBe(3);
});
it('allows only one handoff reconciler to inspect an item concurrently',async()=>{
  const f=await fixture('diagnostic');let release!:()=>void;const held=new Promise<void>(r=>release=r);
  let entered!:()=>void;const start=new Promise<void>(r=>entered=r);
  const inspect=vi.fn(async(runId:string)=>{entered();await held;return runId===f.next.id?f.consumerProof:f.producerProof;});
  const first=reconcileDeferredHandoffs(inspect);await start;
  try{await reconcileDeferredHandoffs(inspect);expect(inspect).toHaveBeenCalledTimes(2);}finally{release();await first;}
  expect((await pool.query("SELECT 1 FROM events WHERE kind='HANDOFF_APPLIED'")).rowCount).toBe(1);
});
