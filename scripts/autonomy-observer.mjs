// Separate deterministic observer. No application imports, model calls or DB writes.
import { createRequire } from 'node:module';
import { readFile, writeFile, rename, mkdir, statfs, appendFile } from 'node:fs/promises';
import { getGlmRequestGateStatus } from '../bridge/glm-request-slot.mjs';
import { scoreQualification } from '../server/autonomy-score.ts';
const { Client } = createRequire(import.meta.url)('pg');
const directory = process.env.AGENTOS_AUTONOMY_DIR || '/var/lib/agentos-autonomy';
const goal = process.env.AGENTOS_AUTONOMY_GOAL_ID || '00000000-0000-4000-8000-000000000002';
if (!/^[a-f0-9-]{36}$/.test(goal)) throw Error('OBSERVER_GOAL_INVALID');
const safeCode = e => /^[A-Z0-9_]{4,100}$/.test(e?.code ?? '') ? e.code : 'OBSERVATION_FAILED';
const url = new URL(process.env.DATABASE_URL);
if (!['/agentos','/agentos_test'].includes(url.pathname) || url.port !== '15482' ||
    !['127.0.0.1','localhost','[::1]'].includes(url.hostname) || url.searchParams.has('host')) throw Error('OBSERVER_DATABASE_SCOPE');
await mkdir(directory, { recursive: true, mode: 0o750 });
const prior = await readFile(directory+'/latest.json','utf8').then(JSON.parse).catch(()=>null);
const now = new Date().toISOString();
const out = { version: 1, observedAt: now, goalId: goal, readOnly: true, complete: false,
  errors: [], qualified: false, qualification: { total: 60, evaluated: 0, autonomousPassed: 0,
    pending: 60, requiredPasses: 54, requiredRepairs: 3, acceptedRepairs: 0, requiredHours: 24,
    reason: 'Ensaios de qualificação ainda não executados; atividade não comprova autonomia.' } };
const client = new Client({ connectionString: process.env.DATABASE_URL,
  application_name: 'agentos_autonomy_readonly', connectionTimeoutMillis: 3000 });
try {
  await client.connect();
  await client.query('BEGIN READ ONLY');
  await client.query("SET LOCAL statement_timeout='4000ms'");
  await client.query("SET LOCAL lock_timeout='1000ms'");
  const query = async (sql, values=[]) => (await client.query(sql, values)).rows;
  out.goal = (await query('SELECT id,title,state FROM goals WHERE id=$1',[goal]))[0] ?? null;
  out.tasks = await query('SELECT stage,state,count(*)::int count FROM tasks WHERE goal_id=$1 GROUP BY stage,state',[goal]);
  out.planning = await query(`SELECT id,state,attempt,started_at,heartbeat_at,error,next_attempt_at
    FROM planning_runs WHERE objective_id=$1 ORDER BY created_at DESC LIMIT 5`,[goal]);
  out.active = await query(`SELECT r.id,r.task_id,t.title,t.stage,r.model,r.state,r.started_at,r.heartbeat_at
    FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.state IN('queued','running','cancelling','reconciling')
    ORDER BY r.created_at LIMIT 150`);
  out.nonGlmActive = out.active.filter(r=>r.model!=='glm-5.3-flash').length;
  out.focalActive = await query(`SELECT r.id,r.task_id,t.title,t.stage,r.model,r.state,r.started_at,r.heartbeat_at
    FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 AND r.state IN('queued','running','cancelling','reconciling')
    ORDER BY r.created_at LIMIT 150`,[goal]);
  out.acceptances24h = await query(`SELECT t.stage,count(*)::int records,count(DISTINCT a.task_id)::int tasks
    FROM acceptances a JOIN tasks t ON t.id=a.task_id WHERE t.goal_id=$1 AND a.passed
    AND a.created_at>now()-interval '24 hours' GROUP BY t.stage`,[goal]);
  out.finalDeliveries = await query(`SELECT t.id,t.title,r.id run_id,a.created_at accepted_at
    FROM tasks t JOIN LATERAL(SELECT id FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    LEFT JOIN plan_revisions p ON p.objective_id=t.goal_id AND p.revision=t.plan_revision
    JOIN LATERAL(SELECT passed,created_at FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC LIMIT 1)a ON true
    WHERE t.goal_id=$1 AND t.stage='verify' AND t.state='accepted' AND a.passed
    AND (p.plan->'incremental' IS NULL OR p.plan->'incremental'->'pendingRequirementIds'='[]'::jsonb)
    AND EXISTS(SELECT 1 FROM artifacts x WHERE x.run_id=r.id AND x.digest ~ '^[a-f0-9]{64}$')
    ORDER BY a.created_at DESC LIMIT 100`,[goal]);
  out.verifiedLots = await query(`SELECT t.id,r.id run_id,a.created_at accepted_at,p.revision,
      p.plan->'incremental'->>'selectedRequirementId' requirement_id
    FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    JOIN plan_revisions p ON p.objective_id=t.goal_id AND p.revision=t.plan_revision
    JOIN LATERAL(SELECT passed,created_at FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC LIMIT 1)a ON true
    WHERE t.goal_id=$1 AND t.stage='verify' AND t.state='accepted' AND a.passed AND p.plan ? 'incremental'
    AND EXISTS(SELECT 1 FROM artifacts x WHERE x.run_id=r.id AND x.name='candidate.json' AND x.digest ~ '^[a-f0-9]{64}$')
    ORDER BY a.created_at DESC LIMIT 100`,[goal]);
  out.schema = await query("SELECT id,value->>'state' state FROM settings WHERE id IN('schema:migration-active','schema:backlog-active')");
  out.policy = (await query('SELECT value FROM settings WHERE id=$1',['workforce:'+goal]))[0]?.value ?? null;
  await client.query('COMMIT');
  out.gate = await getGlmRequestGateStatus({timeoutMs:2000});
  const disk=await statfs('/');out.disk={availableBytes:disk.bavail*disk.bsize,totalBytes:disk.blocks*disk.bsize};
  out.complete=true;
} catch(error) {out.errors.push(safeCode(error));try{await client.query('ROLLBACK');}catch{}}
finally {await client.end().catch(()=>{});}
const gap=prior ? Date.parse(now)-Date.parse(prior.observedAt) : Infinity;
out.continuousObservationSince=out.complete && prior?.complete && prior.goalId===goal && gap>=0 && gap<=90000 ? prior.continuousObservationSince : now;
out.qualification.observedHours=(Date.parse(now)-Date.parse(out.continuousObservationSince))/3600000;
out.qualification.coverageComplete=false; // Independent fixed-cohort trials have not been executed yet.
// Evaluation receipts live outside every worker workspace. Missing receipts are
// pending, never successful. The external evaluator owns their publication.
try {
  const spec=JSON.parse(await readFile('/etc/agentos/autonomy-cohort-v1.json','utf8'));
  const receipts=await readFile('/var/lib/agentos-evaluations/glm-v1/results.json','utf8')
    .catch(e=>{if(e.code==='ENOENT')return '{}';throw e;});
  if(receipts.length>1024*1024)throw Error('EVALUATION_RECEIPTS_SIZE');
  const input=JSON.parse(receipts);
  const seconds=Math.max(0,(Date.parse(now)-Math.max(Date.parse(spec.startedAt),Date.parse(out.continuousObservationSince)))/1000);
  const score=scoreQualification(spec,input.trials??[],input.acceptedRepairs??[],{
    now:Date.parse(now),uninterruptedSeconds:seconds,
    criticalViolations:input.audit?.criticalViolations??1,
    coverageComplete:out.complete && input.audit?.coverageComplete===true &&
      input.audit?.contractId===spec.id && Date.parse(input.audit?.through)>=Date.parse(now)-60000 &&
      Date.parse(input.audit?.from)===Date.parse(spec.startedAt) && out.nonGlmActive===0 && out.gate?.maxObserved<=6,
  });
  out.qualification={...out.qualification,...score,acceptedRepairs:score.repairsPassed,
    reason:score.qualified?'Contrato de qualificação comprovado.':'Qualificação pendente; cada ensaio exige recibo do avaliador independente.'};
  out.qualified=score.qualified;
}catch(e){if(e.code!=='ENOENT')out.errors.push('EVALUATION_RECEIPTS_INVALID');}
out.status=!out.complete ? 'observation_failed' : out.schema.some(s=>s.state!=='complete') ? 'schema_pending' :
  out.nonGlmActive ? 'model_policy_pending' : !out.finalDeliveries.length ? 'no_verified_delivery' : 'measuring';
// Append evidence before replacing current snapshot. A crash cannot erase prior observations.
await appendFile(directory+'/observations-'+now.slice(0,10)+'.jsonl',JSON.stringify(out)+'\n',{mode:0o640});
await writeFile(directory+'/latest.json.tmp',JSON.stringify(out),{mode:0o640});
await rename(directory+'/latest.json.tmp',directory+'/latest.json');
console.log(JSON.stringify({observedAt:now,complete:out.complete,status:out.status,
  active:out.active?.length,finalDeliveries:out.finalDeliveries?.length,errors:out.errors}));
