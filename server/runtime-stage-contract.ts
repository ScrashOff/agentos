import {createHash} from 'node:crypto';
import {z} from 'zod';

const digest=z.string().regex(/^[a-f0-9]{64}$/);
const integer=z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const timestamp=z.string().datetime({offset:true});
const count=integer.max(1_000_000_000);
/** Worker-authored facts are unverified observations, never an execution receipt.
 * Only bounded case labels survive; detail text, commands and logs are excluded. */
export const runtimeStageReportFactsSchema=z.object({
  version:z.literal(1),suiteExecuted:z.boolean().nullable(),
  observedCounts:z.object({total:count,examined:count.max(2000),passed:count,failed:count,unknown:count}).strict().nullable(),
  reportedCounts:z.object({total:count.nullable(),passed:count.nullable(),failed:count.nullable()}).strict().optional(),
  inconsistency:z.boolean(),
  failedCases:z.array(z.object({index:count.max(1999),name:z.string().min(1).max(160)
    .refine(value=>!/[\u0000-\u001f\u007f]/.test(value)),ok:z.literal(false),
    detailClass:z.enum(['timeout','assertion_mismatch','missing_input','process_error','unclassified']),
    detailDigest:digest.nullable()}).strict()).max(6),
  truncated:z.boolean(),
}).strict().superRefine((facts,ctx)=>{
  const observed=facts.observedCounts,reported=facts.reportedCounts;
  const invalid=(message:string)=>ctx.addIssue({code:'custom',message});
  if(!observed?.total && reported?.total==null && reported?.passed==null && reported?.failed==null)
    invalid('Facts require observed cases or a reported count.');
  if(observed && (observed.examined>observed.total || observed.passed+observed.failed+observed.unknown!==observed.examined))
    invalid('Observed case counts are inconsistent.');
  if(facts.failedCases.length>(observed?.failed??0) || facts.failedCases.some(item=>item.index>=(observed?.examined??0)) ||
    new Set(facts.failedCases.map(item=>item.index)).size!==facts.failedCases.length)
    invalid('Failed cases must reference distinct examined indices.');
  if(observed && (observed.examined<observed.total || facts.failedCases.length<observed.failed) && !facts.truncated)
    invalid('Omitted cases require an explicit truncation marker.');
  const inconsistent=!!reported && (
    !!observed && ((reported.total!==null && reported.total!==observed.total) ||
      observed.examined===observed.total && ((reported.passed!==null && reported.passed!==observed.passed) ||
        (reported.failed!==null && reported.failed!==observed.failed))) ||
    reported.total!==null && reported.passed!==null && reported.failed!==null && reported.total<reported.passed+reported.failed);
  if(facts.inconsistency!==inconsistent)invalid('Count disagreement must be explicit.');
});
export type RuntimeStageReportFacts=z.infer<typeof runtimeStageReportFactsSchema>;
const evidencePath=z.string().max(600).regex(/^(?:data|evidence)\/[A-Za-z0-9_./-]+$/)
  .refine(value=>!value.split('/').some(part=>!part||part==='.'||part==='..') &&
    !/(?:\.env\b|credentials?|auth\.json|id_rsa|private[_-]?key)/i.test(value));
export const runtimeStageObservationSchema=z.object({
  category:z.enum(['unproductive_wait','stage_failure']),evidenceDigest:digest,
  callId:z.string().regex(/^[A-Za-z0-9_.:-]{1,200}$/),toolName:z.enum(['bash','stage_report']),
  // Free-form telemetry statements never enter the manager's context.
  statement:z.string().max(1000).optional(),
  wait:z.object({commandDigest:digest,completedCalls:integer.min(4),
    firstCompletedAt:timestamp,lastCompletedAt:timestamp}).strict().optional(),
  reports:z.array(z.object({path:evidencePath,digest,size:integer.max(4*1024*1024),
    mtimeMs:z.number().finite().nonnegative(),passed:z.boolean().nullable(),exitCode:z.number().int().min(-1).max(255).nullable(),
    facts:runtimeStageReportFactsSchema.optional()}).strict()).max(24),
}).strict().superRefine((observation,ctx)=>{
  if(!observation.reports.some(report=>report.facts))return;
  const report=observation.reports[0];
  if(observation.category!=='stage_failure'||observation.reports.length!==1||!report.facts||
    observation.evidenceDigest!==stageDigest([report.path,report.digest,stageFactsDigest(report.facts)]))
    ctx.addIssue({code:'custom',message:'Report facts must bind the exact path, file hash and canonical facts digest.'});
});
export const runtimeStageSnapshotSchema=z.object({version:z.literal(1),policy:z.literal('runtime-stage-observation-v1'),
  authority:z.literal('review_only'),runId:z.uuid(),runFence:z.uuid(),workspace:z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/),
  checkedAt:timestamp,available:z.literal(true),truncated:z.boolean().optional(),trace:z.object({digest,size:integer,lastSequence:integer}).strict(),
  observations:z.array(runtimeStageObservationSchema).max(24)}).strict();
export type RuntimeStageObservation=z.infer<typeof runtimeStageObservationSchema>;
export type RuntimeStageSnapshot=z.infer<typeof runtimeStageSnapshotSchema>;
export const stageDigest=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
/** Stable across object property ordering, identical to the bridge projection. */
export const stageFactsDigest=(facts:RuntimeStageReportFacts)=>stageDigest([
  facts.version,facts.suiteExecuted,
  facts.observedCounts?[facts.observedCounts.total,facts.observedCounts.examined,facts.observedCounts.passed,facts.observedCounts.failed,facts.observedCounts.unknown]:null,
  facts.reportedCounts?[facts.reportedCounts.total,facts.reportedCounts.passed,facts.reportedCounts.failed]:null,
  facts.inconsistency,facts.failedCases.map(item=>[item.index,item.name,item.ok,item.detailClass,item.detailDigest]),facts.truncated,
]);

/** Observation admits a review only. No age, heartbeat, report or repetition
 * grants stop/retry/acceptance authority. Partial reports remain unverified. */
export function checkedRuntimeStageSnapshot(value:unknown,now=Date.now()):RuntimeStageSnapshot {
  const result=runtimeStageSnapshotSchema.parse(value),checked=Date.parse(result.checkedAt);
  if(checked>now+5000||now-checked>30000)throw Error('RUNTIME_STAGE_OBSERVATION_STALE');
  const keys=new Set<string>();
  for(const observation of result.observations){
    const key=observation.category+':'+observation.evidenceDigest;
    if(keys.has(key))throw Error('RUNTIME_STAGE_DUPLICATE_OBSERVATION');keys.add(key);
    if(observation.category==='unproductive_wait'){
      const wait=observation.wait;
      if(!wait||Date.parse(wait.lastCompletedAt)-Date.parse(wait.firstCompletedAt)<60000||
        Date.parse(wait.lastCompletedAt)>checked+5000||checked-Date.parse(wait.lastCompletedAt)>15*60000)
        throw Error('RUNTIME_STAGE_WAIT_INVALID');
    }else if(observation.wait||!observation.reports.some(report=>report.passed===false||
      (report.exitCode!==null&&report.exitCode!==0)))throw Error('RUNTIME_STAGE_REPORT_INVALID');
    if(observation.reports.some(report=>report.mtimeMs>checked+5000))throw Error('RUNTIME_STAGE_REPORT_FUTURE');
  }
  return result;
}

export function stageObservationStatement(observation:RuntimeStageObservation,followThrough=false){
  return observation.category==='unproductive_wait'
    ? `${observation.wait!.completedCalls} completed tool returns repeated the same observed wait without changed referenced evidence. ${followThrough?'Another completed observation window followed the previous consumed guidance or explicit no-action; its effect is not proven. ':''}This is a nonfatal review signal, not proof that the operation stopped or failed, and not an exit receipt. Inspect the owned operation and retain all task criteria.`
    : 'A partial local report contains a negative result. Its file hash is observed; execution provenance and the final task verdict remain unverified. It may be an intentional negative test. Inspect the report and original criteria without accepting or retrying the task.';
}
