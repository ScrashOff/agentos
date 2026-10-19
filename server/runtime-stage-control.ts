import {execFile} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {promisify} from 'node:util';
import {config} from './config.js';
import {bridgeScript} from './bridge-path.js';
import {pool,tx,lock,type DB} from './db.js';
import {taskExecutionMetadataVersion} from './task-execution-metadata.js';
import {checkedRuntimeStageSnapshot,stageDigest,stageObservationStatement,
  type RuntimeStageObservation} from './runtime-stage-contract.js';

const exec=promisify(execFile);
let cursor:string|null=null;
export async function inspectRuntimeStage(runId:string,workspace:string){
  const {stdout}=await exec('ssh',['-T','-o','BatchMode=yes','-o','ConnectTimeout=8',config.bridgeHost,
    '/opt/deepseek-harness/node/bin/node',bridgeScript('inspect-runtime-stage.mjs'),runId,workspace],
    {timeout:20000,maxBuffer:128*1024,windowsHide:true});
  return JSON.parse(stdout);
}
async function candidates(db:DB,runId:string|null=null,after:string|null=null){
  return (await db.query(`SELECT r.id,r.fence,r.task_id,r.context_id,r.attempt,t.goal_id,
    metadata.workspace,metadata.base_workspace scope
    FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
    JOIN task_execution_metadata metadata ON metadata.task_id=t.id AND metadata.goal_id=t.goal_id
      AND metadata.version=$3 AND metadata.execution_type='object'
    JOIN contexts c ON c.id=r.context_id AND c.run_id=r.id
    JOIN settings management ON management.id='management:'||g.id AND management.value->>'enabled'='true'
    JOIN settings supervision ON supervision.id='supervision:'||g.id AND supervision.value->>'enabled'='true'
    JOIN settings workforce ON workforce.id='workforce:'||g.id AND workforce.value->>'enabled'='true'
    LEFT JOIN settings autonomy ON autonomy.id='autonomy:'||g.project
    WHERE ($1::uuid IS NULL OR r.id=$1) AND r.state='running' AND t.state='running' AND g.state='active'
      AND t.stage IS DISTINCT FROM 'coordinate' AND r.attempt=t.attempt_count
      AND r.runtime='harness' AND metadata.control->>'kind'='harness'
      AND COALESCE(autonomy.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR autonomy.value->>'enabled'='true')
      AND EXISTS(SELECT 1 FROM events e WHERE e.kind='RUNTIME_GUIDANCE_AVAILABLE' AND e.entity_id=t.id::text
        AND e.payload->>'runId'=r.id::text AND e.payload->>'runFence'=r.fence::text
        AND e.payload->>'contextVersion'=r.context_id::text)
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED'
        AND (e.payload->>'runId'=r.id::text OR e.entity_id IN(r.id::text,t.id::text)))
    ORDER BY CASE WHEN $2::uuid IS NULL OR r.id>$2 THEN 0 ELSE 1 END,r.id LIMIT 24`,
    [runId,after,taskExecutionMetadataVersion])).rows;
}

type Episode={episodeId:string;generation:number;causeDigest:string;eventId:string;observation:RuntimeStageObservation;
  postReview?:{key:string;completedCalls:number;lastCompletedAt:string};previousEventId?:string;previousDispositionRef?:string};
/** A proposed/uncertain delivery owns its intent. Consumption or no-action is
 * review disposition, never evidence that the original problem was repaired. */
async function disposition(db:DB,current:any,eventId:string,causeDigest:string){
  // One advisory already owned for this actor suppresses every other cause.
  // An ambiguous send is reconciled, never replaced by another diagnosis.
  if((await db.query(`SELECT 1 FROM settings WHERE id LIKE 'runtime-guidance:%'
    AND value->>'runId'=$1 AND value->>'runFence'=$2 AND value->>'contextVersion'=$3
    AND value->>'taskId'=$4 AND value->>'goalId'=$5
    AND value->>'state' IN('proposed','dispatching','queued_next_step','uncertain') LIMIT 1`,
    [current.id,current.fence,current.context_id,current.task_id,current.goal_id])).rowCount)return {pending:true};
  const rows=(await db.query(`SELECT ma.receipt,owned.value action,mr.id manager_run_id
    FROM management_actions ma JOIN runs mr ON mr.id=ma.run_id JOIN tasks mt ON mt.id=mr.task_id
    LEFT JOIN settings owned ON owned.id='runtime-guidance:'||(ma.receipt->>'actionId')
    WHERE mt.goal_id=$1::uuid AND mt.stage='coordinate' AND mr.state='submitted'
      AND (SELECT passed FROM acceptances a WHERE a.run_id=mr.id AND a.task_id=mt.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
      AND ((ma.receipt->>'kind'='guide' AND ma.receipt->>'code'='PROPOSED'
        AND owned.value->>'runId'=$2 AND owned.value->>'runFence'=$3 AND owned.value->>'contextVersion'=$4
        AND owned.value->>'taskId'=$5 AND owned.value->>'goalId'=$1::text AND owned.value->>'expectedAttempt'=$7
        AND owned.value->'facts' @> jsonb_build_array(jsonb_build_object('evidenceRef','event:'||$6::text)))
      OR (ma.receipt->>'code'='LIVE_FAILURE_NO_ACTION' AND ma.receipt->>'eventId'=$6
        AND ma.receipt->>'failureKey'=$8
        AND ma.receipt->>'runId'=$2 AND ma.receipt->>'runFence'=$3 AND ma.receipt->>'contextVersion'=$4
        AND ma.receipt->>'taskId'=$5 AND ma.receipt->>'goalId'=$1::text
        AND ma.receipt->>'decision'='continue_observing' AND length(trim(ma.receipt->>'reason')) BETWEEN 40 AND 1000))
    ORDER BY mr.created_at DESC,ma.action_index DESC`,
    [current.goal_id,current.id,current.fence,current.context_id,current.task_id,eventId,String(current.attempt),
      stageDigest([current.goal_id,current.task_id,current.id,current.fence,current.context_id,causeDigest])])).rows;
  if(rows.some(row=>['proposed','dispatching','queued_next_step','uncertain'].includes(row.action?.state)))return {pending:true};
  for(const row of rows){
    if(['consumed','effectverified'].includes(row.action?.state))return {pending:false,key:'guide:'+row.action.actionId};
    if(row.receipt.code==='LIVE_FAILURE_NO_ACTION')return {pending:false,key:'no-action:'+row.manager_run_id};
  }
  return {pending:false};
}

/** The only writes are observation ledger + event, atomically. Worker state,
 * frozen contract, leases, artifacts and acceptance are never touched here. */
export async function recordRuntimeStageObservation(observed:any,raw:unknown){
  const snapshot=checkedRuntimeStageSnapshot(raw);
  const frozen=(await pool.query(`SELECT instruction_snapshot->'task'->>'id' task_id,
    instruction_snapshot->'task'->>'goal_id' goal_id,
    instruction_snapshot->'task'->'execution'->>'workspace' workspace
    FROM runs WHERE id=$1 AND fence=$2`,[observed.id,observed.fence])).rows[0];
  return tx(async db=>{
    await lock(db);
    const current=(await candidates(db,observed.id))[0];
    if(!current||stageDigest(current)!==stageDigest(observed)||snapshot.runId!==current.id||
      snapshot.runFence!==current.fence||snapshot.workspace!==current.workspace||frozen?.workspace!==current.workspace||
      frozen.task_id!==current.task_id||frozen.goal_id!==current.goal_id)return {recorded:false,events:0};
    checkedRuntimeStageSnapshot(snapshot);
    const key='runtime-stage:'+current.id;
    const prior=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[key])).rows[0]?.value;
    if(prior&&(prior.runFence!==current.fence||prior.contextVersion!==current.context_id||prior.workspace!==current.workspace))
      return {recorded:false,events:0};
    if(prior && Date.parse(prior.checkedAt)>Date.parse(snapshot.checkedAt))return {recorded:false,events:0};
    const episodes:Record<string,Episode>={...(prior?.episodes??{})};
    const activeEventIds:string[]=snapshot.truncated?[...(prior?.activeEventIds??[])]:[];let emitted=0;
    for(const observation of snapshot.observations){
      const episodeKey=observation.category+':'+observation.evidenceDigest;
      let episode=episodes[episodeKey];let followThrough=false;
      if(episode&&observation.wait){
        const review=await disposition(db,current,episode.eventId,episode.causeDigest);
        if(review.pending)delete episode.postReview;
        else if(review.key){
          if(episode.postReview?.key!==review.key)episode.postReview={key:review.key,
            completedCalls:observation.wait.completedCalls,lastCompletedAt:observation.wait.lastCompletedAt};
          else if((observation.wait.completedCalls-episode.postReview.completedCalls>=4||
            (Date.parse(observation.wait.firstCompletedAt)>Date.parse(episode.postReview.lastCompletedAt)&&observation.wait.completedCalls>=4))&&
            Date.parse(observation.wait.lastCompletedAt)-Date.parse(episode.postReview.lastCompletedAt)>=60000){
            episode={...episode,previousEventId:episode.eventId,previousDispositionRef:review.key,
              generation:episode.generation+1,eventId:'',postReview:undefined};
            followThrough=true;
          }
        }
      }
      if(!episode?.eventId){
        const generation=episode?.generation??0;
        const episodeId=episode?.episodeId??randomUUID(),causeDigest=stageDigest([episodeKey,episodeId,generation]);
        const payload={version:1,authority:'review_only',nonfatal:true,acceptance:false,
          goalId:current.goal_id,taskId:current.task_id,runId:current.id,runFence:current.fence,
          contextVersion:current.context_id,expectedAttempt:current.attempt,workspace:current.workspace,
          category:observation.category,semanticCauseDigest:causeDigest,
          evidenceDigest:observation.evidenceDigest,toolName:observation.toolName,callId:observation.callId,
          statement:stageObservationStatement(observation,followThrough),generation,
          previousEventId:episode?.previousEventId??null,previousDispositionRef:episode?.previousDispositionRef??null,
          observation:{...observation,statement:undefined},
          trace:snapshot.trace,checkedAt:snapshot.checkedAt};
        const eventId=(await db.query(`INSERT INTO events(kind,entity_id,payload)
          VALUES('RUNTIME_STAGE_OBSERVATION',$1,$2) RETURNING id::text`,[current.task_id,JSON.stringify(payload)])).rows[0].id;
        episode={episodeId,generation,causeDigest,eventId,observation,...(episode?.previousEventId?
          {previousEventId:episode.previousEventId,previousDispositionRef:episode.previousDispositionRef}:{})};emitted++;
      }else episode={...episode,observation};
      if(observation.category==='stage_failure' && observation.reports.length===1){
        // Directly observed replacement of this path supersedes its old active
        // evidence even when other paths were omitted by a bounded inspection.
        // Immutable old events/episodes remain available for audit; adding facts
        // is a new cause, never a retrofit onto the prior report hash or review.
        for(const priorEpisode of Object.values(episodes)){
          if(priorEpisode.eventId===episode.eventId || priorEpisode.observation.category!=='stage_failure' ||
            priorEpisode.observation.reports.length!==1 ||
            priorEpisode.observation.reports[0].path!==observation.reports[0].path)continue;
          const old=activeEventIds.indexOf(priorEpisode.eventId);if(old>=0)activeEventIds.splice(old,1);
        }
      }
      if(episode.previousEventId){const old=activeEventIds.indexOf(episode.previousEventId);if(old>=0)activeEventIds.splice(old,1);}
      episodes[episodeKey]=episode;if(!activeEventIds.includes(episode.eventId))activeEventIds.push(episode.eventId);
    }
    // A single sample that omits evidence does not erase its disposition. Keep
    // a bounded history separately from active events; full history is durable.
    const activeKeys=new Set(snapshot.observations.map(o=>o.category+':'+o.evidenceDigest));
    const inactiveKeys=Object.keys(episodes).filter(key=>!activeKeys.has(key)&&!activeEventIds.includes(episodes[key].eventId));
    while(Object.keys(episodes).length>128&&inactiveKeys.length)delete episodes[inactiveKeys.shift()!];
    const value={version:1,authority:'review_only',runId:current.id,runFence:current.fence,
      contextVersion:current.context_id,workspace:current.workspace,goalId:current.goal_id,taskId:current.task_id,
      checkedAt:snapshot.checkedAt,trace:snapshot.trace,activeEventIds,episodes};
    await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',[key,JSON.stringify(value)]);
    return {recorded:true,events:emitted};
  });
}

export async function reconcileRuntimeStage(inspect=inspectRuntimeStage){
  const result={inspected:0,recorded:0,events:0,unavailable:0,changed:0};
  if(!config.bridgeHost||!config.remoteExecution)return result;
  const rows=await candidates(pool,null,cursor);if(rows.length)cursor=rows.at(-1).id;
  let next=0;
  await Promise.all(Array.from({length:Math.min(3,rows.length)},async()=>{
    while(next<rows.length){const current=rows[next++];
      try{
        const raw=await inspect(current.id,current.workspace);result.inspected++;
        if(raw?.available!==true){result.unavailable++;continue;}
        const outcome=await recordRuntimeStageObservation(current,raw);
        if(outcome.recorded)result.recorded++;else result.changed++;result.events+=outcome.events;
      }catch{result.unavailable++;}
    }
  }));
  return result;
}
