import {randomUUID} from 'node:crypto';
import {pool,tx,lock,event,type DB} from './db.js';
import {requestStrategyPlanning,scopeSourceRevision} from './planning-demand.js';
import {prepareDeferredStrategy,deliveryTransitionSchema} from './planner-strategy-intent.js';
export {deliveryTransitionSchema} from './planner-strategy-intent.js';
export async function installDeliveryTransitions(db:DB=pool){
  await db.query(`CREATE TABLE IF NOT EXISTS delivery_transitions(
    id uuid PRIMARY KEY,goal_id uuid NOT NULL REFERENCES goals(id),review_run_id uuid NOT NULL UNIQUE REFERENCES runs(id),
    task_id uuid REFERENCES tasks(id),expected_attempt int,scope text,source_revision text,strategy_key text,
    state text NOT NULL,proposal jsonb NOT NULL,receipt jsonb NOT NULL DEFAULT '{}',
    planning_request_id uuid,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
    CREATE INDEX IF NOT EXISTS delivery_transition_goal ON delivery_transitions(goal_id,created_at DESC);`);
}

/** The accepted manager proposes semantics. Software binds it to an actual
 * attempt/demand, never invents acceptance, retries or changes a live worker. */
export async function recordDeliveryTransition(db:DB,goalId:string,reviewRunId:string,proposal:unknown){
  await lock(db);
  const existing=(await db.query('SELECT receipt FROM delivery_transitions WHERE review_run_id=$1',[reviewRunId])).rows[0];
  if(existing)return existing.receipt;
  const parsed=deliveryTransitionSchema.safeParse(proposal);
  if(!parsed.success)return {code:'TRANSITION_FORMAT',reason:'Use uma tarefa/tentativa e nome exato de artefato, ou plan com escopo, revisão da fonte e estratégia.'};
  const p=parsed.data,id=randomUUID();
  const authority=(await db.query(`SELECT g.state,COALESCE(a.value->>'enabled','true') autonomy,
    m.value->>'enabled' management,s.value->>'enabled' supervision FROM goals g
    LEFT JOIN settings a ON a.id='autonomy:'||g.project LEFT JOIN settings m ON m.id='management:'||g.id
    LEFT JOIN settings s ON s.id='supervision:'||g.id WHERE g.id=$1`,[goalId])).rows[0];
  const accepted=(await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
    WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate' AND r.state='submitted'
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed=true)`,[reviewRunId,goalId])).rowCount;
  let code='TRANSITION_BOUND',state='tracking',taskId:string|null=null,planningId:string|null=null,deferred:any=null;
  if(!accepted || authority?.state!=='active' || authority?.autonomy!=='true' || authority?.management!=='true' || authority?.supervision!=='true'){
    code='TRANSITION_AUTHORITY';state='rejected';
  }else if(p.mode==='plan'){
    if(!p.scope || !p.sourceRevision || !p.strategyKey){code='TRANSITION_BINDING_REQUIRED';state='rejected';}
    else {
      const requested=await requestStrategyPlanning(db,goalId,reviewRunId,p);
      code=requested.code;planningId=requested.requestId??null;
      state=planningId?'planning':'rejected';
      if(code==='PLANNER_ALREADY_ACTIVE'){
        deferred=await prepareDeferredStrategy(db,goalId,reviewRunId,p,{originalReceipt:{code:'PLANNER_ALREADY_ACTIVE'}});
        state=deferred.state;code=deferred.receipt.code;
      }
    }
  }else if(!p.taskId || p.expectedAttempt===undefined || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(p.expectedArtifact)){
    code='TRANSITION_BINDING_REQUIRED';state='rejected';
  }else{
    const task=(await db.query('SELECT goal_id,attempt_count,state FROM tasks WHERE id=$1',[p.taskId])).rows[0];
    if(task?.goal_id!==goalId || task.attempt_count!==p.expectedAttempt || ['cancelled','superseded'].includes(task.state)){
      code='TRANSITION_STALE';state='rejected';
    }else taskId=p.taskId;
  }
  const receipt={...(deferred?.receipt??{}),code,transitionId:id,taskId,planningRequestId:planningId,
    ...(state==='rejected'?{reason:'A proposta não criou trabalho. Corrija o vínculo/autoridade ou apresente estratégia distinta; os atores e critérios foram preservados.'}:{})};
  await db.query(`INSERT INTO delivery_transitions(id,goal_id,review_run_id,task_id,expected_attempt,scope,
    source_revision,strategy_key,state,proposal,receipt,planning_request_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
    [id,goalId,reviewRunId,taskId,p.expectedAttempt??null,p.scope??null,p.sourceRevision??null,p.strategyKey??null,
      state,JSON.stringify(p),JSON.stringify(receipt),planningId]);
  await event(db,'DELIVERY_TRANSITION_BOUND',goalId,receipt);
  return receipt;
}

export async function readDeliveryTransitions(db:DB,goalId:string,focus?:{caseId?:string;scope?:string}){
  if(!(await db.query("SELECT to_regclass('public.delivery_transitions') present")).rows[0].present)return [];
  return (await db.query(`SELECT id,task_id,expected_attempt,scope,state,proposal,receipt,planning_request_id,updated_at
    FROM delivery_transitions WHERE goal_id=$1
    ORDER BY (state IN('awaiting_planner','strategy_revalidation') AND receipt#>>'{intent,binding,caseId}'=$3) DESC NULLS LAST,
      (state IN('awaiting_planner','strategy_revalidation') AND scope=$2) DESC NULLS LAST,
      created_at DESC LIMIT 8`,[goalId,focus?.scope??null,focus?.caseId??null])).rows;
}
export async function readScopeRevisions(db:DB,goalId:string,scopes:string[]){
  if(!(await db.query("SELECT to_regclass('public.planning_requests') present")).rows[0].present)return [];
  const out=[];
  for(const scope of [...new Set(scopes)].filter(s=>/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(s)))
    out.push({scope,sourceRevision:await scopeSourceRevision(db,goalId,scope)});
  return out;
}

export async function reconcileDeliveryTransitions(){
  return tx(async db=>{
    await lock(db);
    const rows=(await db.query(`SELECT d.* FROM delivery_transitions d JOIN goals g ON g.id=d.goal_id
      WHERE g.state='active' AND d.state IN('tracking','executing','waiting_supervision','planning','replanning_required')
      ORDER BY d.created_at LIMIT 100 FOR UPDATE OF d`)).rows;
    for(const d of rows){
      let state=d.state,receipt:any=d.receipt;
      if(d.planning_request_id){
        const request=(await db.query('SELECT state,receipt,task_id FROM planning_requests WHERE id=$1',[d.planning_request_id])).rows[0];
        if(!request){state='replanning_required';receipt={code:'PLANNING_REQUEST_MISSING'};}
        else{
          state=request.state==='applied'?'fulfilled':request.state==='superseded'?'superseded':
            ['blocked_external','awaiting_supervision'].includes(request.state)?'replanning_required':'planning';
          receipt={...(d.receipt?.intent?{intent:d.receipt.intent}:{}),code:'PLANNING_'+request.state.toUpperCase(),requestId:d.planning_request_id,taskId:request.task_id,detail:request.receipt};
        }
      }else if(d.task_id){
        const task=(await db.query(`SELECT t.state,t.attempt_count,r.id run_id,r.state run_state,
          EXISTS(SELECT 1 FROM runs x WHERE x.task_id=t.id AND x.state IN('queued','running','cancelling','reconciling')) live,
          v.passed FROM tasks t LEFT JOIN LATERAL(SELECT id,state FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
          LEFT JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC LIMIT 1)v ON true
          WHERE t.id=$1`,[d.task_id])).rows[0];
        if(task?.live){state='executing';receipt={code:'ACTOR_LIVE',taskId:d.task_id,attempt:task.attempt_count};}
        else if(task?.state==='accepted' && task.passed===true && task.attempt_count>=d.expected_attempt){
          const artifact=(await db.query('SELECT id,digest FROM artifacts WHERE run_id=$1 AND name=$2',[task.run_id,d.proposal.expectedArtifact])).rows[0];
          state=artifact?'fulfilled':'replanning_required';receipt={code:artifact?'EXPECTED_ARTIFACT_ACCEPTED':'EXPECTED_ARTIFACT_ABSENT',
            taskId:d.task_id,attempt:task.attempt_count,runId:task.run_id,...(artifact?{artifactId:artifact.id,digest:artifact.digest}:{})};
        }else if(['cancelled','superseded'].includes(task?.state)) {state='superseded';receipt={code:'TASK_'+task.state.toUpperCase()};}
        else if(['failed','recovery_blocked'].includes(task?.state)){
          state='waiting_supervision';receipt={code:'SUPERVISION_REQUIRED',taskId:d.task_id,attempt:task.attempt_count,
            cases:(await db.query("SELECT id,state FROM supervision_cases WHERE task_id=$1 AND state IN('open','decided','delegated','waiting_evidence') ORDER BY id",[d.task_id])).rows};
        }else {state='tracking';receipt={code:'TASK_PENDING',taskId:d.task_id,taskState:task?.state};}
      }
      // JSONB reorders object keys. Compare semantic JSON in PostgreSQL rather
      // than repeatedly treating that serialization difference as progress.
      const changed=await db.query(`UPDATE delivery_transitions SET state=$2,receipt=$3::jsonb,updated_at=now()
        WHERE id=$1 AND (state IS DISTINCT FROM $2 OR receipt IS DISTINCT FROM $3::jsonb) RETURNING id`,
        [d.id,state,JSON.stringify(receipt)]);
      if(!changed.rowCount)continue;
      await event(db,'DELIVERY_TRANSITION_CHANGED',d.goal_id,{transitionId:d.id,state,...receipt});
    }
  });
}
