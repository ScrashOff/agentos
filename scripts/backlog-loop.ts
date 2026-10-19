import { pool } from '../server/db.js';
import { reconcileCanonicalRepairRejections } from '../server/semantic-retry.js';
import {reconcilePlanningDemand} from '../server/planning-demand-runtime.js';
import {WorkspacePreparationWorker} from '../server/workspace-preparation.js';
import {reconcileDeferredHandoffs} from '../server/deferred-handoffs.js';
import {reconcileDeliveryConsumptions} from '../server/delivery-consumption.js';
import { ReconciliationLoop } from '../server/reconciliation-loop.js';
import { recordWorkerReadiness } from '../server/worker-readiness.js';
import { reconcileBacklog } from '../server/backlog.js';
import { accountBridge } from '../server/bridge.js';
import { recoverMissingOpenCodeArtifacts } from '../server/missing-artifacts.js';
import { requireBacklogSchemaReady } from '../server/backlog-schema-readiness.js';
import { recoverAnalyticalDeliveries } from '../server/analytical-delivery.js';
import { reconcileRuntimeGuidance } from '../server/runtime-guidance.js';
import { recordManagementReviews, recordManagementTransitions } from '../server/management.js';
import { reconcileFollowups } from '../server/followups.js';
import {
  detectSupervisionCases,
  wakeSupervisors,
  recordSupervisionDecisions,
  applySupervisionDecisions,
  runSupervisorJobs,
  closeSupervisorJobs,
  resumeSupervisedWork,
} from '../server/supervision.js';
import {reconcileDeliveryTransitions} from '../server/delivery-transitions.js';
import {reconcileCompletedNativeArtifacts} from '../server/native-completed-recovery.js';
import {reconcileStorageRecovery} from '../server/storage-recovery.js';
import {inspectStopped} from '../server/continuation.js';
import {reconcileRuntimeProgress} from '../server/runtime-progress-control.js';
import {reconcileRuntimeStage} from '../server/runtime-stage-control.js';
import {reconcileDisabledWorkforceLanes} from '../server/workforce-disabled-lane.js';
// Every controller-specific installer is covered by the explicit preparer.
// No ownership, preparation recovery or loop starts without both receipts.
await requireBacklogSchemaReady();
const preparationWorker=new WorkspacePreparationWorker();
await preparationWorker.reconcileOwners();
const owner = await pool.connect();
if (!(await owner.query('SELECT pg_try_advisory_lock(4280066) owned')).rows[0].owned)
  throw Error('BACKLOG_OWNER_EXISTS');
let running = true;
let planningStartedAt=0;
let planningPhases:Array<{name:string;durationMs:number}>=[];
async function recordPlanningFailure(error: unknown) {
  const candidate = (error as any)?.code ?? (error as any)?.message;
  const code = /^[A-Z][A-Z0-9_]{2,80}$/.test(candidate ?? '') ? candidate : 'PLANNING_CONTROLLER_ERROR';
  await pool.query(
    "INSERT INTO settings VALUES('planning-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({ checkedAt: new Date().toISOString(), state: 'error', code,
      durationMs:planningStartedAt?Date.now()-planningStartedAt:null,phases:planningPhases })],
  );
}
const preparationLoop = new ReconciliationLoop(
  () => preparationWorker.tick(), recordPlanningFailure,
);
const planningLoop = new ReconciliationLoop(async () => {
  await owner.query('SELECT 1');
  const started = Date.now();
  planningStartedAt=started;planningPhases=[];
  await reconcilePlanningDemand(phase=>planningPhases.push(phase));
  await pool.query(
    "INSERT INTO settings VALUES('planning-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({ checkedAt: new Date().toISOString(), state: 'healthy', durationMs: Date.now() - started,phases:planningPhases })],
  );
}, recordPlanningFailure);
// Stop-proof collection may wait on remote hosts. It must not hold up scoped
// planning or the notification consumer while preserving all handoff guards.
const deliveryLoop = new ReconciliationLoop(async()=>{
  const started=Date.now();
  await reconcileCompletedNativeArtifacts();
  await reconcileDeferredHandoffs();
  await reconcileDeliveryConsumptions();
  await pool.query("INSERT INTO settings VALUES('delivery-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({checkedAt:new Date().toISOString(),state:'healthy',durationMs:Date.now()-started})]);
},async(error:unknown)=>{
  const value=(error as any)?.code??(error as any)?.message;
  const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(value??'')?value:'DELIVERY_CONTROLLER_ERROR';
  await pool.query("INSERT INTO settings VALUES('delivery-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({checkedAt:new Date().toISOString(),state:'error',code})]);
});
// Collection may wait for several remote stop proofs. Keep its real promise
// on the same backlog owner, but outside the semantic reconciliation pass.
// The existing claim cooldown is one minute; shutdown awaits this lane too.
let missingArtifactsStartedAt = 0;
const missingArtifactsLoop = new ReconciliationLoop(async () => {
  missingArtifactsStartedAt = Date.now();
  await owner.query('SELECT 1');
  await pool.query("INSERT INTO settings VALUES('missing-artifacts-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:'running',inFlight:true,startedAt:new Date(missingArtifactsStartedAt).toISOString(),
      checkedAt:new Date().toISOString()})]);
  const recovered = await recoverMissingOpenCodeArtifacts();
  const finishedAt = new Date().toISOString();
  await pool.query("INSERT INTO settings VALUES('missing-artifacts-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:'healthy',inFlight:false,startedAt:new Date(missingArtifactsStartedAt).toISOString(),
      finishedAt,checkedAt:finishedAt,durationMs:Date.now()-missingArtifactsStartedAt,recoveredCount:recovered.length})]);
},async(error:unknown)=>{
  const value=(error as any)?.code??(error as any)?.message;
  const code=/^[A-Z0-9_]{3,80}$/.test(value??'')?value:'MISSING_ARTIFACTS_CONTROLLER_ERROR';
  const finishedAt=new Date().toISOString();
  await pool.query("INSERT INTO settings VALUES('missing-artifacts-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:'error',inFlight:false,code,startedAt:new Date(missingArtifactsStartedAt).toISOString(),
      finishedAt,checkedAt:finishedAt,durationMs:Date.now()-missingArtifactsStartedAt})]);
},60000);
// Shared host capacity is independent from semantic work and planner latency.
// The backlog owner guarantees one loop; recovery still fences each source run.
const storageLoop=new ReconciliationLoop(()=>reconcileStorageRecovery(inspectStopped),async(error:unknown)=>{
  const value=(error as any)?.code??(error as any)?.message;
  const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(value??'')?value:'STORAGE_RECOVERY_ERROR';
  await pool.query("INSERT INTO settings VALUES('storage-recovery-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({checkedAt:new Date().toISOString(),state:'error',code})]);
});
// A persisted coordinator job has already passed admission. Its dispatcher
// must not wait for artifact recovery or a long analytical snapshot. The
// existing executor's active guard and this same backlog owner keep one actor.
const supervisorDispatchLoop=new ReconciliationLoop(runSupervisorJobs,async(error:unknown)=>{
  const value=(error as any)?.code??(error as any)?.message;
  const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(value??'')?value:'SUPERVISOR_DISPATCH_ERROR';
  await pool.query("INSERT INTO settings VALUES('supervisor-dispatch-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({checkedAt:new Date().toISOString(),state:'error',code})]);
});
const runtimeProgressLoop = new ReconciliationLoop(async () => {
  const started=Date.now(), result=await reconcileRuntimeProgress();
  await pool.query("INSERT INTO settings VALUES('runtime-progress-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:result.unavailable?'degraded':'healthy',checkedAt:new Date().toISOString(),durationMs:Date.now()-started,...result})]);
},async(error:unknown)=>{
  const value=(error as any)?.code??(error as any)?.message;
  const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(value??'')?value:'RUNTIME_PROGRESS_INSPECTION_FAILED';
  await pool.query("INSERT INTO settings VALUES('runtime-progress-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:'error',checkedAt:new Date().toISOString(),code})]);
},60000);
// Observations wake semantic supervision without withdrawing heartbeat or
// waiting for the worker conversation to finish. Independent from stop policy.
const runtimeStageLoop=new ReconciliationLoop(async()=>{
  const started=Date.now(),result=await reconcileRuntimeStage();
  await pool.query("INSERT INTO settings VALUES('runtime-stage-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:result.unavailable?'degraded':'healthy',authority:'review_only',
      checkedAt:new Date().toISOString(),durationMs:Date.now()-started,...result})]);
},async()=>{
  await pool.query("INSERT INTO settings VALUES('runtime-stage-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:'error',authority:'review_only',checkedAt:new Date().toISOString(),code:'RUNTIME_STAGE_OBSERVATION_FAILED'})]);
},60000);
// Inspect historical receipts outside the admission transaction and keep this
// recovery independent of manager generation and provider dispatch latency.
const workforceRecoveryLoop = new ReconciliationLoop(async () => {
  const started=Date.now(),result=await reconcileDisabledWorkforceLanes();
  await pool.query("INSERT INTO settings VALUES('workforce-recovery-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:'healthy',checkedAt:new Date().toISOString(),durationMs:Date.now()-started,...result})]);
},async(error:unknown)=>{
  const value=(error as any)?.code??(error as any)?.message;
  const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(value??'')?value:'WORKFORCE_RECOVERY_ERROR';
  await pool.query("INSERT INTO settings VALUES('workforce-recovery-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify({state:'error',checkedAt:new Date().toISOString(),code})]);
},60000);
owner.on('notification',message=>{
  if(message.channel==='agentos_planning_wake')planningLoop.requestWake();
});
await owner.query('LISTEN agentos_planning_wake');
function requestStop() {
  running = false;
  planningLoop.requestStop();
  preparationLoop.requestStop();
  deliveryLoop.requestStop();
  missingArtifactsLoop.requestStop();
  storageLoop.requestStop();
  supervisorDispatchLoop.requestStop();
  runtimeProgressLoop.requestStop();
  runtimeStageLoop.requestStop();
  workforceRecoveryLoop.requestStop();
  preparationWorker.requestStop();
}
owner.on('error', requestStop);
await recordWorkerReadiness('agentos-backlog', [4280066]);
process.on('SIGTERM', requestStop);
process.on('SIGINT', requestStop);
// Planning/refill is independent of slow artifact collection and semantic
// supervision. The PostgreSQL owner and admission guards remain shared.
preparationLoop.start();
planningLoop.start();
deliveryLoop.start();
missingArtifactsLoop.start();
storageLoop.start();
supervisorDispatchLoop.start();
runtimeProgressLoop.start();
runtimeStageLoop.start();
workforceRecoveryLoop.start();
while (running) {
  const cycleStarted = Date.now();
  const phases: Array<{ name: string; durationMs: number; failed?: boolean }> = [];
  const phase = async (name: string, work: () => Promise<unknown>) => {
    const started = Date.now();
    try {
      await work();
      phases.push({ name, durationMs: Date.now() - started });
    } catch (error) {
      phases.push({ name, durationMs: Date.now() - started, failed: true });
      throw error;
    }
  };
  try {
    await owner.query('SELECT 1');
    await phase('followups', reconcileFollowups);
    await phase('analytical-deliveries', recoverAnalyticalDeliveries);
    await phase('detect-supervision', detectSupervisionCases);
    await phase('canonical-repair-rejections', reconcileCanonicalRepairRejections);
    await phase('record-supervision', recordSupervisionDecisions);
    // Preserve the accepted decision's exact attempt before a legitimate retry
    // advances it. Slow analytical observations remain outside this phase.
    await phase('management-transitions', recordManagementTransitions);
    await phase('apply-supervision', applySupervisionDecisions);
    await phase('resume-supervised', resumeSupervisedWork);
    await phase('management-reviews', recordManagementReviews);
    await phase('runtime-guidance', reconcileRuntimeGuidance);
    // Persist the completed planner's work before the periodic wake changes its attempt.
    await phase('backlog', reconcileBacklog);
    await phase('delivery-transitions', reconcileDeliveryTransitions);
    await phase('wake-supervisors', wakeSupervisors);
    await pool.query(
      "INSERT INTO settings VALUES('backlog-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
      [JSON.stringify({ checkedAt: new Date().toISOString(), state: 'healthy',
        durationMs: Date.now() - cycleStarted, phases })],
    );
  } catch (e: any) {
    await pool
      .query(
        "INSERT INTO settings VALUES('backlog-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
        [
          JSON.stringify({
            checkedAt: new Date().toISOString(),
            state: 'error',
            code: e.code ?? 'BACKLOG_ERROR',
            durationMs: Date.now() - cycleStarted,
            phases,
          }),
        ],
      )
      .catch(() => {});
  }
  await new Promise((r) => setTimeout(r, 5000));
}
await Promise.all([planningLoop.close(), preparationLoop.close(), deliveryLoop.close(), missingArtifactsLoop.close(), storageLoop.close(), supervisorDispatchLoop.close(), runtimeProgressLoop.close(), runtimeStageLoop.close(), workforceRecoveryLoop.close()]);
await preparationWorker.close();
await closeSupervisorJobs();
await owner.query('SELECT pg_advisory_unlock(4280066)');
owner.release();
accountBridge.close();
await pool.end();
