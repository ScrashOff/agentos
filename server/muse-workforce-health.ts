import { quotaFor } from './accounts.js';
import type { WorkforceHealthInput } from './workforce-health.js';
import { isMuseBacklogExecution } from './muse-backlog-delivery.js';
import { reservesWorkforceBacklog } from './workforce-reserve.js';

const live = new Set(['queued', 'running', 'cancelling', 'reconciling']);
const closed = new Set(['accepted', 'cancelled', 'superseded']);
const schedulable = new Set(['ready', 'blocked_dependencies', 'blocked_failed',
  'waiting_quota', 'waiting_resource', 'waiting_configuration', 'retry_scheduled']);

/** Snapshot estimates only; admission remains with schedule() and its SQL guards. */
export function buildMuseWorkforceHealth(input: WorkforceHealthInput,
  leases: Record<string, number>, glmAdmissions: number, now: number) {
  const target = Math.max(0, input.policy.museTarget ?? 0);
  const pending = input.tasks.filter(t => !['coordinate','review'].includes(t.stage ?? '') &&
    t.execution?.kind === 'harness' &&
    (t.execution?.workforceLane === 'muse' || /^muse-/i.test(t.model ?? '')) && !closed.has(t.state));
  const occupiedTasks = pending.filter(t => t.has_live_run || live.has(t.state));
  const occupied = occupiedTasks.length;
  const blocked = pending.filter(t => !t.has_live_run && !live.has(t.state) && t.dependency_blocked).length;
  const due = (t: typeof pending[number]) => !t.next_attempt_at || new Date(t.next_attempt_at).getTime() <= now;
  const ready = pending.filter(t => !t.has_live_run && !live.has(t.state) && !t.dependency_blocked && schedulable.has(t.state) && due(t));
  const delayed = pending.filter(t => !t.has_live_run && !live.has(t.state) && !t.dependency_blocked && schedulable.has(t.state) && !due(t)).length;
  const parked = pending.length - occupied - blocked - ready.length - delayed;
  const issues = new Map<string,number>();
  const issue = (code:string,count=1) => issues.set(code,(issues.get(code)??0)+count);
  if(blocked)issue('DEPENDENCIES',blocked);
  if(parked)issue('SUPERVISION_OR_EVIDENCE',parked);
  if(delayed)issue('RETRY_DELAY',delayed);
  const admission = input.admission?.muse ?? {allowed:true,service:'agentos-workers'};
  if(!admission.allowed)issue('ROLLOUT_DRAIN');
  const authorized=(input.museAccounts??[]).filter(({account}) =>
    input.policy.workerAccounts?.includes(account.id) && account.provider==='muse');
  const usable=authorized.filter(({account}) => {
    if(!account.enabled || !account.bridge_id){issue('ACCOUNT_CONFIGURATION');return false;}
    const q=quotaFor(account,account.model),stale=account.observed_at && now-new Date(account.observed_at).getTime()>600000;
    if(q.status==='auth_required'){issue('AUTH_REQUIRED');return false;}
    if(q.status==='exhausted'){issue('QUOTA_EXHAUSTED');return false;}
    if((q.status==='unknown'||stale)&&!account.allow_unknown){issue('QUOTA_UNKNOWN');return false;}
    return true;
  });
  const configured=input.remoteExecution&&input.bridgeConfigured;
  if(!configured)issue('EXECUTOR_CONFIGURATION');
  const headroom={workforce:target-occupied,
    account:usable.reduce((n,{account,occupied})=>n+Math.max(0,account.max_concurrency-occupied),0),
    global:input.limits.maxRuns-input.globalOccupied-glmAdmissions,
    objective:(input.limits.objectiveSlots??Number.MAX_SAFE_INTEGER)-input.goalOccupied-glmAdmissions,
    remote:leases['host:agentos-remote']?0:input.limits.remoteSlots-(leases['remote-slot']??0)};
  for(const [key,value] of Object.entries(headroom))if(value<=0&&target>0)issue(
    key==='remote'&&leases['host:agentos-remote']?'HOST_EXCLUSIVE':key.toUpperCase()+'_CAPACITY');
  const enabled=input.policy.enabled&&input.goalState==='active'&&target>0;
  const freeSlots=enabled&&configured&&admission.allowed?Math.max(0,Math.min(...Object.values(headroom))):0;
  const simulated={...leases};
  const accountSlots=new Map(usable.map(({account,occupied})=>[account.id,Math.max(0,account.max_concurrency-occupied)]));
  let available=0;
  for(const t of ready){
    const scoped=usable.filter(({account})=>(!t.allowed_accounts?.length||t.allowed_accounts.includes(account.id))&&
      (!t.account_id||t.account_id===account.id));
    const route=scoped.find(({account})=>(account.models??[]).includes(t.model??account.model)&&
      (accountSlots.get(account.id)??0)>0);
    if(!route){issue(scoped.length?'MODEL_OR_ACCOUNT_CAPACITY':'ACCOUNT_SCOPE');continue;}
    if(available>=freeSlots)continue;
    const resources=t.resources??[];
    if(resources.some(resource=>{
      if(resource==='remote-slot'&&simulated['host:agentos-remote'])return true;
      if(resource==='host:agentos-remote'&&simulated['remote-slot'])return true;
      const cap=resource==='browser'?input.limits.browserSlots:resource==='remote-slot'?input.limits.remoteSlots:1;
      return(simulated[resource]??0)>=cap;
    })){issue('RESOURCE_LEASE');continue;}
    available++;
    accountSlots.set(route.account.id,accountSlots.get(route.account.id)!-1);
    for(const resource of resources)simulated[resource]=(simulated[resource]??0)+1;
  }
  const reservePending=pending.filter(t=>isMuseBacklogExecution(t.execution)&&reservesWorkforceBacklog(t)).length;
  const reserveReady=ready.filter(t=>isMuseBacklogExecution(t.execution)).length;
  const reserveDeficit=Math.max(0,target-reserveReady);
  const incomplete=input.tasks.some(t=>!['coordinate','review'].includes(t.stage??'')&&!closed.has(t.state));
  const needsPlanning=enabled&&incomplete&&reserveDeficit>0;
  const refillPossible=enabled&&available>0;
  let status='backlog_missing',reason='Sem tarefa Muse pronta; o supervisor precisa delimitar trabalho útil independente.';
  if(!enabled){status='inactive';reason='Meta Muse ou objetivo não está ativo.';}
  else if(!admission.allowed){status='rollout';reason='Muse aguarda atualização confirmada do executor; tarefas prontas permanecem na fila.';}
  else if(!configured||!usable.length){status='configuration';reason='Conta ou executor Muse indisponível; confira os gargalos registrados.';}
  else if(refillPossible){status='refilling';reason=`${available} tarefa(s) Muse podem ocupar vagas pelo scheduler.`;}
  else if(ready.length){status='capacity';reason='Tarefas Muse prontas aguardam vagas da conta ou recursos.';}
  else if(occupied>=target){status='full';reason='Meta de sessões Muse ocupada; prepare reserva útil para as próximas vagas.';}
  else if(blocked||parked||delayed){status='blocked';reason=`Sem tarefa Muse pronta: ${blocked} aguardam dependências, ${parked} supervisão/insumos e ${delayed} o prazo de retomada.`;}
  return {target,running:occupiedTasks.filter(t=>(t.live_state??t.state)==='running').length,
    queued:occupiedTasks.filter(t=>(t.live_state??t.state)==='queued').length,
    confirming:occupiedTasks.filter(t=>['cancelling','reconciling'].includes(t.live_state??t.state)).length,
    occupied,ready:ready.length,available,blocked,parked,delayed,deficit:Math.max(0,target-occupied),
    pending:reservePending,totalPending:pending.length,pendingLimit:2*target,reserveTarget:target,reserveReady,reserveDeficit,freeSlots,
    status,reason,admission,needsPlanning,refillPossible,bottlenecks:[...issues].map(([code,count])=>({code,count}))};
}
