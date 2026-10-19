import type {LiveFailureFocus} from './manager-live-failure.js';
/** Wake debounce and admitted review focus are independent. The reader's
 * pending list already excludes owned guidance, explicit no-action and stale
 * actors. Keep the current cause across context recovery without new wakes. */
export function selectPendingLiveFailureAffinity(pending:LiveFailureFocus[],previous:any,preferred?:LiveFailureFocus|null){
 const current=previous?.kind==='live_failure'?pending.find(f=>
  ['failureKey','eventId','goalId','taskId','runId','runFence','contextVersion','expectedAttempt'].every(k=>(f as any)[k]===previous[k])):undefined;
 return (preferred?pending.find(f=>f.failureKey===preferred.failureKey):undefined)??current??pending[0]??null;
}
