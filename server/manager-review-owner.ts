const liveCaseRuns = new Set(['queued','running','cancelling','reconciling']);

/** An open case may retain a completed coordinator review as immutable history.
 * Releasing its review slot is not stop proof or authority to apply a decision. */
export function managerReviewOwnerReleased(entry:any) {
  const historical=entry.decision_run_id!=null && entry.decision_review_id===entry.decision_run_id &&
    typeof entry.goal_id==='string' && entry.decision_review_goal_id===entry.goal_id &&
    entry.decision_review_stage==='coordinate' &&
    ['submitted','failed','cancelled'].includes(entry.decision_review_state);
  return entry.state==='open' && (entry.decision_run_id==null || historical) && entry.has_live_run===false &&
    !liveCaseRuns.has(entry.current_run_state) &&
    (['failed','submitted'].includes(entry.current_run_state) ||
      (entry.current_run_state==null && entry.current_attempt==null));
}
