import {it,expect} from 'vitest';
import {managerCasePriority,managerMaterialConsumptionReview,selectManagerFocus} from '../server/manager-focus.js';
import {managerReviewOwnerReleased} from '../server/manager-review-owner.js';

const goal='11111111-1111-4111-8111-111111111111';
const reviewer='22222222-2222-4222-8222-222222222222';
function material() {
 const row:any={id:'33333333-3333-4333-8333-333333333333',goal_id:goal,
  task_id:'44444444-4444-4444-8444-444444444444',state:'open',work_kind:'repair',
  decision_run_id:reviewer,decision_review_id:reviewer,decision_review_state:'submitted',
  decision_review_goal_id:goal,decision_review_stage:'coordinate',has_live_run:false,
  source_run_id:'55555555-5555-4555-8555-555555555555',current_run_id:'55555555-5555-4555-8555-555555555555',
  current_run_state:'submitted',current_task_state:'recovery_blocked',current_attempt:1,
  pending_dependencies:0,fresh_handoffs:1,created_at:'2026-10-17T05:00:00Z'};
 row.materialConsumptionReview={version:1,authority:'review_only',actionValidationRequired:true,
  goalId:goal,caseId:row.id,consumerTaskId:row.task_id,consumerRunId:row.current_run_id,consumerAttempt:1,
  identityDigest:'a'.repeat(64),obligations:[{obligationId:'66666666-6666-4666-8666-666666666666'}]};
 return row;
}

it.each(['submitted','failed','cancelled'])('allows material review after a canonically bound %s reviewer without changing its history',state=>{
 const row=material();row.decision_review_state=state;
 const before=structuredClone(row);
 expect(managerReviewOwnerReleased(row)).toBe(true);
 expect(managerMaterialConsumptionReview(row)).toBe(row.materialConsumptionReview);
 expect(managerCasePriority(row)).toBe(4);
 expect(selectManagerFocus([row],null)?.caseId).toBe(row.id);
 expect(row).toEqual(before);
});

it.each([
 ['queued reviewer',{decision_review_state:'queued'}],['running reviewer',{decision_review_state:'running'}],
 ['cancelling reviewer',{decision_review_state:'cancelling'}],['uncertain reviewer',{decision_review_state:'reconciling'}],
 ['unknown reviewer state',{decision_review_state:'unknown'}],['missing reviewer PK',{decision_review_id:null}],
 ['foreign reviewer PK',{decision_review_id:'77777777-7777-4777-8777-777777777777'}],
 ['foreign goal',{decision_review_goal_id:'88888888-8888-4888-8888-888888888888'}],
 ['non-coordinator',{decision_review_stage:'work'}],['missing reviewer goal',{decision_review_goal_id:null}],
 ['decided case',{state:'decided'}],['delegated case',{state:'delegated'}],
 ['waiting case',{state:'waiting_evidence'}],['live consumer',{has_live_run:true}],
 ['uncertain consumer',{current_run_state:'reconciling'}],['different current run',{current_run_id:'99999999-9999-4999-8999-999999999999'}],
 ['different current attempt',{current_attempt:2}],['executable consumer',{current_task_state:'ready'}],
] as const)('does not lend material priority with %s',(_name,change)=>{
 const row=Object.assign(material(),change),before=structuredClone(row);
 expect(managerMaterialConsumptionReview(row)).toBeNull();expect(managerCasePriority(row)).toBe(2);
 expect(row).toEqual(before);
});

it('does not elevate a repair by work kind or terminal reviewer without a valid material hint',()=>{
 const row=material();delete row.materialConsumptionReview;
 expect(managerReviewOwnerReleased(row)).toBe(true);expect(managerCasePriority(row)).toBe(2);
 row.materialConsumptionReview={...material().materialConsumptionReview,obligations:[]};
 expect(managerCasePriority(row)).toBe(2);
 row.materialConsumptionReview={...material().materialConsumptionReview,authority:'accepted'};
 expect(managerCasePriority(row)).toBe(2);
});

it('preserves the previously unowned material case path',()=>{
 const row=material();row.decision_run_id=null;row.decision_review_id=null;
 delete row.decision_review_state;delete row.decision_review_goal_id;delete row.decision_review_stage;
 expect(managerMaterialConsumptionReview(row)).toBe(row.materialConsumptionReview);expect(managerCasePriority(row)).toBe(4);
});
