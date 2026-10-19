/** These failures have no completed QA outcome. Each fresh recheck still
 * requires its own accepted current revision and physical stop proof. */
export const verificationInterruptionCodes=['MODEL_OUTPUT_LIMIT','MODEL_CONTEXT_LIMIT','CONTEXT_TOO_LARGE','INCOMPLETE_TURN'] as const;
export const interruptedVerificationReason='APPLIED_INTERRUPTED_REVISION_RECONCILED';
export const interruptedVerification=(run:any)=>run?.state==='failed'&&
 (verificationInterruptionCodes as readonly string[]).includes(run.error_code);
