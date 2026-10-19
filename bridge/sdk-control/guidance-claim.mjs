/** Called exclusively from the installed SDK's typed agent/inbox/claimed hook. */
export function ownedGuidanceClaim(control,registry,payload){
 const {agent,message,turn}=payload??{};
 if(control.closed||!control.initialized||!control.agent||agent!==control.agent||registry.get(control.runId)!==agent||String(agent.session.id)!==control.runId||agent.status!=='running'||!Number.isSafeInteger(turn)||turn<0)return null;
 for(const {receipt} of control.actions.values())if(receipt.messageId===message?.id)
  return Object.freeze({version:1,source:'sdk-agent-inbox-claimed',sessionId:control.runId,runId:control.runId,runFence:control.runFence,actionId:receipt.actionId,messageId:receipt.messageId,turn});
 return null;
}
