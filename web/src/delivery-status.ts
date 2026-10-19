type Row=Record<string,any>;
const pendingOrder:Record<string,number>={open:0,decided:1,delegated:2,waiting_evidence:3};

export function deliveryStatus(game:Row,detail:Row,data:Row){
  const ids=new Set(game.tasks.map((t:Row)=>t.id));
  const snapshot=detail.deliveryFlow;
  const pending=(snapshot?.cases??[]).filter((c:Row)=>ids.has(c.task_id))
    .sort((a:Row,b:Row)=>(pendingOrder[a.state]??9)-(pendingOrder[b.state]??9)||Date.parse(a.since)-Date.parse(b.since));
  const blocked=(snapshot?.consumptions??[]).filter((c:Row)=>ids.has(c.task_id) && c.state==='blocked');
  const milestone=data.settings?.['operation-progress:'+detail.goal.id]?.milestones?.find((m:Row)=>m.scope===game.id);
  const integration=game.tasks.find((t:Row)=>t.id===milestone?.integration?.taskId);
  const candidate=integration?.attempt_count!==undefined &&
    (data.runs??[]).some((r:Row)=>r.id===milestone?.integration?.runId && r.task_id===integration.id && r.attempt===integration.attempt_count)
    ? milestone?.integration?.candidateDigest:null;
  const lastAccepted=(snapshot?.accepted??[]).find((a:Row)=>ids.has(a.task_id))?.accepted_at??null;
  const focus=game.focus,flow=focus.task?.execution?.continuation;
  let owner='Agendador',next='Iniciar a próxima etapa elegível.',reason='Aguardando etapa anterior ou capacidade.';
  if(game.complete){owner='Concluído';next='Entrega e verificação aceitas.';reason='Critérios finais atendidos.';}
  else if(focus.action){owner='Você';next=focus.detail;reason='Existe uma solicitação explícita para você.';}
  else if(pending.length){
    owner='Supervisão';const state=pending[0].state;
    reason=state==='open'?'Decisão técnica pendente.':state==='decided'?'Decisão registrada; efeito ainda pendente.':state==='delegated'?'Correção delegada em acompanhamento.':'Evidência necessária para decidir.';
    next=state==='open'?'Encaminhar a causa para correção.':state==='decided'?'Aplicar a decisão e conferir a retomada.':state==='delegated'?'Receber o resultado e retomar a entrega.':'Recuperar a evidência e retomar a decisão.';
  }else if(blocked.length){
    const code=blocked[0].code??'';
    if(code==='SOURCE_REVIEW_WAIT_CONSUMER'){
      owner='Agente executor';reason='A execução atual ainda está em andamento.';next='Aguardar seu resultado para conferir a fonte pendente.';
    }else if(/^(PRODUCER_|SOURCE_)/.test(code)){
      owner='Supervisão';reason='Reparo aguardando conferência da origem.';next='Comprovar a fonte e encaminhar seu consumo.';
    }else{
      owner='Supervisão';reason='Impedimento técnico no consumo da entrega.';next='Conferir a evidência da composição ou QA e encaminhar a correção.';
    }
  }
  else if(focus.kind==='running'){owner='Agente executor';reason=focus.label;next='Publicar o resultado para validação.';}
  else if(flow?.owner==='supervisor' || focus.kind==='stalled'){owner='Supervisão';reason=focus.label;next='Resolver o impedimento e confirmar a próxima execução.';}
  else if(focus.task?.state==='submitted'){owner='Validação';reason='Entrega ainda sem aceite.';next='Conferir os critérios e encaminhar eventual correção.';}
  else if(focus.task?.state==='waiting_quota'){reason='Aguardando cota.';next='Admitir uma rota autorizada quando disponível.';}
  const taskId=pending[0]?.task_id??blocked[0]?.task_id??focus.task?.id;
  return {owner,next,reason,taskId,lastAccepted,candidate,pendingCases:pending.reduce((n:number,c:Row)=>n+c.total,0),
    observedAt:snapshot?.observedAt??null};
}
