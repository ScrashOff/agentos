import {it,expect} from 'vitest';
import {deliveryStatus} from '../web/src/delivery-status';

function fixture(){
  const task={id:'integration',attempt_count:2,state:'running',execution:{}};
  const game={id:'project/scope',complete:false,tasks:[task],focus:{task,kind:'running',label:'Executando',action:false}};
  const detail:any={goal:{id:'goal'},deliveryFlow:{observedAt:'2026-10-10T18:00:00Z',cases:[],consumptions:[],accepted:[]}};
  const data:any={runs:[{id:'run2',task_id:'integration',attempt:2}],settings:{}};
  return {task,game,detail,data};
}
it('shows a decided case as supervision with pending effect, not a user approval or completed delivery',()=>{
  const f=fixture();f.detail.deliveryFlow.cases=[{task_id:f.task.id,state:'decided',total:1,since:'2026-10-10T17:00:00Z'}];
  const result=deliveryStatus(f.game,f.detail,f.data);
  expect(result.owner).toBe('Supervisão');expect(result.reason).toContain('efeito ainda pendente');
  expect(result.next).toContain('conferir a retomada');expect(result.lastAccepted).toBeNull();
});
it('keeps a blocked accepted repair distinct from accepted integration',()=>{
  const f=fixture();f.detail.deliveryFlow.consumptions=[{task_id:f.task.id,state:'blocked',code:'PRODUCER_PROOF_CHANGED'}];
  const result=deliveryStatus(f.game,f.detail,f.data);
  expect(result.owner).toBe('Supervisão');expect(result.next).toContain('Comprovar a fonte');expect(result.candidate).toBeNull();
});
it('never displays an old candidate as the candidate of the new attempt',()=>{
  const f=fixture();f.data.settings['operation-progress:goal']={milestones:[{scope:f.game.id,integration:{taskId:f.task.id,runId:'old-run',candidateDigest:'a'.repeat(64)}}]};
  expect(deliveryStatus(f.game,f.detail,f.data).candidate).toBeNull();
  f.data.settings['operation-progress:goal'].milestones[0].integration.runId='run2';
  expect(deliveryStatus(f.game,f.detail,f.data).candidate).toBe('a'.repeat(64));
});
it('distinguishes a composition failure from a source review waiting for a live actor',()=>{
  const f=fixture();f.detail.deliveryFlow.consumptions=[{task_id:f.task.id,state:'blocked',code:'COMPOSITION_REPAIR_NOT_CONSUMED'}];
  expect(deliveryStatus(f.game,f.detail,f.data)).toMatchObject({owner:'Supervisão',reason:'Impedimento técnico no consumo da entrega.'});
  f.detail.deliveryFlow.consumptions[0].code='SOURCE_REVIEW_WAIT_CONSUMER';
  expect(deliveryStatus(f.game,f.detail,f.data).owner).toBe('Agente executor');
});
it('prioritizes an explicit human request over automatic ownership and excludes another scope',()=>{
  const f=fixture();f.detail.deliveryFlow.cases=[{task_id:'other',state:'open',total:2}];
  f.game.focus.action=true;
  expect(deliveryStatus(f.game,f.detail,f.data)).toMatchObject({owner:'Você',pendingCases:0});
});
