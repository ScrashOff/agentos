import {expect,it} from 'vitest';
import {planningRecovery} from '../server/planning-recovery.js';
import {selectManagerCaseWindow} from '../server/manager-focus.js';

it('routes actual model silence to bounded-backoff transport recovery without consuming invalid-plan attempts',()=>{
 const result=planningRecovery('MODEL_IDLE_TIMEOUT',false,false,{invalid_plan:1,transient:2});
 expect(result).toMatchObject({category:'transient',count:3,retry:true,delaySeconds:40,counts:{invalid_plan:1,transient:3}});
 expect(planningRecovery('MODEL_IDLE_TIMEOUT',false,true).category).toBe('interruption');
 expect(planningRecovery('STOP_UNCONFIRMED',false,false).retry).toBe(false);
});

it('includes current stopped idle failures in the supervisor window but grants no retry or live interruption authority',()=>{
 const focus={id:'delivery',current_run_state:'submitted'};
 const idle={id:'idle',state:'open',source_run_id:'run',current_run_id:'run',current_run_state:'failed',
   current_error_code:'MODEL_IDLE_TIMEOUT',has_live_run:false};
 const cases=[focus,idle],before=structuredClone(cases);
 const selected=selectManagerCaseWindow(cases,{caseId:'delivery'});
 expect(selected.map(c=>c.id)).toEqual(['delivery','idle']);
 expect(selected[1]).toMatchObject({operationalRecovery:true});
 expect(selected[1]).not.toHaveProperty('retryAuthorized');
 expect(cases).toEqual(before);
 for(const guard of [{has_live_run:true},{current_run_state:'running'},{source_run_id:'obsolete'},{state:'decided'}])
   expect(selectManagerCaseWindow([focus,{...idle,...guard}],{caseId:'delivery'}).map(c=>c.id)).toEqual(['delivery']);
});
