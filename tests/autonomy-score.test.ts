import { expect, it } from 'vitest';
import { scoreQualification, type Qualification, type TrialResult } from '../server/autonomy-score.js';
const spec:Qualification={id:'fixed-v1',startedAt:'2026-10-19T12:00:00Z',trials:Array.from({length:60},(_,i)=>'trial-'+i),minimumPasses:54,minimumHours:24,requiredRepairs:['routing','supervision','delivery']};
const results:TrialResult[]=spec.trials.map(id=>({id,passed:true,humanIntervention:false,evidenceSha256:'a'.repeat(64),checkedBy:'deterministic',finishedAt:'2026-10-20T12:00:00Z'}));
const observed={now:Date.parse('2026-10-20T12:01:00Z'),uninterruptedSeconds:86460,criticalViolations:0,coverageComplete:true};
it('keeps all 60 trials in the denominator; pending or repeated reports never earn extra credit',()=>{
  const s=scoreQualification(spec,[...results.slice(0,5),results[0]],[],observed);
  expect(s).toMatchObject({total:60,evaluated:5,pending:55,autonomousPassed:5,qualified:false});
});
it('requires completed cohort, fixed repairs, full observation and all safety invariants',()=>{
  expect(scoreQualification(spec,results,spec.requiredRepairs,observed).qualified).toBe(true);
  for(const change of [{uninterruptedSeconds:100},{criticalViolations:1},{coverageComplete:false}])
    expect(scoreQualification(spec,results,spec.requiredRepairs,{...observed,...change}).qualified).toBe(false);
  expect(scoreQualification(spec,results.slice(0,59),spec.requiredRepairs,observed).qualified).toBe(false);
  expect(scoreQualification(spec,results,[],observed).qualified).toBe(false);
});
it('does not count assisted, forged, contradictory or out-of-cohort evidence as autonomous',()=>{
  const altered=results.map((r,i)=>({...r,humanIntervention:i<7}));
  expect(scoreQualification(spec,altered,spec.requiredRepairs,observed)).toMatchObject({autonomousPassed:53,qualified:false});
  expect(scoreQualification(spec,[...results,{...results[0],passed:false}],spec.requiredRepairs,observed)).toMatchObject({ambiguous:1,qualified:false});
  expect(scoreQualification(spec,[...results,{...results[0],id:'bonus'}],spec.requiredRepairs,observed)).toMatchObject({rejected:1,qualified:false});
});
