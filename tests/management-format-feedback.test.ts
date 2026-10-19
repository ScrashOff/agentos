import {expect,it} from 'vitest';
import {z} from 'zod';
import {managementFormatFeedback} from '../server/management-format-feedback.js';

it('reports the exact strategy field and static expected pattern without copying model values',()=>{
 const parsed=z.object({nextTransition:z.object({strategyKey:z.string().regex(/^[a-z][a-z_-]{9,99}$/)})})
  .safeParse({nextTransition:{strategyKey:'residual-repair-r167'}});
 expect(parsed.success).toBe(false);if(parsed.success)throw Error('EXPECTED_INVALID');
 const feedback=managementFormatFeedback(parsed.error.issues);
 expect(feedback).toEqual({issues:[{code:'invalid_format',path:['nextTransition','strategyKey'],
  expectedPattern:'^[a-z][a-z_-]{9,99}$'}],issueCount:1,issuesTruncated:false});
 expect(JSON.stringify(feedback)).not.toContain('residual-repair-r167');
});

it('bounds issue count and paths and never includes validator messages or payload values',()=>{
 const issues=Array.from({length:30},()=>({code:'invalid_type',path:['contextRequests',2,'unexpected/secret-value'],
  message:'PRIVATE MODEL TEXT',received:'PRIVATE PAYLOAD',expected:'PRIVATE EXPECTED',keys:['PRIVATE KEY']}));
 const feedback=managementFormatFeedback(issues);
 expect(feedback).toMatchObject({issueCount:30,issuesTruncated:true});expect(feedback.issues).toHaveLength(12);
 expect(feedback.issues[0]).toEqual({code:'invalid_type',path:['contextRequests',2,'[field]']});
 expect(JSON.stringify(feedback)).not.toMatch(/PRIVATE|secret-value/);
});
