import { it, expect } from 'vitest';
import { selectMemoryContext } from '../server/memory-selection.js';
import { managerTextHash } from '../server/manager-context.js';

const memory = (id: string, characters: number, required = false, score = 1) => ({
  id,
  content: id.slice(0, 1).repeat(characters),
  required,
  score,
});
it('deduplicates an optional operator report already delivered in full while keeping every mandatory decision', () => {
  const content = 'Keep rules and RTP fixed; no deployment.\n' + 'Required factual evidence. '.repeat(100);
  const required = { id: 'decision', content, required: true, score: 1 };
  const optional = { id: 'operator-report', content, required: false, score: 1 };
  const distinct = { id: 'independent', content: content + 'Contrary evidence must remain.', required: false, score: 0.9 };
  const selection = selectMemoryContext([required, optional, distinct], [optional, distinct],
    'coordinate', [managerTextHash(content)]);
  expect(selection.selected).toEqual([required, distinct]);
  expect(selection.selected[0].content).toBe(content);
  expect(selection.omitted).toEqual([{ id: optional.id, characters: content.length,
    reason: 'already_in_manager_snapshot', hash: managerTextHash(content) }]);
  expect(selectMemoryContext([optional], [optional], 'work', [managerTextHash(content)]).selected).toEqual([optional]);
});
it('never lets an oversized optional memory block valid required facts and smaller relevant context', () => {
  const required = memory('required', 1000, true);
  const huge = memory('huge', 61000, false, 1);
  const small = memory('small', 4000, false, 0.9);
  const selection = selectMemoryContext([required, huge, small], [huge, small], 'work');
  expect(selection.selected).toEqual([required, small]);
  expect(selection.omitted).toEqual([
    { id: 'huge', characters: 61000, reason: 'optional_context_budget' },
  ]);
  expect(required.content).toHaveLength(1000);
});
it('reserves bounded optional room for recurring managers while retaining mandatory history', () => {
  const required = memory('required', 25000, true);
  const optional = memory('optional', 1000);
  const expanded = selectMemoryContext([required], [optional], 'coordinate');
  expect(expanded.selected).toEqual([required, optional]);
  expect(expanded).toMatchObject({retrievalBudget:33000,nominalRetrievalBudget:18000,
    requiredCharacters:25000,optionalBudgetCharacters:8000});
  const short = memory('current', 1000, true);
  const history = [memory('relevant', 15000), memory('older', 6000, false, 0.8)];
  const selection = selectMemoryContext([short], history, 'coordinate');
  expect(selection.selected).toEqual([short, history[0]]);
  expect(selection.retrievalBudget).toBe(18000);
});
it('still rejects required overflow, keeps rank ordering and prevents duplicate memory injections', () => {
  expect(() => selectMemoryContext([memory('required', 60001, true)], [], 'coordinate')).toThrow(
    'obrigatório',
  );
  const candidates = Array.from({ length: 10 }, (_, i) => memory('item-' + i, 100, false, i / 10));
  const original = structuredClone(candidates);
  const selection = selectMemoryContext([], [...candidates, candidates[9]], 'work');
  expect(selection.selected).toHaveLength(8);
  expect(selection.selected[0].id).toBe('item-9');
  expect(new Set(selection.selected.map((m) => m.id)).size).toBe(8);
  expect(candidates).toEqual(original);
});

it('retains all nine observed mandatory memories and can retrieve the two audit-sized facts', () => {
  const required = Array.from({length:9},(_,i)=>memory('required-'+i,i===8?5347:2000,true));
  const tooLarge = memory('too-large',8001,false,1);
  const audits = [
    {...memory('audit',3041,false,0.9),title:'Current game blockers',version:1,source:'verified-audit'},
    {...memory('supplement',2438,false,0.8),title:'Integration observations',version:1,source:'verified-supplement'},
  ];
  const before = structuredClone([...required,tooLarge,...audits]);
  const selection = selectMemoryContext([...required,tooLarge,...audits],[tooLarge,...audits],'coordinate');
  expect(selection).toMatchObject({requiredCharacters:21347,retrievalBudget:29347,
    nominalRetrievalBudget:18000,optionalBudgetCharacters:8000});
  expect(selection.selected).toEqual([...required,...audits]);
  expect(selection.omitted).toEqual([{id:'too-large',characters:8001,reason:'optional_context_budget'}]);
  expect([...required,tooLarge,...audits]).toEqual(before);
  expect(selection.selected.reduce((n,m)=>n+m.content.length,0)).toBe(26826);
});

it('caps managers with 58k mandatory characters at 60k and skips larger optional facts', () => {
  const required = memory('required',58000,true);
  const large = memory('large',2001,false,1),exact = memory('exact',2000,false,0.9);
  const selection = selectMemoryContext([required,large,exact],[large,exact],'coordinate');
  expect(selection).toMatchObject({requiredCharacters:58000,retrievalBudget:60000,
    optionalBudgetCharacters:2000});
  expect(selection.selected).toEqual([required,exact]);
  expect(selection.omitted).toEqual([{id:'large',characters:2001,reason:'optional_context_budget'}]);
});

it('keeps the mandatory 60k ceiling and gives no optional allowance beyond it', () => {
  const required = memory('required',60000,true),optional = memory('optional',1);
  const selection = selectMemoryContext([required,optional],[optional],'coordinate');
  expect(selection.selected).toEqual([required]);
  expect(selection).toMatchObject({retrievalBudget:60000,optionalBudgetCharacters:0});
  expect(() => selectMemoryContext([memory('overflow',60001,true)],[],'work')).toThrow();
});

it('preserves rank thresholds, inline deduplication and the eight optional memory limit', () => {
  const required = memory('required',21347,true),zero = memory('zero',50,false,0);
  const inline = memory('inline',100,false,1);
  const optional = Array.from({length:9},(_,i)=>memory('fact-'+i,100,false,0.99-i/100));
  const selection = selectMemoryContext([required,zero,inline,...optional],
    [zero,inline,...optional,optional[0]],'coordinate',[managerTextHash(inline.content)]);
  expect(selection.selected).toEqual([required,...optional.slice(0,8)]);
  expect(selection.selected).not.toContain(zero);
  expect(selection.omitted).toEqual([
    {id:inline.id,characters:100,reason:'already_in_manager_snapshot',hash:managerTextHash(inline.content)},
    {id:optional[8].id,characters:100,reason:'optional_count'},
  ]);
});

it('keeps worker retrieval at 60k and the ordinary small manager window at 18k', () => {
  const required = memory('required',21347,true),large = memory('large',30000);
  const worker = selectMemoryContext([required,large],[large],'work');
  expect(worker).toMatchObject({retrievalBudget:60000,nominalRetrievalBudget:60000,
    optionalBudgetCharacters:38653});
  expect(worker.selected).toEqual([required,large]);
  expect(selectMemoryContext([],[],'coordinate')).toMatchObject({retrievalBudget:18000,
    nominalRetrievalBudget:18000,optionalBudgetCharacters:18000});
});
