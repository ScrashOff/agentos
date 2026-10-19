import { readModelPolicy } from '../bridge/model-policy.mjs';
import { config } from './config.js';
import { accountBridge } from './bridge.js';

export async function rankContext(
  goal: string,
  blocks: Array<{ id: string; text: string }>,
  constraints: string[] = [],
) {
  if (readModelPolicy()) throw Error('MODEL_POLICY_GLM_ONLY');
  if (!config.layaUrl && !config.bridgeHost) throw Error('Laya não configurado');
  const scores: Record<string, number> = {};
  for (let i = 0; i < blocks.length; i += 16) {
    const batch = blocks.slice(i, i + 16).map((b) => ({ id: b.id, text: b.text.slice(0, 1800) }));
    // The deployed ranker's instruction head is 192 tokens. This is a retrieval
    // query, not the worker contract: complete requirements remain in its context.
    const input = {
      goal: goal.slice(0, 320),
      constraints: constraints.length ? [constraints.join(' ').slice(0, 100)] : [],
      blocks: batch,
    };
    let result: any;
    if (config.layaUrl) {
      const r = await fetch(new URL('/context-rank', config.layaUrl), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        redirect: 'error',
        signal: AbortSignal.timeout(8000),
        body: JSON.stringify(input),
      });
      if (!r.ok) throw Error('Laya indisponível');
      result = await r.json();
    } else result = await accountBridge.request('context.rank', input, { timeout: 10000 });
    if (
      result.truncated !== false ||
      batch.some(
        (b) =>
          !Number.isFinite(result.scores?.[b.id]) ||
          result.scores[b.id] < 0 ||
          result.scores[b.id] > 1,
      )
    )
      throw Error('Resposta incompleta do Laya');
    for (const b of batch) scores[b.id] = result.scores[b.id];
  }
  return scores;
}
