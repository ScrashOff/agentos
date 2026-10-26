import { pool } from './db.js';
import { buildProviderHealth, type ProviderHealth, type ProviderSample } from './provider-health.js';

// Short enough that a provider which just started failing shows it, long enough
// that one slow reply does not read as an outage.
const WINDOW_MINUTES = 30;

/** Health per account over the recent window, derived from recorded model events. */
export async function providerHealth(accountIds: string[]): Promise<Record<string, ProviderHealth>> {
  if (!accountIds.length) return {};
  const { rows } = await pool.query(
    `SELECT payload->>'accountId' account_id, kind, payload->>'code' code,
            (payload->>'providerMs')::bigint provider_ms,
            (payload->>'queuedMs')::bigint queued_ms,
            floor(extract(epoch from created_at)*1000)::bigint at
       FROM events
      WHERE kind IN('MODEL_RESPONSE','MODEL_ERROR')
        AND created_at > now() - ($2 || ' minutes')::interval
        AND payload->>'accountId' = ANY($1::text[])
      ORDER BY created_at`,
    [accountIds, String(WINDOW_MINUTES)],
  );
  const samples: ProviderSample[] = rows.map((r: any) => ({
    accountId: r.account_id,
    at: Number(r.at),
    ok: r.kind === 'MODEL_RESPONSE',
    code: r.code,
    providerMs: r.provider_ms === null ? null : Number(r.provider_ms),
    queuedMs: r.queued_ms === null ? null : Number(r.queued_ms),
  }));
  return Object.fromEntries(accountIds.map((id) => [id, buildProviderHealth(samples, id)]));
}

export const providerHealthWindowMinutes = WINDOW_MINUTES;
