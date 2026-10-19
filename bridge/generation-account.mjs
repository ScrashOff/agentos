/** Resolve only the requested account. Generation must not refresh unrelated
 * providers, native Muse sessions, or the production Harness's whole catalog. */
export async function resolveGenerationAccount(accountId, { own, inheritedState, catalog, familyOf }) {
  const saved = own().find(account => account.id === accountId);
  if (saved) return {
    id: saved.id, provider: saved.family, family: saved.family, source: 'agentos',
    models: catalog(saved.family, null, saved),
    authStatus: saved.credential || saved.family === 'agy' ? 'connected' : 'disconnected',
    quota: { status: 'unknown', groups: [] },
  };
  if (!accountId?.startsWith('harness:')) throw Error('ACCOUNT_MISSING');
  const data = await inheritedState();
  const accounts = [...data.accounts, ...(data.agyAccounts ?? [])];
  if (data.qwen?.configured) accounts.push({ provider: 'qwen-cloud', connected: true });
  const account = accounts.find(row => 'harness:' + row.provider === accountId);
  if (!account) throw Error('ACCOUNT_MISSING');
  const family = familyOf(account.provider), quota = data.quotas?.accounts?.[account.provider] ?? {};
  return {
    id: accountId, provider: account.provider, family, source: 'harness',
    models: catalog(family, account.provider), quota,
    authStatus: quota.reconnectRequired ? 'reconnect_required' : account.connected ? 'connected' : 'disconnected',
  };
}
