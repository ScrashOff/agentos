import test from 'node:test';
import assert from 'node:assert/strict';
import { museAccountPaths, museQuota, parseMuseLoginNotice } from '../bridge/muse-accounts.mjs';
test('Muse auth metadata can only reference the matching isolated account profile', () => {
  const id = '12345678-1234-1234-1234-123456789012',
    home = '/var/lib/agentos-bridge/muse/' + id;
  assert.equal(
    museAccountPaths({ id: 'agentos:muse:' + id, home, authPath: home + '/.config/muse/auth.json' })
      .home,
    home,
  );
  assert.throws(
    () =>
      museAccountPaths({
        id: 'agentos:muse:' + id,
        home: '/var/lib/deepseek-harness',
        authPath: '/etc/passwd',
      }),
    /MUSE_ACCOUNT_INVALID/,
  );
});
test('Muse login presents only the official device page and code, never arbitrary output', () => {
  const notice = parseMuseLoginNotice(
    'Open https://auth.meta.com/device?user_code=ABCD-1234\ncredential=do-not-expose',
  );
  assert.equal(notice.userCode, 'ABCD-1234');
  assert.equal(notice.url, 'https://auth.meta.com/device?user_code=ABCD-1234');
  assert.ok(!JSON.stringify(notice).includes('do-not-expose'));
  assert.equal(parseMuseLoginNotice('https://auth.meta.com.evil.example/device'), null);
  assert.equal(parseMuseLoginNotice('https://user:secret@auth.meta.com/device'), null);
});
test('Muse quota uses native observed percentages and clamps over-limit remaining to zero', () => {
  const quota = museQuota({
    observedAtMs: 1000,
    tier: 'high',
    window: { usedPercent: 20, resetsAtMs: 2000 },
    weekly: { usedPercent: 105, resetsAtMs: 3000 },
  });
  assert.equal(quota.status, 'ok');
  assert.deepEqual(
    quota.groups[0].windows.map((w) => w.remainingPercent),
    [80, 0],
  );
  assert.equal(quota.groups[0].windows[0].resetsAt, new Date(2000).toISOString());
  assert.deepEqual(museQuota(undefined), { status: 'unknown', groups: [] });
  assert.equal(museQuota({ observedAtMs: 1000, window: { utilization: 10 } }).status, 'unknown');
});
