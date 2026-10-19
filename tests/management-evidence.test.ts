import { it, expect } from 'vitest';
import { managementReportExcerpt } from '../server/management-evidence.js';

it('preserves actionable failed evidence while excluding nested credentials and provider headers', () => {
  const excerpt = managementReportExcerpt({
    environment: {
      headers: { authorization: 'private fixture' },
      credential: 'private fixture',
      node: '24',
    },
    passed: false,
    rootCause: 'Genuine native bundle absent',
    cases: [{ status: 'failed', actual: 'No native canvas', cookie: 'private fixture' }],
  });
  const value = JSON.parse(excerpt.text);
  expect(value.passed).toBe(false);
  expect(value.rootCause).toBe('Genuine native bundle absent');
  expect(value.cases[0].actual).toBe('No native canvas');
  expect(excerpt.text).not.toContain('private fixture');
  expect(value.environment.node).toBe('24');
});
it('bounds excerpts, promotes causal facts and explicitly marks truncated reports', () => {
  const excerpt = managementReportExcerpt(
    {
      ...Object.fromEntries(
        Array.from({ length: 40 }, (_, i) => ['metadata-' + i, 'E'.repeat(3000)]),
      ),
      environment: 'E'.repeat(3000),
      diagnosis: 'First causal fact',
      cases: Array.from({ length: 100 }, () => ({ result: 'R'.repeat(3000) })),
    },
    200,
  );
  expect(excerpt.text.length).toBeLessThanOrEqual(200);
  expect(() => JSON.parse(excerpt.text)).not.toThrow();
  expect(excerpt.text).toContain('First causal fact');
  expect(excerpt.truncated).toBe(true);
});

it('keeps the real consumption frontier ahead of long disclaimers and passing setup cases', () => {
  const excerpt = managementReportExcerpt(
    {
      limitations: Array.from({ length: 12 }, () =>
        'Diagnostic only, not game acceptance. '.repeat(20),
      ),
      suiteExecuted: true,
      exitCode: 0,
      cases: [
        ...Array.from({ length: 8 }, (_, i) => ({
          name: 'setup-' + i,
          verdict: 'pass',
          details: 'Verified baseline '.repeat(100),
        })),
        {
          name: 'delivery-consumption-frontier',
          verdict: 'pass',
          details:
            'Both HTTP endpoints completed with genuine bytes, but the entry source was never compiled. C-consumption-unresolved; final game acceptance remains red.',
        },
      ],
    },
    1200,
  );
  const report = JSON.parse(excerpt.text);
  expect(report.suiteExecuted).toBe(true);
  expect(report.exitCode).toBe(0);
  expect(report.cases[0].name).toBe('delivery-consumption-frontier');
  expect(report.cases[0].details).toContain('C-consumption-unresolved');
  expect(excerpt.projection.arrayIndices['/cases'][0]).toBe(8);
  expect(excerpt.projection.omittedPathCount).toBeGreaterThan(0);
  expect(excerpt.text.length).toBeLessThanOrEqual(1200);
});

it('retains a singular finding that distinguishes locale consumption from genuinely missing bytes', () => {
  const excerpt = managementReportExcerpt(
    {
      metadata: 'M'.repeat(10000),
      limitations: ['Full gameplay remains unapproved.'],
      cases: Array.from({ length: 12 }, () => ({ status: 'pass', details: 'setup '.repeat(100) })),
      finding: {
        enDefault: 'Four genuine resources remain HTTP404.',
        nativeAliasUnderPt: 'pt-BR never requested the already accepted native-config alias.',
        ptShellAfterObservation: 'Runner exceeded150s; native consumption remains unproved.',
      },
      suiteExecuted: true,
      exitCode: 1,
    },
    1400,
  );
  const report = JSON.parse(excerpt.text);
  expect(report.finding.enDefault).toContain('HTTP404');
  expect(report.finding.nativeAliasUnderPt).toContain('pt-BR never requested');
  expect(report.finding.ptShellAfterObservation).toContain('exceeded150s');
  expect(report.exitCode).toBe(1);
  expect(excerpt.text.length).toBeLessThanOrEqual(1400);
});

it('never produces broken JSON when quoting, arrays or nested failures exhaust the limit', () => {
  for (const limit of [2, 16, 80, 200, 700]) {
    const excerpt = managementReportExcerpt(
      {
        passed: false,
        firstFailure: { evidence: '"quoted"\\\n'.repeat(200) },
        cases: Array.from({ length: 30 }, () => ({ status: 'failed', actual: 'X'.repeat(2000) })),
      },
      limit,
    );
    expect(() => JSON.parse(excerpt.text)).not.toThrow();
    expect(excerpt.text.length).toBeLessThanOrEqual(limit);
    expect(excerpt.truncated).toBe(true);
  }
});

it('bounds strings without leaking a credential assigned within a report value', () => {
  const excerpt = managementReportExcerpt(
    {
      finding: 'auth attempt API_KEY=private_test_value; no native entry consumed',
      passed: false,
      secret: 'private_field_value',
    },
    400,
  );
  expect(excerpt.text).not.toContain('private_test_value');
  expect(excerpt.text).not.toContain('private_field_value');
  expect(JSON.parse(excerpt.text).passed).toBe(false);
});

it('keeps the native first failure visible ahead of a large accepted composition report', () => {
  const excerpt = managementReportExcerpt(
    {
      suiteExecuted: true,
      command:
        'CANDIDATE_ROOT=data/accepted-source node --test qa-protocol/native-frontier.test.mjs',
      exitCode: 1,
      composition: {
        packages: Array.from({ length: 12 }, () => 'capture provenance '.repeat(180)),
      },
      testResults: { pass: 3, fail: 1 },
      frontier: {
        method: 'Natural Chromium boot; no scene or configuration forced.',
        milestonesReached: ['import map consumed', 'genuine vendor executed', 'six setters linked'],
        firstFailure: {
          classification: 'boot continuation does not establish',
          transportFailure: false,
          missingBytes: false,
          evidence: 'No event after six genuine setters; native scene remains null.',
        },
      },
      limitations: ['Diagnostic delivery; final game acceptance remains pending.'],
    },
    1500,
  );
  expect(excerpt.text).toContain('native scene remains null');
  expect(excerpt.text).toContain('"transportFailure":false');
  expect(excerpt.text).toContain('"missingBytes":false');
  expect(excerpt.text).toContain('"exitCode":1');
  expect(excerpt.text.length).toBeLessThanOrEqual(1500);
  expect(excerpt.truncated).toBe(true);
});

it('retains a late first failure inside a noisy frontier without leaking its private fields', () => {
  const excerpt = managementReportExcerpt(
    {
      frontier: {
        ...Object.fromEntries(
          Array.from({ length: 40 }, (_, i) => ['trace-' + i, 'large trace '.repeat(100)]),
        ),
        firstFailure: {
          evidence: 'The genuine app body is registered but never activated.',
          authorization: 'private first-failure fixture',
          headers: { cookie: 'private first-failure fixture' },
        },
      },
    },
    600,
  );
  expect(excerpt.text).toContain('registered but never activated');
  expect(excerpt.text).not.toContain('private first-failure fixture');
  expect(excerpt.text.length).toBeLessThanOrEqual(600);
  expect(excerpt.truncated).toBe(true);
});
