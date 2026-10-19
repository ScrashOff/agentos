const privateField =
  /(?:authorization|credential|password|secret|private.?key|api.?key|access.?token|refresh.?token|cookie|headers|^token$|^key$)/i;
const priorities = [
  'complete',
  'receipts',
  'conflicts',
  'passed',
  'fixVerified',
  'suiteExecuted',
  'exitCode',
  'name',
  'id',
  'title',
  'status',
  'verdict',
  'rootCause',
  'firstFailure',
  'finding',
  'findings',
  'diagnosis',
  'defect',
  'frontier',
  'incompleteCompositionAndMissingInputs',
  'firstNativeTransitionAfterConfig',
  'failures',
  'results',
  'testResults',
  'cases',
  'tests',
  'missingInputs',
  'integratorAction',
  'nextSteps',
  'nativeBoot',
  'nativeState',
  'command',
  'requirements',
  'instruction',
  'currentInstruction',
  'description',
  'actual',
  'evidence',
  'details',
];
export const redactManagementText = (s: string) =>
  s
    .replace(/sk-[\w.-]{24,}/g, '[credential omitted]')
    .replace(/Bearer\s+[\w.-]{30,}/gi, '[credential omitted]')
    .replace(
      /((?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)\s*[:=]\s*)[^\s,;]+/gi,
      '$1[credential omitted]',
    );

const rank = (key: string) => {
  const index = priorities.indexOf(key);
  return index >= 0 ? index : key === 'limitations' ? 100 : 50;
};
const caseRank = (entry: any) => {
  if (!entry || typeof entry !== 'object') return 3;
  if (
    entry.passed === false ||
    /^(?:fail|failed|failure|blocked|conflict)$/i.test(
      String(entry.status ?? entry.result ?? entry.verdict ?? ''),
    )
  )
    return 0;
  if (
    /frontier|residual|unfulfilled|unresolved|consumption|first.failure|native.transition/i.test(
      String(entry.name ?? entry.id ?? entry.title ?? ''),
    )
  )
    return 1;
  return 2;
};

/** Structured reports are planning data, never permissions or a statement of acceptance. */
export function managementReportExcerpt(value: unknown, maxCharacters = 3500) {
  const limit = Math.max(2, Math.floor(Number.isFinite(maxCharacters) ? maxCharacters : 3500));
  const omitted = new Set<string>();
  const arrayIndices: Record<string, number[]> = {};
  const pointer = (path: string, key: string | number) =>
    path + '/' + String(key).replace(/~/g, '~0').replace(/\//g, '~1');
  const size = (v: any) => (JSON.stringify(v) ?? 'null').length;
  function project(v: any, budget: number, path: string, depth = 0): any {
    if (depth > 6) {
      omitted.add(path);
      return null;
    }
    if (typeof v === 'string') {
      const clean = redactManagementText(v);
      if (clean !== v) omitted.add(path);
      if (size(clean) <= budget && clean.length <= 1800) return clean;
      omitted.add(path);
      const suffix = '… [omitted]';
      let low = 0,
        high = Math.min(clean.length, 1800);
      while (low < high) {
        const mid = Math.ceil((low + high) / 2);
        if (size(clean.slice(0, mid) + suffix) <= budget) low = mid;
        else high = mid - 1;
      }
      return size(suffix) <= budget ? clean.slice(0, low) + suffix : '';
    }
    if (Array.isArray(v)) {
      const selected = v.map((entry, index) => ({ entry, index }));
      if (/\/(?:cases|tests|results|failures|findings|receipts)$/.test(path))
        selected.sort((a, b) => caseRank(a.entry) - caseRank(b.entry) || a.index - b.index);
      const result: any[] = [],
        indices: number[] = [];
      for (const { entry, index } of selected) {
        const available = budget - size(result) - (result.length ? 1 : 0);
        if (result.length >= 12 || available < 16) {
          omitted.add(pointer(path, index));
          continue;
        }
        const item = project(entry, available, pointer(path, index), depth + 1);
        if (size([...result, item]) <= budget) {
          result.push(item);
          indices.push(index);
        } else omitted.add(pointer(path, index));
      }
      if (indices.some((index, position) => index !== position)) arrayIndices[path] = indices;
      return result;
    }
    if (v && typeof v === 'object') {
      const result: Record<string, unknown> = {};
      for (const key of Object.keys(v)) if (privateField.test(key)) omitted.add(pointer(path, key));
      const entries = Object.entries(v)
        .filter(([key]) => !privateField.test(key))
        .sort(([a], [b]) => rank(a) - rank(b));
      for (const [key, entry] of entries) {
        const at = pointer(path, key);
        const available = budget - size(result) - size(key) - 2;
        if (Object.keys(result).length >= 30 || available < 4) {
          omitted.add(at);
          continue;
        }
        // Keep an early trace/metadata field from consuming all later semantic facts.
        const childBudget =
          rank(key) >= 50
            ? Math.min(available, Math.max(40, Math.floor(budget * 0.35)))
            : available;
        const projected = project(entry, childBudget, at, depth + 1);
        if (size({ ...result, [key]: projected }) <= budget) result[key] = projected;
        else omitted.add(at);
      }
      return result;
    }
    if (size(v) <= budget) return v;
    omitted.add(path);
    return null;
  }
  const report = project(value, limit, '');
  const serialized = JSON.stringify(report) ?? 'null';
  // JSON is always syntactically complete. Missing fields never imply a passing check.
  return {
    text: serialized.length <= limit ? serialized : '{}',
    truncated: omitted.size > 0 || serialized.length > limit,
    projection: {
      version: 2,
      format: 'bounded-valid-json',
      omittedPaths: [...omitted].slice(0, 8),
      omittedPathCount: omitted.size,
      arrayIndices: Object.fromEntries(Object.entries(arrayIndices).slice(0, 4)),
      complete: omitted.size === 0 && serialized.length <= limit,
    },
  };
}
