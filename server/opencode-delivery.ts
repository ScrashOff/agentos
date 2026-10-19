import { hash } from './memory.js';
import { verifyProtocolReport } from '../bridge/protocol-report-contract.mjs';

type Artifact = { name: string; content: Buffer; mime: string };
const invalid = () =>
  Object.assign(Error('OPENCODE_DELIVERY_INVALID'), { code: 'OPENCODE_DELIVERY_INVALID' });
const sha256 = /^[a-f0-9]{64}$/;
const relativePath = (value: unknown): value is string =>
  typeof value === 'string' &&
  value.length > 0 &&
  !/[\\\u0000-\u001f\u007f]/.test(value) &&
  value.split('/').every((part) => part.length > 0 && part !== '.' && part !== '..');
const scopedPath = (value: unknown): value is string =>
  relativePath(value) && /^(qa-protocol|evidence)\//.test(value);

// Parse file headers only outside hunks: source lines can themselves start with --- or +++.
function changedPaths(patch: string) {
  const lines = patch.split('\n');
  if (lines.at(-1) === '') lines.pop();
  let i = 0;
  const paths = new Set<string>();
  const headerPath = (line: string | undefined, prefix: string, side: string) => {
    if (!line?.startsWith(prefix)) throw invalid();
    const value = line.slice(prefix.length);
    if (value === '/dev/null') return null;
    if (!value.startsWith(side) || !scopedPath(value.slice(side.length))) throw invalid();
    return value.slice(side.length);
  };
  while (i < lines.length) {
    const before = headerPath(lines[i++], '--- ', 'a/');
    const after = headerPath(lines[i++], '+++ ', 'b/');
    if ((!before && !after) || (before && after && before !== after)) throw invalid();
    let changes = 0,
      hunks = 0;
    while (lines[i]?.startsWith('@@ ')) {
      const hunk = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?:.*)$/.exec(lines[i++]);
      if (!hunk) throw invalid();
      let oldRemaining = Number(hunk[2] ?? 1),
        newRemaining = Number(hunk[4] ?? 1);
      hunks++;
      while (oldRemaining || newRemaining) {
        const line = lines[i++];
        if (line === undefined) throw invalid();
        if (line.startsWith(' ')) {
          oldRemaining--;
          newRemaining--;
        } else if (line.startsWith('-')) {
          oldRemaining--;
          changes++;
        } else if (line.startsWith('+')) {
          newRemaining--;
          changes++;
        } else throw invalid();
        if (oldRemaining < 0 || newRemaining < 0) throw invalid();
        if (lines[i] === '\\ No newline at end of file') i++;
      }
    }
    if (!hunks || !changes) throw invalid();
    if (before) paths.add(before);
    if (after) paths.add(after);
  }
  return paths;
}

/** Validates delivery of a regression suite, not acceptance of the game under test. */
export function verifyOpenCodeDelivery(result: any, expectedTestFile: string): Artifact[] {
  if (
    !scopedPath(expectedTestFile) ||
    !expectedTestFile.startsWith('qa-protocol/') ||
    !Array.isArray(result?.artifacts) ||
    result.artifacts.length > 30
  )
    throw invalid();
  const seen = new Set<string>();
  let totalBytes = 0;
  const artifacts: Artifact[] = result.artifacts.map((item: any) => {
    if (
      !item ||
      typeof item.name !== 'string' ||
      !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(item.name) ||
      seen.has(item.name) ||
      typeof item.base64 !== 'string' ||
      !sha256.test(item.digest ?? '')
    )
      throw invalid();
    seen.add(item.name);
    const content = Buffer.from(item.base64, 'base64');
    totalBytes += content.length;
    if (
      content.length > 8 * 1024 * 1024 ||
      totalBytes > 24 * 1024 * 1024 ||
      content.toString('base64') !== item.base64 ||
      hash(content) !== item.digest
    )
      throw Object.assign(Error('ARTIFACT_CORRUPT'), { code: 'ARTIFACT_CORRUPT' });
    return {
      name: item.name,
      content,
      mime: typeof item.mime === 'string' ? item.mime : 'application/octet-stream',
    };
  });
  const text = (name: string) => {
    const artifact = artifacts.find((item) => item.name === name);
    if (!artifact || !artifact.content.toString('utf8').trim()) throw invalid();
    return artifact.content.toString('utf8');
  };
  let candidate: any, report: any;
  try {
    candidate = JSON.parse(text('candidate.json'));
    report = JSON.parse(text('protocol-report.json'));
  } catch {
    throw invalid();
  }
  if (
    !candidate ||
    !sha256.test(candidate.baseDigest ?? '') ||
    !sha256.test(candidate.candidateDigest ?? '') ||
    !Array.isArray(candidate.files) ||
    !candidate.files.every(
      (file: any) => file && relativePath(file.path) && sha256.test(file.sha256 ?? ''),
    ) ||
    !candidate.files.some((file: any) => file.path === expectedTestFile)
  )
    throw invalid();
  if (!changedPaths(text('changes.patch')).has(expectedTestFile)) throw invalid();
  verifyProtocolReport(report, expectedTestFile);
  text('protocol-tests.log');
  return artifacts;
}

/** Non-protocol tasks still require a current-run envelope and canonical bytes.
 * This validates packaging only; validateTask retains every substantive criterion. */
export function verifyGenericNativeDelivery(result: any, runId: string, execution: any): Artifact[] {
  if (!execution || Object.hasOwn(execution, 'testFile') || execution.workKind === 'planning' ||
      result?.manifest?.version !== 1 || result.manifest.runId !== runId || !sha256.test(result.manifest.digest ?? '') ||
      !Array.isArray(result.artifacts) || result.artifacts.length < 2 || result.artifacts.length > 32)
    throw Object.assign(Error('GENERIC_DELIVERY_MANIFEST_REQUIRED'), { code: 'GENERIC_DELIVERY_MANIFEST_REQUIRED' });
  let total = 0;
  const seen = new Set<string>();
  const artifacts: Artifact[] = result.artifacts.map((item: any) => {
    if (!item || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(item.name ?? '') || seen.has(item.name) ||
        typeof item.base64 !== 'string' || !sha256.test(item.digest ?? '')) throw invalid();
    seen.add(item.name);
    const content = Buffer.from(item.base64, 'base64');
    total += content.length;
    if (content.length > 8 * 1024 * 1024 || total > 24 * 1024 * 1024 ||
        content.toString('base64') !== item.base64 || hash(content) !== item.digest)
      throw Object.assign(Error('ARTIFACT_CORRUPT'), { code: 'ARTIFACT_CORRUPT' });
    return { name: item.name, content, mime: typeof item.mime === 'string' ? item.mime : 'application/octet-stream' };
  });
  let candidate: any;
  try { candidate = JSON.parse(artifacts.find(a => a.name === 'candidate.json')!.content.toString()); }
  catch { throw invalid(); }
  const patch = artifacts.find(a => a.name === 'changes.patch');
  if (!patch || !sha256.test(candidate?.candidateDigest ?? '') || !sha256.test(candidate?.baseDigest ?? '') ||
      !Array.isArray(candidate.files) || candidate.files.some((f: any) => !relativePath(f?.path) || !sha256.test(f?.sha256 ?? '')))
    throw invalid();
  // The native runtime's source mount is unchanged: only qa-protocol/evidence
  // are canonical writable paths. Empty patches are legitimate read-only work.
  if (patch.content.length) changedPaths(patch.content.toString());
  return artifacts;
}
