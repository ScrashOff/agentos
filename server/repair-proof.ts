import { createHash } from 'node:crypto';

const fail = (suffix: string): never => { throw Object.assign(Error('REPAIR_SOURCE_PROOF_' + suffix), { code: 'REPAIR_SOURCE_PROOF_' + suffix }); };
const sha = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
const forbidden = new Set(['.', '..', '.git', 'assets', 'node_modules', 'evidence', 'data', 'rules', '.agentos-snapshot.json', '.ssh', '.aws', '.codex', '.claude']);
function safePath(name: string) {
  return /^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*$/.test(name) &&
    !name.split('/').some(part => forbidden.has(part) || part.startsWith('.env'));
}
function text(bytes: Buffer) {
  if (!bytes.length || bytes.length > 8 * 1024 * 1024) fail('SIZE');
  const value = bytes.toString('utf8');
  if (!Buffer.from(value).equals(bytes) || value.includes('\0')) fail('ENCODING');
  return value;
}

// JSON.parse silently discards duplicate keys, including escaped equivalents.
// Parse bounded JSON with explicit key uniqueness so the report has one meaning.
function uniqueJson(value: string): any {
  let at = 0;
  const whitespace = () => { while (at < value.length && /[\x20\t\r\n]/.test(value[at])) at++; };
  const string = () => {
    const start = at++;
    while (at < value.length) {
      if (value[at] === '\\') { at += 2; continue; }
      if (value[at++] === '"') {
        try { return JSON.parse(value.slice(start, at)); } catch { fail('JSON'); }
      }
    }
    return fail('JSON');
  };
  const parse = (depth: number): any => {
    if (depth > 64) fail('JSON');
    whitespace();
    if (value[at] === '"') return string();
    if (value[at] === '{') {
      at++; whitespace(); const result = Object.create(null), keys = new Set();
      if (value[at] === '}') { at++; return result; }
      for (;;) {
        whitespace(); if (value[at] !== '"') fail('JSON');
        const key = string(); if (keys.has(key)) fail('JSON_DUPLICATE_KEY'); keys.add(key);
        whitespace(); if (value[at++] !== ':') fail('JSON');
        result[key] = parse(depth + 1); whitespace();
        if (value[at] === '}') { at++; return result; }
        if (value[at++] !== ',') fail('JSON');
      }
    }
    if (value[at] === '[') {
      at++; whitespace(); const result: any[] = [];
      if (value[at] === ']') { at++; return result; }
      for (;;) {
        result.push(parse(depth + 1)); whitespace();
        if (value[at] === ']') { at++; return result; }
        if (value[at++] !== ',') fail('JSON');
      }
    }
    const token = /(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/y;
    token.lastIndex = at; const found = token.exec(value);
    if (!found) fail('JSON');
    at = token.lastIndex; return JSON.parse(found![0]);
  };
  const parsed = parse(0); whitespace(); if (at !== value.length) fail('JSON');
  return parsed;
}

type FileChange = { path: string; added: boolean; removed: boolean };
/** Strict text unified diff / git diff, matching the compositor's safe subset.
 * This validates syntax and coverage only, never executes or applies the patch. */
function diffFiles(value: string): FileChange[] {
  if (!value.endsWith('\n')) fail('PATCH_FORMAT');
  const lines = value.slice(0, -1).split('\n'), files: FileChange[] = [], seen = new Set<string>();
  let at = 0;
  const header = (line: string, sign: string, prefix: string) => {
    if (!line?.startsWith(sign)) return fail('PATCH_FORMAT');
    const name = line.slice(4).split('\t')[0];
    if (name === '/dev/null') return null;
    if (!name.startsWith(prefix) || !safePath(name.slice(2))) return fail('PATCH_PATH');
    return name.slice(2);
  };
  while (at < lines.length) {
    let gitPath: string | undefined, mode: 'new' | 'deleted' | undefined;
    if (lines[at].startsWith('diff --git ')) {
      const match = /^diff --git a\/([^ ]+) b\/([^ ]+)$/.exec(lines[at++]);
      if (!match || match[1] !== match[2] || !safePath(match[1])) fail('PATCH_PATH');
      gitPath = match![1];
      let indexSeen = false;
      while (at < lines.length && !lines[at].startsWith('--- ')) {
        const line = lines[at++], declaredMode = /^(new|deleted) file mode (100644|100755)$/.exec(line);
        if (declaredMode && !mode && !indexSeen) mode = declaredMode[1] as 'new' | 'deleted';
        else if (/^index [a-f0-9]{4,64}\.\.[a-f0-9]{4,64}(?: (?:100644|100755))?$/.test(line) && !indexSeen) indexSeen = true;
        else fail('PATCH_FORMAT');
      }
    }
    const before = header(lines[at++], '--- ', 'a/'), after = header(lines[at++], '+++ ', 'b/');
    const name = before ?? after;
    if (!name || (before !== null && after !== null && before !== after) ||
      (gitPath !== undefined && gitPath !== name) || seen.has(name)) fail('PATCH_PATH');
    if ((mode === 'new' && before !== null) || (mode === 'deleted' && after !== null)) fail('PATCH_FORMAT');
    seen.add(name!); if (seen.size > 128) fail('PATCH_PATH');
    let hunks = 0, edits = 0, oldEnd = 0, newEnd = 0, delta = 0;
    while (at < lines.length && lines[at].startsWith('@@')) {
      const hunk = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$/.exec(lines[at++]);
      if (!hunk) fail('PATCH_FORMAT');
      const oldStart = Number(hunk![1]), oldCount = Number(hunk![2] ?? 1), newStart = Number(hunk![3]), newCount = Number(hunk![4] ?? 1);
      if ([oldStart, oldCount, newStart, newCount].some(n => !Number.isSafeInteger(n)) ||
        (oldCount === 0 && newCount === 0) ||
        (!oldStart && oldCount > 0) || (!newStart && newCount > 0) ||
        (before === null && (oldStart !== 0 || oldCount !== 0)) ||
        (after === null && (newStart !== 0 || newCount !== 0))) fail('PATCH_FORMAT');
      const oldPosition = oldStart + (oldCount === 0 ? 1 : 0), newPosition = newStart + (newCount === 0 ? 1 : 0);
      if (oldPosition < oldEnd || newPosition < newEnd || newPosition - oldPosition !== delta) fail('PATCH_FORMAT');
      oldEnd = oldPosition + oldCount; newEnd = newPosition + newCount; delta += newCount - oldCount;
      let oldLeft = oldCount, newLeft = newCount, contentSeen = false, marker = false;
      while (oldLeft > 0 || newLeft > 0) {
        const line = lines[at++];
        if (line === '\\ No newline at end of file' && contentSeen && !marker) { marker = true; continue; }
        if (line === undefined || ![' ', '+', '-'].includes(line[0])) fail('PATCH_FORMAT');
        if (line[0] !== '+') oldLeft--;
        if (line[0] !== '-') newLeft--;
        if (oldLeft < 0 || newLeft < 0) fail('PATCH_FORMAT');
        if (line[0] !== ' ') edits++;
        contentSeen = true; marker = false;
      }
      if (lines[at] === '\\ No newline at end of file' && contentSeen && !marker) at++;
      hunks++;
    }
    if (!hunks || !edits) fail('PATCH_FORMAT');
    files.push({ path: name!, added: before === null, removed: after === null });
  }
  if (!files.length) fail('PATCH_FORMAT');
  return files;
}

export function verifyRepairSourceProof(patch: Buffer, report: Buffer) {
  const files = diffFiles(text(patch)), value = uniqueJson(text(report));
  if (!value || typeof value !== 'object' || Array.isArray(value) || value.fixVerified !== true) fail('REPORT');
  if (!Array.isArray(value.sourceHashes) || value.sourceHashes.length !== files.length) fail('COVERAGE');
  const seen = new Set<string>();
  for (const item of value.sourceHashes) {
    const file = files.find(file => file.path === item?.path);
    if (!file || seen.has(item.path) || item.before === item.after ||
      [item.before, item.after].some(digest => digest !== null && (typeof digest !== 'string' || !/^[a-f0-9]{64}$/.test(digest))) ||
      (item.before === null) !== file.added || (item.after === null) !== file.removed) fail('COVERAGE');
    seen.add(item.path);
  }
  return { version: 1 as const, patchSha256: sha(patch), reportSha256: sha(report),
    files: files.map(file => file.path).sort(), applicabilityVerified: false as const, qaVerified: false as const };
}
