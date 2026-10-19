import { createHash } from 'node:crypto';

const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const name = /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/;
const sourcePath =
  /^(?:qa-protocol|qa-suite)\/(?:[a-zA-Z0-9][a-zA-Z0-9_.-]*\/)*[a-zA-Z0-9][a-zA-Z0-9_.-]*\.(?:mjs|cjs|js|ts|json)$/;
const limits = {
  files: 24,
  fileBytes: 1024 * 1024,
  extraBytes: 4 * 1024 * 1024,
  totalBytes: 24 * 1024 * 1024,
};

function stableId(value) {
  const hex = hash(value).slice(0, 32).split('');
  hex[12] = '5';
  hex[16] = '8';
  const s = hex.join('');
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
}

// Existing-file hunks require a baseline; only a complete addition is full source.
function additions(text, reject) {
  const starts = [...text.matchAll(/^--- /gm)].map((m) => m.index);
  const result = [];
  for (let i = 0; i < starts.length; i++) {
    const lines = text.slice(starts[i], starts[i + 1] ?? text.length).split('\n');
    if (lines[0].replace(/\r$/, '') !== '--- /dev/null') continue;
    const target = lines[1]?.replace(/\r$/, '').match(/^\+\+\+ b\/(.+)$/)?.[1];
    if (
      !target ||
      target.length > 512 ||
      !sourcePath.test(target) ||
      target.split('/').some((p) => p === '..')
    ) {
      reject('path');
      continue;
    }
    const hunk = lines[2]?.replace(/\r$/, '').match(/^@@ -0,0 \+1(?:,(\d+))? @@$/);
    if (!hunk) {
      reject('incomplete');
      continue;
    }
    const count = Number(hunk[1] ?? 1);
    const content = [];
    let valid = count > 0,
      noNewline = false;
    for (let n = 3; n < lines.length; n++) {
      const line = lines[n];
      if (line === '' && n === lines.length - 1) continue;
      if (line === '\\ No newline at end of file' && content.length === count && !noNewline) {
        noNewline = true;
        continue;
      }
      if (!line.startsWith('+') || content.length >= count || noNewline) {
        valid = false;
        break;
      }
      content.push(line.slice(1) + '\n');
    }
    if (!valid || content.length !== count) {
      reject('incomplete');
      continue;
    }
    const value = content.join('');
    result.push({ path: target, bytes: Buffer.from(noNewline ? value.slice(0, -1) : value) });
  }
  return result;
}

export function materializeSourceInputs(inputs = [], options = {}) {
  const bounds = { ...limits, ...options };
  const skipped = {};
  const reject = (reason) => {
    skipped[reason] = (skipped[reason] ?? 0) + 1;
  };
  const decoded = inputs.map((input) => {
    if (!uuid.test(input.id) || !name.test(input.name))
      throw Object.assign(new Error('INVALID_INPUT'), { code: 'INVALID_INPUT' });
    const bytes = Buffer.from(input.base64, 'base64');
    if (bytes.length > 8 * 1024 * 1024 || hash(bytes) !== input.digest)
      throw Object.assign(new Error('INPUT_DIGEST'), { code: 'INPUT_DIGEST' });
    return { ...input, bytes };
  });
  const manifests = [];
  for (const input of decoded.filter((v) => v.name === 'candidate.json')) {
    try {
      const candidate = JSON.parse(input.bytes.toString('utf8'));
      if (!Array.isArray(candidate.files)) continue;
      manifests.push({
        input,
        files: new Map(
          candidate.files
            .filter((f) => sourcePath.test(f.path) && /^[0-9a-f]{64}$/.test(f.sha256))
            .map((f) => [f.path, f.sha256]),
        ),
      });
    } catch {
      reject('manifest');
    }
  }
  const extra = [],
    sources = [],
    seen = new Set();
  let extraBytes = 0;
  const originalBytes = decoded.reduce((sum, v) => sum + v.bytes.length, 0);
  for (const patch of decoded.filter((v) =>
    ['changes.patch', 'continuation-changes.patch'].includes(v.name),
  )) {
    for (const source of additions(patch.bytes.toString('utf8'), reject)) {
      const digest = hash(source.bytes);
      const manifest = manifests.find((m) => m.files.get(source.path) === digest);
      if (!manifest) {
        reject('manifest_hash');
        continue;
      }
      const identity = source.path + ':' + digest;
      if (seen.has(identity)) continue;
      seen.add(identity);
      const basename = source.path.split('/').at(-1);
      if (!name.test(basename)) {
        reject('name');
        continue;
      }
      if (decoded.some((v) => v.name === basename && v.digest === digest)) continue;
      // Reserve space for the bounded provenance index too.
      if (
        source.bytes.length > bounds.fileBytes ||
        extra.length >= bounds.files ||
        extraBytes + source.bytes.length + 64 * 1024 > bounds.extraBytes ||
        originalBytes + extraBytes + source.bytes.length + 64 * 1024 > bounds.totalBytes
      ) {
        reject('capacity');
        continue;
      }
      const id = stableId(`${manifest.input.id}:${patch.id}:${identity}`);
      if (inputs.some((v) => v.id === id)) {
        reject('collision');
        continue;
      }
      extra.push({ id, name: basename, digest, base64: source.bytes.toString('base64') });
      extraBytes += source.bytes.length;
      sources.push({
        path: source.path,
        sha256: digest,
        inputId: id,
        candidateInputId: manifest.input.id,
        patchInputId: patch.id,
      });
    }
  }
  if (!extra.length) return { inputs, instruction: '', telemetry: { materialized: 0, skipped } };
  const bytes = Buffer.from(
    JSON.stringify(
      {
        version: 1,
        sources,
        skipped,
        note: 'Input integrity only. This does not establish task or game acceptance.',
      },
      null,
      2,
    ) + '\n',
  );
  const indexId = stableId('source-files:' + hash(bytes));
  if (inputs.some((v) => v.id === indexId))
    return {
      inputs,
      instruction: '',
      telemetry: { materialized: 0, skipped: { ...skipped, collision: 1 } },
    };
  extra.push({
    id: indexId,
    name: 'source-files.json',
    digest: hash(bytes),
    base64: bytes.toString('base64'),
  });
  return {
    inputs: [...inputs, ...extra],
    instruction: `\nFull QA source files were recovered from supplied canonical patches and verified against supplied candidate SHA256 manifests. Original paths and provenance: /home/agent/inputs/${indexId}/source-files.json. Copy only into authorized writable paths and rerun against the actual candidate. Hash verification establishes input integrity, not acceptance.`,
    telemetry: { materialized: sources.length, bytes: extraBytes, indexId, skipped },
  };
}
