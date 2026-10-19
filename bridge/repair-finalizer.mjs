import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { writeFile } from 'node:fs/promises';

const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const sha = value => createHash('sha256').update(value).digest('hex');
const fields = ['goalId', 'taskId', 'runId', 'runFence', 'contextVersion'];
const fail = code => Object.assign(Error(code), {code});

export async function prepareRepairFinalization(params, inspect, home) {
  if (params.workKind !== 'repair' || !params.managed || params.readOnlySource !== true ||
      !fields.every(key => uuid.test(params[key] ?? ''))) return null;
  const base = await inspect();
  if (!Array.isArray(base.files) || base.files.length > 20000 || !/^[a-f0-9]{64}$/.test(base.candidateDigest ?? ''))
    throw fail('REPAIR_FINALIZATION_SNAPSHOT');
  const files = {};
  for (const file of base.files) {
    if (typeof file.path !== 'string' || !/^[a-f0-9]{64}$/.test(file.sha256 ?? '') || Object.hasOwn(files, file.path))
      throw fail('REPAIR_FINALIZATION_SNAPSHOT');
    Object.defineProperty(files, file.path, {value: file.sha256, enumerable: true});
  }
  const snapshot = {version: 1, binding: Object.fromEntries(fields.map(key => [key, params[key]])),
    sourceSnapshotDigest: base.candidateDigest, files};
  if (home) await writeFile(`${home}/repair-source-snapshot.json`, JSON.stringify(snapshot), {flag: 'wx', mode: 0o600});
  return snapshot;
}

export const repairFinalizationInstruction = `
RUNTIME_REPAIR_FINALIZATION_V1: O controlador gerará implementation.patch e sourceHashes dos bytes reais após a parada confirmada. Preserve a base original; edite somente data/repair-candidate. No repair-report.json declare changedFiles como lista de caminhos relativos exatos; removedFiles é uma lista explícita somente para exclusões intencionais. Arquivo ausente de uma cópia parcial NÃO é exclusão. A base desta geração é /home/agent/repair-source-snapshot.json: quando o reparo exigir outra fonte aceita, declare essa limitação em vez de confundir bases. Preserve fixVerified, comandos, exit codes e logs reais. Não invente hashes nem sintaxe de patch. Publique repair-report.json e as outras evidências da tarefa no envelope; implementation.patch não é obrigatório nesse envelope porque será acrescentado pelo runtime. Caso publique um patch próprio, seus bytes serão preservados separadamente. A geração estrutural nunca aprova testes ou o jogo.
`;

async function generate(input) {
  return new Promise((resolve, reject) => {
    const child = spawn('/usr/bin/python3', ['-B', fileURLToPath(new URL('./repair-finalizer.py', import.meta.url))],
      {env: {PATH: '/usr/bin:/bin', PYTHONIOENCODING: 'utf-8'}, stdio: ['pipe', 'pipe', 'ignore'], windowsHide: true});
    let output = '', size = 0, failure;
    const timer = setTimeout(() => {failure = fail('REPAIR_FINALIZATION_TIMEOUT'); child.kill('SIGKILL');}, 20000);
    child.on('error', error => {clearTimeout(timer); reject(error);});
    child.stdin.on('error', () => {});
    child.stdout.on('data', chunk => {size += chunk.length; if (size > 24 * 1024 * 1024) {failure = fail('REPAIR_FINALIZATION_SIZE'); child.kill('SIGKILL');} else output += chunk;});
    child.once('close', code => {
      clearTimeout(timer);
      if (failure) return reject(failure);
      if (code !== 0) return reject(fail('REPAIR_FINALIZATION_EXECUTOR'));
      try {resolve(JSON.parse(output));} catch {reject(fail('REPAIR_FINALIZATION_RESULT'));}
    });
    child.stdin.end(JSON.stringify(input));
  });
}

/** Only called after confirmed sandbox stop, before canonical publication. */
export async function finalizeRepairArtifacts({params, workspace, snapshot, stopped, artifacts, emit = () => {}}) {
  if (!snapshot) return {generated: false, code: 'NOT_ENABLED'};
  try {
    const binding = Object.fromEntries(fields.map(key => [key, params[key]]));
    if (stopped !== true || fields.some(key => snapshot.binding[key] !== binding[key])) throw fail('REPAIR_FINALIZATION_IDENTITY_STOP');
    const reports = artifacts.filter(a => a.name === 'repair-report.json');
    if (reports.length !== 1 || sha(Buffer.from(reports[0].base64, 'base64')) !== reports[0].digest) throw fail('REPAIR_FINALIZATION_REPORT');
    const result = await generate({root: workspace, snapshot, binding, stopped, report: reports[0].base64});
    if (!result.ok) throw fail(result.code);
    const added = [['implementation.patch', result.patch, 'text/plain'], ['repair-report.json', result.report, 'application/json'],
      ['runtime-repair-finalization.json', Buffer.from(JSON.stringify(result.provenance)).toString('base64'), 'application/json']]
      .map(([name, base64, mime]) => ({name, base64, mime, digest: sha(Buffer.from(base64, 'base64'))}));
    const preserve = artifacts.filter(a => ['implementation.patch', 'repair-report.json'].includes(a.name));
    if (artifacts.some(a => a.name === 'runtime-repair-finalization.json') || preserve.some(a => artifacts.some(other => other.name === 'worker-' + a.name)))
      throw fail('REPAIR_FINALIZATION_NAME_COLLISION');
    if (artifacts.length + added.length > 30 || [...artifacts, ...added].reduce((sum, a) => sum + Buffer.from(a.base64, 'base64').length, 0) > 24 * 1024 * 1024)
      throw fail('REPAIR_FINALIZATION_SIZE');
    for (const item of preserve) item.name = 'worker-' + item.name;
    artifacts.push(...added);
    emit('REPAIR_ARTIFACTS_FINALIZED', {...result.provenance, reportDigest: added[1].digest});
    return {generated: true, patchDigest: added[0].digest, reportDigest: added[1].digest};
  } catch (error) {
    const code = /^REPAIR_FINALIZATION_[A-Z_]+$/.test(error.code ?? '') ? error.code : 'REPAIR_FINALIZATION_FAILED';
    emit('REPAIR_FINALIZATION_UNAVAILABLE', {runId: params.runId, code});
    // Preserve the entire original delivery and its negative/missing criteria.
    return {generated: false, code};
  }
}
