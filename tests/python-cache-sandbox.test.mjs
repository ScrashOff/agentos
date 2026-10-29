import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, mkdir, readFile, writeFile, readdir, cp, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const environment = {PATH: '/usr/bin:/bin', LANG: 'C.UTF-8'};
const workspaceId = 'cachetest/isolated';
const managerPath = fileURLToPath(new URL('../bridge/workspace-manager.py', import.meta.url));

async function fixture() {
  const root = await mkdtemp(path.join(tmpdir(), 'agentos-python-cache-'));
  const workspace = path.join(root, 'workspaces', workspaceId);
  const home = path.join(root, 'home');
  const data = path.join(root, 'data');
  await mkdir(workspace, {recursive: true});
  await mkdir(home);
  await writeFile(path.join(workspace, 'valid.py'), 'answer = 42\n');
  await writeFile(path.join(workspace, 'invalid.py'), 'def broken(:\n    pass\n');
  const key = createHash('sha256').update(workspaceId).digest('hex');
  await cp(workspace, path.join(data, 'workspace-baselines', key), {recursive: true});
  return {root, workspace, home, data};
}

function pythonInSandbox(f, prefix, args) {
  const sandbox = [
    '--unshare-all', '--die-with-parent', '--new-session', '--clearenv',
    '--ro-bind', '/usr', '/usr', '--symlink', 'usr/bin', '/bin',
    '--symlink', 'usr/lib', '/lib', '--symlink', 'usr/lib64', '/lib64',
    '--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp',
    '--dir', '/etc', '--ro-bind', '/etc/ld.so.cache', '/etc/ld.so.cache',
    '--ro-bind', f.workspace, '/workspace', '--bind', f.home, '/home/agent',
    '--chdir', '/workspace', '--setenv', 'HOME', '/home/agent',
    '--setenv', 'PATH', environment.PATH, '--setenv', 'LANG', environment.LANG,
  ];
  if (prefix !== undefined) sandbox.push('--setenv', 'PYTHONPYCACHEPREFIX', prefix);
  return spawnSync('/usr/bin/bwrap', [...sandbox, '--', '/usr/bin/python3', ...args], {
    env: environment, encoding: 'utf8', timeout: 15000,
  });
}

async function files(root) {
  const result = {};
  async function visit(directory, relative = '') {
    for (const item of await readdir(directory, {withFileTypes: true})) {
      const name = path.posix.join(relative, item.name);
      if (item.isDirectory()) await visit(path.join(directory, item.name), name);
      else result[name] = createHash('sha256').update(await readFile(path.join(directory, item.name))).digest('hex');
    }
  }
  await visit(root);
  return result;
}

function inspectFixture(f) {
  // The real guard runs with fixture-only roots and locks; never invoke prepare or live data.
  const program = `import importlib.util, json, pathlib, sys
spec = importlib.util.spec_from_file_location('cache_test_manager', sys.argv[1])
manager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(manager)
manager.ROOT = pathlib.Path(sys.argv[2])
manager.DATA = pathlib.Path(sys.argv[3])
try:
    result = manager.inspect({'workspace': sys.argv[4]})
except ValueError as error:
    result = {'error': str(error)}
print(json.dumps(result))
`;
  const result = spawnSync('/usr/bin/python3', ['-B', '-c', program, managerPath,
    path.join(f.root, 'workspaces'), f.data, workspaceId], {
    env: environment, encoding: 'utf8', timeout: 15000,
  });
  assert.equal(result.status, 0, JSON.stringify({error: result.error?.code, stderr: result.stderr}));
  return JSON.parse(result.stdout);
}

function success(result) {
  assert.equal(result.status, 0, JSON.stringify({error: result.error?.code, stderr: result.stderr}));
}

for (const executor of ['runtime-runner.mjs', 'opencode-executor.mjs', 'muse-runner.mjs']) {
  test(`${executor}: py_compile uses private cache with read-only source and preserves validation failures`, async () => {
    const source = await readFile(new URL(`../bridge/${executor}`, import.meta.url), 'utf8');
    const settings = [...source.matchAll(/['"]--setenv['"],\s*['"]PYTHONPYCACHEPREFIX['"],\s*['"]([^'"]+)['"]/g)];
    assert.equal(settings.length, 1, 'one explicit cache location in the cleared sandbox environment');
    const prefix = settings[0][1];
    assert.equal(prefix, '/home/agent/.cache/python');
    const f = await fixture();
    try {
      const before = await files(f.workspace);
      success(pythonInSandbox(f, prefix, ['-m', 'py_compile', '/workspace/valid.py']));
      const homeFiles = await files(f.home);
      assert.ok(Object.keys(homeFiles).some(name => /^\.cache\/python\/workspace\/valid\.[^/]+\.pyc$/.test(name)),
        'the compiled module must be present in the per-execution home');
      assert.deepEqual(await files(f.workspace), before, 'compile must not add bytecode to the candidate');
      let inspected = inspectFixture(f);
      assert.equal(inspected.patch, '');
      assert.equal(inspected.baseDigest, inspected.candidateDigest);

      const invalid = pythonInSandbox(f, prefix, ['-m', 'py_compile', '/workspace/invalid.py']);
      assert.equal(invalid.status, 1, invalid.stderr);
      assert.match(invalid.stderr, /SyntaxError/);
      const deniedWrite = pythonInSandbox(f, prefix, ['-c', "open('/workspace/valid.py', 'w').write('changed')"]);
      assert.equal(deniedWrite.status, 1, deniedWrite.stderr);
      assert.match(deniedWrite.stderr, /Read-only file system/);
      assert.deepEqual(await files(f.workspace), before);
      inspected = inspectFixture(f);
      assert.equal(inspected.patch, '');
      assert.equal(inspected.baseDigest, inspected.candidateDigest);

      // A genuine binary source change must still be rejected by workspace-manager.
      await writeFile(path.join(f.workspace, 'deliberate.bin'), Buffer.from([0xff, 0xfe, 0, 1]));
      assert.deepEqual(inspectFixture(f), {error: 'PATCH_BINARY_SOURCE'});
    } finally {
      await rm(f.root, {recursive: true, force: true});
    }
  });
}

test('without PYTHONPYCACHEPREFIX the read-only workspace reproduces the py_compile failure', async () => {
  const f = await fixture();
  try {
    const before = await files(f.workspace);
    const result = pythonInSandbox(f, undefined, ['-m', 'py_compile', '/workspace/valid.py']);
    assert.equal(result.status, 1, result.stderr);
    assert.match(result.stderr, /Read-only file system/);
    assert.deepEqual(await files(f.workspace), before);
    assert.deepEqual(await files(f.home), {});
  } finally {
    await rm(f.root, {recursive: true, force: true});
  }
});
