import test from 'node:test';
import assert from 'node:assert/strict';
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');

async function treeSnapshot(root, relative = '') {
  const result = [];
  for (const entry of (await readdir(path.join(root, relative), { withFileTypes: true }))
    .sort((a, b) => a.name.localeCompare(b.name))) {
    const name = path.posix.join(relative, entry.name);
    if (entry.isDirectory()) {
      result.push({ name, type: 'directory' }, ...await treeSnapshot(root, name));
    } else {
      assert.ok(entry.isFile(), 'The fixture release must contain only files and directories');
      const bytes = await readFile(path.join(root, name));
      result.push({ name, type: 'file', size: bytes.length, sha256: hash(bytes) });
    }
  }
  return result;
}

test('actual workspace operations use adjacent helpers without changing the immutable release tree', { timeout: 15000 }, async () => {
  const root = await mkdtemp(path.join(tmpdir(), 'agentos-immutable-workspace-'));
  const bridge = path.join(root, 'bridge');
  const variables = ['PYTHONDONTWRITEBYTECODE', 'PYTHONPYCACHEPREFIX'];
  const saved = variables.map(key => [key, process.env[key]]);
  try {
    // The launcher itself must disable bytecode, regardless of shell configuration.
    for (const key of variables) delete process.env[key];
    await mkdir(bridge);
    await copyFile(new URL('../bridge/workspaces.mjs', import.meta.url), path.join(bridge, 'workspaces.mjs'));
    await writeFile(path.join(bridge, 'adjacent_workspace_fixture.py'),
      'VALUE = "helper-from-candidate-release"\n');
    await writeFile(path.join(bridge, 'workspace-manager.py'), [
      'import json, os, sys',
      'import adjacent_workspace_fixture as helper',
      'params = json.load(sys.stdin)',
      'print(json.dumps({"request": params, "value": helper.VALUE,',
      '  "manager": os.path.realpath(__file__), "helper": os.path.realpath(helper.__file__),',
      '  "bytecode_disabled": sys.dont_write_bytecode}))',
      '',
    ].join('\n'));
    const before = await treeSnapshot(root);
    const digest = hash(JSON.stringify(before));
    const { workspaceOperation } = await import(pathToFileURL(path.join(bridge, 'workspaces.mjs')).href);
    for (const operation of ['storage', 'prepare']) {
      const request = { operation, workspace: 'fixture/read-only-release', marker: 'acentuação' };
      const response = await workspaceOperation(request);
      assert.deepEqual(response.request, request);
      assert.equal(response.value, 'helper-from-candidate-release');
      assert.equal(response.manager, path.join(bridge, 'workspace-manager.py'));
      assert.equal(response.helper, path.join(bridge, 'adjacent_workspace_fixture.py'));
      assert.equal(response.bytecode_disabled, true, 'The Python invocation must use -B');
      const after = await treeSnapshot(root);
      assert.deepEqual(after, before, 'A runtime operation must not add pycache or alter published files');
      assert.equal(hash(JSON.stringify(after)), digest);
      assert.ok(after.every(entry => !entry.name.includes('__pycache__') && !entry.name.endsWith('.pyc')));
    }
  } finally {
    for (const [key, value] of saved) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await rm(root, { recursive: true, force: true });
  }
});
