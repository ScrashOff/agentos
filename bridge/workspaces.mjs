import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
export function workspaceOperation(params) {
  return new Promise((resolve, reject) => {
    const p = spawn('python3', ['-B', fileURLToPath(new URL('./workspace-manager.py', import.meta.url))], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let out = '';
    p.stdin.on('error', () => {});
    p.stdout.on('data', (b) => {
      out += b;
      if (out.length > 32 * 1024 * 1024) {
        p.kill('SIGKILL');
        reject(
          Object.assign(Error('WORKSPACE_OUTPUT_TOO_LARGE'), {
            code: 'WORKSPACE_OUTPUT_TOO_LARGE',
          }),
        );
      }
    });
    p.stderr.resume();
    p.on('error', reject);
    p.on('close', (code) => {
      try {
        const value = JSON.parse(out);
        if (code || value.error)
          reject(
            Object.assign(Error(value.error ?? 'WORKSPACE_ERROR'), {
              code: value.error ?? 'WORKSPACE_ERROR',
            }),
          );
        else resolve(value);
      } catch (e) {
        reject(e);
      }
    });
    p.stdin.end(JSON.stringify(params));
  });
}
