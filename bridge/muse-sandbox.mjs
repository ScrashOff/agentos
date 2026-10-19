// Only invoked inside the mount, PID and network namespaces created by muse-runner.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';

const settings = JSON.parse(await readFile('/home/agent/muse-run.json', 'utf8'));
const proxy = http.createServer((req, res) => {
  if (
    !(
      (req.method === 'GET' && req.url === '/muse-code/models') ||
      (req.method === 'POST' && req.url === '/v1/responses')
    )
  ) {
    res.writeHead(404).end();
    return;
  }
  const upstream = http.request(
    { socketPath: '/run/model.sock', path: req.url, method: req.method, headers: req.headers },
    (reply) => {
      res.writeHead(reply.statusCode, {
        'content-type': reply.headers['content-type'] ?? 'application/json',
        'cache-control': 'no-store',
      });
      reply.pipe(res);
    },
  );
  upstream.on('error', () => {
    if (!res.headersSent) res.writeHead(502);
    res.end();
  });
  res.once('close', () => upstream.destroy());
  req.pipe(upstream);
});
await new Promise((resolve, reject) => {
  proxy.once('error', reject);
  proxy.listen(17821, '127.0.0.1', resolve);
});
const child = spawn(
  '/opt/agentos-muse/muse',
  [
    'exec',
    '--json',
    '--provider',
    'meta',
    '--prompt-file',
    '/home/agent/prompt.txt',
    '--output-schema',
    '/home/agent/result-schema.json',
    '--model',
    settings.model,
    '--base-url',
    'http://127.0.0.1:17821/v1',
    '--workspace',
    '/workspace',
    '--session-id',
    settings.runId,
    '--worktree',
    'off',
    '--reasoning-effort',
    settings.reasoning,
    '--disable-web-tools',
    '--disable-reminders',
    '--no-foreign-personal-context',
    '--yolo',
  ],
  {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, META_API_KEY: 'agentos-private-gateway' },
  },
);
child.stdout.pipe(process.stdout);
child.stderr.pipe(process.stderr);
const close = (code) => {
  proxy.closeAllConnections();
  proxy.close();
  process.exit(code ?? 1);
};
child.once('error', () => close(1));
child.once('close', close);
