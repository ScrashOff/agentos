import { existsSync } from 'node:fs';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
if (!existsSync('.env')) {
  const password = randomBytes(24).toString('hex'),
    token = randomBytes(32).toString('hex');
  const template = await readFile('.env.example', 'utf8');
  const content = template
    .replace('POSTGRES_PASSWORD=replace-with-a-new-random-value', 'POSTGRES_PASSWORD=' + password)
    .replace(
      'postgresql://agentos:replace-with-a-new-random-value@',
      'postgresql://agentos:' + password + '@',
    )
    .replace('AGENTOS_API_TOKEN=replace-with-a-new-random-value', 'AGENTOS_API_TOKEN=' + token);
  await writeFile('.env', content, { flag: 'wx', mode: 0o600 });
  console.log('Created private local configuration. Credentials were not displayed.');
} else console.log('Existing local configuration preserved.');
await mkdir('.local', { recursive: true });
