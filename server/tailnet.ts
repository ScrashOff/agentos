import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { isIP } from 'node:net';
import { config } from './config.js';
const exec = promisify(execFile);
const cache = new Map<string, { until: number; allowed: boolean }>();
export async function trustedTailnet(
  req: { ip: string; headers: Record<string, any> },
  lookup = async (ip: string) =>
    JSON.parse(
      (
        await exec('tailscale', ['whois', '--json', ip], {
          timeout: 5000,
          maxBuffer: 100000,
          windowsHide: true,
        })
      ).stdout,
    ),
) {
  if (
    !config.tailnetHost ||
    (req.headers.host ?? '').split(':')[0] !== config.tailnetHost ||
    !['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(req.ip)
  )
    return false;
  if (
    typeof req.headers['tailscale-user-login'] === 'string' &&
    req.headers['tailscale-user-login']
  )
    return true;
  // Tagged devices have no user identity header. Serve appends the actual peer IP.
  const peer =
    String(req.headers['x-forwarded-for'] ?? '')
      .split(',')
      .at(-1)
      ?.trim() ?? '';
  if (!isIP(peer)) return false;
  const old = cache.get(peer);
  if (old && old.until > Date.now()) return old.allowed;
  try {
    const info = await lookup(peer),
      node = info.Node;
    const allowed = Boolean(
      node &&
        !node.Expired &&
        node.Name?.endsWith('.' + config.tailnetHost.split('.').slice(1).join('.') + '.') &&
        node.Addresses?.some((a: string) => a.split('/')[0] === peer),
    );
    if (cache.size > 512) cache.clear();
    cache.set(peer, { allowed, until: Date.now() + 30000 });
    return allowed;
  } catch {
    return false;
  }
}
