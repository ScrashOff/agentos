import {it,expect} from 'vitest';
import {bridgeScript} from '../server/bridge-path.js';
it('pins remote helpers to the running immutable controller release',()=>{
 const base='file:///opt/agentos/releases/managed-24f4de65-5f23-47c5-b841-942da1f2d801/server/bridge-path.ts';
 expect(bridgeScript('opencode-executor.mjs',base)).toBe('/opt/agentos/releases/managed-24f4de65-5f23-47c5-b841-942da1f2d801/bridge/opencode-executor.mjs');
});
it('never sends a development checkout to the remote host or interpolates script arguments',()=>{
 expect(bridgeScript('server.mjs','file:///tmp/test/server/bridge-path.ts')).toBe('/opt/agentos-bridge/server.mjs');
 expect(()=>bridgeScript('../server.mjs')).toThrow('INVALID_BRIDGE_SCRIPT');
 expect(()=>bridgeScript('server.mjs; id')).toThrow('INVALID_BRIDGE_SCRIPT');
});
