import {pool} from '../server/db.js';
import {accountBridge} from '../server/bridge.js';
import {executeDetachedHarness} from '../server/harness-supervision.js';
import {superviseNativeRun} from '../server/native-supervision.js';
import {nativeSupervisorSelfProof} from './native-supervisor-unit.js';

try {
  if(process.argv[2]!=='supervise')throw Error('HARNESS_SUPERVISOR_COMMAND_INVALID');
  pool.options.max=4;
  await superviseNativeRun(process.argv[3],process.argv[4],nativeSupervisorSelfProof,executeDetachedHarness);
}finally{accountBridge.close();await pool.end();}
