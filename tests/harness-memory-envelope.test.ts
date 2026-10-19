import {afterEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {executeRuntime} from '../server/runtime.js';
import {config} from '../server/config.js';
import {accountBridge} from '../server/bridge.js';

afterEach(()=>{vi.restoreAllMocks();config.remoteExecution=false;});
function input():any {
  config.remoteExecution=true;
  const runId=randomUUID(),fence=randomUUID(),launchToken=randomUUID(),taskId=randomUUID(),workspace='memory/envelope';
  const resources={version:1,id:randomUUID(),runtime:'harness',runId,runFence:fence,launchToken,workspace,
    memoryMiB:3072,state:'reserved',reservedAt:new Date().toISOString(),
    oomSource:{runId:randomUUID(),attempt:1,terminationEventId:'1',stopEventId:'2'}};
  const task={id:taskId,goal_id:randomUUID(),title:'Fixture',description:'No model',criteria:[],execution:{kind:'harness',workspace}};
  return {run:{id:runId,task_id:taskId,fence,runtime:'harness',memoryReservation:resources,launchToken,
      instruction_snapshot:{schema:1,task:structuredClone(task)}},task,account:{provider:'codex',bridge_id:'fixture',model:'fixture'},
    context:{content:'',manifest:{selected:[],dependencyArtifacts:[],executionInputs:[]}},signal:new AbortController().signal,onEvent:vi.fn()};
}
it('transports the validated owner allocation with the frozen workspace and explicit fence/token',async()=>{
  const i=input();i.task.execution.workspace='changed/live';
  const request=vi.spyOn(accountBridge,'request').mockResolvedValue({summary:'fixture',artifacts:[],evidence:{stopped:true}});
  await executeRuntime(i);
  expect(request.mock.calls[0][1]).toMatchObject({runId:i.run.id,runFence:i.run.fence,launchToken:i.run.launchToken,
    workspace:'memory/envelope',memoryReservation:i.run.memoryReservation});
});
it.each(['fence','token','workspace','released','runtime'])('rejects mismatched %s allocation before remote prepare/execute',async mode=>{
  const i=input(),request=vi.spyOn(accountBridge,'request');
  if(mode==='fence')i.run.memoryReservation.runFence=randomUUID();
  if(mode==='token')i.run.launchToken=randomUUID();
  if(mode==='workspace')i.run.memoryReservation.workspace='other/workspace';
  if(mode==='released')i.run.memoryReservation.state='released';
  if(mode==='runtime')i.run.memoryReservation.runtime='opencode';
  await expect(executeRuntime(i)).rejects.toMatchObject({code:'RUNTIME_MEMORY_RESERVATION_INVALID'});
  expect(request).not.toHaveBeenCalled();
});
