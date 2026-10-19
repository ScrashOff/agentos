import {it,expect,vi,afterEach} from 'vitest';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {stoppedRunProof} from '../bridge/stopped-run-proof.mjs';
const dirs:string[]=[];
const id='ed569397-19a7-4bcc-8487-64cc8b63d91b';
afterEach(async()=>{for(const dir of dirs.splice(0))await rm(dir,{recursive:true,force:true});});
async function fixture(){
 const dir=await mkdtemp(path.join(tmpdir(),'agentos-stop-proof-'));dirs.push(dir);
 const file=path.join(dir,'candidate.txt');await writeFile(file,'preserved candidate');
 const io={
  readState:async()=>JSON.parse(await readFile(path.join(dir,'state.json'),'utf8')),
  inspectUnit:vi.fn(async(unit:string)=>({unit,stopped:true,mainPid:0,populated:false,activeState:'inactive',checkedAt:new Date().toISOString()})),
  inspectWorkspace:vi.fn(async(workspace:string)=>({workspace,digest:createHash('sha256').update(await readFile(file)).digest('hex')})),
 };
 return {dir,file,io};
}
it('does not infer a stopped execution from a missing state file without a not-started receipt',async()=>{
 const {io}=await fixture();
 await expect(stoppedRunProof(id,undefined,io)).rejects.toMatchObject({code:'ENOENT'});
 expect(io.inspectUnit).not.toHaveBeenCalled();
 expect(io.inspectWorkspace).not.toHaveBeenCalled();
});
it('inspects a never-started run twice and preserves its actual candidate bytes',async()=>{
 const {file,io}=await fixture();const before=await readFile(file);
 const proof=await stoppedRunProof(id,'pilot/fix',io);
 expect(proof).toMatchObject({runId:id,workspace:'pilot/fix',state:'not_started',process:{mainPid:0,populated:false}});
 expect(io.inspectUnit).toHaveBeenCalledTimes(2);
 expect(io.inspectWorkspace).toHaveBeenCalledWith('pilot/fix');
 expect(proof.candidate.digest).toBe(createHash('sha256').update(before).digest('hex'));
 expect(await readFile(file)).toEqual(before);
});
it('refuses a not-started claim when an actual runtime record exists',async()=>{
 const {dir,io}=await fixture();await writeFile(path.join(dir,'state.json'),JSON.stringify({runId:id,workspace:'pilot/fix',state:'failed'}));
 await expect(stoppedRunProof(id,'pilot/fix',io)).rejects.toThrow('NOT_STARTED_RECEIPT_CONFLICT');
 expect(io.inspectUnit).not.toHaveBeenCalled();
});
it('rejects a live unit despite the not-started receipt',async()=>{
 const {io}=await fixture();io.inspectUnit.mockResolvedValueOnce({unit:'agentos-run-'+id,stopped:false,mainPid:123,populated:true,activeState:'active',checkedAt:new Date().toISOString()});
 await expect(stoppedRunProof(id,'pilot/fix',io)).rejects.toThrow('STOP_UNCONFIRMED');
 expect(io.inspectWorkspace).not.toHaveBeenCalled();
});
it('rejects a unit that becomes populated while the candidate is inspected',async()=>{
 const {io}=await fixture();io.inspectUnit.mockResolvedValueOnce({unit:'agentos-run-'+id,stopped:true,mainPid:0,populated:false,activeState:'inactive',checkedAt:new Date().toISOString()});
 io.inspectUnit.mockResolvedValueOnce({unit:'agentos-run-'+id,stopped:false,mainPid:123,populated:true,activeState:'active',checkedAt:new Date().toISOString()});
 await expect(stoppedRunProof(id,'pilot/fix',io)).rejects.toThrow('STOP_UNCONFIRMED');
 expect(io.inspectWorkspace).toHaveBeenCalledTimes(1);
});
it('retains the normal recorded-runtime identity check',async()=>{
 const {dir,io}=await fixture();await writeFile(path.join(dir,'state.json'),JSON.stringify({runId:'wrong',workspace:'pilot/fix',state:'failed'}));
 await expect(stoppedRunProof(id,undefined,io)).rejects.toThrow('RUN_IDENTITY_MISMATCH');
 expect(io.inspectUnit).not.toHaveBeenCalled();
});
