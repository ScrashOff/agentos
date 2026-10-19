import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,symlink,mkdir,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
import {PROGRESS_POLICY,parseReadOnlyPoll,analyzePollTrace,readProgressFile,proveFailedPolls,progressDigest} from '../bridge/runtime-progress.mjs';
import {inspectRuntimeProgress,checkpointRuntimeProgress,sameProgressProof} from '../bridge/inspect-runtime-progress.mjs';
const command='sleep 295; cd /workspace && grep -c "CASE" evidence/test.log';
const compositeCommand='sleep 290; ps aux | grep stable-cdp | grep -v grep | wc -l; tail -30 /workspace/evidence/integration/r-wild-bandito-stable-cdp.log';
const base=Date.now()-600000;
const log='CASE PASS boot\nCASE FAIL candidate materialization\n';
const metadata=text=>({text,digest:progressDigest(text),size:Buffer.byteLength(text),mtimeMs:base-1000,ctimeMs:base-1000,dev:1,ino:2});
function calls(n=4,cmd=command) {
  return Array.from({length:n},(_,i)=>[
    {type:'tool/call',seq:i*2+1,time:base+i*70000,data:{name:'bash',callId:'call-'+i,arguments:JSON.stringify({command:cmd})}},
    {type:'tool/result',seq:i*2+2,time:base+i*70000+60000,data:{message:{content:[{type:'tool-result',toolCallId:'call-'+i,content:[{type:'text',text:'2\n[Command finished with exit code 0]'}]}]}}}
  ]).flat();
}
const serialize=rows=>rows.map(row=>JSON.stringify(row)).join('\n')+'\n';
function compositeCalls(n=4,count='1',text=log) {
  const rows=calls(n,compositeCommand);
  for(const row of rows.filter(r=>r.type==='tool/result'))row.data.message.content[0].content[0].text=
    count+'\n'+text+'[Command finished with exit code 0]';
  return rows;
}
function fixture() {
  const runId=randomUUID(),runFence=randomUUID(),workspace='progress-tests/example',rows=calls();
  const state=metadata(JSON.stringify({runId,runFence,workspace,runtime:'harness',state:'running'}));
  const trace=metadata(serialize(rows)),evidence=metadata(log);
  const process={unit:`agentos-run-${runId}.service`,mainPid:123,activeState:'active',subState:'running',startTicks:'456',controlGroup:`/system.slice/agentos-run-${runId}.service`};
  const files=[];
  const io={readFile:async(file,root,max)=>{files.push({file,root,max});return file.endsWith('state.json')?state:file.endsWith('.jsonl')?trace:evidence;},inspectProcess:async()=>process};
  return {runId,runFence,workspace,rows,state,trace,evidence,process,io,files};
}
test('parser admits explicit literal read-only polling and rejects shell effects, aliases and secret paths',()=>{
  assert.equal(parseReadOnlyPoll(command).path,'evidence/test.log');
  assert.equal(parseReadOnlyPoll('cd /workspace && wc -l evidence/test.log').mode,'line-count');
  assert.equal(parseReadOnlyPoll('cd /workspace && tail -n 4 evidence/test.log').lines,4);
  for(const cmd of ['grep -c "CASE" evidence/test.log',command+'; rm -rf x',command+' > evidence/out',command+' | cat',command.replace('evidence/test.log','../test.log'),command.replace('evidence/test.log','evidence/.env'),command.replace('"CASE"','"$(id)"'),command.replace('evidence/test.log','/etc/passwd')])assert.equal(parseReadOnlyPoll(cmd),null,cmd);
});

test('recognizes the actual compound Wild poll using only a fixed literal read-only pipeline',()=>{
  const poll=parseReadOnlyPoll(compositeCommand);
  assert.equal(poll.mode,'process-count-tail');assert.equal(poll.lines,30);
  assert.equal(poll.path,'evidence/integration/r-wild-bandito-stable-cdp.log');
  assert.equal(poll.commandDigest,'ad1bf7237b7645378cc9155b11daaf18ecd490dfa935959e192f19e855b39306');
  assert.equal(parseReadOnlyPoll(compositeCommand.replace('tail -30','tail -n 30')).lines,30);
  assert.equal(parseReadOnlyPoll(compositeCommand.replace('sleep 290; ','' )).mode,'process-count-tail');
});

test('compound grammar refuses mutations, expansions, alternate pipes and unbound paths',()=>{
  const bad=[compositeCommand+'; touch evidence/changed',compositeCommand+' > evidence/out',
    compositeCommand+' &',compositeCommand+' | cat',compositeCommand.replace('sleep 290','sleep $(id)'),
    compositeCommand.replace('stable-cdp','`id`'),compositeCommand.replace('stable-cdp','${FILTER}'),
    compositeCommand.replace('stable-cdp','"stable-cdp"'),compositeCommand.replace('stable-cdp','-f'),
    compositeCommand.replace('stable-cdp','stable*'),compositeCommand.replace('ps aux','ps aux; kill 1'),
    compositeCommand.replace('grep -v grep','tee evidence/out'),compositeCommand.replace('grep -v grep','grep -v other'),
    compositeCommand.replace('wc -l','xargs sh'),compositeCommand.replace('wc -l','wc -l 2>&1'),
    compositeCommand.replace('tail -30','tail -f'),compositeCommand.replace('tail -30','tail -1000'),
    compositeCommand.replace('/workspace/evidence/','evidence/'),
    compositeCommand.replace('/workspace/evidence/','/etc/'),
    compositeCommand.replace('/workspace/evidence/','/workspace/../evidence/'),
    compositeCommand.replace('/workspace/evidence/','/workspace/evidence//'),
    compositeCommand.replace('r-wild-bandito-stable-cdp.log','.env'),
    compositeCommand.replace('; tail','\nrm -f evidence/test.log; tail')];
  for(const cmd of bad)assert.equal(parseReadOnlyPoll(cmd),null,cmd);
});

test('compound proof binds exact count plus file tail and still requires an immutable explicit failure',()=>{
  const rows=compositeCalls(),text=serialize(rows),window=analyzePollTrace(text);
  assert.ok(window);const proof=proveFailedPolls(window,metadata(log),metadata(text));
  assert.equal(proof.completedPolls,4);assert.equal(proof.failure.matches,1);
  assert.equal(proof.file.path,'evidence/integration/r-wild-bandito-stable-cdp.log');
  assert.equal(proof.resultDigest,progressDigest('1\n'+log+'[Command finished with exit code 0]'));
  assert.equal(JSON.stringify(proof).includes('stable-cdp |'),false);
  const healthy='CASE PASS boot\nCASE PASS candidate\n',healthyTrace=serialize(compositeCalls(4,'1',healthy));
  assert.equal(proveFailedPolls(analyzePollTrace(healthyTrace),metadata(healthy),metadata(healthyTrace)),null);
  assert.equal(proveFailedPolls(window,{...metadata(log),ctimeMs:base+1},metadata(text)),null);
  assert.equal(proveFailedPolls(window,metadata(log+'CASE PASS progressed\n'),metadata(text)),null);
});

test('process counts are observations, never a substitute for failed evidence or complete results',()=>{
  for(const count of ['0','1','   2']) {
    const text=serialize(compositeCalls(4,count));
    assert.ok(proveFailedPolls(analyzePollTrace(text),metadata(log),metadata(text)));
  }
  for(const count of ['running','-1','1\nextra','1 2','1000000']) {
    const text=serialize(compositeCalls(4,count));
    assert.equal(proveFailedPolls(analyzePollTrace(text),metadata(log),metadata(text)),null,count);
  }
  const changed=compositeCalls();changed.at(-1).data.message.content[0].content[0].text='2\n'+log+'[Command finished with exit code 0]';
  assert.equal(analyzePollTrace(serialize(changed)),null);
  const partial=compositeCalls();partial.at(-1).data.message.content[0].partial=true;
  assert.equal(analyzePollTrace(serialize(partial)),null);
  const few=serialize(compositeCalls(3));assert.equal(analyzePollTrace(few),null);
});

test('compound tail is validated against the last requested lines, with no hidden result suffix',()=>{
  const file=Array.from({length:40},(_,i)=>i===0?'CASE FAIL previous check':'line '+i).join('\n')+'\n';
  const tail=file.trimEnd().split('\n').slice(-30).join('\n')+'\n';
  const text=serialize(compositeCalls(4,'1',tail));
  assert.ok(proveFailedPolls(analyzePollTrace(text),metadata(file),metadata(text)));
  const wrong=serialize(compositeCalls(4,'1',tail+'unexpected\n'));
  assert.equal(proveFailedPolls(analyzePollTrace(wrong),metadata(file),metadata(wrong)),null);
});

test('inspector proves the compound poll with unchanged state, trace, file and physical identity',async()=>{
  const f=fixture();f.trace=metadata(serialize(compositeCalls()));
  f.io.readFile=async file=>file.endsWith('state.json')?f.state:file.endsWith('.jsonl')?f.trace:f.evidence;
  const proof=await inspectRuntimeProgress(f.runId,f.workspace,f.io);
  assert.equal(proof.stalled,true);assert.equal(proof.evidence.completedPolls,4);
  assert.equal(proof.process.startTicks,f.process.startTicks);assert.equal(proof.state.runFence,f.runFence);
  let observed=0;f.io.readFile=async file=>file.endsWith('state.json')?f.state:file.endsWith('.jsonl')?f.trace:
    (++observed===1?f.evidence:{...f.evidence,digest:'f'.repeat(64)});
  assert.equal((await inspectRuntimeProgress(f.runId,f.workspace,f.io)).stalled,false);
});
test('four completed identical polls plus immutable explicit failure prove a stall without exposing content',()=>{
  const rows=calls(),text=serialize(rows),proof=proveFailedPolls(analyzePollTrace(text),metadata(log),metadata(text));
  assert.equal(proof.completedPolls,4);assert.equal(proof.failure.matches,1);
  assert.equal(proof.pendingPollId,null);assert.equal(proof.file.digest,progressDigest(log));
  const result=JSON.stringify(proof);assert.equal(result.includes('materialization'),false);assert.equal(result.includes('sleep 295'),false);
});
test('one identical pending poll is identified but never counted; another action breaks the proof',()=>{
  const rows=calls(5).slice(0,-1),text=serialize(rows),proof=proveFailedPolls(analyzePollTrace(text),metadata(log),metadata(text));
  assert.equal(proof.completedPolls,4);assert.equal(proof.pendingPollId,'call-4');
  rows.at(-1).data.arguments=JSON.stringify({command:'cd /workspace && node tests/check.mjs'});
  assert.equal(analyzePollTrace(serialize(rows)),null);
});
for(const [name,change] of [
  ['acknowledgment',rows=>rows.at(-1).data.message.content[0].content[0].text='started'],
  ['partial output',rows=>rows.at(-1).data.message.content[0].content[0].text='partial output\n2\n[Command finished with exit code 0]'],
  ['truncated metadata',rows=>rows.at(-1).data.message.content[0].truncated=true],
  ['nonzero exit',rows=>rows.at(-1).data.message.content[0].content[0].text='2\n[Command finished with exit code 1]'],
  ['error result',rows=>rows.at(-1).data.message.content[0].isError=true],
  ['duplicate receipt',rows=>rows.push({...rows.at(-1),seq:9,time:base+400000})],
  ['out-of-order sequence',rows=>rows.at(-1).seq=2],
  ['out-of-order time',rows=>rows.at(-1).time=base],
  ['changed output',rows=>rows.at(-1).data.message.content[0].content[0].text='3\n[Command finished with exit code 0]'],
  ['new instruction',rows=>rows.push({type:'agent/inbox/spliced'})],
  ['completed turn',rows=>rows.push({type:'turn/end'})],
])test(name+' never becomes stalled evidence',()=>{const rows=calls();change(rows);assert.equal(analyzePollTrace(serialize(rows)),null);});
test('age, no output, failing test alone and healthy long-running test do not establish a stall',()=>{
  assert.equal(analyzePollTrace(serialize(calls(3))),null);
  const short=calls();short.forEach((row,i)=>row.time=base+i);assert.equal(analyzePollTrace(serialize(short)),null);
  const text=serialize(calls()),window=analyzePollTrace(text);
  assert.equal(proveFailedPolls(window,metadata('CASE PASS boot\nCASE PASS candidate\n'),metadata(text)),null);
  assert.equal(proveFailedPolls(window,{...metadata(log),ctimeMs:base+1},metadata(text)),null);
  assert.equal(proveFailedPolls(window,metadata(log+'CASE PASS new progress\n'),metadata(text)),null);
});
test('a different tool or parallel tool between equivalent polls resets the observation window',()=>{
  for(const data of [{name:'write_file',callId:'write',arguments:'{}'},{name:'bash',callId:'other',arguments:JSON.stringify({command})}]) {
    const rows=calls();rows.splice(5,0,{type:'tool/call',seq:5.5,time:base+180000,data});
    rows.forEach((row,i)=>row.seq=i+1);assert.equal(analyzePollTrace(serialize(rows)),null);
  }
});
test('inspector reads bounded exact state/trace/evidence and produces only hashes with process identity',async()=>{
  const f=fixture(),result=await inspectRuntimeProgress(f.runId,f.workspace,f.io);
  assert.equal(result.stalled,true);assert.equal(result.state.runFence,f.runFence);assert.equal(result.policy,PROGRESS_POLICY);
  assert.equal(result.process.startTicks,'456');assert.equal(result.evidence.completedPolls,4);
  assert.equal(f.files.length,6);assert.ok(f.files.every(f=>f.max<=16*1024*1024));
  assert.equal(JSON.stringify(result).includes('materialization'),false);
});
for(const kind of ['state','trace','evidence','process'])test('inspector refuses '+kind+' changes during evidence collection',async()=>{
  const f=fixture(),seen=new Map();
  const read=f.io.readFile;
  f.io.readFile=async(...args)=>{const value=await read(...args),n=(seen.get(args[0])??0)+1;seen.set(args[0],n);return n===2&&value===f[kind]?{...value,digest:'f'.repeat(64)}:value;};
  let processes=0;if(kind==='process')f.io.inspectProcess=async()=>({...f.process,mainPid:++processes===1?123:124});
  assert.equal((await inspectRuntimeProgress(f.runId,f.workspace,f.io)).stalled,false);
});
test('wrong run/workspace, cancelled state, unreadable file and future events fail closed',async()=>{
  for(const mutate of [f=>f.state=metadata(JSON.stringify({runId:randomUUID(),workspace:f.workspace,runtime:'harness',state:'running'})),f=>f.state=metadata(JSON.stringify({runId:f.runId,workspace:f.workspace,runtime:'harness',state:'cancelled'})),f=>f.trace=metadata(serialize(calls().map(row=>({...row,time:row.time+10000000}))))]) {
    const f=fixture();mutate(f);f.io.readFile=async(file)=>file.endsWith('state.json')?f.state:file.endsWith('.jsonl')?f.trace:f.evidence;
    assert.equal((await inspectRuntimeProgress(f.runId,f.workspace,f.io)).stalled,false);
  }
  const f=fixture();f.io.readFile=async()=>{throw Error('private bytes must not escape');};
  const result=await inspectRuntimeProgress(f.runId,f.workspace,f.io);assert.equal(result.stalled,false);assert.equal(JSON.stringify(result).includes('private bytes'),false);
});
test('checkpoint is persisted before caller can act and binds exact fence, evidence and process',async()=>{
  const f=fixture(),proof=await inspectRuntimeProgress(f.runId,f.workspace,f.io);let written;
  const result=await checkpointRuntimeProgress(proof,f,new AbortController().signal,{inspect:async()=>proof,writeCheckpoint:async bytes=>{written=bytes;}});
  assert.equal(result.digest,progressDigest(written));assert.equal(JSON.parse(written).runFence,f.runFence);
  assert.equal(result.name,'progress-stall.json');assert.equal(JSON.parse(written).proof.stalled,true);
});
test('checkpoint never overwrites and cancellation, stale evidence or changed fence never authorizes action',async()=>{
  const f=fixture(),proof=await inspectRuntimeProgress(f.runId,f.workspace,f.io),abort=new AbortController();abort.abort();let writes=0;
  const io={inspect:async()=>proof,writeCheckpoint:async()=>{writes++;}};
  await assert.rejects(checkpointRuntimeProgress(proof,f,abort.signal,io),/CANCELLED/);
  await assert.rejects(checkpointRuntimeProgress(proof,{...f,runFence:randomUUID()},null,io),/PROGRESS_PROOF_STALE/);
  await assert.rejects(checkpointRuntimeProgress(proof,f,null,{...io,inspect:async()=>({...proof,checkedAt:'invalid'})}),/PROGRESS_PROOF_STALE/);
  await assert.rejects(checkpointRuntimeProgress(proof,f,null,{...io,inspect:async()=>({...proof,process:{...proof.process,mainPid:999}})}),/PROGRESS_PROOF_STALE/);
  assert.equal(writes,0);
  await assert.rejects(checkpointRuntimeProgress(proof,f,null,{...io,writeCheckpoint:async()=>{throw Object.assign(Error('exists'),{code:'EEXIST'});}}),{code:'EEXIST'});
  assert.equal(sameProgressProof(proof,{...proof,evidence:{...proof.evidence,resultDigest:'e'.repeat(64)}}),false);
});
test('real file reader rejects symlinks, directories, malformed UTF-8 and over-limit evidence',async t=>{
  const root=await mkdtemp(tmpdir()+'/agentos-progress-');t.after(()=>rm(root,{recursive:true,force:true}));
  await mkdir(root+'/evidence');await writeFile(root+'/evidence/test.log',log);
  const actual=await readProgressFile(root+'/evidence/test.log',root,4096);assert.equal(actual.digest,progressDigest(log));
  await symlink(root+'/evidence/test.log',root+'/evidence/linked.log');
  await assert.rejects(readProgressFile(root+'/evidence/linked.log',root,4096),/UNSAFE_PROGRESS_PATH/);
  await assert.rejects(readProgressFile(root+'/evidence',root,4096),/PROGRESS_FILE_UNAVAILABLE/);
  await assert.rejects(readProgressFile(root+'/evidence/test.log',root,2),/PROGRESS_FILE_UNAVAILABLE/);
  await writeFile(root+'/evidence/binary.log',Buffer.from([0xff,0xfe]));
  await assert.rejects(readProgressFile(root+'/evidence/binary.log',root,4096));
});
