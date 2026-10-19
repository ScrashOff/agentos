import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {createOperationsClient,createOperationsMcpHandler,operationsToolRequest,operationsTools} from '../scripts/agentos-operations-client.mjs';

const goal='11111111-1111-4111-8111-111111111111',run='22222222-2222-4222-8222-222222222222';
const token='test-only-observer-credential-1234567890';
test('four read-only tools map to fixed scoped routes and reject arbitrary commands/URLs',()=>{
 assert.equal(operationsTools.length,4);assert.ok(operationsTools.every(t=>t.annotations.readOnlyHint&&!t.annotations.destructiveHint));
 assert.deepEqual(operationsToolRequest('agentos_objective_snapshot',{goalId:goal}),{method:'GET',path:`/goals/${goal}/snapshot`});
 assert.equal(operationsToolRequest('agentos_objective_changes',{goalId:goal,cursor:'a.b?x=2'}).path,`/goals/${goal}/changes?cursor=a.b%3Fx%3D2`);
 assert.equal(operationsToolRequest('agentos_inspect_work_item',{goalId:goal,kind:'run',id:run}).path,`/goals/${goal}/items/run/${run}`);
 for(const args of [{goalId:goal,url:'https://elsewhere.invalid'},{goalId:'../../admin'},{goalId:goal,command:'retry'}])
  assert.throws(()=>operationsToolRequest('agentos_objective_snapshot',args),/OPERATIONS_ARGUMENTS_INVALID/);
 assert.throws(()=>operationsToolRequest('retry',{goalId:goal}),/OPERATIONS_TOOL_UNKNOWN/);
});
test('private URL, static origin and credential requirements cannot be overridden by tool arguments',()=>{
 for(const baseUrl of ['http://public.example','https://public.example','https://user:pass@host.ts.net','https://host.ts.net/?token=anything','https://host.ts.net/admin'])
  assert.throws(()=>createOperationsClient({baseUrl,token}),/OPERATIONS_PRIVATE_URL_REQUIRED/);
 assert.throws(()=>createOperationsClient({token:'short'}),/OPERATIONS_CREDENTIAL_REQUIRED/);
 assert.ok(createOperationsClient({baseUrl:'https://private.ts.net:9445/api/operations/v1',token}));
});
test('HTTP client uses header credential, prohibits redirects and returns structured API values',async()=>{
 let captured;
 const client=createOperationsClient({token,fetchImpl:async(url,options)=>{captured={url,options};return new Response(JSON.stringify({schemaVersion:1,goalId:goal}),{status:200});}});
 const result=await client.call('agentos_objective_snapshot',{goalId:goal});
 assert.equal(result.goalId,goal);assert.equal(captured.options.headers.Authorization,'Bearer '+token);
 assert.equal(captured.options.redirect,'error');assert.ok(!captured.url.includes(token));assert.ok(captured.options.signal);
});
test('HTTP failure and accidental credential echo never emit raw body/headers',async()=>{
 const failed=createOperationsClient({token,fetchImpl:async()=>new Response(JSON.stringify({error:'OPERATIONS_SCOPE',message:'private failure detail'}),{status:403})});
 await assert.rejects(failed.capabilities(),error=>error.message==='OPERATIONS_HTTP_403:OPERATIONS_SCOPE');
 const echo=createOperationsClient({token,fetchImpl:async()=>new Response(JSON.stringify({schemaVersion:1,echo:token}))});
 await assert.rejects(echo.capabilities(),error=>error.message==='OPERATIONS_CREDENTIAL_ECHO_REJECTED'&&!error.message.includes(token));
 const escaped=Array.from(token,char=>'\\u'+char.charCodeAt(0).toString(16).padStart(4,'0')).join('');
 const escapedEcho=createOperationsClient({token,fetchImpl:async()=>new Response('{"schemaVersion":1,"echo":"'+escaped+'"}')});
 await assert.rejects(escapedEcho.capabilities(),error=>error.message==='OPERATIONS_CREDENTIAL_ECHO_REJECTED'&&!error.message.includes(token));
 const transport=createOperationsClient({token,fetchImpl:async()=>{throw Error('connection error containing '+token);}});
 await assert.rejects(transport.capabilities(),error=>error.message==='OPERATIONS_TRANSPORT_UNAVAILABLE');
});
test('HTTP response and evidence page budgets are bounded',async()=>{
 const large=createOperationsClient({token,fetchImpl:async()=>new Response('x'.repeat(256*1024+1))});
 await assert.rejects(large.capabilities(),/OPERATIONS_RESPONSE_TOO_LARGE/);
 const reference={kind:'artifact',artifactId:run,expectedDigest:'a'.repeat(64)};
 assert.throws(()=>operationsToolRequest('agentos_read_evidence',{goalId:goal,reference,limit:8001}),/OPERATIONS_ARGUMENTS_INVALID/);
 assert.throws(()=>operationsToolRequest('agentos_read_evidence',{goalId:goal,reference:{...reference,path:'/etc/private'}}),/OPERATIONS_ARGUMENTS_INVALID/);
});
test('MCP initializes, advertises only four read-only tools and returns structured results',async()=>{
 let calls=0;
 const handle=createOperationsMcpHandler({call:async(name,args)=>{calls++;return {schemaVersion:1,name,goalId:args.goalId};}});
 const init=await handle({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-11-25',capabilities:{},clientInfo:{name:'test',version:'1'}}});
 assert.equal(init.result.protocolVersion,'2025-11-25');assert.deepEqual(init.result.capabilities,{tools:{listChanged:false}});
 assert.equal(await handle({jsonrpc:'2.0',method:'notifications/initialized'}),null);
 const list=await handle({jsonrpc:'2.0',id:2,method:'tools/list'});assert.equal(list.result.tools.length,4);
 const result=await handle({jsonrpc:'2.0',id:3,method:'tools/call',params:{name:'agentos_objective_snapshot',arguments:{goalId:goal}}});
 assert.equal(result.result.structuredContent.goalId,goal);assert.equal(JSON.parse(result.result.content[0].text).goalId,goal);assert.equal(calls,1);
});
test('MCP refuses premature/unknown mutations and sanitizes failed calls',async()=>{
 const handle=createOperationsMcpHandler({call:async()=>{throw Error('private arbitrary stack '+token);}});
 assert.equal((await handle({jsonrpc:'2.0',id:1,method:'tools/list'})).error.code,-32002);
 await handle({jsonrpc:'2.0',id:2,method:'initialize',params:{protocolVersion:'2024-11-05'}});
 await handle({jsonrpc:'2.0',method:'notifications/initialized'});
 assert.equal((await handle({jsonrpc:'2.0',id:3,method:'tools/call',params:{name:'cancel_task'}})).error.code,-32602);
 const failure=await handle({jsonrpc:'2.0',id:4,method:'tools/call',params:{name:'agentos_objective_snapshot',arguments:{goalId:goal}}});
 assert.equal(failure.result.isError,true);assert.equal(failure.result.content[0].text,'OPERATIONS_REQUEST_FAILED');
 assert.ok(!JSON.stringify(failure).includes(token));assert.ok(!('structuredContent' in failure.result));
});
test('real stdio process drains oversized messages and performs scoped reads without exposing credentials',{timeout:15000},async()=>{
 const received=[];
 const server=createServer((req,res)=>{received.push({url:req.url,authorization:req.headers.authorization});res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify({schemaVersion:1,goalId:goal,readOnly:true}));});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 let child,deadline;try{
  const port=server.address().port,script=fileURLToPath(new URL('../scripts/agentos-operations.mjs',import.meta.url));
  child=spawn(process.execPath,[script,'--mcp-stdio'],{env:{...process.env,AGENTOS_OPERATIONS_URL:`http://127.0.0.1:${port}`,AGENTOS_OPERATIONS_TOKEN:token},stdio:['pipe','pipe','pipe'],windowsHide:true});
  deadline=setTimeout(()=>child.kill(),10000);
  let stdout='',stderr='';child.stdout.on('data',chunk=>{stdout+=chunk;});child.stderr.on('data',chunk=>{stderr+=chunk;});
  const exit=new Promise((resolve,reject)=>{child.on('error',reject);child.on('close',resolve);});
  child.stdin.write('x'.repeat(40000)+'\n');child.stdin.end([
   {jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-11-25'}},
   {jsonrpc:'2.0',method:'notifications/initialized'},
   {jsonrpc:'2.0',id:2,method:'tools/list'},
   {jsonrpc:'2.0',id:3,method:'tools/call',params:{name:'agentos_objective_snapshot',arguments:{goalId:goal}}},
  ].map(value=>JSON.stringify(value)).join('\n')+'\n');
  assert.equal(await exit,0);assert.equal(stderr,'');assert.ok(!stdout.includes(token));
  const messages=stdout.trim().split('\n').map(value=>JSON.parse(value));assert.equal(messages.length,4);assert.equal(messages[0].error.code,-32600);assert.equal(messages[3].result.structuredContent.goalId,goal);
  assert.deepEqual(received,[{url:`/api/operations/v1/goals/${goal}/snapshot`,authorization:'Bearer '+token}]);
 }finally{clearTimeout(deadline);if(child&&child.exitCode===null)child.kill();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});
