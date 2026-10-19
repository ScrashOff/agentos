#!/usr/bin/env node
import {createOperationsClient,createOperationsMcpHandler} from './agentos-operations-client.mjs';

// Bound bytes before assembling a line. Oversized messages are drained through
// their newline, so a client cannot force an unbounded readline allocation.
async function* boundedLines(input,max=32768){
 let parts=[],size=0,dropping=false;
 for await(const chunk of input){
  const bytes=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);let start=0;
  while(start<bytes.length){
   const end=bytes.indexOf(10,start),part=bytes.subarray(start,end<0?bytes.length:end);
   if(!dropping){if(size+part.length>max){parts=[];size=0;dropping=true;}else{parts.push(part);size+=part.length;}}
   if(end<0)break;
   yield dropping?null:Buffer.concat(parts,size).toString('utf8');parts=[];size=0;dropping=false;start=end+1;
  }
 }
 if(dropping)yield null;else if(size)yield Buffer.concat(parts,size).toString('utf8');
}

try{
 const client=createOperationsClient({baseUrl:process.env.AGENTOS_OPERATIONS_URL,
  token:process.env.AGENTOS_OPERATIONS_TOKEN});
 if(process.argv[2]==='--mcp-stdio'){
  if(process.argv.length!==3)throw Error('OPERATIONS_ARGUMENTS_INVALID');
  const handle=createOperationsMcpHandler(client);
  for await(const line of boundedLines(process.stdin)){
   if(line===null){process.stdout.write(JSON.stringify({jsonrpc:'2.0',id:null,error:{code:-32600,message:'Message too large'}})+'\n');continue;}
   let request;try{request=JSON.parse(line);}catch{process.stdout.write(JSON.stringify({jsonrpc:'2.0',id:null,error:{code:-32700,message:'Parse error'}})+'\n');continue;}
   const result=await handle(request);if(result)process.stdout.write(JSON.stringify(result)+'\n');
  }
 }else if(process.argv[2]==='capabilities'&&process.argv.length===3){
  process.stdout.write(JSON.stringify(await client.capabilities())+'\n');
 }else if(process.argv[2]==='call'&&process.argv.length===5){
  let args;try{args=JSON.parse(process.argv[4]);}catch{throw Error('OPERATIONS_ARGUMENTS_INVALID');}
  process.stdout.write(JSON.stringify(await client.call(process.argv[3],args))+'\n');
 }else throw Error('OPERATIONS_USAGE: capabilities | call <read-only-tool-name> <JSON> | --mcp-stdio');
}catch(error){
 process.stderr.write(error instanceof Error&&/^OPERATIONS_[A-Z0-9_: <>|a-z-]+$/.test(error.message)?error.message+'\n':'OPERATIONS_CLIENT_FAILED\n');
 process.exitCode=1;
}
