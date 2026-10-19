import {createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {z} from 'zod';
import {config} from './config.js';
import {DomainError} from './contracts.js';

export const operationsPrefix='/api/operations/v1';
export const operationHash=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
export const canonicalOperationValue=(value:any):any=>Array.isArray(value)?value.map(canonicalOperationValue):
 value&&typeof value==='object'&&!(value instanceof Date)?Object.fromEntries(Object.keys(value).sort().map(k=>[k,canonicalOperationValue(value[k])])):value;
export const operationDigest=(value:unknown)=>operationHash(JSON.stringify(canonicalOperationValue(value)));
export type OperationsPrincipal={id:string;goalIds:string[]|null;projects:string[];scopes:('observe'|'evidence')[]};
const principals=new WeakMap<object,OperationsPrincipal>();
const serviceCredentials=z.array(z.object({id:z.string().regex(/^[a-z0-9][a-z0-9._-]{0,79}$/),
 tokenSha256:z.string().regex(/^[a-f0-9]{64}$/),goalIds:z.array(z.uuid()).max(100).default([]),
 projects:z.array(z.string().min(1).max(160)).max(30).default([]),
 scopes:z.array(z.enum(['observe','evidence'])).min(1).max(2)}).strict()).max(30);
const same=(a:string,b:string)=>a.length===b.length&&timingSafeEqual(Buffer.from(a),Buffer.from(b));

/** Optional read-only service credentials. Never authorize any legacy/admin route. */
export function authorizeOperationsRequest(req:{url:string;method:string;headers:Record<string,any>}) {
 const pathname=req.url.split('?')[0];
 if(!pathname.startsWith(operationsPrefix+'/'))return false;
 if(req.method!=='GET'&&!(req.method==='POST'&&pathname===operationsPrefix+'/evidence/read'))return false;
 const token=String(req.headers.authorization??'').match(/^Bearer ([^\s]{20,2048})$/)?.[1];
 const encoded=process.env.AGENTOS_OPERATIONS_ACCESS_JSON;
 if(!token||!encoded)return false;
 let credentials:z.infer<typeof serviceCredentials>;
 try{credentials=serviceCredentials.parse(JSON.parse(encoded));}
 catch{throw new DomainError('OPERATIONS_ACCESS_CONFIGURATION','A configuração de acesso operacional é inválida.',503);}
 const digest=operationHash(token),found=credentials.find(c=>same(c.tokenSha256,digest));
 if(!found)return false;
 principals.set(req,{id:found.id,goalIds:found.goalIds,projects:found.projects,scopes:found.scopes});
 return true;
}

/** Legacy callers still pass the application's existing authentication hook. */
export function operationsPrincipal(req:object):OperationsPrincipal {
 return principals.get(req)??{id:'existing-agentos-operator',goalIds:null,projects:[],scopes:['observe','evidence']};
}
export function assertOperationsScope(principal:OperationsPrincipal,goal:{id:string;project:string},scope:'observe'|'evidence'='observe') {
 if(!principal.scopes.includes(scope)||principal.goalIds!==null&&!principal.goalIds.includes(goal.id)&&!principal.projects.includes(goal.project))
  throw new DomainError('OPERATIONS_SCOPE','Este principal não possui acesso a este objetivo.',403);
}

/** Apply to the complete bounded source BEFORE pagination, including multiline secrets. */
export function redactOperationsText(value:string) {
 return value.replace(/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)/g,'[REDACTED_PRIVATE_KEY]')
  .replace(/\bBearer\s+[A-Za-z0-9._~+\/-]+/gi,'Bearer [REDACTED]')
  .replace(/\b(?:sk|rk|ghp|github_pat)[-_][A-Za-z0-9._-]{8,}/gi,'[REDACTED_TOKEN]')
  .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g,'[REDACTED_JWT]')
  .replace(/(["']?(?:api[_ -]?key|password|passwd|secret|client_secret|access_token|refresh_token|authorization|cookie)["']?\s*[:=]\s*)("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s,;}]+)/gi,'$1"[REDACTED]"')
  .replace(/https?:\/\/[^\s<>"')]+/gi,'[REDACTED_URL]')
  .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi,'[REDACTED_EMAIL]')
  .replace(/[A-Za-z0-9_~+\/-]{48,}={0,2}/g,'[REDACTED_LONG_VALUE]')
  .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g,'');
}
export const operationalLabel=(value:unknown,max=160)=>typeof value==='string'?redactOperationsText(value).slice(0,max):null;
export const operationalCode=(value:unknown)=>typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,100}$/.test(value)&&redactOperationsText(value)===value?value:null;

const baselineSchema=z.object({revision:z.string().regex(/^[a-f0-9]{64}$/),
 sections:z.record(z.string().max(50),z.string().regex(/^[a-f0-9]{64}$/))}).strict();
const cursorSchema=z.object({version:z.literal(1),goalId:z.uuid(),principal:z.string().regex(/^[a-f0-9]{64}$/),
 issuedAt:z.number().int(),expiresAt:z.number().int(),baseline:baselineSchema}).strict();
export type OperationsBaseline=z.infer<typeof baselineSchema>;
function principalDigest(principal:OperationsPrincipal){return operationDigest({...principal,goalIds:principal.goalIds?.slice().sort()??null,
 projects:[...principal.projects].sort(),scopes:[...principal.scopes].sort()});}
export function encodeOperationsCursor(goalId:string,principal:OperationsPrincipal,baseline:OperationsBaseline,now=Date.now()) {
 const payload=Buffer.from(JSON.stringify({version:1,goalId,principal:principalDigest(principal),issuedAt:now,expiresAt:now+3600000,baseline})).toString('base64url');
 return payload+'.'+createHmac('sha256',config.apiToken).update('agentos-operations-v1:'+payload).digest('base64url');
}
export function decodeOperationsCursor(raw:string,goalId:string,principal:OperationsPrincipal,now=Date.now()) {
 const invalid=()=>new DomainError('OPERATIONS_CURSOR_INVALID','Cursor inválido para este objetivo ou principal.',400);
 if(raw.length>8192)throw invalid();
 const parts=raw.split('.');if(parts.length!==2||!/^[A-Za-z0-9_-]+$/.test(parts[0])||!/^[A-Za-z0-9_-]{43}$/.test(parts[1]))throw invalid();
 const signature=createHmac('sha256',config.apiToken).update('agentos-operations-v1:'+parts[0]).digest('base64url');
 if(!same(signature,parts[1]))throw invalid();
 let value:z.infer<typeof cursorSchema>;try{value=cursorSchema.parse(JSON.parse(Buffer.from(parts[0],'base64url').toString('utf8')));}catch{throw invalid();}
 if(value.goalId!==goalId||value.principal!==principalDigest(principal)||value.issuedAt>now+5000)throw invalid();
 if(value.expiresAt<now)throw new DomainError('OPERATIONS_CURSOR_EXPIRED','O cursor expirou; consulte um novo snapshot.',409);
 return value.baseline;
}
