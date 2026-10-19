import type {DB} from './db.js';
import {
  managerContextSectionNames, managerContextSectionDigest, managerContextSectionByteLimit,
  type ManagerContextSectionBlob, type ManagerContextSectionName,
} from './manager-context-sections.js';

export {managerContextSectionByteLimit} from './manager-context-sections.js';
const pageCharacters = 12000;
const names = new Set<string>(managerContextSectionNames);
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const sha256 = /^[a-f0-9]{64}$/;

/** Sections contain the exact JSON spelling, not a PostgreSQL JSON value.
 * This preserves escaped NUL and lone surrogates. Reader offsets are UTF-16
 * code units of that spelling; a page may split a literal surrogate pair. */
function validBlob(blob:ManagerContextSectionBlob):boolean {
  if(!blob || !names.has(blob.section) || typeof blob.text!=='string' ||
    !sha256.test(blob.digest) || !Number.isSafeInteger(blob.characters) ||
    blob.characters!==blob.text.length || Buffer.byteLength(blob.text,'utf8')>managerContextSectionByteLimit ||
    managerContextSectionDigest(blob.section,blob.text)!==blob.digest)return false;
  try { return JSON.stringify(JSON.parse(blob.text))===blob.text; }
  catch { return false; }
}

/** Called only by canonical schema preparation, never by a context read. */
export async function installManagerContextSections(db:DB) {
  await db.query(`CREATE TABLE IF NOT EXISTS management_context_sections(
    goal_id uuid NOT NULL REFERENCES goals(id),
    digest text NOT NULL CHECK(digest ~ '^[a-f0-9]{64}$'),
    section text NOT NULL CHECK(section IN (${managerContextSectionNames.map(name=>"'"+name+"'").join(',')})),
    text text NOT NULL CHECK(octet_length(text)<=${managerContextSectionByteLimit}),
    characters integer NOT NULL CHECK(characters>=0 AND characters<=${managerContextSectionByteLimit}),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(goal_id,digest)
  )`);
}

/** The caller owns the final source-CAS transaction. No overwrite, invented
 * run, event, memory or outbox is created. Any conflict aborts that transaction. */
export async function persistManagerContextSections(db:DB,goalId:string,blobs:readonly ManagerContextSectionBlob[]) {
  if(!uuid.test(goalId) || !Array.isArray(blobs) || blobs.length>managerContextSectionNames.length)
    throw Error('MANAGER_CONTEXT_SECTION_INVALID');
  const sections=new Set<string>();
  for(const blob of blobs) {
    if(typeof blob?.text==='string' && Buffer.byteLength(blob.text,'utf8')>managerContextSectionByteLimit)
      throw Error('MANAGER_CONTEXT_SECTION_TOO_LARGE');
    if(!validBlob(blob) || sections.has(blob.section))throw Error('MANAGER_CONTEXT_SECTION_INVALID');
    sections.add(blob.section);
  }
  // A deterministic key order also avoids deadlocks for repeated multi-section
  // inserts. Read-after-conflict checks the immutable winner, including bytes.
  for(const blob of [...blobs].sort((a,b)=>a.digest.localeCompare(b.digest))) {
    await db.query(`INSERT INTO management_context_sections(goal_id,digest,section,text,characters)
      VALUES($1,$2,$3,$4,$5) ON CONFLICT(goal_id,digest) DO NOTHING`,
      [goalId,blob.digest,blob.section,blob.text,blob.characters]);
    const stored=(await db.query(`SELECT section,digest,characters,
      CASE WHEN octet_length(text)<=$3 THEN text END text
      FROM management_context_sections WHERE goal_id=$1 AND digest=$2`,
      [goalId,blob.digest,managerContextSectionByteLimit])).rows[0];
    if(!stored || !validBlob(stored) || stored.section!==blob.section || stored.text!==blob.text ||
      stored.characters!==blob.characters)throw Error('MANAGER_CONTEXT_SECTION_CONFLICT');
  }
}

export type ManagerContextSectionRequest={section:ManagerContextSectionName;digest:string;offset:number};

/** Exact goal/key lookup, bounded verification, then UTF-16 paging. No writes
 * or live source reconstruction; a stored reference is not decision authority. */
export async function readManagerContextSection(db:DB,goalId:string,request:ManagerContextSectionRequest) {
  if(!uuid.test(goalId) || !request || !names.has(request.section) || !sha256.test(request.digest) ||
    !Number.isSafeInteger(request.offset) || request.offset<0)
    return {status:'INVALID_REQUEST' as const};
  const row=(await db.query(`SELECT section,digest,characters,
    CASE WHEN octet_length(text)<=$3 THEN text END text
    FROM management_context_sections WHERE goal_id=$1 AND digest=$2`,
    [goalId,request.digest,managerContextSectionByteLimit])).rows[0];
  if(!row)return {status:'NOT_FOUND' as const};
  if(row.section!==request.section || !validBlob(row))return {status:'INTEGRITY_FAILED' as const};
  const identity={digest:row.digest as string,totalCharacters:row.characters as number};
  if(request.offset>row.characters)return {...identity,status:'PAGE_OUT_OF_RANGE' as const,nextOffset:0};
  const text:string=row.text.slice(request.offset,request.offset+pageCharacters);
  const end=request.offset+text.length;
  return {...identity,status:'OK' as const,text,nextOffset:end<row.characters?end:null,truncated:end<row.characters};
}
