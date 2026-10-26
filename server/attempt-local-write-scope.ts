/** These roots are excluded from source snapshots/patches by workspace-manager.
 * Their names may coincide in different isolated task workspaces. This grants
 * no source permission, shared storage or implicit transfer to a consumer. */
export function isAttemptLocalWriteScope(value:string):boolean{
 if(!/^[a-zA-Z0-9._/-]+$/.test(value)||value.startsWith('/')||value.includes('//')||value.includes('..')||value.split('/').some(part=>['.','.git','node_modules','assets'].includes(part)))return false;
 const root=value.replace(/\/+$/,'').split('/')[0];
 return root==='evidence'||root==='data';
}
