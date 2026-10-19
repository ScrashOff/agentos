"""Apply hash-bound accepted patches in an owned, stopped AgentOS snapshot.
No shell, report commands, network, production source or worker acceptance.
"""
import base64,hashlib,json,os,pathlib,re,subprocess,tempfile

sha=lambda b:hashlib.sha256(b).hexdigest()
def decode(ref):
    b=base64.b64decode(ref['base64'],validate=True)
    if len(b)>8*1024*1024 or sha(b)!=ref['digest']: raise ValueError('COMPOSITION_INPUT_DIGEST')
    return b
def safe_path(value):
    p=pathlib.PurePosixPath(value)
    if not re.fullmatch(r'[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*',value) or p.is_absolute(): return False
    return not any(x in {'.','..','.git','assets','node_modules','evidence','data','rules','.agentos-snapshot.json'} or
                   x.startswith('.env') or x in {'.ssh','.aws','.codex','.claude'} for x in p.parts)
def patch_paths(patch):
    paths=set()
    if b'GIT binary patch' in patch or b'new file mode 120000' in patch or b'old mode' in patch or b'new mode' in patch:
        raise ValueError('COMPOSITION_PATCH_TYPE')
    for line in patch.decode('utf-8').splitlines():
        if line.startswith(('--- ','+++ ')):
            name=line[4:].split('\t')[0]
            if name=='/dev/null': continue
            if not name.startswith(('a/','b/')) or not safe_path(name[2:]): raise ValueError('COMPOSITION_PATCH_PATH')
            paths.add(name[2:])
        if line.startswith(('rename from ','rename to ','copy from ','copy to ')):
            raise ValueError('COMPOSITION_PATCH_TYPE')
    if not paths or len(paths)>128: raise ValueError('COMPOSITION_PATCH_PATH')
    return paths
def compose(params,workspace,locks,sources,source_hash,data):
    dry_run=params.get('dryRun') is True
    plan=params.get('composition',{})
    version=plan.get('version')
    if version not in [1,2] or not isinstance(plan.get('patches'),list) or not 0<len(plan['patches'])<=128:
        raise ValueError('COMPOSITION_FORMAT')
    selection=plan.get('selection') if version==2 else None
    if version==2:
        target_proof=selection.get('target') if isinstance(selection,dict) else None
        active=selection.get('active') if isinstance(selection,dict) else None
        superseded=selection.get('superseded') if isinstance(selection,dict) else None
        selection_body={k:v for k,v in selection.items() if k!='digest'} if isinstance(selection,dict) else {}
        selection_digest=hashlib.sha256(json.dumps(selection_body,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        if (selection.get('version')!=1 or selection.get('authority')!='supervisor-effective-source-manifest' or
            not isinstance(selection.get('consumerTaskId'),str) or not isinstance(selection.get('consumerRunId'),str) or
            not isinstance(selection.get('consumerRunFence'),str) or selection.get('workspace')!=params.get('workspace') or
            selection.get('digest')!=selection_digest or not isinstance(target_proof,dict) or
            target_proof.get('consumerTaskId')!=selection.get('consumerTaskId') or
            target_proof.get('workspace')!=params.get('workspace') or
            not isinstance(target_proof.get('consumerRunId'),str) or
            not re.fullmatch('[a-f0-9]{64}',plan.get('expectedCandidateDigest','')) or
            plan.get('expectedCandidateDigest')!=target_proof.get('candidateDigest') or
            not re.fullmatch('[a-f0-9]{64}',target_proof.get('candidateArtifactDigest','')) or
            not isinstance(target_proof.get('candidateArtifactId'),str) or
            not isinstance(active,list) or not isinstance(superseded,list)):
            raise ValueError('COMPOSITION_SELECTION_INVALID')
        active_ids=[]
        for item in active:
            if (not isinstance(item,dict) or not isinstance(item.get('producerTaskId'),str) or
                not isinstance(item.get('producerRunId'),str) or not isinstance(item.get('patchArtifactId'),str) or
                not isinstance(item.get('reportArtifactId'),str) or
                not re.fullmatch('[a-f0-9]{64}',item.get('patchDigest','')) or
                not re.fullmatch('[a-f0-9]{64}',item.get('reportDigest',''))):
                raise ValueError('COMPOSITION_SELECTION_INVALID')
            active_ids.append(item['producerTaskId'])
        superseded_ids=[]
        for item in superseded:
            if (not isinstance(item,dict) or not isinstance(item.get('producerTaskId'),str) or
                not isinstance(item.get('producerRunId'),str) or not isinstance(item.get('patchArtifactId'),str) or
                not isinstance(item.get('reportArtifactId'),str) or
                not re.fullmatch('[a-f0-9]{64}',item.get('patchDigest','')) or
                not re.fullmatch('[a-f0-9]{64}',item.get('reportDigest',''))):
                raise ValueError('COMPOSITION_SELECTION_INVALID')
            superseded_ids.append(item['producerTaskId'])
        if len(set(active_ids))!=len(active_ids) or len(set(superseded_ids))!=len(superseded_ids) or set(active_ids)&set(superseded_ids):
            raise ValueError('COMPOSITION_SELECTION_AMBIGUOUS')
        expected={(item['producerTaskId'],item['patchArtifactId'],item['patchDigest'],item['reportArtifactId'],item['reportDigest']) for item in active}
        actual={(entry.get('producerTaskId'),entry.get('patch',{}).get('id'),entry.get('patch',{}).get('digest'),
                 entry.get('report',{}).get('id'),entry.get('report',{}).get('digest')) for entry in plan['patches']}
        if expected!=actual or len(actual)!=len(plan['patches']):
            raise ValueError('COMPOSITION_SELECTION_ACTIVE_SET')
    target_id=params['workspace'];target=workspace(target_id)
    if not target.name.startswith('o') or not (target/'.agentos-snapshot.json').is_file():
        raise ValueError('COMPOSITION_UNMANAGED')
    receipts=[]
    with locks([target_id]):
        marker=json.loads((target/'.agentos-snapshot.json').read_text())
        if marker.get('workspace')!=target_id: raise ValueError('COMPOSITION_UNMANAGED')
        before=sources(target);current=dict(before)
        target_digest_mismatch=version==2 and source_hash(before)!=plan['expectedCandidateDigest']
        for entry in plan['patches']:
            ref={k:entry[k].get('id') for k in ['patch','report']}
            receipt={'producerTaskId':entry.get('producerTaskId'),'inputs':ref}
            try:
                if target_digest_mismatch:
                    receipt.update(status='conflict',code='COMPOSITION_TARGET_DIGEST_MISMATCH')
                    receipts.append(receipt);continue
                patch=decode(entry['patch']);report=json.loads(decode(entry['report']))
                names=patch_paths(patch)
                if report.get('fixVerified') is not True: raise ValueError('COMPOSITION_UNVERIFIED_REPAIR')
                hashes=report.get('sourceHashes')
                if not isinstance(hashes,list) or len(hashes)!=len(names): raise ValueError('COMPOSITION_SOURCE_PROOF')
                pairs={}
                for item in hashes:
                    name=item.get('path');pre,post=item.get('before'),item.get('after')
                    if name not in names or name in pairs or pre==post or any(v is not None and not re.fullmatch('[a-f0-9]{64}',v) for v in [pre,post]):
                        raise ValueError('COMPOSITION_SOURCE_PROOF')
                    pairs[name]=(pre,post)
                if set(pairs)!=names: raise ValueError('COMPOSITION_SOURCE_PROOF')
                observed={name:sha(current[name]) if name in current else None for name in names}
                receipt.update({'patchDigest':sha(patch),'reportDigest':entry['report']['digest'],'files':sorted(names)})
                if all(observed[name]==pairs[name][1] for name in names):
                    receipt['status']='already_applied';receipts.append(receipt);continue
                conflicts=[name for name in names if observed[name]!=pairs[name][0]]
                if conflicts:
                    receipt.update(status='conflict',code='PREIMAGE_MISMATCH',conflicts=sorted(conflicts));receipts.append(receipt);continue
                with tempfile.TemporaryDirectory(prefix='compose-',dir=data) as temporary:
                    stage=pathlib.Path(temporary)
                    for name in names:
                        if name in current:
                            dest=stage/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(current[name])
                    env={'PATH':'/usr/bin:/bin','HOME':str(stage),'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null'}
                    for args in [['--check'],[]]:
                        checked=subprocess.run(['git','-c','core.hooksPath=/dev/null','apply','--whitespace=nowarn',*args,'-'],
                            input=patch,cwd=stage,env=env,capture_output=True,timeout=30)
                        if checked.returncode: raise ValueError('COMPOSITION_PATCH_CONFLICT')
                    changes={name:(stage/name).read_bytes() if (stage/name).is_file() else None for name in names}
                    if any((sha(b) if b is not None else None)!=pairs[name][1] for name,b in changes.items()):
                        raise ValueError('COMPOSITION_POSTIMAGE_MISMATCH')
                    for name in names:
                        dest=target/name
                        if dest.parent.resolve()!=dest.parent or dest.is_symlink(): raise ValueError('COMPOSITION_TARGET_PATH')
                    # All patches are staged and verified before any target write.
                    # On a partial crash, the next call detects mixed pre/post
                    # images as a conflict, never reports a full application.
                    for name,b in changes.items():
                        dest=target/name
                        if not dry_run:
                            if b is None: dest.unlink()
                            else:
                                dest.parent.mkdir(parents=True,exist_ok=True)
                                replacement=stage/name
                                if dest.exists(): replacement.chmod(dest.stat().st_mode & 0o777)
                                os.replace(replacement,dest)
                        if b is None: current.pop(name,None)
                        else: current[name]=b
                receipt['status']='compatible' if dry_run else 'applied'
            except (ValueError,KeyError,TypeError,UnicodeError,subprocess.TimeoutExpired) as e:
                receipt.update(status='conflict',code=str(e) if isinstance(e,ValueError) and re.fullmatch('COMPOSITION_[A-Z_]+',str(e)) else 'COMPOSITION_INVALID_PROOF')
            receipts.append(receipt)
        after=sources(target)
        return {'version':1,'workspace':target_id,'beforeDigest':source_hash(before),'afterDigest':source_hash(after),
                'dryRun':dry_run,'projectedDigest':source_hash(current),
                'complete':all(r['status'] in ['applied','already_applied','compatible'] for r in receipts),
                'receipts':receipts,'acceptance':False,'independentValidationRequired':True}
