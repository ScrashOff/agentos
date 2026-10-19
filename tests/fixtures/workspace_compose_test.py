import base64,contextlib,difflib,hashlib,importlib.util,json,pathlib,tempfile,sys
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]/'bridge'))
from workspace_compose import compose
sha=lambda b:hashlib.sha256(b).hexdigest()
def ref(name,b): return {'id':name,'digest':sha(b),'base64':base64.b64encode(b).decode()}
def entry(name,before,after):
    patch=''.join(difflib.unified_diff(before.decode().splitlines(True),after.decode().splitlines(True),fromfile='a/'+name,tofile='b/'+name)).encode()
    report=json.dumps({'fixVerified':True,'sourceHashes':[{'path':name,'before':sha(before),'after':sha(after)}]}).encode()
    return {'producerTaskId':'fixture','patch':ref('patch',patch),'report':ref('report',report)}
@contextlib.contextmanager
def locks(names): yield
def sources(root): return {str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file() and not p.name.startswith('.')}
def source_hash(files): return sha(json.dumps({k:sha(v) for k,v in sorted(files.items())}).encode())
with tempfile.TemporaryDirectory() as tmp:
    root=pathlib.Path(tmp);target=root/'owned';target.mkdir();(target/'.agentos-snapshot.json').write_text('{"workspace":"fixture/owned"}')
    (target/'one.js').write_bytes(b'original\n');(target/'two.js').write_bytes(b'original\n')
    first=entry('one.js',b'original\n',b'fixed\n');second=entry('two.js',b'original\n',b'also fixed\n')
    def run(entries,dry=False,version=1,expected=None,selection=None):
        plan={'version':version,'patches':entries}
        if version==2: plan.update(expectedCandidateDigest=expected,selection=selection)
        return compose({'workspace':'fixture/owned','dryRun':dry,'composition':plan},lambda _:target,locks,sources,source_hash,root)
    snapshot=sources(target)
    checked=run([first,second],True)
    assert checked['dryRun'] and checked['complete'] and checked['beforeDigest']==checked['afterDigest']
    assert checked['projectedDigest']!=checked['afterDigest'] and sources(target)==snapshot
    assert [r['status'] for r in checked['receipts']]==['compatible','compatible']
    # A lying postimage cannot be approved even by the read-only check.
    bad=entry('one.js',b'original\n',b'fixed\n');report=json.loads(base64.b64decode(bad['report']['base64']));report['sourceHashes'][0]['after']='0'*64;bad['report']=ref('report',json.dumps(report).encode())
    assert not run([bad],True)['complete'] and sources(target)==snapshot
    result=run([first,second]);assert result['complete'] and [r['status'] for r in result['receipts']]==['applied','applied']
    assert result['acceptance'] is False and result['independentValidationRequired'] is True
    assert [r['status'] for r in run([first,second])['receipts']]==['already_applied','already_applied']
    conflict=entry('one.js',b'original\n',b'incompatible\n');result=run([conflict]);assert not result['complete'] and result['receipts'][0]['code']=='PREIMAGE_MISMATCH'
    assert (target/'one.js').read_bytes()==b'fixed\n'
    corrupt={**first,'patch':{**first['patch'],'digest':'0'*64}};assert run([corrupt])['receipts'][0]['code']=='COMPOSITION_INPUT_DIGEST'
    mismatch=entry('two.js',b'also fixed\n',b'new\n');report=json.loads(base64.b64decode(mismatch['report']['base64']));report['sourceHashes'][0]['after']='0'*64;mismatch['report']=ref('report',json.dumps(report).encode())
    assert run([mismatch])['receipts'][0]['code']=='COMPOSITION_POSTIMAGE_MISMATCH' and (target/'two.js').read_bytes()==b'also fixed\n'
    for name in ['../outside','.git/config','assets/game.js','.env','rules/rtp.js','.agentos-snapshot.json']:
        assert run([entry(name,b'original\n',b'forbidden\n')])['receipts'][0]['code']=='COMPOSITION_PATCH_PATH'
    (target/'.agentos-snapshot.json').unlink()
    try: run([first]);raise AssertionError('unowned target accepted')
    except ValueError as e: assert str(e)=='COMPOSITION_UNMANAGED'
    (target/'.agentos-snapshot.json').write_text('{"workspace":"fixture/owned"}')
    # The old accepted producer is retained as proof but is explicitly superseded.
    # Only the new patch, whose sourceHashes match this stopped candidate, is active.
    stale=entry('one.js',b'original\n',b'obsolete\n')
    active=entry('one.js',b'fixed\n',b'fixed-again\n')
    before=source_hash(sources(target))
    selection={'version':1,'authority':'supervisor-effective-source-manifest','consumerTaskId':'consumer',
      'consumerRunId':'run-next','consumerRunFence':'fence-next','workspace':'fixture/owned',
      'target':{'workspace':'fixture/owned','consumerTaskId':'consumer','consumerRunId':'run-current','candidateArtifactId':'candidate-current',
        'candidateArtifactDigest':'a'*64,'candidateDigest':before},
      'active':[{'producerTaskId':'new-producer','producerRunId':'new-run','patchArtifactId':'patch',
        'patchDigest':active['patch']['digest'],'reportArtifactId':'report','reportDigest':active['report']['digest']}],
      'superseded':[{'producerTaskId':'old-producer','producerRunId':'old-run','patchArtifactId':'patch',
        'patchDigest':stale['patch']['digest'],'reportArtifactId':'report','reportDigest':stale['report']['digest']}]}
    # New authority metadata is covered by the existing bridge selection digest.
    # The bridge still checks the actual candidate and patch images under its lock.
    def witness(obligation,event,run,attempt,revision):
        return {'version':1,'obligationId':obligation,'eventId':event,'caseId':'case-'+event,
          'decisionRunId':'review-'+event,'consumerRunId':run,'consumerAttempt':attempt,
          'revisionId':'revision-'+str(revision),'revisionOriginId':'origin','revisionNumber':revision}
    selection['targetAuthority']={'version':1,'authority':'applied_consumer_revision',
      'consumerRunId':'run-current','consumerAttempt':6,'revisionOriginId':'origin',
      'targetResolutionWitnesses':[witness('target','106','run-current',6,12)],
      'historicalResolutionWitnesses':[witness('old-'+str(i),str(100+i),'run-'+str(i),i,i*2) for i in range(1,6)]}
    selection['digest']=sha(json.dumps(selection,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())
    selected={'producerTaskId':'new-producer','patch':active['patch'],'report':active['report']}
    altered=json.loads(json.dumps(selection));altered['targetAuthority']['historicalResolutionWitnesses'][0]['revisionNumber']=12
    unchanged=sources(target)
    try: run([selected],version=2,expected=before,selection=altered);raise AssertionError('unhashed authority change accepted')
    except ValueError as e: assert str(e)=='COMPOSITION_SELECTION_INVALID'
    assert sources(target)==unchanged
    applied=run([selected],version=2,expected=before,selection=selection)
    assert applied['complete'] and applied['receipts'][0]['status']=='applied'
    assert (target/'one.js').read_bytes()==b'fixed-again\n'
    # Candidate drift is caught under the workspace lock before any patch write.
    (target/'one.js').write_bytes(b'drifted\n')
    drift_snapshot=sources(target);drifted_digest=source_hash(drift_snapshot)
    stale_expected=run([selected],version=2,expected=before,selection=selection)
    assert not stale_expected['complete'] and stale_expected['receipts'][0]['code']=='COMPOSITION_TARGET_DIGEST_MISMATCH'
    assert sources(target)==drift_snapshot
    assert source_hash(sources(target))==drifted_digest
    forged={**selection,'active':[{'producerTaskId':'foreign-producer','producerRunId':'foreign-run',
      'patchArtifactId':'patch','patchDigest':active['patch']['digest'],'reportArtifactId':'report',
      'reportDigest':active['report']['digest']}]}
    forged.pop('digest');forged['digest']=sha(json.dumps(forged,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())
    try: run([selected],version=2,expected=before,selection=forged);raise AssertionError('forged selected set accepted')
    except ValueError as e: assert str(e)=='COMPOSITION_SELECTION_ACTIVE_SET'
print(json.dumps({'passed':True,'checks':15,'productionTouched':False}))
