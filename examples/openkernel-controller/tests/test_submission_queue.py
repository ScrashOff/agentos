"""CPU-only regression tests, injected provider and disposable durable state."""
import copy
import datetime
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import submission_queue as sq

P = 'muse_bf16_proj'
IDENT = {'goal':'goal-1','task':'task-1','run':'run-1','fence':'fence-1'}
NOW = 1793275200.0

def provider():
    return {'tracks':{'kernels':{'open':True}},'problems':[
        {'name':p,'limits':{'checks':{'per_day':20,'used':0,'left':20},'scored':{'per_day':10,'used':0,'left':10}}}
        for p in sorted(sq.KNOWN_PROBLEMS)]}

def job(job_id, problem=P, mode='check'):
    return {'id':job_id,'board':problem,'mode':mode,'content_hash':'e'*64,'status':'done',
        'result':{'status':'ok','passed':True,'checks':{'ok':True,'flagged':[],'rejected':[]},
                  'returncode':0,'error':None,'flags':[],'speedup':1.2,'speedup_ci':[1.1,1.3]}}

class Transport:
    def __init__(self, response=None):
        self.provider=provider();self.posts=[];self.gets=[];self.jobs=[];self.results={};self.response=response;self.callback=None
    def get(self,path):
        self.gets.append(path)
        if path=='/api/v1/problems':return 200,copy.deepcopy(self.provider)
        if path=='/api/v1/jobs':return 200,{'jobs':copy.deepcopy(self.jobs)}
        return (200,copy.deepcopy(self.results[path])) if path in self.results else (404,{})
    def post(self,path,payload):
        self.posts.append((path,copy.deepcopy(payload)))
        if self.callback:self.callback(path,payload)
        if isinstance(self.response,Exception):raise self.response
        return self.response or (200,{'job_id':'job-'+str(len(self.posts)),'content_hash':'e'*64})

def create(root, text='answer = 42\n', problem=P):
    source=Path(root)/'kernels'/problem/'submission.py';source.parent.mkdir(parents=True,exist_ok=True);source.write_text(text)
    return hashlib.sha256(source.read_bytes()).hexdigest()

def queue(root,transport,clock=None,**kw):
    clock=clock or sq.Clock(NOW);registry={}
    for p in (Path(root)/'kernels').glob('*/submission.py'):
        sha=hashlib.sha256(p.read_bytes()).hexdigest()
        registry[sha]={'qa_digest':'b'*64,'artifact_digest':'a'*64,'qa_artifact_digest':'c'*64,
            'accepted':True,'independent':True,'ts':clock.time(),'performance_hypothesis':'Reviewed reduction in memory traffic.'}
    return sq.SubmissionQueue(str(Path(root)/'state'),transport,clock,workspace_root=root,identity=IDENT,
                              qa_registry=registry,**kw)

def payload(sha):return dict(IDENT,source_sha256=sha,artifact_digest='a'*64,candidate_digest='b'*64)
def doc(sha,action='submit_check',occurrence='occ-1'):
    return {'cycleId':'cycle-1','occurrenceId':occurrence,'attemptId':'attempt-1','actionKey':action,'version':1,
            'parameters':{'project':'openkernel','problem':P},'input':{'payload':payload(sha)}}

def consumer(root,sha,counter,output):
    class CountTransport(Transport):
        def post(self,path,payload):
            with counter.get_lock():counter.value+=1
            return super().post(path,payload)
    q=queue(root,CountTransport());output.put(q.submit(P,'check',payload(sha)))

class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='okq-regression-');self.addCleanup(self.tmp.cleanup)
        self.root=self.tmp.name;self.sha=create(self.root);self.t=Transport();self.q=queue(self.root,self.t)
    def submit(self,**kwargs):return self.q.submit(P,'check',payload(self.sha),**kwargs)
    def complete(self):
        result=self.submit();self.t.results['/api/v1/jobs/'+result['job_id']]=job(result['job_id'])
        observed=self.q.observe(problem=P,mode='check',sha=self.sha)
        self.assertTrue(observed['gpu_verified']);return result['job_id']
    def test_exact_bytes_and_official_mode_then_real_observe_unlocks_scored(self):
        identifier=self.complete()
        self.assertEqual(self.t.posts[0],('/api/v1/problems/'+P+'/submissions',{'code':'answer = 42\n','mode':'check'}))
        again=queue(self.root,self.t)
        result=again.submit(P,'scored',payload(self.sha));self.assertTrue(result['ok']);self.assertEqual(self.t.posts[-1][1]['mode'],'run')
        receipt=json.loads((Path(self.root)/'state/results'/f'{identifier}.json').read_text())
        self.assertTrue(receipt['result']['checks']['ok'])
        self.assertEqual(again._load_state()['entries'][f'{P}|check|{self.sha}']['source_sha256'],self.sha)
        self.assertNotEqual(self.sha,receipt['content_hash'])
    def test_two_processes_one_post_and_one_duplicate(self):
        ctx=multiprocessing.get_context('fork');counter=ctx.Value('i',0);output=ctx.Queue()
        processes=[ctx.Process(target=consumer,args=(self.root,self.sha,counter,output)) for _ in range(2)]
        for p in processes:p.start()
        for p in processes:p.join(10);self.assertEqual(p.exitcode,0)
        rows=[output.get(timeout=2) for _ in processes]
        self.assertEqual(counter.value,1);self.assertEqual(sum(r['ok'] for r in rows),1)
    def test_intent_is_durable_before_post(self):
        def before(path,body):
            state=self.q._load_state();entry=state['entries'][f'{P}|check|{self.sha}']
            self.assertEqual(entry['state'],'INTENT');self.assertEqual(self.q.journal.records()[-1]['event'],'intent')
        self.t.callback=before;self.assertTrue(self.submit()['ok'])
    def test_unbound_or_unaccepted_candidates_never_reach_network(self):
        variants=[]
        for field in ('goal','task','run','fence','source_sha256','artifact_digest','candidate_digest'):
            value=payload(self.sha);value.pop(field);variants.append(value)
        for value in variants:self.assertFalse(self.q.submit(P,'check',value)['ok'])
        self.q.qa_registry[self.sha]['accepted']=False;self.assertFalse(self.submit()['ok'])
        self.q.qa_registry[self.sha]['accepted']=True;self.q.qa_registry[self.sha]['independent']=False;self.assertFalse(self.submit()['ok'])
        self.assertEqual(self.t.gets,[]);self.assertEqual(self.t.posts,[])
    def test_stale_future_qa_and_wrong_artifact_rejected(self):
        for ts in (NOW-8*86400,NOW+1):
            self.q.qa_registry[self.sha]['ts']=ts;self.assertFalse(self.submit()['ok'])
        self.q.qa_registry[self.sha]['ts']=NOW;self.q.qa_registry[self.sha]['artifact_digest']='d'*64
        self.assertFalse(self.submit()['ok']);self.assertEqual(self.t.gets,[])
    def test_symlink_and_fifo_are_not_candidate_sources(self):
        source=Path(self.root)/'kernels'/P/'submission.py';source.unlink();source.symlink_to('/etc/passwd')
        self.assertFalse(self.submit()['ok']);source.unlink();os.mkfifo(source)
        self.assertFalse(self.submit()['ok']);self.assertEqual(self.t.gets,[])
    def test_source_swap_after_validation_cannot_change_posted_bytes(self):
        get=self.t.get
        def mutate(path):
            create(self.root,'changed_after_validation = True\n');return get(path)
        self.t.get=mutate
        self.assertTrue(self.submit()['ok']);self.assertEqual(self.t.posts[0][1]['code'],'answer = 42\n')
    def test_generic_timeout_and_5xx_keep_unknown_across_restart_and_new_day(self):
        for response in (RuntimeError('post may have reached provider'),sq.TransportUnknownOutcome('private'),(500,{'error':'upstream'}),(200,{})):
            with self.subTest(response=type(response).__name__),tempfile.TemporaryDirectory() as root:
                sha=create(root);t=Transport(response);clock=sq.Clock(NOW);q=queue(root,t,clock)
                result=q.submit(P,'check',payload(sha));self.assertEqual(result['code'],'unknown_outcome');self.assertEqual(result['effect'],'write')
                clock.advance(86400);restarted=queue(root,t,clock)
                self.assertEqual(restarted.submit(P,'check',payload(sha))['code'],'unknown_outcome');self.assertEqual(len(t.posts),1)
                self.assertEqual(restarted.reconcile(P,'check',sha)['code'],'unknown_outcome')
    def test_only_explicit_transport_no_effect_releases_intent(self):
        self.t.response=sq.TransportNoEffect('local preflight');result=self.submit()
        self.assertEqual(result['effect'],'none');self.assertEqual(self.q._load_state()['entries'],{})
        self.assertEqual(self.q._load_state()['rate'],[])
        self.t.response=None;self.assertTrue(self.submit()['ok'])
    def test_confirmed_http_rejection_causes_are_distinct(self):
        for status,code in [(400,'invalid_candidate'),(401,'auth_rejected'),(403,'auth_rejected'),(409,'track_closed'),(429,'rate_limited')]:
            with self.subTest(status=status),tempfile.TemporaryDirectory() as root:
                sha=create(root);t=Transport((status,{}));q=queue(root,t)
                result=q.submit(P,'check',payload(sha));self.assertEqual(result['code'],code);self.assertEqual(result['effect'],'none')
                self.assertEqual(q._load_state()['entries'],{})
    def test_state_corruption_and_missing_state_after_journal_fail_closed(self):
        self.assertTrue(self.submit()['ok']);state=Path(self.q._state_path());state.write_text('{broken')
        with self.assertRaises(sq.StateUnavailable):queue(self.root,self.t).submit(P,'check',payload(self.sha))
        state.unlink()
        with self.assertRaises(sq.StateUnavailable):queue(self.root,self.t).submit(P,'check',payload(self.sha))
        self.assertEqual(len(self.t.posts),1)
    def test_persistence_failure_after_ack_keeps_effect_write_and_blocks_replay(self):
        save=self.q._save_state
        def fail_after_ack(state):
            if any(e['state']=='SUBMITTED' for e in state['entries'].values()):raise OSError('disk full')
            save(state)
        self.q._save_state=fail_after_ack
        result=sq.run_cycle_action(doc(self.sha),self.q);self.assertEqual(result['effect'],'write');self.assertEqual(result['code'],'unknown_outcome')
        restarted=queue(self.root,self.t);self.assertEqual(restarted.submit(P,'check',payload(self.sha))['code'],'unknown_outcome');self.assertEqual(len(self.t.posts),1)
    def test_post_journal_failure_does_not_change_ack_to_effect_none(self):
        journal=self.q._journal
        def fail(event,**kw):
            if event=='job_id':raise OSError('journal full')
            return journal(event,**kw)
        self.q._journal=fail
        result=sq.run_cycle_action(doc(self.sha),self.q);self.assertEqual(result['effect'],'write')
        self.assertEqual(queue(self.root,self.t).submit(P,'check',payload(self.sha))['code'],'duplicate');self.assertEqual(len(self.t.posts),1)
    def test_live_quota_exhaustion_and_track_closed_prevent_post(self):
        row=next(x for x in self.t.provider['problems'] if x['name']==P);row['limits']['checks'].update(used=20,left=0)
        self.assertEqual(self.submit()['code'],'quota_exhausted');self.assertEqual(self.t.posts,[])
        row['limits']['checks'].update(used=0,left=20);self.t.provider['tracks']['kernels']['open']=False
        self.assertEqual(self.submit()['code'],'track_closed');self.assertEqual(self.t.posts,[])
    def test_local_quota_floor_and_utc_reset_with_new_candidate(self):
        row=next(x for x in self.t.provider['problems'] if x['name']==P);row['limits']['checks'].update(per_day=1,used=0,left=1)
        self.assertTrue(self.submit()['ok']);sha2=create(self.root,'answer=43\n');q2=queue(self.root,self.t,self.q.clock)
        self.assertEqual(q2.submit(P,'check',payload(sha2))['code'],'quota_exhausted')
        self.q.clock.advance(86400);q3=queue(self.root,self.t,self.q.clock)
        self.assertTrue(q3.submit(P,'check',payload(sha2))['ok'])
    def test_daily_budget_is_per_problem(self):
        self.assertTrue(self.submit()['ok']);other='muse_decode_layer';sha=create(self.root,'other = 1\n',other)
        row=next(x for x in self.t.provider['problems'] if x['name']==other);row['limits']['checks'].update(per_day=1,used=0,left=1)
        self.assertTrue(queue(self.root,self.t).submit(other,'check',payload(sha))['ok'])
    def test_rate_six_persists_across_instances_and_expires(self):
        clock=self.q.clock
        for i in range(6):
            sha=create(self.root,f'answer={i}\n');self.assertTrue(queue(self.root,self.t,clock).submit(P,'check',payload(sha))['ok'])
        sha=create(self.root,'answer=7\n');q=queue(self.root,self.t,clock)
        self.assertEqual(q.submit(P,'check',payload(sha))['code'],'rate_limited')
        clock.advance(61);self.assertTrue(q.submit(P,'check',payload(sha))['ok'])
    def test_external_jobs_share_rate_limit_without_private_counter(self):
        timestamp=datetime.datetime.fromtimestamp(NOW-2,datetime.timezone.utc).isoformat()
        self.t.jobs=[{'id':f'external-{i}','created_at':timestamp} for i in range(6)]
        self.assertEqual(self.submit()['code'],'rate_limited');self.assertEqual(self.t.posts,[])
    def test_official_numeric_epoch_jobs_schema_is_supported(self):
        # Actual API receipts use fractional Unix seconds, not ISO-only strings.
        self.q.clock=sq.Clock(1793277913.0)
        self.q.qa_registry[self.sha]['ts']=self.q.clock.time()
        self.t.jobs=[{'id':f'fixture-{i}','created_at':1793277911.824655} for i in range(6)]
        self.assertEqual(self.submit()['code'],'rate_limited');self.assertEqual(self.t.posts,[])
        self.q.clock.advance(61)
        self.assertTrue(self.submit()['ok'])
    def test_invalid_or_nonfinite_job_timestamp_fails_closed(self):
        for value in (None,True,[],float('nan'),float('inf'),'not-a-date'):
            self.t.jobs=[{'id':'fixture','created_at':value}]
            self.assertEqual(self.submit()['code'],'provider_recent_jobs_unavailable')
        self.assertEqual(self.t.posts,[])
    def test_official_jobs_collection_uses_submitted_at_and_job_id(self):
        self.q.clock=sq.Clock(1793278723.0);self.q.qa_registry[self.sha]['ts']=self.q.clock.time()
        self.t.jobs=[{'mode':'check','job_id':f'fixture-{i}','problem':P,'submitted_at':1793278721.1952398,
            'status':'done','passed':True,'score_us':1.2,'score':1.0,'result_status':'ok','flags':[]} for i in range(6)]
        self.assertEqual(self.submit()['code'],'rate_limited');self.assertEqual(self.t.posts,[])
        self.q.clock.advance(61);self.assertTrue(self.submit()['ok'])
    def test_dry_run_does_not_reserve_budget_rate_or_entry(self):
        for _ in range(10):self.assertTrue(self.submit(dry_run=True)['ok'])
        self.assertFalse(Path(self.q._state_path()).exists());self.assertEqual(self.t.posts,[])
    def test_verified_terminal_candidate_never_resubmitted(self):
        self.complete();self.assertEqual(self.submit()['code'],'duplicate');self.assertEqual(len(self.t.posts),1)
    def test_pending_and_bad_terminal_results_cannot_unlock_scored(self):
        identifier=self.submit()['job_id']
        variants=[]
        for field,value in [('status','failed'),('passed',False),('returncode',1),('error','failed correctness'),('flags',['flag'])]:
            row=job(identifier);row['result'][field]=value;variants.append(row)
        for field in ('flagged','rejected'):
            row=job(identifier);row['result']['checks'][field]=['issue'];variants.append(row)
        for level in ('body','result'):
            for field in ('flags','flagged','rejected'):
                row=job(identifier);target=row if level=='body' else row['result'];target[field]=['issue'];variants.append(row)
        pending=job(identifier);pending['status']='running';variants.append(pending)
        for row in variants:
            self.t.results['/api/v1/jobs/'+identifier]=row
            observed=self.q.observe(problem=P,mode='check',sha=self.sha);self.assertFalse(observed['gpu_verified'])
            self.assertEqual(self.q.submit(P,'scored',payload(self.sha))['code'],'check_required')
    def test_foreign_job_and_provider_hash_mismatch_rejected(self):
        identifier=self.submit()['job_id']
        for field,value in [('id','other-job'),('board','muse_moe_w4a16'),('mode','run'),('content_hash','f'*64)]:
            row=job(identifier);row[field]=value;self.t.results['/api/v1/jobs/'+identifier]=row
            self.assertFalse(self.q.observe(problem=P,mode='check',sha=self.sha)['ok'])
        before=len(self.t.gets);self.assertFalse(self.q.observe(job_id='foreign')['ok']);self.assertEqual(len(self.t.gets),before)
    def test_changed_source_cannot_adopt_prior_successful_check(self):
        self.complete();sha=create(self.root,'answer=99\n')
        self.assertEqual(queue(self.root,self.t).submit(P,'scored',payload(sha))['code'],'check_required')
    def test_scored_requires_reviewed_hypothesis(self):
        self.complete();self.q.qa_registry[self.sha].pop('performance_hypothesis')
        self.assertEqual(self.q.submit(P,'scored',payload(self.sha))['code'],'reviewed_performance_hypothesis_required')
    def test_changed_action_payload_cannot_replay_receipt(self):
        original=doc(self.sha);self.assertTrue(sq.run_cycle_action(original,self.q)['ok'])
        changed=copy.deepcopy(original);changed['input']['payload']['candidate_digest']='d'*64
        self.assertFalse(sq.run_cycle_action(changed,self.q)['ok']);self.assertEqual(len(self.t.posts),1)
    def test_actual_cycle_action_key_is_not_dispatch_operation(self):
        value=doc(self.sha);value['actionKey']='occ-1:submit-bf16';value['parameters']['action']='submit_scored'
        result=sq.run_cycle_action(value,self.q,operation='submit_check')
        self.assertTrue(result['ok']);self.assertEqual(self.t.posts[0][1]['mode'],'check')
        value['artifacts']=[{'id':'changed-qa-artifact'}]
        self.assertFalse(sq.run_cycle_action(value,self.q,operation='submit_check')['ok']);self.assertEqual(len(self.t.posts),1)
    def test_private_result_redacts_tokens_and_has_private_mode(self):
        identifier=self.submit()['job_id'];row=job(identifier);row['result']['log_tail']='Bearer fixture-private-token';row['headers']={'Authorization':'secret'}
        self.t.results['/api/v1/jobs/'+identifier]=row
        with mock.patch.dict(os.environ,{sq.TOKEN_ENV:'fixture-private-token'}):
            observed=self.q.observe(problem=P,mode='check',sha=self.sha)
        receipt=Path(self.root)/'state/results'/f'{identifier}.json'
        self.assertNotIn('fixture-private-token',receipt.read_text());self.assertNotIn('Authorization',receipt.read_text())
        self.assertNotIn('fixture-private-token',json.dumps(observed));self.assertEqual(receipt.stat().st_mode & 0o077,0)
    def test_real_system_clock_moves_and_binding_is_pinned_external(self):
        clock=sq.system_clock()
        with mock.patch('submission_queue.time.time',return_value=5):self.assertEqual(clock.time(),5)
        with mock.patch('submission_queue.time.time',return_value=8):self.assertEqual(clock.time(),8)
        file=Path(self.root)/'binding.json';file.write_text(json.dumps({'project':'openkernel','workspace_root':self.root}));file.chmod(0o600)
        sha=hashlib.sha256(file.read_bytes()).hexdigest()
        with self.assertRaises(ValueError):sq.load_binding(str(file),sha)
        with self.assertRaises(ValueError):sq.load_binding(str(file),'f'*64)

if __name__=='__main__':unittest.main()
