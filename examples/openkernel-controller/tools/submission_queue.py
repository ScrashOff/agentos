#!/usr/bin/env python3
"""Controlled kernel adapter: trusted binding, durable effects, no implicit retry."""
from __future__ import annotations
import argparse
import datetime
import fcntl
import hashlib
import json
import math
import os
import re
import stat
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager

STATE_DIR_ENV = "OPENKERNEL_QUEUE_STATE"
TOKEN_ENV = "OPENKERNEL_CONTROLLER_TOKEN"
DEFAULT_DAILY_LIMITS = {"check": 20, "scored": 10}  # informational; live limits decide
DEFAULT_RATE_LIMITS = {"post_per_min_token": 6, "post_per_min_account": 10}
KNOWN_PROBLEMS = {"muse_attention_decode", "muse_moe_w4a16", "muse_prefill_layer", "muse_decode_layer", "muse_bf16_proj"}
CAUSES = {"AUTH":"auth_rejected", "RATE_LIMIT":"rate_limited", "TRACK_CLOSED":"track_closed",
          "INVALID_CANDIDATE":"invalid_candidate", "QUOTA_EXHAUSTED":"quota_exhausted",
          "UNCERTAIN":"unknown_outcome", "SCOPE":"scope_rejected", "CHECK_REQUIRED":"check_required",
          "DUPLICATE":"duplicate", "BAD_INPUT":"bad_input", "OK":"ok"}
HEX = re.compile(r"^[a-f0-9]{64}$")
JOB = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
MAX_SOURCE = 1024 * 1024

class TransportNoEffect(Exception):
    """Local preflight failed before entering the network send operation."""
class TransportUnknownOutcome(Exception):
    """A write may have reached the provider. Never authorize a replay."""
class TransportReadError(Exception):
    """Read-only transport failed; no submission was attempted."""
class StateUnavailable(Exception):
    pass

def redact(text, secrets=()):
    text = re.sub(r"(?i)bearer\s+[^\s\"']+", "Bearer [REDACTED]", str(text))
    text = re.sub(r"(?i)(authorization\s*[:=]\s*)[^\r\n]+", r"\1[REDACTED]", text)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return text

def sha256_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def read_source(root, problem):
    if problem not in KNOWN_PROBLEMS or os.path.abspath(root) != os.path.realpath(root):
        raise ValueError("scope_rejected: source scope")
    opened = []
    try:
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW); opened.append(fd)
        for part in ("kernels", problem):
            fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd); opened.append(fd)
        fd = os.open("submission.py", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd); opened.append(fd)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("scope_rejected: not a regular source")
        pieces = []; total = 0
        while True:
            block = os.read(fd, min(65536, MAX_SOURCE + 1 - total))
            if not block:
                break
            pieces.append(block); total += len(block)
            if total > MAX_SOURCE:
                raise ValueError("invalid_candidate: source too large")
        raw = b"".join(pieces)
        return raw, raw.decode("utf-8")
    except (OSError, UnicodeError):
        raise ValueError("scope_rejected: source unavailable") from None
    finally:
        for fd in reversed(opened):
            os.close(fd)

def safe_source_path(root, problem):
    read_source(root, problem)
    return os.path.join(os.path.abspath(root), "kernels", problem, "submission.py")

def utc_day(ts):
    return time.strftime("%Y-%m-%d", time.gmtime(ts))

def atomic_json(path, value):
    directory = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".durable-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(value, f, sort_keys=True, allow_nan=False); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
        dfd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(dfd)
        finally: os.close(dfd)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

class JsonlJournal:
    def __init__(self, path): self.path = path
    def append(self, record):
        raw = (json.dumps(record, sort_keys=True, allow_nan=False) + "\n").encode()
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        try:
            while raw:
                raw = raw[os.write(fd, raw):]
            os.fsync(fd)
        finally: os.close(fd)
    def records(self):
        if not os.path.exists(self.path): return []
        try:
            with open(self.path) as f: return [json.loads(x) for x in f if x.strip()]
        except (OSError, ValueError): raise StateUnavailable("journal_unavailable") from None

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): return None

class HttpTransport:
    """One HTTPS request, no redirect/proxy/retry. Loopback is for CPU fixtures only."""
    def __init__(self, base_url="https://openkernelchallenge.dev", token_env=TOKEN_ENV,
                 timeout=30, max_response_bytes=1024*1024, allow_loopback=False):
        parsed = urllib.parse.urlsplit(base_url)
        official = parsed.scheme == "https" and parsed.netloc == "openkernelchallenge.dev"
        local = allow_loopback and parsed.scheme == "http" and parsed.hostname == "127.0.0.1" and not parsed.username and not parsed.password
        if not (official or local) or parsed.path not in ("", "/", "/api/v1") or parsed.query or parsed.fragment:
            raise TransportNoEffect("transport_origin_rejected")
        self.base_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
        self.token_env = token_env; self.timeout = timeout; self.max_response_bytes = max_response_bytes
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    def _request(self, method, path, payload=None):
        allowed_get = path in ("/api/v1/problems", "/api/v1/me", "/api/v1/jobs") or re.fullmatch(r"/api/v1/jobs/[A-Za-z0-9_-]{1,128}", path)
        allowed_post = any(path == "/api/v1/problems/" + p + "/submissions" for p in KNOWN_PROBLEMS)
        if not ((method == "GET" and allowed_get) or (method == "POST" and allowed_post)):
            raise TransportNoEffect("transport_path_rejected")
        token = os.environ.get(self.token_env)
        if not token or "\r" in token or "\n" in token:
            raise TransportNoEffect("controller_token_unavailable")
        try:
            data = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8") if method == "POST" else None
            request = urllib.request.Request(self.base_url + path, data=data, method=method,
                headers={"Authorization":"Bearer " + token, "Accept":"application/json", "Content-Type":"application/json", "User-Agent":"AgentOS-controlled-submission/1"})
        except Exception:
            raise TransportNoEffect("transport_input_rejected") from None
        error_type = TransportUnknownOutcome if method == "POST" else TransportReadError
        try:
            try: response = self.opener.open(request, timeout=self.timeout)
            except urllib.error.HTTPError as error: response = error
            with response:
                status_code = response.status
                raw = response.read(self.max_response_bytes + 1)
                if len(raw) > self.max_response_bytes: raise error_type("transport_response_too_large")
                try: body = json.loads(raw)
                except (ValueError, UnicodeError): raise error_type("transport_invalid_json") from None
                if not isinstance(body, (dict, list)): raise error_type("transport_invalid_shape")
                return status_code, body
        except (TransportUnknownOutcome, TransportReadError): raise
        except Exception:
            raise error_type("transport_request_incomplete") from None
    def get(self, path): return self._request("GET", path)
    def post(self, path, payload): return self._request("POST", path, payload)

class DryRunTransport:
    def __init__(self): self.calls = []
    def get(self, path): self.calls.append(("GET", path)); return 200, {"dry_run":True}
    def post(self, path, payload): raise TransportNoEffect("dry_run_has_no_post")

class Clock:
    def __init__(self, start=0.0): self._t = start
    def time(self): return self._t
    def advance(self, seconds): self._t += seconds
class SystemClock:
    def time(self): return time.time()
def system_clock(): return SystemClock()

class SubmissionQueue:
    def __init__(self, state_dir, transport, clock, account="operator", workspace_root=".",
                 identity=None, qa_registry=None, limits=None, account_lock_path=None):
        self.state_dir = os.path.abspath(state_dir)
        os.makedirs(self.state_dir, mode=0o700, exist_ok=True)
        if os.path.realpath(self.state_dir) != self.state_dir: raise ValueError("scope_rejected: state symlink")
        self.transport = transport; self.clock = clock; self.account = account
        self.workspace_root = os.path.abspath(workspace_root)
        self.identity = identity or {}; self.qa_registry = qa_registry or {}
        self.lock_path = account_lock_path or os.path.join(self.state_dir, "account.lock")
        self.journal = JsonlJournal(os.path.join(self.state_dir, "journal.jsonl"))
        self.thread_lock = threading.Lock(); self._thread_state = threading.local(); self.possible_effect = False
    @property
    def possible_effect(self): return getattr(self._thread_state,"possible_effect",False)
    @possible_effect.setter
    def possible_effect(self,value): self._thread_state.possible_effect=value
    def _secrets(self): return [os.environ.get(TOKEN_ENV), os.environ.get(getattr(self.transport, "token_env", TOKEN_ENV))]
    def _safe(self, text): return redact(text, self._secrets())
    def _scrub(self, value):
        if isinstance(value, str): return self._safe(value)
        if isinstance(value, list): return [self._scrub(x) for x in value]
        if isinstance(value, dict):
            return {self._safe(str(k)):self._scrub(v) for k,v in value.items() if str(k).lower() not in ("authorization", "token", "access_token", "headers")}
        return value
    @contextmanager
    def account_lock(self):
        with self.thread_lock:
            fd = os.open(self.lock_path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            try: fcntl.flock(fd, fcntl.LOCK_EX); yield
            finally: fcntl.flock(fd, fcntl.LOCK_UN); os.close(fd)
    def _state_path(self): return os.path.join(self.state_dir, "queue_state.json")
    def _load_state(self):
        try:
            with open(self._state_path(), encoding="utf-8") as f: state = json.load(f)
        except FileNotFoundError:
            if os.path.exists(self.journal.path): raise StateUnavailable("state_missing_after_journal") from None
            return {"version":2, "account":self.account, "entries":{}, "budgets":{}, "rate":[], "bindings":{}, "day":utc_day(self.clock.time())}
        except (OSError, ValueError): raise StateUnavailable("state_unavailable") from None
        if state.get("version") != 2 or state.get("account") != self.account or not all(isinstance(state.get(x),dict) for x in ("entries","budgets","bindings")) or not isinstance(state.get("rate"),list):
            raise StateUnavailable("state_schema_or_account_mismatch")
        return state
    def _save_state(self, state): atomic_json(self._state_path(), state)
    def _entry_key(self, problem, mode, sha): return "|".join((problem,mode,sha))
    def _roll_day(self, state):
        day = utc_day(self.clock.time())
        if state["day"] != day: state["day"] = day; state["budgets"] = {}
        return state
    def _journal(self, event, **kw): self.journal.append({"event":event,"ts":self.clock.time(),**kw})
    def _fail(self, code, effect="none", **kw): return {"ok":False,"code":code,"effect":effect,**kw}
    def _validate_candidate(self, problem, payload):
        for field in ("goal","task","run","fence"):
            if not isinstance(self.identity.get(field),str) or not self.identity[field] or payload.get(field) != self.identity[field]:
                raise ValueError("scope_rejected: immutable identity mismatch")
        raw, code = read_source(self.workspace_root, problem)
        sha = hashlib.sha256(raw).hexdigest()
        if payload.get("source_sha256") != sha: raise ValueError("invalid_candidate: source sha256 mismatch")
        qa = self.qa_registry.get(sha)
        if not isinstance(qa,dict) or qa.get("accepted") is not True or qa.get("independent") is not True:
            raise ValueError("scope_rejected: trusted acceptance and independent QA required")
        for field in ("artifact_digest","candidate_digest"):
            expected = qa.get("qa_digest") if field == "candidate_digest" else qa.get(field)
            if not isinstance(expected,str) or not HEX.fullmatch(expected) or payload.get(field) != expected:
                raise ValueError("invalid_candidate: canonical digest mismatch")
        if not isinstance(qa.get("qa_artifact_digest"),str) or not HEX.fullmatch(qa["qa_artifact_digest"]):
            raise ValueError("scope_rejected: QA artifact missing")
        age = self.clock.time() - qa.get("ts",0)
        if age < 0 or age > 7*86400: raise ValueError("scope_rejected: stale QA")
        return {"source_sha256":sha,"code":code,"qa":qa}
    def _get(self, path):
        try: return self.transport.get(path)
        except Exception: return 0, {}
    def _provider(self):
        status, body = self._get("/api/v1/problems")
        if status in (401,403): return None, self._fail(CAUSES["AUTH"])
        if status == 429: return None, self._fail(CAUSES["RATE_LIMIT"])
        if status != 200 or not isinstance(body,dict) or not isinstance(body.get("problems"),list): return None,self._fail("provider_observation_unavailable")
        return body, None
    def inspect(self, dry_run=False):
        body, failure = self._provider()
        if failure: return failure
        return {"ok":True,"code":"ok","effect":"read","tracks":self._scrub(body.get("tracks",{})),
                "problems":[{"name":p.get("name"),"limits":p.get("limits")} for p in body["problems"] if p.get("name") in KNOWN_PROBLEMS],"dry_run":dry_run}
    def _quota(self, state, problem, mode, body):
        if body.get("tracks",{}).get("kernels",{}).get("open") is not True: return None,self._fail(CAUSES["TRACK_CLOSED"])
        rows = [p for p in body["problems"] if p.get("name") == problem]
        if len(rows) != 1: return None,self._fail("provider_observation_unavailable")
        q = rows[0].get("limits",{}).get("checks" if mode == "check" else "scored",{})
        if any(type(q.get(k)) is not int or q[k] < 0 for k in ("per_day","used","left")): return None,self._fail("provider_quota_unavailable")
        key = problem + "|" + mode
        used = max(q["used"], state["budgets"].get(key,0))
        if q["left"] <= 0 or used >= q["per_day"]: return None,self._fail(CAUSES["QUOTA_EXHAUSTED"],problem=problem,mode=mode)
        return (key,used),None
    def _rate(self, state):
        now = self.clock.time()
        local = [row for row in state["rate"] if now-row["at"] < 60]
        status, body = self._get("/api/v1/jobs")
        if status != 200 or not isinstance(body,(dict,list)): return None,self._fail("provider_recent_jobs_unavailable")
        jobs = body.get("jobs") if isinstance(body,dict) else body
        if not isinstance(jobs,list): return None,self._fail("provider_recent_jobs_unavailable")
        recent = set()
        for job in jobs:
            try:
                # /jobs lists submitted_at; /jobs/{id} uses created_at.
                value=job.get("submitted_at",job.get("created_at"))
                if type(value) in (int,float):ts=float(value)
                elif isinstance(value,str):ts=datetime.datetime.fromisoformat(value.replace("Z","+00:00")).timestamp()
                else:raise ValueError("invalid_timestamp")
                if not math.isfinite(ts):raise ValueError("invalid_timestamp")
                if now-ts < 60: recent.add(str(job.get("id") or job["job_id"]))
            except (KeyError,ValueError,TypeError,OverflowError):
                return None,self._fail("provider_recent_jobs_unavailable")
        local_ids = {r["job_id"] for r in local if r.get("job_id")}
        count = len(local) + len(recent-local_ids)
        # One approved token in the account lane: six is also <= account ten.
        if count >= min(DEFAULT_RATE_LIMITS.values()): return None,self._fail(CAUSES["RATE_LIMIT"])
        return local,None
    def _uncertain(self, state, entry):
        entry["state"]="UNCERTAIN"
        self._save_state(state); self._journal("uncertain",problem=entry["problem"],mode=entry["mode"],source_sha256=entry["source_sha256"])
        return self._fail(CAUSES["UNCERTAIN"],"write",detail="reconcile existing outcome; never resend")
    def submit(self, problem, mode, payload, dry_run=False):
        self.possible_effect = False
        if mode not in ("check","scored"): return self._fail(CAUSES["BAD_INPUT"])
        try: info = self._validate_candidate(problem,payload)
        except ValueError as error:
            code=str(error).split(":",1)[0]
            return self._fail(code if code in CAUSES.values() else CAUSES["SCOPE"])
        except TypeError: return self._fail(CAUSES["SCOPE"])
        sha = info["source_sha256"]; key = self._entry_key(problem,mode,sha)
        with self.account_lock():
            state = self._roll_day(self._load_state())
            existing = state["entries"].get(key)
            if existing:
                uncertain = existing["state"] in ("INTENT","UNCERTAIN")
                return self._fail(CAUSES["UNCERTAIN"] if uncertain else CAUSES["DUPLICATE"],"write" if uncertain else "none",job_id=existing.get("job_id"),entry_state=existing["state"])
            if any(e["problem"]==problem and e["state"] in ("INTENT","UNCERTAIN") for e in state["entries"].values()):
                return self._fail(CAUSES["UNCERTAIN"],"write")
            if mode == "scored":
                checked = state["entries"].get(self._entry_key(problem,"check",sha))
                if not checked or checked.get("check_ok") is not True or checked.get("candidate_digest") != payload["candidate_digest"] or checked.get("artifact_digest") != payload["artifact_digest"]:
                    return self._fail(CAUSES["CHECK_REQUIRED"])
                if not isinstance(info["qa"].get("performance_hypothesis"),str) or not info["qa"]["performance_hypothesis"].strip():
                    return self._fail("reviewed_performance_hypothesis_required")
            provider, failure = self._provider()
            if failure: return failure
            quota, failure = self._quota(state,problem,mode,provider)
            if failure: return failure
            rates, failure = self._rate(state)
            if failure: return failure
            if dry_run: return {"ok":True,"code":"ok","effect":"none","dry_run":True,"problem":problem,"mode":mode,"source_sha256":sha}
            budget_key, used = quota
            entry = {"problem":problem,"mode":mode,"source_sha256":sha,"state":"INTENT","created":self.clock.time(),
                     "candidate_digest":payload["candidate_digest"],"artifact_digest":payload["artifact_digest"],
                     "identity":dict(self.identity),"qa_artifact_digest":info["qa"]["qa_artifact_digest"],"job_id":None,"provider_content_hash":None}
            state["entries"][key] = entry; state["budgets"][budget_key]=used+1
            rate_entry={"at":self.clock.time(),"job_id":None}; state["rate"]=rates+[rate_entry]
            self._save_state(state); self._journal("intent",problem=problem,mode=mode,source_sha256=sha)
            self.possible_effect = True
            try:
                status, body = self.transport.post("/api/v1/problems/"+problem+"/submissions",{"code":info["code"],"mode":"check" if mode=="check" else "run"})
            except TransportNoEffect:
                self.possible_effect=False; state["entries"].pop(key);state["budgets"][budget_key]=used;state["rate"]=rates;self._save_state(state)
                return self._fail("transport_not_sent")
            except Exception:
                return self._uncertain(state,entry)
            confirmed_rejection={400:CAUSES["INVALID_CANDIDATE"],401:CAUSES["AUTH"],403:CAUSES["AUTH"],404:CAUSES["INVALID_CANDIDATE"],409:CAUSES["TRACK_CLOSED"],413:CAUSES["INVALID_CANDIDATE"],422:CAUSES["INVALID_CANDIDATE"],429:CAUSES["RATE_LIMIT"]}
            if status in confirmed_rejection:
                self.possible_effect=False;state["entries"].pop(key);state["budgets"][budget_key]=used;self._save_state(state)
                self._journal("confirmed_rejection",problem=problem,mode=mode,status=status)
                return self._fail(confirmed_rejection[status],status=status)
            job_id = body.get("job_id") if isinstance(body,dict) else None
            if not (200<=status<300) or not isinstance(job_id,str) or not JOB.fullmatch(job_id): return self._uncertain(state,entry)
            provider_hash=body.get("content_hash")
            if provider_hash is not None and (not isinstance(provider_hash,str) or not HEX.fullmatch(provider_hash)):
                return self._uncertain(state,entry)
            entry.update(state="SUBMITTED",job_id=job_id,provider_content_hash=provider_hash);rate_entry["job_id"]=job_id
            self._save_state(state);self._journal("job_id",problem=problem,mode=mode,source_sha256=sha,job_id=job_id)
            return {"ok":True,"code":"ok","effect":"write","job_id":job_id,"receipt":True,"gpu_verified":False}
    def observe(self, problem=None,mode=None,sha=None,job_id=None,dry_run=False):
        with self.account_lock():
            state=self._load_state()
            matches=[e for e in state["entries"].values() if (not problem or e["problem"]==problem) and (not mode or e["mode"]==mode) and (not sha or e["source_sha256"]==sha) and (not job_id or e.get("job_id")==job_id)]
            if len(matches)!=1 or not matches[0].get("job_id"): return self._fail(CAUSES["SCOPE"])
            entry=matches[0];job_id=entry["job_id"]
            if dry_run:return {"ok":True,"effect":"read","dry_run":True,"job_id":job_id}
            status,body=self._get("/api/v1/jobs/"+job_id)
            if status in (401,403):return self._fail(CAUSES["AUTH"],"read")
            if status!=200 or not isinstance(body,dict):return self._fail("job_observation_unavailable","read")
            if body.get("id")!=job_id or body.get("board")!=entry["problem"] or body.get("mode")!=("check" if entry["mode"]=="check" else "run"):
                return self._fail("job_identity_mismatch","read")
            content_hash=body.get("content_hash")
            if not isinstance(content_hash,str) or not HEX.fullmatch(content_hash) or (entry.get("provider_content_hash") and entry["provider_content_hash"]!=content_hash):
                return self._fail("job_content_identity_mismatch","read")
            entry["provider_content_hash"]=content_hash
            result=body.get("result") or {};checks=result.get("checks") or {}
            verified=(body.get("status")=="done" and result.get("status")=="ok" and result.get("passed") is True and checks.get("ok") is True
                      and type(result.get("returncode")) is int and result["returncode"]==0
                      and not any(row.get(k) for row in (body,result,checks) for k in ("error","flags","flagged","rejected"))
                      and not body.get("duplicate_of"))
            terminal=body.get("status") in ("done","failed","rejected","duplicate")
            entry["state"]="VERIFIED" if verified else ("REJECTED" if terminal else "PENDING")
            entry["check_ok"]=verified and entry["mode"]=="check";entry["gpu_verified"]=verified
            entry["observed_at"]=self.clock.time()
            folder=os.path.join(self.state_dir,"results");os.makedirs(folder,mode=0o700,exist_ok=True)
            receipt=os.path.join(folder,job_id+".json");atomic_json(receipt,self._scrub(body))
            entry["result_receipt_sha256"]=sha256_file(receipt)
            self._save_state(state);self._journal("observed",job_id=job_id,state=entry["state"],gpu_verified=verified)
            numeric=lambda v:type(v) in (int,float) and math.isfinite(v)
            speedup=result.get("speedup");ci=result.get("speedup_ci")
            return {"ok":True,"code":"ok","effect":"read","job_id":job_id,"status":body.get("status"),"gpu_verified":verified,
                    "checks_ok":checks.get("ok"),"returncode":result.get("returncode"),"error":self._safe(str(result.get("error") or ""))[:500],
                    "speedup":speedup if numeric(speedup) else None,"speedup_ci":ci if isinstance(ci,list) and len(ci)==2 and all(numeric(v) for v in ci) else None,
                    "private_result_sha256":entry["result_receipt_sha256"]}
    def reconcile(self,problem,mode,sha,dry_run=False):
        # Official jobs expose provider content_hash, not the local raw source SHA.
        # Never adopt by a guessed hash/filename; operator must attach verified receipt.
        with self.account_lock():
            entry=self._load_state()["entries"].get(self._entry_key(problem,mode,sha))
            if not entry:return self._fail(CAUSES["BAD_INPUT"])
            if entry.get("job_id"):return {"ok":True,"effect":"read","job_id":entry["job_id"],"code":"observe_existing_job"}
            return self._fail(CAUSES["UNCERTAIN"],"write",detail="no provable local-SHA linkage in official jobs; operator reconciliation required")
    def bind_action(self,doc,operation):
        if not all(isinstance(doc.get(k),str) and doc[k] for k in ("cycleId","occurrenceId","actionKey")):
            raise ValueError("bad_input: cycle identity missing")
        key="|".join(doc[k] for k in ("cycleId","occurrenceId","actionKey"))
        body={k:v for k,v in doc.items() if k!="attemptId"}
        body["dispatchOperation"]=operation
        digest=hashlib.sha256(json.dumps(body,sort_keys=True,allow_nan=False,separators=(",",":")).encode()).hexdigest()
        with self.account_lock():
            state=self._load_state();prior=state["bindings"].get(key)
            if prior and prior!=digest:raise ValueError("scope_rejected: changed action input")
            if not prior:state["bindings"][key]=digest;self._save_state(state)

def run_cycle_action(doc,queue,dry_run=False,operation=None):
    params=doc.get("parameters") or {};inp=doc.get("input") or {};payload=inp.get("payload",inp)
    # AgentOS actionKey is occurrenceId:stepKey, not the operation name. The
    # registered fixed CLI argument is authoritative; legacy direct tests may
    # pass an operation as actionKey, but production always supplies --action.
    action=operation or params.get("action") or doc.get("actionKey")
    queue.possible_effect=False
    try:
        if params.get("project","openkernel")!="openkernel":return queue._fail(CAUSES["SCOPE"])
        if action not in ("inspect","submit_check","submit_scored","observe","reconcile"):return queue._fail(CAUSES["BAD_INPUT"])
        if not dry_run:queue.bind_action(doc,action)
        if action=="inspect":return queue.inspect(dry_run)
        problem=params.get("problem") or payload.get("problem")
        if action in ("submit_check","submit_scored"):return queue.submit(problem,"check" if action=="submit_check" else "scored",payload,dry_run)
        if action=="reconcile":return queue.reconcile(problem,params.get("mode"),params.get("source_sha256"),dry_run)
        return queue.observe(problem,params.get("mode") or payload.get("mode"),params.get("source_sha256") or payload.get("source_sha256"),params.get("job_id") or payload.get("job_id"),dry_run)
    except Exception:
        return queue._fail(CAUSES["UNCERTAIN"] if queue.possible_effect else "controller_preflight_failed","write" if queue.possible_effect else "none")

def load_binding(path,expected_sha):
    if not path or not expected_sha or not HEX.fullmatch(expected_sha):raise ValueError("binding_not_pinned")
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd,"rb") as f:
        info=os.fstat(f.fileno());raw=f.read(1024*1024+1)
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022 or len(raw)>1024*1024 or hashlib.sha256(raw).hexdigest()!=expected_sha:
        raise ValueError("binding_invalid")
    manifest=json.loads(raw)
    if manifest.get("project")!="openkernel":raise ValueError("binding_project_rejected")
    workspace=os.path.realpath(manifest["workspace_root"])
    if os.path.commonpath([os.path.realpath(path),workspace])==workspace:raise ValueError("binding_inside_untrusted_workspace")
    return manifest

def main(argv=None):
    parser=argparse.ArgumentParser();parser.add_argument("--dry-run",action="store_true")
    parser.add_argument("--action",required=True,choices=("inspect","submit_check","submit_scored","observe","reconcile"));args=parser.parse_args(argv)
    try:
        raw=sys.stdin.buffer.read(65537)
        if len(raw)>65536:raise ValueError("input_too_large")
        doc=json.loads(raw)
        binding=load_binding(os.environ.get("OPENKERNEL_BINDING_FILE"),os.environ.get("OPENKERNEL_BINDING_SHA256"))
        queue=SubmissionQueue(binding["state_dir"],HttpTransport(),system_clock(),account=binding["account"],workspace_root=binding["workspace_root"],
            identity=binding["identity"],qa_registry=binding["qa_registry"],account_lock_path=binding["account_lock_path"])
        result=run_cycle_action(doc,queue,args.dry_run,operation=args.action)
    except Exception:
        result={"ok":False,"code":"controller_binding_or_input_invalid","effect":"none"}
    print(json.dumps(result,allow_nan=False))
    # AgentOS parses structured rejections only after a successful process exit.
    # ok/effect encode the outcome; exit 0 never means verified GPU success.
    return 0
if __name__=="__main__":sys.exit(main())
