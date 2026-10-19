"""A parent process holds global admission and workspace locks for the child lifetime."""
import fcntl, hashlib, json, os, pathlib, signal, subprocess, sys
def capacity(root):
    config=root/'runtime-capacity.json'
    value=json.loads(config.read_text()).get('maxRuns') if config.exists() else 2
    if isinstance(value,bool) or not isinstance(value,int) or not 1<=value<=18:
        raise ValueError('INVALID_RUNTIME_CAPACITY')
    return value
root=pathlib.Path('/var/lib/agentos-bridge'); (root/'locks').mkdir(parents=True,exist_ok=True)
handles=[]
try:
    legacy=open(root/'worker.lock','a'); handles.append(legacy);fcntl.flock(legacy,fcntl.LOCK_SH|fcntl.LOCK_NB)
    workspace=sys.argv[1]; memory=int(sys.argv[2]); command=sys.argv[3:]
    own=open(root/'locks'/('workspace-'+hashlib.sha256(workspace.encode()).hexdigest()),'a');handles.append(own);fcntl.flock(own,fcntl.LOCK_EX|fcntl.LOCK_NB)
    acquired=False
    for i in range(capacity(root)):
        h=open(root/'locks'/('slot-'+str(i)),'a')
        try: fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB);handles.append(h);acquired=True;break
        except BlockingIOError:h.close()
    if not acquired: raise BlockingIOError()
    available=int(next(line.split()[1] for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:')))//1024
    if available<memory+512: print('AGENTOS_HOST_CAPACITY',file=sys.stderr);sys.exit(75)
    child=subprocess.Popen(command)
    def stop(sig,frame):
        child.send_signal(sig)
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    sys.exit(child.wait())
except BlockingIOError:
    print('AGENTOS_SOURCE_BUSY',file=sys.stderr);sys.exit(75)
finally:
    for h in handles:h.close()
