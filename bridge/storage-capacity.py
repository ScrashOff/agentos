"""Short admission lock and process-owned byte reservations for workspace copies."""
import contextlib,datetime,fcntl,json,os,pathlib,shutil,uuid

HEADROOM=2*1024**3

@contextlib.contextmanager
def allocation_lock(data):
    root=pathlib.Path(data)/'storage-reservations'
    root.mkdir(parents=True,exist_ok=True)
    with (root/'admission.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        yield root

def reserved_bytes(root):
    total=0
    for p in root.glob('*.json'):
        if p.is_symlink(): raise ValueError('STORAGE_RESERVATION_INVALID')
        try:
            with p.open('r+') as held:
                try: fcntl.flock(held,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:
                    value=json.load(held)
                    amount=value.get('bytes')
                    if not isinstance(amount,int) or isinstance(amount,bool) or amount<0: raise ValueError('STORAGE_RESERVATION_INVALID')
                    total+=amount
                # Unlocked records have no owner and reserve nothing. Never use
                # a timestamp or a PID observation to revoke an active owner.
        except FileNotFoundError: continue
    return total

def capacity(workspaces,data):
    with allocation_lock(data) as root:
        usage=shutil.disk_usage(workspaces);reserved=reserved_bytes(root)
        return {'version':1,'resource':'agentos-workspaces','freeBytes':usage.free,
            'reservedBytes':reserved,'availableBytes':max(0,usage.free-reserved),
            'headroomBytes':HEADROOM,'available':usage.free-reserved>=HEADROOM,
            'checkedAt':datetime.datetime.now(datetime.timezone.utc).isoformat()}

@contextlib.contextmanager
def reserve(workspaces,data,amount):
    if not isinstance(amount,int) or amount<0: raise ValueError('STORAGE_RESERVATION_INVALID')
    handle=None;filename=None
    try:
        with allocation_lock(data) as root:
            if shutil.disk_usage(workspaces).free-reserved_bytes(root)<amount+HEADROOM:
                raise ValueError('DISK_CAPACITY')
            filename=root/(uuid.uuid4().hex+'.json')
            handle=filename.open('x+')
            fcntl.flock(handle,fcntl.LOCK_EX)
            json.dump({'bytes':amount},handle);handle.flush();os.fsync(handle.fileno())
        yield
    finally:
        if handle:
            with allocation_lock(data):
                filename.unlink(missing_ok=True)
                handle.close()
