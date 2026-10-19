"""Exercise actual publication code with only temporary files and fake I/O.

No systemd, network, database or /opt paths are accessed by the compiled main.
"""
import ast, contextlib, fcntl, hashlib, io, json, multiprocessing, os
import pathlib, shutil, sys, tempfile, time, uuid


def publisher(root, state, occurrence, ready, complete, result):
    source = pathlib.Path('scripts/agentos-release-action.py').read_text()
    tree = ast.parse(source)
    selected = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in ['main', 'digest']], type_ignores=[])

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *_args): pass

    class FakeRequest:
        @staticmethod
        def urlopen(*_args, **_kwargs): return Response()

    class FakeUrllib:
        request = FakeRequest

    def fake_source(_candidate, _record):
        # At this point the swap has happened, but pending is not written yet.
        other = open(state / 'publication.lock', 'a')
        try:
            try:
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result['lockedThroughPending'] = False
            except BlockingIOError:
                result['lockedThroughPending'] = True
        finally:
            other.close()

    namespace = {'ROOT': root, 'STATE': state, 'sys': sys, 'json': json, 'uuid': uuid,
                 'pathlib': pathlib, 'os': os, 'time': time, 'fcntl': fcntl,
                 'hashlib': hashlib, 'shutil': shutil, 'urllib': FakeUrllib,
                 'run': lambda *_args, **_kwargs: '', 'publish_source': fake_source}
    exec(compile(selected, 'sandboxed-release-action.py', 'exec'), namespace)
    sys.argv = ['sandboxed-release-action.py', 'publish']
    sys.stdin = io.StringIO(json.dumps({'occurrenceId': occurrence}))
    ready.set()
    with contextlib.redirect_stdout(io.StringIO()):
        namespace['main']()
    complete.set()


if __name__ == '__main__':
    # Linux fork keeps all test processes inside this one isolated invocation.
    ctx = multiprocessing.get_context('fork')
    with tempfile.TemporaryDirectory(prefix='agentos-rollout-publication-') as temp:
        root = pathlib.Path(temp) / 'root'
        state = pathlib.Path(temp) / 'state'
        root.mkdir(); state.mkdir()
        before = root / 'before'; after = root / 'after'
        before.mkdir(); after.mkdir(); (after / 'source.txt').write_text('reviewed')
        (root / 'current').symlink_to(before)
        occurrence = str(uuid.uuid4())
        checksum = hashlib.sha256(b'source.txtreviewed').hexdigest()
        (state / (occurrence + '.json')).write_text(json.dumps({'base': str(before), 'candidate': str(after), 'digest': checksum}))
        ready, complete = ctx.Event(), ctx.Event()
        with ctx.Manager() as manager:
            result = manager.dict()
            with open(state / 'publication.lock', 'a') as holder:
                fcntl.flock(holder, fcntl.LOCK_EX)
                child = ctx.Process(target=publisher, args=(root, state, occurrence, ready, complete, result))
                child.start()
                try:
                    if not ready.wait(3): raise RuntimeError('Publisher did not start')
                    result['lockedBeforeSwap'] = not complete.wait(0.15) and (root / 'current').resolve() == before and not (state / 'pending-workers.json').exists()
                    fcntl.flock(holder, fcntl.LOCK_UN)
                    if not complete.wait(5): raise RuntimeError('Publisher did not finish after lock release')
                    child.join(2)
                    if child.exitcode != 0: raise RuntimeError('Sandbox publication failed')
                    result['publishedAfterRelease'] = (root / 'current').resolve() == after and json.loads((state / 'pending-workers.json').read_text())['candidate'] == str(after)
                    print(json.dumps(dict(result)))
                finally:
                    if child.is_alive(): child.terminate(); child.join(2)
