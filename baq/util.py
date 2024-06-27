import hashlib
import os
from threading import Event, Lock

try:
    from datetime import UTC
except ImportError:
    # datetime.UTC is new in Python 3.11
    from datetime import timezone
    UTC = timezone.utc


default_block_size = int(os.environ.get('BAQ_BLOCK_SIZE') or 128 * 1024)


def sha1_file(file_path, length=None):
    with file_path.open('rb') as f:
        h = hashlib.sha1()
        if length is None:
            while True:
                block = f.read(65536)
                if not block:
                    break
                h.update(block)
        else:
            remaining = length
            while remaining > 0:
                block = f.read(min(remaining, 65536))
                if not block:
                    break
                h.update(block)
                remaining -= len(block)
        return h


def split(items, n):
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class SimpleFuture:

    def __init__(self):
        self._waiting = True
        self._result = None
        self._exception = None
        self._event = Event()
        self._lock = Lock()

    def set_result(self, value):
        with self._lock:
            assert self._waiting
            self._result = value
            self._waiting = False
        self._event.set()

    def set_exception(self, value):
        with self._lock:
            assert self._waiting
            self._exception = value
            self._waiting = False
        self._event.set()

    def result(self):
        self._event.wait()
        with self._lock:
            assert not self._waiting
            if self._exception:
                raise self._exception
            return self._result

    def waiting(self):
        with self._lock:
            return self._waiting


def none_if_keyerror(callable):
    try:
        return callable()
    except KeyError:
        return None


def walk_files(path):
    for p in sorted(path.iterdir()):
        yield p
        if p.is_dir():
            yield from walk_files(p)
