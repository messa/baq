from contextlib import ExitStack
import hashlib
from logging import getLogger
from pathlib import Path
import re
from threading import Lock


logger = getLogger(__name__)


class FileBackend:

    def __init__(self, backup_url):
        '''
        Note: The backup_url file://example will mean ./example (relative path).
        Pass file:///example to mean /example (absolute path).
        '''
        assert isinstance(backup_url, str)
        m = re.match(r'^file://([^?]+)$', backup_url)
        backup_dir, = m.groups()
        self.backup_dir = Path(backup_dir).resolve()

    def get_cache_name(self):
        return hashlib.sha1(str(self.backup_dir).encode()).hexdigest()

    def get_data_collector(self, backup_id):
        return FileDataCollector(backup_id, self.backup_dir)


class FileDataCollector:

    def __init__(self, backup_id, backup_dir):
        assert isinstance(backup_id, str)
        assert isinstance(backup_dir, Path)
        self.backup_id = backup_id
        self.backup_dir = backup_dir
        self.current_file = None

    def __enter__(self):
        self.stack = ExitStack()
        self.stack.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stack.__exit__(exc_type, exc_val, exc_tb)

    def store_block(self, data):
        '''
        Returns tuple (filename, offset)
        '''
        assert isinstance(data, bytes)
        raise Exception('TODO')


class TempDirDataCollector:
    '''
    Unused.
    This class can be used in a file backend, or deleted.
    '''

    def __init__(self, backup_id, temp_dir):
        assert isinstance(backup_id, str)
        assert isinstance(temp_dir, Path)
        self.backup_id = backup_id
        self.temp_dir = temp_dir
        self.next_number = 0
        self.current_name = None
        self.current_file = None
        self.file_mutex = Lock()

    def store_block(self, data):
        '''
        Returns tuple (filename, offset)
        '''
        assert isinstance(data, bytes)
        with self.file_mutex:
            if not self.current_name:
                self.current_name = f'baq.{self.backup_id}.data-{self.next_number:06d}'
                self.current_file = (self.temp_dir / self.current_name).open(mode='wb')
                self.next_number += 1
            offset = self.current_file.tell()
            #self.current_file.write(data)
            self.current_file.write(b'xxx')
            self.current_file.flush()
            return self.current_name, offset
