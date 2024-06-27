
from pathlib import Path
from threading import Lock


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
