'''
Storage backend using normal filesystem files.
Mostly used for testing.
'''

from logging import getLogger
from pathlib import Path
from shutil import copyfile

from .helpers import DataChunkInfo


logger = getLogger(__name__)


class FileBackend:

    def __init__(self, directory):
        self.directory = Path(directory)
        if not self.directory.is_dir():
            logger.info('Creating directory %s', self.directory)
            self.directory.mkdir()
        self.next_data_file_number = 0
        self.current_data_file = None

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.directory}>'

    def write_data_chunk(self, backup_id, chunk):
        assert isinstance(backup_id, str)
        assert isinstance(chunk, bytes)
        if self.current_data_file and self.current_data_file.tell() > 10 * 2**30:
            self.current_data_file.close()
            self.current_data_file = None
        if not self.current_data_file:
            self.current_data_file_name = f'baq.{backup_id}.data.{self.next_data_file_number:05d}'
            self.next_data_file_number += 1
            logger.debug('Creating new data file %s', self.current_data_file_name)
            self.current_data_file = open(self.directory / self.current_data_file_name, mode='wb')
        assert self.current_data_file_name.startswith(f'baq.{backup_id}.data.')
        df_pos = self.current_data_file.tell()
        self.current_data_file.write(chunk)
        self.current_data_file.flush()
        df_size = self.current_data_file.tell() - df_pos
        return DataChunkInfo(name=self.current_data_file_name, offset=df_pos, size=df_size)

    def close_data_file(self):
        if self.current_data_file:
            self.current_data_file.close()
            self.current_data_file = None

    def read_data_chunk(self, filename, offset, size):
        with (self.directory / filename).open(mode='rb') as f:
            f.seek(offset)
            return f.read(size)

    def store_file(self, src_path, name):
        assert isinstance(src_path, Path)
        assert isinstance(name, str)
        dst_path = self.directory / name
        if dst_path.exists():
            raise Exception('File already exists: {}'.format(dst_path))
        try:
            src_path.rename(dst_path)
        except OSError:
            # possibly "Invalid cross-device link", so let's copy it instead of rename
            copyfile(src_path, dst_path)
        logger.debug('Saved file %s', dst_path)

    def list_files(self):
        return sorted(p.name for p in self.directory.iterdir() if  p.name.startswith('baq.'))

    def retrieve_file(self, name, dst_path):
        assert isinstance(name, str)
        assert isinstance(dst_path, Path)
        copyfile(self.directory / name, dst_path)
