'''
Storage backend for AWS S3, and possibly other cloud services with similar API.
'''

from logging import getLogger

from .helpers import DataChunkInfo


logger = getLogger(__name__)


class S3Backend:

    def __init__(self):
        raise Exception('NIY')

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.directory}>'

    def write_data_chunk(self, backup_id, chunk):
        assert isinstance(backup_id, str)
        assert isinstance(chunk, bytes)
        raise Exception('NIY')
        return DataChunkInfo(name=self.current_data_file_name, offset=df_pos, size=df_size)

    def close_data_file(self):
        raise Exception('NIY')

    def read_data_chunk(self, filename, offset, size):
        raise Exception('NIY')

    def store_file(self, src_path, name):
        assert isinstance(src_path, Path)
        assert isinstance(name, str)
        raise Exception('NIY')

    def list_files(self):
        raise Exception('NIY')

    def retrieve_file(self, name, dst_path):
        assert isinstance(name, str)
        assert isinstance(dst_path, Path)
        raise Exception('NIY')
