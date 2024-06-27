from collections import namedtuple
import gzip
import json
from logging import getLogger
from pathlib import Path
from sys import intern
from time import monotonic as monotime

from ..util import default_block_size


logger = getLogger(__name__)


class BackupMetaReader:

    DirectoryMeta = namedtuple('DirectoryMeta', 'st_mtime_ns st_atime_ns st_mode st_uid st_gid owner group')
    FileMeta = namedtuple('FileMeta', 'blocks st_mtime_ns st_atime_ns st_mode st_uid st_gid owner group original_size original_sha1')
    FileBlock = namedtuple('FileBlock', 'offset size sha3 aes_key store_file store_offset store_size')

    def __init__(self, meta_path):
        assert isinstance(meta_path, Path)
        self.directories = {}
        self.files = {}
        self.blocks = {} # sha3 -> FileBlock
        start_time = monotime()
        with gzip.open(meta_path, 'rb') as f:
            logger.info('Reading previous backup metadata from %s', meta_path)
            records = (json.loads(line) for line in f)
            header = next(records)
            assert header['baq_backup']['format_version'] == 1
            self.block_size = header['baq_backup'].get('block_size', default_block_size)
            assert isinstance(self.block_size, int)
            while True:
                try:
                    record = next(records)
                except StopIteration:
                    break
                if d := record.get('directory'):
                    self.directories[d['path']] = self.DirectoryMeta(
                        int(d['st_mtime_ns']), int(d['st_atime_ns']), int(d['st_mode'], 8), d['st_uid'], d['st_gid'],
                        intern(d['owner']), intern(d['group']),
                    )
                    del d
                elif f := record.get('file'):
                    file_blocks = []
                    original_size, original_sha1 = None, None
                    while True:
                        file_record = next(records)
                        if fd := file_record.get('file_data'):
                            file_block = self.FileBlock(
                                int(fd['offset']), int(fd['size']),
                                bytes.fromhex(fd['sha3']), bytes.fromhex(fd['aes_key']),
                                intern(fd['store_file']), int(fd['store_offset']), int(fd['store_size']),
                            )
                            file_blocks.append(file_block)
                            self.blocks[file_block.sha3] = file_block
                            del fd
                        elif fs := file_record.get('file_summary'):
                            original_size = fs['size']
                            original_sha1 = bytes.fromhex(fs['sha1'])
                            del fs
                            break
                        else:
                            raise Exception(f'Unknown file record: {file_record!r}')
                    self.files[f['path']] = self.FileMeta(
                        file_blocks,
                        int(f['st_mtime_ns']), int(f['st_atime_ns']), int(f['st_mode'], 8), f['st_uid'], f['st_gid'],
                        intern(f['owner']), intern(f['group']),
                        int(original_size), original_sha1,
                    )
                    del f
                else:
                    raise Exception(f'Unknown record: {record!r}')
            logger.debug(
                'Loaded backup metadata with %d files (%dk blocks) in %.3f s',
                len(self.files), len(self.blocks) / 1000, monotime() - start_time)

    def get_block_by_sha3(self, sha3_digest):
        assert isinstance(sha3_digest, bytes)
        return self.blocks.get(sha3_digest)
