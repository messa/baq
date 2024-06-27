from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
from logging import getLogger
import os
from pathlib import Path
import re
from reprlib import repr as smart_repr
from tempfile import TemporaryDirectory
from threading import Lock, Semaphore
import zstandard

from .backends.s3_backend import S3Backend
from .helpers.encryption import decrypt_aes, decrypt_gpg
from .helpers.metadata_file import BackupMetaReader
from .util import sha1_file, split


logger = getLogger(__name__)

directory_mutex = Lock()


def do_restore(backup_url, local_path):
    assert isinstance(backup_url, str)
    assert isinstance(local_path, Path)
    assert backup_url.startswith('s3://')
    if not local_path.exists():
        logger.info('Creating restore directory %s', local_path)
        local_path.mkdir()
    assert local_path.is_dir()
    with ExitStack() as stack:
        temp_dir = Path(stack.enter_context(TemporaryDirectory(prefix='baq.')))
        logger.info('Restoring %s to %s', backup_url, local_path)

        if m := re.match(r'^(s3://.+)/(baq.[0-9TZ]+.meta)$', backup_url):
            backup_url, meta_filename = m.groups()
            remote = S3Backend(backup_url)
            remote.download_file(meta_filename, temp_dir / 'meta.gpg')
        else:
            raise Exception('Please provide full path to meta file in backup URL')

        decrypt_gpg(temp_dir / 'meta.gpg', temp_dir / 'meta')
        meta = BackupMetaReader(temp_dir / 'meta')

        data_file_map = {} # {backup store file: [(destination file path, block_meta)]}
        for file_path, file_meta in meta.files.items():
            for b in file_meta.blocks:
                data_file_map.setdefault(b.store_file, []).append((file_path, b))

        logger.debug(
            'Will be restoring from files:%s',
            ''.join(f'\n  - {k} ({len(v)} blocks)' for k, v in sorted(data_file_map.items())))

        # TODO: restore from glacier all files in data_file_map.keys()

        restore_pool = stack.enter_context(ThreadPoolExecutor(24, 'restore'))
        write_pool = stack.enter_context(ThreadPoolExecutor(8, 'write'))
        scan_sem = Semaphore(8)
        fetch_sem = Semaphore(16)

        restore_tasks = []
        for data_file_name, data_file_contents in sorted(data_file_map.items()):
            data_file_contents = sorted(data_file_contents, key=lambda x: x[1].store_offset)
            for data_file_contents_chunk in split(data_file_contents, 1000):
                restore_tasks.append((
                    restore_from_data_file,
                    scan_sem,
                    fetch_sem,
                    remote,
                    data_file_name,
                    data_file_contents_chunk,
                    write_pool,
                    local_path))

        restore_tasks.sort(key=lambda t: t[5][0][1].store_offset) # sorry :)

        restore_futures = deque()
        for t in restore_tasks:
            restore_futures.append(
                restore_pool.submit(*t))

        for fut in restore_futures:
            fut.result()

        del restore_pool
        del write_pool

    with ExitStack() as stack:
        for dir_path, _ in meta.directories.items():
            full_path = local_path / dir_path
            full_path.mkdir(exist_ok=True)

        for file_path, file_meta in meta.files.items():
            full_path = local_path / file_path
            if file_meta.original_size == 0:
                with full_path.open('wb'):
                    pass
            if full_path.stat().st_size > file_meta.original_size:
                os.truncate(full_path, file_meta.original_size)
            assert full_path.stat().st_size == file_meta.original_size
            if sha1_file(full_path).digest() == file_meta.original_sha1:
                logger.info('Checksum %s OK', file_path)
            else:
                raise Exception('Checksum failed')
            try:
                os.chown(full_path, file_meta.st_uid, file_meta.st_gid)
            except Exception as e:
                # This may happen if not running under root
                logger.warning(
                    'Failed to chown file %s to uid %r gid %r: %r',
                    full_path, file_meta.st_uid, file_meta.st_gid, e)
            full_path.chmod(file_meta.st_mode & 0o777)
            os.utime(full_path, ns=(file_meta.st_atime_ns, file_meta.st_mtime_ns))

        for dir_path, dir_meta in sorted(meta.directories.items(), reverse=True):
            full_path = local_path / dir_path
            try:
                os.chown(full_path, dir_meta.st_uid, dir_meta.st_gid)
            except Exception as e:
                # This may happen if not running under root
                logger.warning(
                    'Failed to chown directory %s to uid %r gid %r: %r',
                    full_path, dir_meta.st_uid, dir_meta.st_gid, e)
            full_path.chmod(dir_meta.st_mode & 0o777)
            os.utime(full_path, ns=(dir_meta.st_atime_ns, dir_meta.st_mtime_ns))



def restore_from_data_file(scan_sem, fetch_sem, remote, store_file_name, restore_blocks, write_pool, local_path):
    '''
    This function runs for one backup store file and a subset of blocks stored in it.
    The store file containts blocks of backed-up files.
    We have metadata of these blocks in restore_blocks.
    Here we determine which blocks need to be restored.
    (Usually for a restore into empty directory all blocks need to be restored,
    but there may be already some data from previous restore run.)
    Parameter local_path contains path restore directory.
    '''
    assert isinstance(store_file_name, str)
    assert isinstance(local_path, Path)
    try:
        logger.debug(
            'restore_from_data_file store_file_name=%s restore_blocks=%s local_path=%s',
            store_file_name, smart_repr(restore_blocks), local_path)
        filtered_restore_blocks = []
        with scan_sem:
            for original_path, block_meta in restore_blocks:
                already_restored = False
                data_changed = False
                original_full_path = local_path / original_path
                try:
                    with original_full_path.open('rb') as f:
                        f.seek(block_meta.offset)
                        file_data = f.read(block_meta.size)
                        if file_data:
                            if hashlib.sha3_512(file_data).digest() == block_meta.sha3:
                                already_restored = True
                            else:
                                data_changed = file_data.rstrip(b'\x00') != b''
                        del file_data
                except FileNotFoundError:
                    pass
                if already_restored:
                    logger.debug(
                        'File %s offset %d length %d is already restored',
                        original_path, block_meta.offset, block_meta.size)
                else:
                    logger.debug(
                        'File %s offset %d length %d needs to be restored%s',
                        original_path, block_meta.offset, block_meta.size,
                        ' (data changed)' if data_changed else '')
                    filtered_restore_blocks.append((original_path, block_meta))
        restore_blocks = filtered_restore_blocks
        del filtered_restore_blocks
        if not restore_blocks:
            logger.debug('Nothing to restore from %s', store_file_name)
        else:
            retrieve_ranges = [
                (block_meta.store_offset, block_meta.store_size)
                for _, block_meta in restore_blocks
            ]
            with fetch_sem:
                logger.debug('calling retrieve_file_ranges(%s, %s)', store_file_name, smart_repr(retrieve_ranges))
                retrieved_range_data = remote.retrieve_file_ranges(store_file_name, retrieve_ranges)
                logger.debug('retrieve_file_ranges -> %s', retrieved_range_data)
                write_futures = deque()
                for n, ((original_path, block_meta), encrypted_data) in enumerate(zip(restore_blocks, retrieved_range_data), start=1):
                    assert isinstance(original_path, str)
                    assert isinstance(block_meta, BackupMetaReader.FileBlock)
                    assert block_meta.store_file == store_file_name
                    assert isinstance(encrypted_data, bytes)
                    write_futures.append(
                        write_pool.submit(
                            write_restore_block,
                            original_path, block_meta, store_file_name, encrypted_data, local_path))
                    while len(write_futures) > 100:
                        write_futures.popleft().result()

                for f in write_futures:
                    f.result()

        logger.debug('restore_from_data_file done')
    except BaseException as e:
        logger.exception('restore_from_data_file failed: %r', e)
        raise


def write_restore_block(original_path, block_meta, store_file_name, encrypted_data, local_path):
    try:
        logger.debug(
            'Restoring file %s offset %d length %d from %s',
            original_path, block_meta.offset, block_meta.size, store_file_name)
        compressed_data = decrypt_aes(encrypted_data, block_meta.aes_key)
        original_data = zstandard.decompress(compressed_data)
        assert hashlib.sha3_512(original_data).digest() == block_meta.sha3
        original_full_path = local_path / original_path
        with directory_mutex:
            if not original_full_path.parent.exists():
                logger.debug('Creating directory %s', original_full_path.parent)
                original_full_path.parent.mkdir(parents=True)
        with ExitStack() as stack:
            try:
                f = stack.enter_context(original_full_path.open('r+b'))
            except FileNotFoundError:
                try:
                    f = stack.enter_context(original_full_path.open('xb'))
                except FileExistsError:
                    f = stack.enter_context(original_full_path.open('r+b'))
            f.seek(block_meta.offset)
            f.write(original_data)
            f.flush()
    except BaseException as e:
        logger.exception('write_restore_block failed: %r', e)
        raise e
