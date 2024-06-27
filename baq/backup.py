from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime
import gzip
import hashlib
import json
from logging import getLogger
import os
from os import cpu_count
from pathlib import Path
from queue import Queue
from secrets import token_bytes
import shutil
from tempfile import TemporaryDirectory
from threading import Lock
import zstandard

from .backends.s3_backend import S3Backend, S3DataCollector
from .helpers.encryption import encrypt_aes, encrypt_gpg
from .helpers.metadata_file import BackupMetaReader
from .util import SimpleFuture, UTC, none_if_keyerror, walk_files, default_block_size


logger = getLogger(__name__)

cache_dir = Path(os.environ.get('BAQ_CACHE_DIR') or Path('~/.cache/baq').expanduser())
worker_count = cpu_count()


def do_backup(local_path, backup_url, s3_storage_class, encryption_recipients):
    assert isinstance(local_path, Path)
    assert isinstance(backup_url, str)
    assert isinstance(s3_storage_class, str)
    assert backup_url.startswith('s3://')
    with ExitStack() as stack:
        temp_dir = Path(stack.enter_context(TemporaryDirectory(prefix='baq.')))
        logger.info('Backing up %s to %s', local_path, backup_url)
        remote = S3Backend(backup_url, s3_storage_class)
        assert local_path.is_dir()

        cache_name = hashlib.sha1(backup_url.encode()).hexdigest()
        cache_meta_path = cache_dir / cache_name / 'last-meta'

        previous_backup_meta = BackupMetaReader(cache_meta_path) if cache_meta_path.is_file() else None
        block_size = previous_backup_meta.block_size if previous_backup_meta else default_block_size

        backup_id = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
        temp_meta_path = temp_dir / 'meta.wip'
        meta_file = stack.enter_context(gzip.open(temp_meta_path, 'wb'))
        #data_collector = stack.enter_context(DataCollector(backup_id, temp_dir, remote))
        data_collector = stack.enter_context(S3DataCollector(backup_id, remote.bucket_name, remote.key_prefix, remote.storage_class))

        block_cache = {}
        block_cache_mutex = Lock()
        meta_file_mutex = Lock()

        def write_meta(obj):
            assert isinstance(obj, dict)
            obj_json = json.dumps(obj)
            with meta_file_mutex:
                if 'aes_key' not in obj_json:
                    logger.debug('write_meta: %s', obj_json)
                meta_file.write(obj_json.encode('utf-8') + b'\n')

        write_meta({
            'baq_backup': {
                'format_version': 1,
                'block_size': block_size,
            }
        })

        for path in walk_files(local_path):
            assert isinstance(path, Path)
            relative_path = str(path.relative_to(local_path))
            logger.debug('Processing %s', relative_path)

            if path.is_dir():
                st = path.stat()
                write_meta({
                    'directory': {
                        'path': relative_path,
                        'st_mtime_ns': str(st.st_mtime_ns),
                        'st_atime_ns': str(st.st_atime_ns),
                        'st_ctime_ns': str(st.st_ctime_ns),
                        'st_uid': st.st_uid,
                        'st_gid': st.st_gid,
                        'st_mode': oct(st.st_mode),
                        'owner': none_if_keyerror(lambda: path.owner()),
                        'group': none_if_keyerror(lambda: path.group()),
                    }})
            elif path.is_file():
                st = path.stat()
                write_meta({
                    'file': {
                        'path': relative_path,
                        'st_mtime_ns': str(st.st_mtime_ns),
                        'st_atime_ns': str(st.st_atime_ns),
                        'st_ctime_ns': str(st.st_ctime_ns),
                        'st_uid': st.st_uid,
                        'st_gid': st.st_gid,
                        'st_mode': oct(st.st_mode),
                        'owner': none_if_keyerror(lambda: path.owner()),
                        'group': none_if_keyerror(lambda: path.group()),
                    }})
                backup_file_contents(
                    path, write_meta, data_collector, previous_backup_meta,
                    block_cache, block_cache_mutex, block_size)

                st2 = path.stat()
                if (st.st_mtime_ns, st.st_size) != (st2.st_mtime_ns, st2.st_size):
                    logger.info('File has changed while being backed up: %s', path)
            else:
                logger.warning('Unsupported file type: %s', path)

        data_collector.close()
        meta_file.close()

        temp_meta_encrypted_path = temp_meta_path.with_name('meta.wip.gpg')
        encrypt_gpg(temp_meta_path, temp_meta_encrypted_path, encryption_recipients)

        remote.upload_file(temp_meta_encrypted_path, f'baq.{backup_id}.meta')

        if not cache_meta_path.parent.is_dir():
            logger.debug('Creating directory %s', cache_meta_path.parent)
            cache_meta_path.parent.mkdir(parents=True)

        if cache_meta_path.exists():
            cache_meta_path.unlink()
        shutil.move(temp_meta_path, cache_meta_path)
        logger.debug('Metadata file stored in %s', cache_meta_path)


def backup_file_contents(path, write_meta, data_collector, previous_backup_meta, block_cache, block_cache_mutex, block_size):
    '''
    Backup one file.
    '''
    assert isinstance(path, Path)
    aes_key = token_bytes(32)
    wfhash_queue = Queue(10)
    encrypt_queue = Queue(worker_count + 10)
    store_queue = Queue(worker_count + 10)

    def read_thread():
        try:
            bytes_read = 0
            with path.open('rb') as f:
                while True:
                    file_offset = f.tell()
                    block_raw_data = f.read(block_size)
                    if not block_raw_data:
                        break
                    bytes_read += len(block_raw_data)
                    wfhash_queue.put(block_raw_data)
                    encrypted_fut = SimpleFuture()
                    encrypt_queue.put((block_raw_data, encrypted_fut))
                    store_queue.put((file_offset, block_raw_data, encrypted_fut))
            wfhash_queue.put(None)
            for _ in range(worker_count):
                encrypt_queue.put(None)
            store_queue.put(None)
            return bytes_read
        except BaseException as e:
            logger.exception('read_thread failed: %r', e)
            raise e

    def whole_file_hash_thread():
        try:
            whole_file_sha1 = hashlib.sha1()
            while True:
                block_raw_data = wfhash_queue.get()
                if block_raw_data is None:
                    break
                whole_file_sha1.update(block_raw_data)
            return whole_file_sha1.hexdigest()
        except BaseException as e:
            logger.exception('whole_file_hash_thread failed: %r', e)
            raise e

    def compress_and_encrypt_thread():
        try:
            compressed_size = 0
            while True:
                item = encrypt_queue.get()
                if item is None:
                    break
                block_raw_data, encrypted_fut = item
                assert isinstance(encrypted_fut, SimpleFuture)
                block_sha3 = hashlib.sha3_512(block_raw_data).digest()

                already_existing_block_meta = previous_backup_meta and previous_backup_meta.get_block_by_sha3(block_sha3)

                if not already_existing_block_meta:
                    with block_cache_mutex:
                        already_existing_block_meta = block_cache.get(block_sha3)

                if already_existing_block_meta:
                    assert already_existing_block_meta.sha3 == block_sha3
                    encrypted_fut.set_result((already_existing_block_meta, block_sha3, None))
                    compressed_size += already_existing_block_meta.store_size
                    continue

                block_compressed_data = zstandard.compress(block_raw_data, level=9)
                block_encrypted_data = encrypt_aes(block_compressed_data, aes_key)
                encrypted_fut.set_result((None, block_sha3, block_encrypted_data))
                compressed_size += len(block_encrypted_data)
            return compressed_size
        except BaseException as e:
            logger.exception('compress_and_encrypt_thread failed: %r', e)
            raise e

    def store_thread():
        try:
            reused_blocks = 0
            new_blocks = 0
            while True:
                item = store_queue.get()
                if item is None:
                    break
                file_offset, block_raw_data, encrypted_fut = item
                already_existing_block_meta, block_sha3, block_encrypted_data = encrypted_fut.result()

                if not already_existing_block_meta:
                    with block_cache_mutex:
                        already_existing_block_meta = block_cache.get(block_sha3)

                if already_existing_block_meta:
                    assert isinstance(already_existing_block_meta, BackupMetaReader.FileBlock)
                    assert already_existing_block_meta.sha3 == block_sha3
                    write_meta({
                        'file_data': {
                            'offset': file_offset,
                            'size': len(block_raw_data),
                            'sha3': already_existing_block_meta.sha3.hex(),
                            'aes_key': already_existing_block_meta.aes_key.hex(),
                            'store_file': already_existing_block_meta.store_file,
                            'store_offset': already_existing_block_meta.store_offset,
                            'store_size': already_existing_block_meta.store_size,
                        }
                    })
                    reused_blocks += 1
                else:
                    store_filename, store_offset = data_collector.store_block(block_encrypted_data)
                    with block_cache_mutex:
                        block_cache[block_sha3] = BackupMetaReader.FileBlock(
                            offset=file_offset,
                            size=len(block_raw_data),
                            sha3=block_sha3,
                            aes_key=aes_key,
                            store_file=store_filename,
                            store_offset=store_offset,
                            store_size=len(block_encrypted_data))
                    write_meta({
                        'file_data': {
                            'offset': file_offset,
                            'size': len(block_raw_data),
                            'sha3': block_sha3.hex(),
                            'aes_key': aes_key.hex(),
                            'store_file': store_filename,
                            'store_offset': store_offset,
                            'store_size': len(block_encrypted_data),
                        }
                    })
                    new_blocks += 1
            logger.debug('reused_blocks: %r new_blocks: %r', reused_blocks, new_blocks)
        except BaseException as e:
            logger.exception('store_thread failed: %r', e)
            raise e

    with ThreadPoolExecutor(worker_count + 3, 'backup_file') as executor:
        read_thread_fut = executor.submit(read_thread)
        whole_file_hash_fut = executor.submit(whole_file_hash_thread)
        store_fut = executor.submit(store_thread)
        encrypt_futs = [executor.submit(compress_and_encrypt_thread) for _ in range(worker_count)]
        bytes_read = read_thread_fut.result()
        whole_file_hash_hex = whole_file_hash_fut.result()
        store_fut.result()
        compressed_size = sum(fut.result() for fut in encrypt_futs)

    assert wfhash_queue.qsize() == 0
    assert store_queue.qsize() == 0

    write_meta({
        'file_summary': {
            'size': bytes_read,
            'compressed_size': compressed_size,
            'compression_ratio': round(compressed_size / bytes_read, 3) if bytes_read else 0,
            'sha1': whole_file_hash_hex,
        }
    })
