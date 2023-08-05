from base64 import b64encode
from itertools import count
from subprocess import check_output
import boto3
from collections import deque, namedtuple
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime
import gzip
import hashlib
from io import BytesIO
import json
from logging import getLogger
import os
from os import cpu_count
from pathlib import Path
from reprlib import repr as smart_repr
from queue import Queue
import re
from secrets import token_bytes
import shutil
from sys import intern
from tempfile import TemporaryDirectory
from threading import Condition, Event, Lock, local as threading_local
from time import monotonic as monotime
import zstandard


logger = getLogger(__name__)

default_block_size = int(os.environ.get('BAQ_BLOCK_SIZE') or 128 * 1024)
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

        cache_name = hashlib.sha1(f'{local_path.resolve()} {backup_url}'.encode()).hexdigest()
        cache_dir = Path('~/.cache/baq').expanduser() / cache_name
        cache_meta_path = cache_dir / 'last-meta'

        previous_backup_meta = BackupMetaReader(cache_meta_path) if cache_meta_path.is_file() else None
        block_size = previous_backup_meta.block_size if previous_backup_meta else default_block_size

        backup_id = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        temp_meta_path = temp_dir / 'meta.wip'
        meta_file = stack.enter_context(gzip.open(temp_meta_path, 'wb'))
        #data_collector = stack.enter_context(DataCollector(backup_id, temp_dir, remote))
        data_collector = stack.enter_context(S3DataCollector(backup_id, remote.bucket_name, remote.key_prefix, remote.storage_class))

        block_cache = {}
        block_cache_mutex = Lock()

        def write_meta(obj):
            assert isinstance(obj, dict)
            obj_json = json.dumps(obj)
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
                        'owner': none_if_keyerror(path.owner),
                        'group': none_if_keyerror(path.group),
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
                        'owner': none_if_keyerror(path.owner),
                        'group': none_if_keyerror(path.group),
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


def encrypt_gpg(src_path, dst_path, recipients):
    assert src_path.is_file()
    assert not dst_path.exists()
    gpg_cmd = ['gpg2', '--encrypt', '--sign', '--trust-model=always', '--compress-algo=none']
    for r in recipients:
        gpg_cmd.extend(['-r', r])
    gpg_cmd.extend(['-o', str(dst_path), str(src_path)])
    logger.debug('Running %s', ' '.join(gpg_cmd))
    check_output(gpg_cmd)
    assert dst_path.is_file()
    assert dst_path.stat().st_size


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
                        int(f['st_mtime_ns']), int(f['st_atime_ns']), int(d['st_mode'], 8), d['st_uid'], d['st_gid'],
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


def encrypt_aes(data, key):
    from cryptography.hazmat.primitives.ciphers import Cipher
    from cryptography.hazmat.primitives.ciphers.algorithms import AES
    from cryptography.hazmat.primitives.ciphers.modes import CTR
    nonce = token_bytes(16)
    encryptor = Cipher(AES(key), CTR(nonce)).encryptor()
    return nonce + encryptor.update(data) + encryptor.finalize()


def decrypt_aes(encrypted_data, key):
    from cryptography.hazmat.primitives.ciphers import Cipher
    from cryptography.hazmat.primitives.ciphers.algorithms import AES
    from cryptography.hazmat.primitives.ciphers.modes import CTR
    nonce = encrypted_data[:16]
    decryptor = Cipher(AES(key), CTR(nonce)).decryptor()
    return decryptor.update(encrypted_data[16:]) + decryptor.finalize()


assert decrypt_aes(encrypt_aes(b'hello', 32*b'x'), 32*b'x') == b'hello'
assert decrypt_aes(encrypt_aes(b'\x00\x01\x02'*999, 32*b'x'), 32*b'x') == b'\x00\x01\x02'*999


class TempDirDataCollector:

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


class S3DataCollector:

    data_file_size = 100 * 2**30

    def __init__(self, backup_id, bucket_name, key_prefix, storage_class):
        assert isinstance(backup_id, str)
        assert isinstance(bucket_name, str)
        assert isinstance(key_prefix, str)
        assert isinstance(storage_class, str)
        assert not key_prefix or key_prefix.endswith('/')
        self.backup_id = backup_id
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.storage_class = storage_class
        self.file_number = count()
        self.s3_client = boto3.client('s3')
        self.current_file = None
        self.all_files = []

    def __enter__(self):
        self.stack = ExitStack()
        self.stack.__enter__()
        self.create_pool = self.stack.enter_context(ThreadPoolExecutor(1, 's3-create'))
        self.upload_pool = self.stack.enter_context(ThreadPoolExecutor(8, 's3-upload'))
        self.finish_pool = self.stack.enter_context(ThreadPoolExecutor(1, 's3-finish'))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stack.__exit__(exc_type, exc_val, exc_tb)

    def store_block(self, data):
        '''
        Returns tuple (filename, offset)
        '''
        assert isinstance(data, bytes)

        if not self.current_file:
            self.current_file_name = f'baq.{self.backup_id}.data-{next(self.file_number):06d}'
            self.current_file = self.stack.enter_context(S3DataCollectorFile(
                self.s3_client, self.bucket_name, self.key_prefix + self.current_file_name,
                self.storage_class, self.create_pool, self.upload_pool, self.finish_pool))
            self.all_files.append(self.current_file)

        filename, offset = self.current_file_name, self.current_file.tell()
        self.current_file.write(data)

        if offset + len(data) >= self.data_file_size:
            self.current_file.close()
            self.current_file = None

        return filename, offset

    def close(self):
        if self.current_file:
            self.current_file.close()
            self.current_file = None
        self.stack.close()
        assert all(f.closed_successfully is True for f in self.all_files)


class S3DataCollectorFile:

    part_size = 100 * 2**20

    def __init__(self, s3_client, bucket_name, key, storage_class, create_pool, upload_pool, finish_pool):
        assert isinstance(create_pool, ThreadPoolExecutor)
        assert isinstance(upload_pool, ThreadPoolExecutor)
        assert isinstance(finish_pool, ThreadPoolExecutor)
        self.mutex = Lock()
        with self.mutex:
            self.s3_client = s3_client
            self.bucket_name = bucket_name
            self.key = key
            self.storage_class = storage_class
            self.create_pool = create_pool
            self.upload_pool = upload_pool
            self.finish_pool = finish_pool
            self.upload_id = None
            self.offset = 0
            self.parts = []
            self.part_buffer = BytesIO()
            self.waiting_upload_count = 0
            self.waiting_upload_count_cond = Condition()
            self.close_fut = None
            self.create_fut = self.create_pool.submit(self._create_multipart_upload)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.mutex:
            if not self.close_fut:
                self.close_fut = self.finish_pool.submit(self._abort)
        self.close_fut.result()

    def _create_multipart_upload(self):
        try:
            result = self.s3_client.create_multipart_upload(
                ACL='private',
                Bucket=self.bucket_name,
                Key=self.key,
                ChecksumAlgorithm='SHA1',
                StorageClass=self.storage_class)
            with self.mutex:
                assert self.upload_id is None
                self.upload_id = result['UploadId']
        except Exception as e:
            # early logging - otherwise the exception would be logged after other threads cleanup
            logger.exception('Failed to create multipart upload: %r', e)
            raise e

    def tell(self):
        with self.mutex:
            return self.offset

    def write(self, data):
        assert isinstance(data, bytes)
        # this object API is supposed to be called from just one thread, but use mutex to be sure
        with self.waiting_upload_count_cond:
            while self.waiting_upload_count >= 3:
                logger.debug('Waiting for already running uploads to finish')
                self.waiting_upload_count_cond.wait()
        with self.mutex:
            self.part_buffer.write(data)
            self.offset += len(data)
            if self.part_buffer.tell() >= self.part_size:
                self._schedule_upload()

    def close(self):
        with self.mutex:
            if self.part_buffer.tell():
                self._schedule_upload()
            assert self.part_buffer.tell() == 0
            assert self.close_fut is None
            self.close_fut = self.finish_pool.submit(self._complete)

    def _schedule_upload(self):
        with self.waiting_upload_count_cond:
            self.waiting_upload_count += 1
        self.parts.append(self.upload_pool.submit(self._upload, self.part_buffer.getvalue(), len(self.parts) + 1))
        self.part_buffer = BytesIO()

    def _upload(self, part_data, part_number):
        try:
            with self.waiting_upload_count_cond:
                self.waiting_upload_count -= 1
                self.waiting_upload_count_cond.notify_all()
            self.create_fut.result()
            assert self.upload_id
            logger.debug('Multipart upload %s part %d starting', self.key, part_number)
            checksum = hashlib.sha1(part_data).digest()
            upload_response = self.s3_client.upload_part(
                Bucket=self.bucket_name,
                Key=self.key,
                UploadId=self.upload_id,
                PartNumber=part_number,
                Body=part_data,
                ChecksumSHA1=b64encode(checksum).decode('ascii'))
            logger.debug('Multipart upload %s part %d finished', self.key, part_number)
            return upload_response['ETag'], checksum
        except Exception as e:
            # early logging - otherwise the exception would be logged after other threads cleanup
            logger.exception('Failed to upload part: %r', e)
            raise e

    def _abort(self):
        with self.mutex:
            self.part_buffer = None
            self.parts = None
            if self.upload_id:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=self.key,
                    UploadId=self.upload_id)
                logger.debug('Multipart upload aborted: %s', self.key)
            self.upload_id = None

    def _complete(self):
        self.create_fut.result()
        assert self.upload_id
        logger.debug('Going to complete multipart upload: %s', self.key)
        resolved_parts = [fut.result() for fut in self.parts]
        big_checksum = hashlib.sha1(b''.join(part_sha1 for part_etag, part_sha1 in resolved_parts)).digest()
        try:
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=self.key,
                UploadId=self.upload_id,
                MultipartUpload={
                    'Parts': [
                        {
                            'PartNumber': part_number,
                            'ETag': part_etag,
                            'ChecksumSHA1': b64encode(part_sha1).decode('ascii'),
                        }
                        for part_number, (part_etag, part_sha1) in enumerate(resolved_parts, start=1)
                    ]
                },
                ChecksumSHA1=b64encode(big_checksum).decode('ascii'))
            self.closed_successfully = True
            logger.debug('Multipart upload finished successfully: %s', self.key)
        except Exception as e:
            logger.error('Multipart upload %s completion failed: %r', self.key, e)
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket_name,
                Key=self.key,
                UploadId=self.upload_id)


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


class S3Backend:

    def __init__(self, backup_url, storage_class=None):
        assert '?' not in backup_url
        self.storage_class = storage_class
        m = re.match(r'^s3://([^/]+)/(.*)$', backup_url)
        self.bucket_name, self.key_prefix = m.groups()
        self.key_prefix = self.key_prefix.strip('/')
        self.key_prefix = self.key_prefix + '/' if self.key_prefix else ''
        self._thread_local = threading_local()

    def _get_s3_client(self):
        try:
            return self._thread_local.s3_client
        except AttributeError:
            logger.debug('Creating thread-local boto3 client')
            self._thread_local.s3_client = boto3.client('s3')
            return self._thread_local.s3_client

    def upload_file(self, src_path, filename):
        assert isinstance(src_path, Path)
        assert self.storage_class
        key = self.key_prefix + filename
        s3_client = self._get_s3_client()
        s3_client.upload_file(
            src_path, self.bucket_name, key,
            ExtraArgs={
                'ACL': 'private',
                'StorageClass': 'STANDARD_IA',
            })
        logger.info('Uploaded file s3://%s/%s (%.2f MB)', self.bucket_name, key, src_path.stat().st_size / 2**20)

    def download_file(self, filename, dst_path):
        assert isinstance(dst_path, Path)
        assert not dst_path.exists()
        key = self.key_prefix + filename
        s3_client = self._get_s3_client()
        s3_client.download_file(self.bucket_name, key, dst_path)
        logger.info('Downloaded file s3://%s/%s (%.2f MB)', self.bucket_name, key, dst_path.stat().st_size / 2**20)

    def retrieve_file_range(self, filename, offset, size):
        assert isinstance(filename, str)
        assert isinstance(offset, int)
        assert isinstance(size, int)
        key = self.key_prefix + filename
        s3_client = self._get_s3_client()
        res = s3_client.get_object(
            Bucket=self.bucket_name,
            Key=key,
            Range=f'bytes={offset}-{offset+size-1}')
        content = res['Body'].read()
        assert isinstance(content, bytes)
        assert len(content) == size
        return content

    def retrieve_file_ranges(self, filename, offset_size_list):
        logger.debug('retrieve_file_ranges %s %s', filename, smart_repr(offset_size_list))
        assert isinstance(filename, str)
        s3_client = self._get_s3_client()
        offset_size_list = deque(offset_size_list)
        assert len(offset_size_list) > 0
        for offset, size in offset_size_list:
            assert isinstance(offset, int)
            assert isinstance(size, int)
        while offset_size_list:
            consecutive_range_end = offset_size_list[0][0] + offset_size_list[0][1]
            item_count = 1
            for i in range(1, len(offset_size_list)):
                if consecutive_range_end != offset_size_list[i][0]:
                    break
                consecutive_range_end += offset_size_list[i][1]
                item_count += 1
            logger.debug(
                'get_object %s bytes %d - %d size %d for %d items',
                filename, offset_size_list[0][0], consecutive_range_end-1,
                consecutive_range_end-1 - offset_size_list[0][0],
                item_count)
            res = s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.key_prefix + filename,
                Range=f'bytes={offset_size_list[0][0]}-{consecutive_range_end-1}')
            while True:
                offset, size = offset_size_list.popleft()
                assert offset + size <= consecutive_range_end
                logger.debug('Reading offset %d size %d', offset, size)
                data = res['Body'].read(size)
                assert len(data) == size
                yield data
                if offset + size == consecutive_range_end:
                    break
            res['Body'].close()
