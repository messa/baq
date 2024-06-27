from base64 import b64encode
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
from io import BytesIO
from itertools import count
from logging import getLogger
from pathlib import Path
import re
from reprlib import repr as smart_repr
from threading import Condition, Lock, local as threading_local

try:
    import boto3
except ImportError:
    boto3 = None


logger = getLogger(__name__)


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
