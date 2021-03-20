'''
Storage backend for AWS S3, and possibly other cloud services with similar API.
'''

from logging import getLogger
from pathlib import Path
import re

from .helpers import DataChunkInfo


logger = getLogger(__name__)


class S3Backend:

    def __init__(self, url):
        import boto3
        assert url.startswith('s3://')
        self._bucket_name, self._key_prefix = re.match(r'^s3://([^/]+)/([^?]+)$', url).groups()
        self._key_prefix = self._key_prefix.strip('/')
        self._client = boto3.client('s3')
        self._next_data_file_number = 0
        self._multipart_upload_id = None
        self._uploaded_parts = None
        self._data_file_pos = None
        self._part_buffer = None


    def __repr__(self):
        return f'<{self.__class__.__name__} {self.directory}>'

    def write_data_chunk(self, backup_id, chunk):
        assert isinstance(backup_id, str)
        assert isinstance(chunk, bytes)
        if not self._multipart_upload_id:
            assert not self._part_buffer
            self._current_data_file_name = f'baq.{backup_id}.data.{self._next_data_file_number:05d}'
            self._next_data_file_number += 1
            s3_key = self._key_prefix + '/' + self._current_data_file_name
            r = self._client.create_multipart_upload(
                Bucket=self._bucket_name,
                Key=s3_key)
            self._multipart_upload_id = r['UploadId']
            self._uploaded_parts = []
            self._part_buffer = []
            self._data_file_pos = 0
            logger.debug('Initiated multipart upload id %s', self._multipart_upload_id)
        assert self._current_data_file_name.startswith(f'baq.{backup_id}.data.')
        df_offset = self._data_file_pos
        self._part_buffer.append(chunk)
        self._data_file_pos += len(chunk)
        if sum(len(b) for b in self._part_buffer) > 50 * 2**20:
            s3_key = self._key_prefix + '/' + self._current_data_file_name
            r = self._client.upload_part(
                Bucket=self._bucket_name,
                Key=s3_key,
                UploadId=self._multipart_upload_id,
                PartNumber=len(self._uploaded_parts) + 1,
                Body=b''.join(self._part_buffer))
            self._uploaded_parts.append({
                'ETag': r['ETag'],
                'PartNumber': len(self._uploaded_parts) + 1,
            })
            self._part_buffer = []
        return DataChunkInfo(name=self._current_data_file_name, offset=df_offset, size=len(chunk))

    def close_data_file(self):
        if self._part_buffer:
            assert self._multipart_upload_id
            s3_key = self._key_prefix + '/' + self._current_data_file_name
            r = self._client.upload_part(
                Bucket=self._bucket_name,
                Key=s3_key,
                UploadId=self._multipart_upload_id,
                PartNumber=len(self._uploaded_parts) + 1,
                Body=b''.join(self._part_buffer))
            self._uploaded_parts.append({
                'ETag': r['ETag'],
                'PartNumber': len(self._uploaded_parts) + 1,
            })
            self._part_buffer = None
        if self._multipart_upload_id:
            assert not self._part_buffer
            assert self._uploaded_parts
            s3_key = self._key_prefix + '/' + self._current_data_file_name
            r = self._client.complete_multipart_upload(
                Bucket=self._bucket_name,
                Key=s3_key,
                UploadId=self._multipart_upload_id,
                MultipartUpload={'Parts': self._uploaded_parts})
            self._multipart_upload_id = None
            self._uploaded_parts = None
            self._part_buffer = None


    def read_data_chunk(self, name, offset, size):
        assert isinstance(name, str)
        assert isinstance(offset, int)
        assert isinstance(size, int)
        s3_key = self._key_prefix + '/' + name
        logger.info('Retrieving %d bytes on offset %d from s3://%s/%s', size, offset, self._bucket_name, s3_key)
        r = self._client.get_object(
            Bucket=self._bucket_name,
            Key=s3_key,
            Range=f'bytes={offset}-{offset+size-1}')
        chunk = r['Body'].read()
        assert len(chunk) == size
        return chunk

    def store_file(self, src_path, name):
        assert isinstance(src_path, Path)
        assert isinstance(name, str)
        s3_key = self._key_prefix + '/' + name
        with src_path.open(mode='rb') as f:
            logger.info('Uploading s3://%s/%s', self._bucket_name, s3_key)
            self._client.put_object(
                Bucket=self._bucket_name,
                Key=s3_key,
                StorageClass='STANDARD_IA',
                Body=f)

    def list_files(self):
        names = []
        paginator = self._client.get_paginator('list_objects_v2')
        iterator = paginator.paginate(
            Bucket=self._bucket_name,
            Prefix=self._key_prefix)
        for batch in iterator:
            if not batch.get('Contents'):
                continue
            for record in batch['Contents']:
                assert record['Key'].startswith(self._key_prefix + '/')
                name = record['Key'][len(self._key_prefix + '/'):]
                names.append(name)
        return sorted(names)

    def retrieve_file(self, name, dst_path):
        assert isinstance(name, str)
        assert isinstance(dst_path, Path)
        s3_key = self._key_prefix + '/' + name
        logger.info('Retrieving s3://%s/%s', self._bucket_name, s3_key)
        r = self._client.get_object(
            Bucket=self._bucket_name,
            Key=s3_key)
        with dst_path.open(mode='wb') as f:
            while True:
                chunk = r['Body'].read(65536)
                if not chunk:
                    break
                f.write(chunk)
