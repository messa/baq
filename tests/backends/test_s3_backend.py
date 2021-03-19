from datetime import datetime
import os
from os import getpid
from pprint import pprint
from pytest import fixture, skip
from socket import getfqdn


@fixture
def s3_bucket_name():
    bucket_name = os.environ.get('BAQ_TEST_S3_BUCKET')
    if not bucket_name:
        skip('BAQ_TEST_S3_BUCKET not specified')
    return bucket_name


def test_pytest_test_name(request):
    assert request.node.name == 'test_pytest_test_name'


_s3_prefix_date = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')


@fixture
def s3_prefix(request):
    prefix = os.environ.get('BAQ_TEST_S3_PREFIX')
    if not prefix:
        skip('BAQ_TEST_S3_PREFIX not specified')
    prefix = prefix.strip('/')
    prefix += f'/baq-pytest-{_s3_prefix_date}-{getfqdn()}-{getpid()}-{request.node.name}'
    return prefix


def test_s3_access(s3_bucket_name, s3_prefix):
    import boto3
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Body=b'Hello S3!\n',
        Bucket=s3_bucket_name,
        Key=f'{s3_prefix}/hello.txt')
    try:
        r = s3_client.list_objects_v2(
            Bucket=s3_bucket_name,
            Prefix=s3_prefix)
        item, = r['Contents']
        assert item['Key'] == f'{s3_prefix}/hello.txt'
    finally:
        s3_client.delete_object(
            Bucket=s3_bucket_name,
            Key=f'{s3_prefix}/hello.txt')


@fixture
def s3_backend(s3_bucket_name, s3_prefix):
    from baq.backends import S3Backend
    skip()
    return S3Backend()


def test_s3_backend_init(s3_backend):
    assert s3_backend


def test_s3_backend_write_data_chunk(s3_backend, temp_dir):
    skip()
    r = s3_backend.write_data_chunk('backup1', b'Hello!')
    assert r.name == 'baq.backup1.data.00000'
    assert r.offset == 0
    assert r.size == 6
    r = s3_backend.write_data_chunk('backup1', b'Banana')
    assert r.name == 'baq.backup1.data.00000'
    assert r.offset == 6
    assert r.size == 6
    s3_backend.close_data_file()
    data_path = temp_dir / 'backup' / r.name
    assert data_path.read_bytes() == b'Hello!Banana'


def test_s3_backend_read_data_chunk(s3_backend):
    skip()
    r = s3_backend.write_data_chunk('backup1', b'Hello!')
    s3_backend.close_data_file()
    data = s3_backend.read_data_chunk(r.name, r.offset, r.size)
    assert data == b'Hello!'


def test_s3_backend_list_files(s3_backend):
    skip()
    assert s3_backend.list_files() == []
    r = s3_backend.write_data_chunk('backup1', b'Hello!')
    s3_backend.close_data_file()
    assert s3_backend.list_files() == [r.name]


def test_s3_backend_store_and_retrieve_file(s3_backend, temp_dir):
    skip()
    (temp_dir / 'sample.txt').write_bytes(b'Some metadata')
    s3_backend.store_file(temp_dir / 'sample.txt', 'testfile')
    s3_backend.retrieve_file('testfile', temp_dir / 'sample2.txt')
    assert (temp_dir / 'sample2.txt').read_bytes() == b'Some metadata'
