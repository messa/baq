from dataclasses import dataclass
from logging import getLogger
import os
from pathlib import Path
from pytest import fixture, skip
import re


logger = getLogger(__name__)


@dataclass
class E2E_S3_Config:
    bucket_name: str
    path_prefix: str


@fixture(scope='session')
def e2e_s3_config_factory(test_session_id):
    if not os.environ.get('BAQ_E2E_TESTS'):
        skip('E2E tests not enabled')
    if not os.environ.get('BAQ_E2E_S3_PREFIX'):
        skip('BAQ_E2E_S3_PREFIX not specified')
    m = re.match(r'^s3://([^/]+)/?(.*)$', os.environ['BAQ_E2E_S3_PREFIX'])
    if not m:
        raise Exception('BAQ_E2E_S3_PREFIX has invalid format')
    bucket_name, path_prefix = m.groups()
    path_prefix = f'{path_prefix}/{test_session_id}/'.lstrip('/')
    already_used_test_names = set()

    def the_e2e_s3_config_factory(test_name):
        assert test_name not in already_used_test_names
        already_used_test_names.add(test_name)
        return E2E_S3_Config(
            bucket_name=bucket_name,
            path_prefix=path_prefix + test_name + '/')

    yield the_e2e_s3_config_factory

    if os.environ.get('CI'):
        delete_s3_folder(bucket_name, path_prefix)


def delete_s3_folder(bucket_name, path):
    import boto3
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=path):
        for obj in page['Contents']:
            logger.info('Deleting S3 object %s %s', bucket_name, obj['Key'])
            client.delete_object(Bucket=bucket_name, Key=obj['Key'])


@fixture
def e2e_s3_config(e2e_s3_config_factory, request):
    return e2e_s3_config_factory(test_name=request.node.name)


@fixture
def e2e_test_block_device_path():
    '''
    How to setup a block device for testing:

    # Create a 1GB file
    dd if=/dev/zero of=/tmp/test.img bs=1M count=10
    # Create a loop device
    sudo losetup -fP /tmp/test.img
    # Use the loop device
    export BAQ_E2E_TEST_BLOCK_DEVICE=$(sudo losetup -j /tmp/test.img | cut -d: -f1)
    '''
    if not os.environ.get('BAQ_E2E_TESTS'):
        skip('E2E tests not enabled')
    if not os.environ.get('BAQ_E2E_TEST_BLOCK_DEVICE'):
        skip('BAQ_E2E_TEST_BLOCK_DEVICE not specified')
    device_path = Path(os.environ['BAQ_E2E_TEST_BLOCK_DEVICE'])
    logger.debug('e2e_test_block_device_path: %s', device_path)
    assert device_path.is_block_device()
    # This not really necessary, just a sanity check:
    assert str(device_path).startswith('/dev/loop')
    return device_path


@fixture
def s3_client():
    try:
        import boto3
    except ImportError:
        skip('boto3 not installed')
    return boto3.client('s3')


@fixture
def list_s3_keys(s3_client):
    def do_list_s3_keys(bucket_name, prefix):
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return sorted(f['Key'] for f in response['Contents'])
    return do_list_s3_keys
