from dataclasses import dataclass
from logging import getLogger, WARNING
import os
from pathlib import Path
from pytest import fixture, skip
import re
import socket


logger = getLogger(__name__)


# Bucket name used for the local moto-based e2e tests.
MOTO_BUCKET_NAME = 'baq-test'

# Dummy AWS credentials used when running e2e tests against the local moto
# server. The endpoint URL is added once the moto server is actually running.
MOTO_AWS_ENV = {
    'AWS_ACCESS_KEY_ID': 'testing',
    'AWS_SECRET_ACCESS_KEY': 'testing',
    'AWS_DEFAULT_REGION': 'us-east-1',
    'AWS_REGION': 'us-east-1',
}


@dataclass
class E2E_S3_Config:
    bucket_name: str
    path_prefix: str


def using_real_s3():
    '''
    Whether the e2e tests should run against a real AWS S3 bucket instead of
    the local moto server.
    '''
    return bool(os.environ.get('BAQ_E2E_REAL_S3'))


@fixture(scope='session')
def e2e_s3_config_factory(test_session_id, request):
    '''
    Provide a factory that hands out a unique E2E_S3_Config for each test.

    By default the e2e tests run against a local in-process moto S3 server, so
    they need neither AWS credentials nor network access. Set BAQ_E2E_REAL_S3=1
    (together with BAQ_E2E_S3_PREFIX) to run them against a real AWS S3 bucket.
    '''
    if not os.environ.get('BAQ_E2E_TESTS'):
        skip('E2E tests not enabled (set BAQ_E2E_TESTS=1)')
    if using_real_s3():
        yield from _real_s3_config_factory(test_session_id)
    else:
        endpoint_url = request.getfixturevalue('moto_s3_endpoint')
        yield from _moto_s3_config_factory(test_session_id, endpoint_url)


def _real_s3_config_factory(test_session_id):
    if not os.environ.get('BAQ_E2E_S3_PREFIX'):
        skip('BAQ_E2E_S3_PREFIX not specified')
    m = re.match(r'^s3://([^/]+)/?(.*)$', os.environ['BAQ_E2E_S3_PREFIX'])
    if not m:
        raise Exception('BAQ_E2E_S3_PREFIX has invalid format')
    bucket_name, path_prefix = m.groups()
    path_prefix = f'{path_prefix}/{test_session_id}/'.lstrip('/')
    # Only clean up in CI; locally the artifacts are kept for debugging.
    yield from _config_factory(bucket_name, path_prefix, cleanup=bool(os.environ.get('CI')))


def _moto_s3_config_factory(test_session_id, endpoint_url):
    import boto3
    client = boto3.client('s3', endpoint_url=endpoint_url)
    client.create_bucket(Bucket=MOTO_BUCKET_NAME)
    # No cleanup needed - the moto server is discarded at the end of the session.
    yield from _config_factory(MOTO_BUCKET_NAME, f'{test_session_id}/', cleanup=False)


def _config_factory(bucket_name, path_prefix, cleanup):
    already_used_test_names = set()

    def the_e2e_s3_config_factory(test_name):
        if test_name in already_used_test_names:
            raise Exception(f'E2E S3 config already requested for test {test_name!r}')
        already_used_test_names.add(test_name)
        return E2E_S3_Config(
            bucket_name=bucket_name,
            path_prefix=path_prefix + test_name + '/')

    yield the_e2e_s3_config_factory

    if cleanup:
        delete_s3_folder(bucket_name, path_prefix)


@fixture(scope='session')
def moto_s3_endpoint():
    '''
    Run an in-process moto S3 server for the whole test session and point boto3
    (both this process and the spawned baq subprocesses) at it via environment
    variables.
    '''
    try:
        from moto.server import ThreadedMotoServer
    except ImportError:
        skip('moto is not installed (pip install "moto[s3,server]")')

    getLogger('werkzeug').setLevel(WARNING)
    host = '127.0.0.1'
    port = _find_free_tcp_port()
    server = ThreadedMotoServer(ip_address=host, port=port, verbose=False)
    server.start()
    endpoint_url = f'http://{host}:{port}'
    logger.info('Started moto S3 server at %s', endpoint_url)

    env_overrides = dict(MOTO_AWS_ENV, AWS_ENDPOINT_URL_S3=endpoint_url)
    saved_env = {key: os.environ.get(key) for key in env_overrides}
    os.environ.update(env_overrides)
    try:
        yield endpoint_url
    finally:
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        server.stop()
        logger.info('Stopped moto S3 server')


def _find_free_tcp_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def delete_s3_folder(bucket_name, path):
    import boto3
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=path):
        for obj in page.get('Contents', []):
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
def s3_client(e2e_s3_config_factory):
    try:
        import boto3
    except ImportError:
        skip('boto3 not installed')
    # e2e_s3_config_factory makes sure the moto server (if used) is running and
    # AWS_ENDPOINT_URL_S3 is set; for real S3 the endpoint URL stays unset.
    return boto3.client('s3', endpoint_url=os.environ.get('AWS_ENDPOINT_URL_S3'))


@fixture
def list_s3_keys(s3_client):
    def do_list_s3_keys(bucket_name, prefix):
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return sorted(f['Key'] for f in response['Contents'])
    return do_list_s3_keys
