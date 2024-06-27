from base64 import b64decode
from dataclasses import dataclass
from logging import getLogger
import os
from pathlib import Path
from pytest import fixture, skip
import re
from subprocess import STDOUT, check_call
import zlib


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


@fixture
def run_command():
    def do_run_command(cmd):
        assert isinstance(cmd, list)
        cmd = [str(part) for part in cmd]
        print()
        print('Running command:', ' '.join(cmd))
        print('----------------')
        print()
        check_call(cmd, stderr=STDOUT)
        print()
        print()
    return do_run_command


@fixture
def gnupghome(tmp_path, baq_e2e_test_gpg_key_private, baq_e2e_test_gpg_key_id, run_command):
    gnupghome = tmp_path / '.gnupg'
    gnupghome.mkdir(mode=0o700)

    (tmp_path / 'test_key.gpg').write_text(baq_e2e_test_gpg_key_private)
    (tmp_path / 'test_key.trust').write_text(f'{baq_e2e_test_gpg_key_id}:6:\n')

    run_command(['gpg2', '--homedir', gnupghome, '--import', str(tmp_path / 'test_key.gpg')])
    run_command(['gpg2', '--homedir', gnupghome, '--import-ownertrust', str(tmp_path / 'test_key.trust')])
    run_command(['gpg2', '--homedir', gnupghome, '-K'])

    return gnupghome


@fixture
def baq_e2e_test_gpg_key_id():
    return 'B0E7FC7C2C5003C01537A7B67ADADFE8F8B87C08'


@fixture
def baq_e2e_test_gpg_key_private():
    baq_e2e_test_gpg_key_private_compressed = '''
        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
        O+qLUhhpqncAa2DnQV/Wvp293AbSrulrzEf3Q5vLWl8//GXlJufsXd+Sn5mAZSNF0K3NJs0Ng9hJDyAM0FFBzpm6CP+plyRTs4RQ
        LbpKQcpTdjL75DxxdAqeZZ+jaaOJvOZ83kAh8IbK4D3giPa3jF+VkoXyKprp1l6VzcHx5yRBO8K5ZaxNoVrdWtXsjYbUV2tymvpO
        Xz2efl8cBCwSjCTggxWWj+AYSufa4jiJKGIwC4rXXPN5HuHM3KndXz8jrCR2HtauUT4if/JK9SAQDuG6zIRaaORUn03hPujrCs7j
        US4BV9GaMiHupAKyqbYQSflmqT77F37kaOz9DcFa4Hjgi4a69DMd2IH5lM0jMq0eJDdVv5BafnlJ1IhzvWVwYBv0N7lOY+vatu9l
        DY3QcgJdPw72Ncm3KaEZXSpOUavHaNHiu2w1SNxCVpmyieLpm33yRg9+o2HYR9/Nx37BZ/EFtp9JzIs7IL3N2M6bV1hqhMKOs9B2
        bDJB8j6gx5PHywi5vPQwms85gRvoATeXH1GHEAC77lvX7v7wbB+G17l7YkmX1E2H4K+M6oKnw+dcyzCqOPGwQ6ac7eej4dyjcUMY
        VBuAdNXdffpZdxUN6caWtFkiT03OFDioTi/lFRooL7yUmVBcER3zZ1Be8MjDulNgMsYtwAHF75xF8uFzNiuimm11HaNY+MXKfMqp
        mZSOO0IfwbASFbLm13e5Kho+myEnQxNKFwNVqJSKGChQO0SUBoGIyacEGzZbdW/3Io3ad3j1RFiGusyE/vYW+C4HWTSZP1TKSycL
        SDXSphd6nR7+/XF9RlHfN2dUFsouad9WHIpLND25cmoRZy1Q79U2koEhYWzJlQWKeOBBIx1pjkeMFLHvmb4wv8P0tB8J0QINkqZT
        gtmqB3msS3xWMe8lKjou7zmUE95FvvZANPhSYXXSh65+XthyIx4f2d5FvyZndmXG8FL4iXFsPAb9rQlTPnIpTlobmx/L60pGANjb
        /HG3PcV0bvhBI/NHQA3rkcD4p4A4L+z45ckyWQ0kpj29j2qZt1VKl6pc/OI5KwqUqPQJz5d9WF0ZSIPXZ8/0ZPt0ZYPglanxnjre
        qtz4L4JR/wfBAP+NMH8TjPnfhPIpcQdiZ47U6ikcpxdx7EhINwlwICVVXckkjY2Qqs/xvMnt6Xx5rDQaKb5gg4nFDmLJqaJaCqsk
        Or8r2pgirzS1qIpQQIAyKnG9cD+eY6QikiS0iUrCR9ItolrK8WyGF/U5O5TwXtmpBnP4cR9v/gYt5ROH9wAMnJWMoVAozTtwuYde
        XwVkXV2olcPtOpmTdnFrfU4GcsFv13vYTudguEzLGqGo6LCAvVjKiGRGuHQouVuvNmWLQOBdrhx0XxziLeR/Br/1gYo0Lscoawdq
        nKdYI335xKCDgPCEKThNFDRTFSzourNb5DcP1XSk1vGKvUjACozV0gIMUvB4lYMMWpbuEQgUXY9kGsD0bpx3VrYT9YyFZY52FueB
        wTLPuVOVLIuPohyYe8TbjI9H02f+p6Jr/BfOYajm7lYAhI+0EhwxgHNkvfEiNeUE1E0t7EiaP9cRNl+X+HvbSzQfGXWrIKXxfHnI
        QeteK7Q6OiDQOViRNx/Qgnu/Nn3n9GqNPedtBMYdNxz56vlfBlJpi+eC7w/afjUTdUK9ON/fa2oFmq7/kE3TbWiYzMZh+Z+EECwX
        /3zhoheohSnc+UV1F1tVgZgjX2iqArCB63e4xmPvsEAfcmqW60ljm6eOuiT88ikdZOHlsAnr4/C3xBKUN4ZuFo8jigbITjoZ94V0
        ZJzjhUwAKZtsTUhFL9OYKQBr1AzVX94DgX/IX2gH/rUTI1rC/9un+Sf8qW0v
    '''
    return zlib.decompress(b64decode(baq_e2e_test_gpg_key_private_compressed)).decode('ascii')
