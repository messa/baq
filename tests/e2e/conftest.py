from dataclasses import dataclass
from logging import getLogger
import os
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

    yield lambda test_name: E2E_S3_Config(
        bucket_name=bucket_name,
        path_prefix=path_prefix + test_name + '/')

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
