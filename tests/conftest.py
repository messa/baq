from datetime import datetime
from logging import getLogger
from pytest import fixture
import os
from socket import getfqdn
from uuid import uuid4

from baq.util import UTC


logger = getLogger(__name__)


@fixture(scope='session')
def test_session_id():
    now = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
    if os.environ.get('GITHUB_ACTIONS'):
        print('GITHUB_ACTION:', os.environ.get('GITHUB_ACTION'))
        print('GITHUB_ACTOR:', os.environ.get('GITHUB_ACTOR'))
        print('GITHUB_REF:', os.environ.get('GITHUB_REF'))
        print('GITHUB_REF_NAME:', os.environ.get('GITHUB_REF_NAME'))
        print('GITHUB_SHA:', os.environ.get('GITHUB_SHA'))
        print('GITHUB_RUN_ID:', os.environ.get('GITHUB_RUN_ID'))
        print('GITHUB_RUN_NUMBER:', os.environ.get('GITHUB_RUN_NUMBER'))
        parts = [
            now,
            'CI',
            os.environ['GITHUB_ACTOR'],
            os.environ['GITHUB_REF_NAME'],
            os.environ['GITHUB_SHA'][:7],
            os.environ['GITHUB_RUN_ID'],
            os.environ['GITHUB_RUN_NUMBER'],
            uuid4().hex[:5],
        ]
    else:
        parts = [
            now,
            getfqdn(),
            uuid4().hex[:5],
        ]
    test_session_id = '_'.join(part for part in parts if part)
    print(f'test_session_id: {test_session_id}')
    return test_session_id
