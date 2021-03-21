from logging import getLogger
from subprocess import check_output


logger = getLogger(__name__)


def encrypt_with_age(data, recipients, recipients_files):
    assert isinstance(data, bytes)
    cmd = ['age', '--armor']
    if recipients:
        for r in recipients:
            cmd.extend(('--recipient', r))
    if recipients_files:
        for path in recipients_files:
            cmd.extend(('--recipients-file', path))
    logger.debug('Running command: %s', ' '.join(cmd))
    out = check_output(cmd, input=data)
    return out.decode('ascii')


def decrypt_with_age(data, identity_files):
    assert isinstance(data, str)
    cmd = ['age', '--decrypt']
    if identity_files:
        for path in identity_files:
            cmd.extend(('--identity', str(path)))
    logger.debug('Running command: %s', ' '.join(cmd))
    out = check_output(cmd, input=data.encode('ascii'))
    return out
