from argparse import ArgumentParser
from logging import getLogger
from pathlib import Path
import sys

from .operations import backup, restore


logger = getLogger(__name__)

log_format = '%(asctime)s [%(process)d] %(name)-17s %(levelname)5s: %(message)s'


def baq_main():
    p = ArgumentParser()
    p.add_argument('--verbose', '-v', action='store_true')
    p.add_argument('--restore', metavar='BACKUP_ID')
    p.add_argument('--recipient', '-r', action='append')
    p.add_argument('--recipients-file', '-R', action='append')
    p.add_argument('--skip-encryption', action='store_true')
    p.add_argument('--identity', '-i', action='append')
    p.add_argument('path')
    p.add_argument('destination')
    args = p.parse_args()
    setup_logging(args.verbose)
    backend = get_destination_backend(args.destination)
    src_path = Path(args.path).resolve()
    if args.restore:
        restore(src_path, backend, args.restore, args.identity)
    else:
        if not args.recipient and not args.recipients_file:
            if not args.skip_encryption:
                msg = 'ERROR: No --recipient or --recipients-file specified. '
                msg += 'Do you really want to skip the encryption protection? '
                msg += 'If yes, please add parameter --skip-encryption.'
                # The data file chunks will still be encrypted with AES key,
                # so the data itself will not be readable without the metadata file.
                # But the AES key will be stored in metadata file in plaintext, not age-encrypted.
                sys.exit(msg)
        else:
            if args.skip_encryption:
                sys.exit('ERROR: Cannot combine --skip-encryption with --recipient or --recipients-file.')
        backup(src_path, backend, args.recipient, args.recipients_file)


def get_destination_backend(url):
    if url.startswith('file://'):
        from .backends.file import FileBackend
        return FileBackend(url[7:])
    if url.startswith('s3://'):
        from .backends.s3 import S3Backend
        return S3Backend(url)
    raise Exception('Could not recognize backup destination URL format: {}'.format(url))


def setup_logging(verbose):
    from logging import DEBUG, INFO, basicConfig
    basicConfig(
        format=log_format,
        level=DEBUG if verbose else INFO)
    getLogger('botocore').setLevel(INFO)
