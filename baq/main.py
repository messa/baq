from argparse import ArgumentParser
from logging import getLogger
import os
from pathlib import Path
import sys

from .backup import do_backup
from .restore import do_restore


logger = getLogger(__name__)


def baq_main():
    '''
    Main entry point. Parse arguments and call the appropriate function.
    '''
    # TODO: add Sentry init if BAQ_SENTRY_DSN is set
    args = get_argument_parser().parse_args()
    setup_logging(verbose=args.verbose)
    setup_log_file(os.environ.get('BAQ_LOG_FILE'))
    try:
        if args.action == 'backup':
            if not args.recipient:
                sys.exit('No encryption recipients were specified')
            do_backup(
                Path(args.local_path).resolve(),
                args.backup_url,
                s3_storage_class=args.s3_storage_class,
                encryption_recipients=args.recipient)
        elif args.action == 'restore':
            do_restore(
                args.backup_url,
                Path(args.local_path).resolve())
        else:
            raise Exception('Invalid args.action')
    except BaseException as e:
        logger.exception('Failed: %r', e)
        sys.exit(f'Failed: {e}')


def get_argument_parser():
    parser = ArgumentParser()
    parser.add_argument('--verbose', '-v', action='store_true')
    subparsers = parser.add_subparsers(required=True)

    backup_parser = subparsers.add_parser('backup')
    backup_parser.set_defaults(action='backup')
    backup_parser.add_argument('local_path', help='path to back up')
    backup_parser.add_argument('backup_url', help='s3://...')
    backup_parser.add_argument('--s3-storage-class', metavar='<value>', default='STANDARD_IA')
    backup_parser.add_argument('--recipient', '-r', action='append')

    restore_parser = subparsers.add_parser('restore')
    restore_parser.set_defaults(action='restore')
    restore_parser.add_argument('backup_url', help='s3://...')
    restore_parser.add_argument('local_path', help='path where backup will be restores')

    return parser


log_format = '%(asctime)s [%(process)d %(threadName)-10s] %(name)-10s %(levelname)5s: %(message)s'


def setup_logging(verbose):
    from logging import DEBUG, INFO, StreamHandler, Formatter
    getLogger('').setLevel(DEBUG)
    getLogger('boto3.s3').setLevel(INFO)
    getLogger('botocore').setLevel(INFO)
    getLogger('s3transfer').setLevel(INFO)
    # log to stderr
    h = StreamHandler()
    h.setFormatter(Formatter(log_format))
    h.setLevel(DEBUG if verbose else INFO)
    getLogger('').addHandler(h)


def setup_log_file(log_file):
    from logging import DEBUG, Formatter
    from logging.handlers import WatchedFileHandler
    if not log_file:
        return
    h = WatchedFileHandler(log_file)
    h.setFormatter(Formatter(log_format))
    h.setLevel(DEBUG)
    getLogger('').addHandler(h)
