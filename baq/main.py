from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
from logging import getLogger
from pathlib import Path
import re
from subprocess import check_output
import sys
from tempfile import TemporaryDirectory

import zstandard

from .backup import do_backup, S3Backend, BackupMetaReader, decrypt_aes


logger = getLogger(__name__)


def baq_main():
    args = get_argument_parser().parse_args()
    setup_logging()
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


log_format = '%(asctime)s [%(process)d %(threadName)s] %(name)-10s %(levelname)5s: %(message)s'


def setup_logging():
    from logging import basicConfig, DEBUG, INFO
    basicConfig(format=log_format, level=DEBUG)
    getLogger('botocore').setLevel(INFO)
    getLogger('s3transfer').setLevel(INFO)


def do_restore(backup_url, local_path):
    assert isinstance(backup_url, str)
    assert isinstance(local_path, Path)
    assert backup_url.startswith('s3://')
    if not local_path.exists():
        logger.info('Creating restore directory %s', local_path)
        local_path.mkdir()
    assert local_path.is_dir()
    with ExitStack() as stack:
        temp_dir = Path(stack.enter_context(TemporaryDirectory(prefix='baq.')))
        logger.info('Restoring %s to %s', backup_url, local_path)

        if m := re.match(r'^(s3://.+)/(baq.[0-9TZ]+.meta)$', backup_url):
            backup_url, meta_filename = m.groups()
            remote = S3Backend(backup_url)
            remote.download_file(meta_filename, temp_dir / 'meta.gpg')
        else:
            raise Exception('Please provide full path to meta file in backup URL')

        decrypt_gpg(temp_dir / 'meta.gpg', temp_dir / 'meta')
        meta = BackupMetaReader(temp_dir / 'meta')

        data_file_map = {}
        for file_path, file_meta in meta.files.items():
            for b in file_meta.blocks:
                data_file_map.setdefault(b.store_file, []).append((file_path, b))

        # TODO: restore from glacier all files in data_file_map.keys()

        pool = stack.enter_context(ThreadPoolExecutor(8, 'restore'))
        futures = []
        for data_file_name, data_file_contents in sorted(data_file_map.items()):
            futures.append(
                pool.submit(
                    restore_from_data_file,
                    remote,
                    data_file_name,
                    data_file_contents,
                    local_path))

        for fut in futures:
            fut.result()

        for dir_path, dir_meta in meta.directories.items():
            full_path = local_path / dir_path
            full_path.mkdir(exist_ok=True)
        for file_path, file_meta in meta.files.items():
            full_path = local_path / file_path
            if file_meta.original_size == 0:
                with full_path.open('wb'):
                    pass
            if sha1_file(full_path).digest() == file_meta.original_sha1:
                logger.info('Checksum %s OK', file_path)
            else:
                raise Exception('Checksum failed')


def sha1_file(file_path):
    with file_path.open('rb') as f:
        h = hashlib.sha1()
        while True:
            block = f.read(65536)
            if not block:
                break
            h.update(block)
        return h


def restore_from_data_file(remote, store_file_name, restore_blocks, local_path):
    assert isinstance(store_file_name, str)
    assert isinstance(local_path, Path)
    filtered_restore_blocks = []
    for original_path, block_meta in restore_blocks:
        already_restored = False
        original_full_path = local_path / original_path
        try:
            with original_full_path.open('rb') as f:
                f.seek(block_meta.offset)
                file_data = f.read(block_meta.size)
                if file_data and hashlib.sha3_512(file_data).digest() == block_meta.sha3:
                    already_restored = True
                del file_data
        except FileNotFoundError:
            pass
        if already_restored:
            logger.debug(
                'File %s offset %d length %d is already restored',
                original_path, block_meta.offset, block_meta.size)
        else:
            filtered_restore_blocks.append((original_path, block_meta))
    restore_blocks = filtered_restore_blocks
    del filtered_restore_blocks
    retrieve_ranges = [
        (block_meta.store_offset, block_meta.store_size)
        for _, block_meta in restore_blocks
    ]
    retrieved_range_data = remote.retrieve_file_ranges(store_file_name, retrieve_ranges)
    for n, ((original_path, block_meta), encrypted_data) in enumerate(zip(restore_blocks, retrieved_range_data), start=1):
        assert isinstance(original_path, str)
        assert isinstance(block_meta, BackupMetaReader.FileBlock)
        assert block_meta.store_file == store_file_name
        assert isinstance(encrypted_data, bytes)
        logger.debug(
            'Restoring file %s offset %d length %d from %s (%d/%d)',
            original_path, block_meta.offset, block_meta.size, store_file_name, n, len(restore_blocks))
        compressed_data = decrypt_aes(encrypted_data, block_meta.aes_key)
        original_data = zstandard.decompress(compressed_data)
        assert hashlib.sha3_512(original_data).digest() == block_meta.sha3
        original_full_path = local_path / original_path
        if not original_full_path.parent.exists():
            logger.debug('Creating directory %s', original_full_path.parent)
            original_full_path.parent.mkdir(parents=True)
        with ExitStack() as stack:
            try:
                f = stack.enter_context(original_full_path.open('r+b'))
            except FileNotFoundError:
                f = stack.enter_context(original_full_path.open('w+b'))
            f.seek(block_meta.offset)
            if f.read(len(original_data)) == original_data:
                logger.debug('Already restored')
            else:
                f.seek(block_meta.offset)
                assert f.tell() == block_meta.offset
                f.write(original_data)


def decrypt_gpg(src_path, dst_path):
    assert src_path.is_file()
    assert not dst_path.exists()
    gpg_cmd = ['gpg2', '--decrypt', '-o', str(dst_path), str(src_path)]
    logger.debug('Running %s', ' '.join(gpg_cmd))
    check_output(gpg_cmd)
    assert dst_path.is_file()
    assert dst_path.stat().st_size
