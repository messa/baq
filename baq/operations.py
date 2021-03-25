'''
High-level implementation of main operations - backup and restore.
'''

from collections import namedtuple
from contextlib import ExitStack
from datetime import datetime
from functools import partial
import gzip
import hashlib
import json
from logging import getLogger
import os
from pathlib import Path
import re
from reprlib import repr as smart_repr
from tempfile import TemporaryDirectory
from time import monotonic as monotime

from .age_wrapper import encrypt_with_age
from .chunk_adapter import ChunkAdapter
from .decrypt_key_manager import DecryptKeyManager


logger = getLogger(__name__)

chunk_size = 2**20


BackupResult = namedtuple('BackupResult', 'backup_id')


def backup(src_path, backend, recipients, recipients_files, reuse_backup_count=30):
    t0 = monotime()
    encryption_key = os.urandom(32)
    encryption_key_sha1 = hashlib.new('sha1', encryption_key).hexdigest()
    if recipients or recipients_files:
        age_encrypted_encryption_key = encrypt_with_age(encryption_key, recipients=recipients, recipients_files=recipients_files)
    else:
        logger.info('No recipients specified - the data file AES key will be stored in metadata file unencrypted')
        age_encrypted_encryption_key = None
    backup_id = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    adapter = ChunkAdapter(backend)
    logger.info('Backing up %s to %s - backup id %s', src_path, backend, backup_id)
    with ExitStack() as stack:
        stack.callback(backend.close_data_file)
        temp_dir = Path(stack.enter_context(TemporaryDirectory(prefix=f'baq.{backup_id}.')))
        reuse_encryption_keys, reuse_blocks = load_previous_backup_for_reuse(backend, temp_dir, reuse_backup_count)
        meta_path = temp_dir / f'baq.{backup_id}.meta'
        meta_file = stack.enter_context(gzip.open(meta_path, mode='wb'))
        meta_file.write(to_json(generate_header(
            backup_id=backup_id,
            encryption_key=encryption_key,
            encryption_key_sha1=encryption_key_sha1,
            age_encrypted_encryption_key=age_encrypted_encryption_key,
            reuse_encryption_keys=reuse_encryption_keys)))
        for dir_path, dirs, files, dir_fd in os.fwalk(src_path, follow_symlinks=False):
            logger.debug('fwalk -> %s, %s, %s, %s', dir_path, dirs, files, dir_fd)
            dir_stat = os.fstat(dir_fd)
            meta_file.write(to_json({
                'directory': {
                    'path': str(Path(dir_path).relative_to(src_path)),
                    'mode': dir_stat.st_mode,
                    'uid': dir_stat.st_uid,
                    'gid': dir_stat.st_gid,
                    'atime': dir_stat.st_atime,
                    'ctime': dir_stat.st_ctime,
                    'mtime': dir_stat.st_mtime,
                }
            }))
            for file_name in files:
                logger.debug('Processing file %s', file_name)
                with open(file_name, mode='rb', opener=partial(os.open, dir_fd=dir_fd)) as file_stream:
                    file_hash = hashlib.new('sha3_512')
                    file_stat = os.fstat(file_stream.fileno())
                    meta_file.write(to_json({
                        'file': {
                            'path': str(Path(dir_path).relative_to(src_path) / file_name),
                            'mode': file_stat.st_mode,
                            'uid': file_stat.st_uid,
                            'gid': file_stat.st_gid,
                            'atime': file_stat.st_atime,
                            'ctime': file_stat.st_ctime,
                            'mtime': file_stat.st_mtime,
                        }
                    }))
                    while True:
                        pos = file_stream.tell()
                        chunk = file_stream.read(chunk_size)
                        if not chunk:
                            break
                        logger.debug('Read %d bytes from file %s pos %s: %s', len(chunk), file_name, pos, smart_repr(chunk))
                        file_hash.update(chunk)
                        chunk_hash = hashlib.new('sha3_512', chunk).digest()

                        if chunk_hash in reuse_blocks:
                            meta_file.write(to_json({
                                'content': {
                                    'offset': pos,
                                    'sha3_512': chunk_hash.hex(),
                                    'df_name': reuse_blocks[chunk_hash]['df_name'],
                                    'df_offset': reuse_blocks[chunk_hash]['df_offset'],
                                    'df_size': reuse_blocks[chunk_hash]['df_size'],
                                    'encryption_key_sha1': reuse_blocks[chunk_hash]['encryption_key_sha1'],
                                }
                            }))
                        else:
                            chunk_df = adapter.write_data_chunk(backup_id, chunk, encryption_key=encryption_key)
                            meta_file.write(to_json({
                                'content': {
                                    'offset': pos,
                                    'sha3_512': chunk_hash.hex(),
                                    'df_name': chunk_df.name,
                                    'df_offset': chunk_df.offset,
                                    'df_size': chunk_df.size,
                                    'encryption_key_sha1': encryption_key_sha1,
                                }
                            }))
                            reuse_blocks[chunk_hash] = {
                                'df_name': chunk_df.name,
                                'df_offset': chunk_df.offset,
                                'df_size': chunk_df.size,
                                'encryption_key_sha1': encryption_key_sha1,
                            }
                        del chunk
                    meta_file.write(to_json({
                        'file_done': {
                            'sha3_512': file_hash.hexdigest(),
                        }
                    }))
        adapter.close_data_file()
        meta_file.write(to_json({
            'done': {
                'backup_id': backup_id,
                'date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            }
        }))
        meta_file.close()
        backend.store_file(meta_path, name=meta_path.name)
    logger.info('Backup id %s done in %.3f s', backup_id, monotime() - t0)
    return BackupResult(backup_id)


def load_previous_backup_for_reuse(backend, temp_dir, reuse_backup_count):
    reuse_encryption_keys = []
    reuse_blocks = {}
    meta_filename_regex = re.compile(r'^baq\.([0-9TZ]+)\.meta$')
    backend_files = backend.list_files()
    meta_filenames = [name for name in backend_files if meta_filename_regex.match(name)]
    reuse_meta_filename = max(meta_filenames) if meta_filenames else None
    if not reuse_meta_filename:
        return reuse_encryption_keys, reuse_blocks
    reuse_backup_id, = meta_filename_regex.match(reuse_meta_filename).groups()
    logger.info('Loading metadata of backup id %s to be reused for incremental backup', reuse_backup_id)
    reuse_meta_path = temp_dir / reuse_meta_filename
    backend.retrieve_file(reuse_meta_filename, reuse_meta_path)
    with gzip.open(reuse_meta_path, mode='rb') as reuse_meta_file:
        reuse_header = json.loads(reuse_meta_file.readline())['baq_backup']
        logger.debug('Backup %s metadata header:\n%s', reuse_backup_id, json.dumps(reuse_header, indent=2))
        assert reuse_header['file_format_version'] == 'v1'
        assert reuse_backup_id == reuse_header['backup_id']
        assert reuse_header['encryption_keys'][0]['backup_id'] == reuse_backup_id
        reuse_encryption_keys = reuse_header['encryption_keys'][:reuse_backup_count]
        reuse_encryption_keys_sha1 = {k['sha1'] for k in reuse_encryption_keys}
        while True:
            record = json.loads(reuse_meta_file.readline())
            logger.debug('Processing: %s', record)
            if record.get('done'):
                break
            elif record.get('directory'):
                pass
            elif record.get('file'):
                while True:
                    file_record = json.loads(reuse_meta_file.readline())
                    if file_record.get('file_done'):
                        break
                    elif file_record.get('content'):
                        if file_record['content']['encryption_key_sha1'] in reuse_encryption_keys_sha1:
                            reuse_blocks[bytes.fromhex(file_record['content']['sha3_512'])] = {
                            'df_name': file_record['content']['df_name'],
                            'df_offset': file_record['content']['df_offset'],
                            'df_size': file_record['content']['df_size'],
                            'encryption_key_sha1': file_record['content']['encryption_key_sha1'],
                        }
                    else:
                        raise Exception(f"Unknown metadata record: {json.dumps(fil_record)}")
                    del file_record
            else:
                raise Exception(f"Unknown metadata record: {json.dumps(record)}")
            del record
    return reuse_encryption_keys, reuse_blocks


def generate_header(backup_id, encryption_key, encryption_key_sha1, age_encrypted_encryption_key, reuse_encryption_keys):
    current_encryption_key = {
        'backup_id': backup_id,
        'sha1': encryption_key_sha1,
    }
    if age_encrypted_encryption_key:
        current_encryption_key['age_encrypted'] = age_encrypted_encryption_key
    else:
        current_encryption_key['hex'] = encryption_key.hex()
    header = {
        'baq_backup': {
            'file_format_version': 'v1',
            # TODO: add baq version
            'date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'backup_id': backup_id,
            'encryption_keys': [current_encryption_key, *reuse_encryption_keys],
        }
    }
    return header


def restore(src_path, backend, identity_files):
    # Restores TO the src_path - maybe there could be better naming? :)
    # TODO: let user choose what backup_id to restore
    t0 = monotime()
    backend_files = backend.list_files()
    meta_filename_regex = re.compile(r'^baq\.([0-9TZ]+)\.meta$')
    meta_filename = max(name for name in backend_files if meta_filename_regex.match(name))
    backup_id, = meta_filename_regex.match(meta_filename).groups()
    adapter = ChunkAdapter(backend)
    logger.info('Restoring backup id %s from %s to %s', backup_id, backend, src_path)
    with ExitStack() as stack:
        stack.callback(backend.close_data_file)
        temp_dir = Path(stack.enter_context(TemporaryDirectory(prefix=f'baq.{backup_id}.')))
        meta_path = temp_dir / meta_filename
        backend.retrieve_file(meta_filename, meta_path)
        assert meta_path.is_file()
        meta_file = stack.enter_context(gzip.open(meta_path, mode='rb'))
        header = json.loads(meta_file.readline())['baq_backup']
        logger.debug('Metadata header:\n%s', json.dumps(header, indent=2))
        assert header['file_format_version'] == 'v1'
        assert backup_id == header['backup_id']
        key_manager = DecryptKeyManager(header['encryption_keys'], identity_files)
        while True:
            record = json.loads(meta_file.readline())
            logger.debug('Processing: %s', record)
            if record.get('done'):
                break
            elif record.get('directory'):
                restore_directory(src_path, record['directory'])
            elif record.get('file'):
                restore_file(src_path, record['file'], meta_file, adapter, key_manager)
            else:
                raise Exception(f"Unknown metadata record: {json.dumps(record)}")
    logger.info('Restore backup id %s done in %.3f s', backup_id, monotime() - t0)


def restore_directory(root_path, meta_record):
    full_path = root_path / meta_record['path']
    if full_path.is_dir():
        logger.debug('Directory already exists: %s', full_path)
    else:
        logger.debug('Creating directory %s', full_path)
        full_path.mkdir()


def restore_file(root_path, meta_record, meta_file, backend, key_manager):
    full_path = root_path / meta_record['path']
    logger.debug('Restoring file %s', full_path)
    with full_path.open(mode='wb') as f:
        total_hash = hashlib.new('sha3-512')
        while True:
            record = json.loads(meta_file.readline())
            logger.debug('Processing: %s', record)
            if record.get('file_done'):
                assert record['file_done']['sha3_512'] == total_hash.hexdigest()
                break
            elif record.get('content'):
                assert record['content']['offset'] == f.tell()
                encryption_key = None
                if record['content'].get('encryption_key_sha1'):
                    encryption_key = key_manager.get_key(record['content']['encryption_key_sha1'])
                chunk = backend.read_data_chunk(
                    record['content']['df_name'],
                    record['content']['df_offset'],
                    record['content']['df_size'],
                    encryption_key=encryption_key)
                logger.debug('chunk after reading from data file: %r', chunk)
                assert record['content']['sha3_512'] == hashlib.new('sha3-512', chunk).hexdigest()
                f.write(chunk)
                total_hash.update(chunk)
            else:
                raise Exception(f"Unknown metadata record: {json.dumps(record)}")


def to_json(obj):
    assert isinstance(obj, dict)
    line = json.dumps(obj).encode('utf-8')
    logger.debug('to_json: %s', line.decode())
    assert json.loads(line) == obj
    assert b'\n' not in line
    return line + b'\n'
