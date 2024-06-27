import hashlib
from logging import getLogger
import sys


logger = getLogger(__name__)


def get_file_hash(file_path):
    with file_path.open('rb') as f:
        h = hashlib.sha256()
        while True:
            data = f.read(2**20)
            if not data:
                break
            assert isinstance(data, bytes)
            h.update(data)
        return h.hexdigest()


def fill_block_device_with_zeroes(block_device_path):
    with block_device_path.open('r+b') as f:
        while True:
            try:
                f.write(b'\x00' * 2**20)
            except OSError as e:
                if e.errno == 28:
                    # No space left on device, which is expected when writing to a block device
                    break
                else:
                    raise e


def fill_block_device_with_random_data(block_device_path):
    with open('/dev/urandom', 'rb') as f_random:
        with block_device_path.open('r+b') as f:
            while True:
                data = f_random.read(2**20)
                assert data
                try:
                    f.write(data)
                    f.flush()
                except OSError as e:
                    if e.errno == 28:
                        # No space left on device, which is expected when writing to a block device
                        break
                    else:
                        raise e


def test_working_with_block_device(e2e_test_block_device_path):
    fill_block_device_with_zeroes(e2e_test_block_device_path)
    assert e2e_test_block_device_path.open('rb').read(4) == b'\x00\x00\x00\x00'
    with e2e_test_block_device_path.open('r+b') as f:
        f.write(b'Hello')
    assert e2e_test_block_device_path.open('rb').read(6) == b'Hello\x00'
    fill_block_device_with_random_data(e2e_test_block_device_path)
    assert e2e_test_block_device_path.open('rb').read(6) != b'Hello\x00'


def test_backup_from_block_device_and_restore_to_block_device(e2e_s3_config, tmp_path, e2e_test_block_device_path, gnupghome, run_command, list_s3_keys, baq_e2e_test_gpg_key_id):
    fill_block_device_with_zeroes(e2e_test_block_device_path)

    # Write something to the block device
    with e2e_test_block_device_path.open('r+b') as f:
        f.write(b'Hello, world!\n')

    with e2e_test_block_device_path.open('rb') as f:
        assert f.read(20) == b'Hello, world!\n\x00\x00\x00\x00\x00\x00'

    # compute hash of the block device
    src_hash = get_file_hash(e2e_test_block_device_path)
    logger.debug('src_hash: %r', src_hash)

    # Backup
    backup_cmd = [
        '/usr/bin/env',
        f'GNUPGHOME={gnupghome}',
        f'BAQ_CACHE_DIR={tmp_path}/cache',
        sys.executable, '-m', 'baq',
        '--verbose',
        'backup',
        '--s3-storage-class', 'STANDARD',
        '--recipient', baq_e2e_test_gpg_key_id,
        e2e_test_block_device_path,
        f's3://{e2e_s3_config.bucket_name}/{e2e_s3_config.path_prefix}'
    ]
    run_command(backup_cmd)

    # Rewrite the block device with random data
    fill_block_device_with_random_data(e2e_test_block_device_path)
    assert src_hash != get_file_hash(e2e_test_block_device_path)

    # Restore
    s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    assert len(s3_keys) == 2
    assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    assert s3_keys[0].endswith('.data-000000')
    assert s3_keys[1].endswith('.meta')

    restore_dir = tmp_path / 'restore'
    restore_dir.mkdir()
    restore_cmd_factory = lambda metadata_key: [
        '/usr/bin/env',
        f'GNUPGHOME={gnupghome}',
        f'BAQ_CACHE_DIR={tmp_path}/cache',
        sys.executable, '-m', 'baq',
        '--verbose',
        'restore',
        f's3://{e2e_s3_config.bucket_name}/{metadata_key}',
        e2e_test_block_device_path,
    ]
    restore_cmd = restore_cmd_factory(s3_keys[1])
    run_command(restore_cmd)

    assert src_hash == get_file_hash(e2e_test_block_device_path)

    # Update the block device
    with e2e_test_block_device_path.open('r+b') as f:
        f.write(b'Hello, new world!\n')

    with e2e_test_block_device_path.open('rb') as f:
        assert f.read(20) == b'Hello, new world!\n\x00\x00'

    src_hash_2 = get_file_hash(e2e_test_block_device_path)
    logger.debug('src_hash_2: %r', src_hash_2)

    # Backup (second iteration)
    run_command(backup_cmd)

    # Rewrite the block device with random data
    fill_block_device_with_random_data(e2e_test_block_device_path)
    assert src_hash_2 != get_file_hash(e2e_test_block_device_path)

    # Restore (second iteration)
    s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    assert len(s3_keys) == 4
    assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    assert s3_keys[0].endswith('.data-000000')
    assert s3_keys[1].endswith('.meta')
    assert s3_keys[2].endswith('.data-000000')
    assert s3_keys[3].endswith('.meta')

    restore_cmd = restore_cmd_factory(s3_keys[3])
    run_command(restore_cmd)

    assert src_hash_2 == get_file_hash(e2e_test_block_device_path)


def test_backup_from_block_device_and_restore_to_file(e2e_s3_config, tmp_path, e2e_test_block_device_path, gnupghome, run_command, list_s3_keys, baq_e2e_test_gpg_key_id):
    fill_block_device_with_zeroes(e2e_test_block_device_path)

    # Write something to the block device
    with e2e_test_block_device_path.open('r+b') as f:
        f.write(b'Hello, world!\n')

    with e2e_test_block_device_path.open('rb') as f:
        assert f.read(20) == b'Hello, world!\n\x00\x00\x00\x00\x00\x00'

    # compute hash of the block device
    src_hash = get_file_hash(e2e_test_block_device_path)
    logger.debug('src_hash: %r', src_hash)

    # Backup
    backup_cmd = [
        '/usr/bin/env',
        f'GNUPGHOME={gnupghome}',
        f'BAQ_CACHE_DIR={tmp_path}/cache',
        sys.executable, '-m', 'baq',
        '--verbose',
        'backup',
        '--s3-storage-class', 'STANDARD',
        '--recipient', baq_e2e_test_gpg_key_id,
        e2e_test_block_device_path,
        f's3://{e2e_s3_config.bucket_name}/{e2e_s3_config.path_prefix}'
    ]
    run_command(backup_cmd)

    # Rewrite the block device with random data
    fill_block_device_with_random_data(e2e_test_block_device_path)
    assert src_hash != get_file_hash(e2e_test_block_device_path)

    # Restore
    s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    assert len(s3_keys) == 2
    assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    assert s3_keys[0].endswith('.data-000000')
    assert s3_keys[1].endswith('.meta')

    restore_dir = tmp_path / 'restore'
    restore_dir.mkdir()
    restore_cmd_factory = lambda metadata_key: [
        '/usr/bin/env',
        f'GNUPGHOME={gnupghome}',
        f'BAQ_CACHE_DIR={tmp_path}/cache',
        sys.executable, '-m', 'baq',
        '--verbose',
        'restore',
        f's3://{e2e_s3_config.bucket_name}/{metadata_key}',
        restore_dir / 'restored',
    ]
    restore_cmd = restore_cmd_factory(s3_keys[1])
    run_command(restore_cmd)

    assert (restore_dir / 'restored').open('rb').read(20) == b'Hello, world!\n\x00\x00\x00\x00\x00\x00'
    assert src_hash == get_file_hash(restore_dir / 'restored')


def test_backup_from_file_and_restore_to_block_device(e2e_s3_config, tmp_path, e2e_test_block_device_path, gnupghome, baq_e2e_test_gpg_key_id, run_command, list_s3_keys):
    src_dir = tmp_path / 'src'
    src_dir.mkdir()
    (src_dir / 'data').write_bytes(b'Hello, world!\n')

    # Backup
    backup_cmd = [
        '/usr/bin/env',
        f'GNUPGHOME={gnupghome}',
        f'BAQ_CACHE_DIR={tmp_path}/cache',
        sys.executable, '-m', 'baq',
        '--verbose',
        'backup',
        '--s3-storage-class', 'STANDARD',
        '--recipient', baq_e2e_test_gpg_key_id,
        str(src_dir / 'data'),
        f's3://{e2e_s3_config.bucket_name}/{e2e_s3_config.path_prefix}'
    ]
    run_command(backup_cmd)

    # Restore
    fill_block_device_with_zeroes(e2e_test_block_device_path)

    s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    assert len(s3_keys) == 2
    assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    assert s3_keys[0].endswith('.data-000000')
    assert s3_keys[1].endswith('.meta')

    # Restore
    restore_cmd_factory = lambda metadata_key: [
        '/usr/bin/env',
        f'GNUPGHOME={gnupghome}',
        f'BAQ_CACHE_DIR={tmp_path}/cache',
        sys.executable, '-m', 'baq',
        '--verbose',
        'restore',
        f's3://{e2e_s3_config.bucket_name}/{metadata_key}',
        e2e_test_block_device_path,
    ]
    restore_cmd = restore_cmd_factory(s3_keys[1])
    run_command(restore_cmd)

    assert e2e_test_block_device_path.open('rb').read(20) == b'Hello, world!\n\x00\x00\x00\x00\x00\x00'
