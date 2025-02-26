from logging import getLogger
from pytest import skip
import sys


logger = getLogger(__name__)


def test_backup_and_restore_directory_to_file_backend(tmp_path, gnupghome, run_command, baq_e2e_test_gpg_key_id):
    skip('TODO')

    src_dir = tmp_path / 'src'
    src_dir.mkdir()
    (src_dir / 'file1.txt').write_text('This is file1.txt\n')
    (src_dir / 'file1.txt').chmod(0o644)
    (src_dir / 'file2.txt').write_text('This is file2.txt\n' * 10000)
    (src_dir / 'file2.txt').chmod(0o600)
    (src_dir / 'file3.txt').write_text('This is file3.txt\nLorem ipsum dolor sit amet\n')

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
        str(src_dir),
        # TODO f'file://{e2e_s3_config.bucket_name}/{e2e_s3_config.path_prefix}'
    ]
    run_command(backup_cmd)

    # s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    # assert len(s3_keys) == 2
    # assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    # assert s3_keys[0].endswith('.data-000000')
    # assert s3_keys[1].endswith('.meta')

    # Restore
    restore_dir = tmp_path / 'restore'
    # restore_cmd_factory = lambda metadata_key: [
    #     '/usr/bin/env',
    #     f'GNUPGHOME={gnupghome}',
    #     f'BAQ_CACHE_DIR={tmp_path}/cache',
    #     sys.executable, '-m', 'baq',
    #     '--verbose',
    #     'restore',
    #     # TODO f's3://{e2e_s3_config.bucket_name}/{metadata_key}',
    #     restore_dir,
    # ]
    # TODO restore_cmd = restore_cmd_factory(s3_keys[1])
    # run_command(restore_cmd)

    assert (restore_dir / 'file1.txt').read_text() == 'This is file1.txt\n'
    assert (restore_dir / 'file2.txt').read_text() == 'This is file2.txt\n' * 10000
    assert (restore_dir / 'file3.txt').read_text() == 'This is file3.txt\nLorem ipsum dolor sit amet\n'
    assert (restore_dir / 'file1.txt').stat().st_mode & 0o777 == 0o644
    assert (restore_dir / 'file2.txt').stat().st_mode & 0o777 == 0o600
    assert (restore_dir / 'file1.txt').stat().st_mtime_ns == (src_dir / 'file1.txt').stat().st_mtime_ns
    assert (restore_dir / 'file2.txt').stat().st_mtime_ns == (src_dir / 'file2.txt').stat().st_mtime_ns

    # Change some file
    (src_dir / 'file1.txt').write_text('This is file1.txt updated\n')
    (src_dir / 'file3.txt').write_text('This is file3.txt\nText removed\n')

    # Backup (second iteration)
    run_command(backup_cmd)

    # s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    # assert len(s3_keys) == 4
    # assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    # assert s3_keys[0].endswith('.data-000000')
    # assert s3_keys[1].endswith('.meta')
    # assert s3_keys[2].endswith('.data-000000')
    # assert s3_keys[3].endswith('.meta')

    # Restore (second iteration)
    # restore_cmd = restore_cmd_factory(s3_keys[3])
    # run_command(restore_cmd)

    assert (restore_dir / 'file1.txt').read_text() == 'This is file1.txt updated\n'
    assert (restore_dir / 'file2.txt').read_text() == 'This is file2.txt\n' * 10000
    assert (restore_dir / 'file3.txt').read_text() == 'This is file3.txt\nText removed\n'
    assert (restore_dir / 'file1.txt').stat().st_mode & 0o777 == 0o644
    assert (restore_dir / 'file2.txt').stat().st_mode & 0o777 == 0o600
    assert (restore_dir / 'file1.txt').stat().st_mtime_ns == (src_dir / 'file1.txt').stat().st_mtime_ns
    assert (restore_dir / 'file2.txt').stat().st_mtime_ns == (src_dir / 'file2.txt').stat().st_mtime_ns
