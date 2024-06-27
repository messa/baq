import sys


def test_backup_and_restore_single_file(e2e_s3_config, tmp_path, run_command, list_s3_keys, gnupghome, baq_e2e_test_gpg_key_id):
    src_dir = tmp_path / 'src'
    src_dir.mkdir()
    (src_dir / 'file1.txt').write_text('This is file1.txt\n')
    (src_dir / 'file1.txt').chmod(0o600)

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
        str(src_dir / 'file1.txt'),
        f's3://{e2e_s3_config.bucket_name}/{e2e_s3_config.path_prefix}'
    ]
    run_command(backup_cmd)

    s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    assert len(s3_keys) == 2
    assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    assert s3_keys[0].endswith('.data-000000')
    assert s3_keys[1].endswith('.meta')

    # Restore
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
        restore_dir / 'restored.txt',
    ]
    restore_cmd = restore_cmd_factory(s3_keys[1])
    run_command(restore_cmd)

    assert (restore_dir / 'restored.txt').read_text() == 'This is file1.txt\n'
    assert (restore_dir / 'restored.txt').stat().st_mode & 0o777 == 0o600
    assert not (restore_dir / 'file1.txt').exists()

    # Change some file
    (src_dir / 'file1.txt').write_text('This is file1.txt updated\n')

    # Backup (second iteration)
    run_command(backup_cmd)

    s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    assert len(s3_keys) == 4
    assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    assert s3_keys[0].endswith('.data-000000')
    assert s3_keys[1].endswith('.meta')
    assert s3_keys[2].endswith('.data-000000')
    assert s3_keys[3].endswith('.meta')

    # Restore (second iteration)
    restore_cmd = restore_cmd_factory(s3_keys[3])
    run_command(restore_cmd)

    assert (restore_dir / 'restored.txt').read_text() == 'This is file1.txt updated\n'
    assert (restore_dir / 'restored.txt').stat().st_mode & 0o777 == 0o600
    assert not (restore_dir / 'file1.txt').exists()

    # Rename the file
    (src_dir / 'file1renamed.txt').write_text('This is file1renamed.txt\n')
    (src_dir / 'file1renamed.txt').chmod(0o640)

    # Backup (third iteration)
    # Just test backing up from a different path (different file)
    assert backup_cmd[-2] == str(src_dir / 'file1.txt')
    backup_cmd[-2] = str(src_dir / 'file1renamed.txt')
    run_command(backup_cmd)

    s3_keys = list_s3_keys(e2e_s3_config.bucket_name, e2e_s3_config.path_prefix)
    assert len(s3_keys) == 6
    assert all(k.startswith(e2e_s3_config.path_prefix) for k in s3_keys)
    assert s3_keys[0].endswith('.data-000000')
    assert s3_keys[1].endswith('.meta')
    assert s3_keys[2].endswith('.data-000000')
    assert s3_keys[3].endswith('.meta')
    assert s3_keys[4].endswith('.data-000000')
    assert s3_keys[5].endswith('.meta')

    # Restore (third iteration)
    restore_cmd = restore_cmd_factory(s3_keys[5])
    run_command(restore_cmd)

    assert (restore_dir / 'restored.txt').read_text() == 'This is file1renamed.txt\n'
    assert (restore_dir / 'restored.txt').stat().st_mode & 0o777 == 0o640
    assert not (restore_dir / 'file1.txt').exists()
    assert not (restore_dir / 'file1renamed.txt').exists()
