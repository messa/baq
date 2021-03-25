from datetime import datetime
import gzip
import json
import os
from pytest import fixture
from time import sleep

from baq.operations import backup, restore
from baq.backends import FileBackend


@fixture
def sample_age_key(temp_dir):
    secret_key_path = temp_dir / 'age_key'
    secret_key_path.write_text('AGE-SECRET-KEY-1MXGXH5HGD2HZGDH7AZXWQXCNHFZLA6N87WWSPE7GC5JVLML8Q57SS900C8\n')
    public_key = 'age1gjl494dmnd6u4ccpctshmrmdy9mqmuzuat76qtg0uyz68mn8es3sry0d49'
    return public_key


def test_backup_and_restore_without_encryption(temp_dir):
    (temp_dir / 'src').mkdir()
    (temp_dir / 'src/hello.txt').write_text('Hello, World!\n')
    (temp_dir / 'src/dir1').mkdir()
    (temp_dir / 'src/dir1/sample.txt').write_text('This is dir1/sample.txt\n')
    backend = FileBackend(temp_dir / 'backup_target')
    backup_result = backup(temp_dir / 'src', backend=backend, recipients=[], recipients_files=[])
    backup_id = backup_result.backup_id
    (temp_dir / 'restored').mkdir()
    restore(temp_dir / 'restored', backend, [])
    assert (temp_dir / 'src/hello.txt').read_bytes() == (temp_dir / 'restored/hello.txt').read_bytes()
    assert (temp_dir / 'src/dir1/sample.txt').read_bytes() == (temp_dir / 'restored/dir1/sample.txt').read_bytes()
    assert sorted(p.name for p in (temp_dir / 'backup_target').iterdir()) == [
        f'baq.{backup_id}.data.00000',
        f'baq.{backup_id}.meta',
    ]


def test_backup_and_restore(temp_dir, sample_age_key):
    (temp_dir / 'src').mkdir()
    (temp_dir / 'src/hello.txt').write_text('Hello, World!\n')
    (temp_dir / 'src/dir1').mkdir()
    (temp_dir / 'src/dir1/sample.txt').write_text('This is dir1/sample.txt\n')
    backend = FileBackend(temp_dir / 'backup_target')
    backup_result = backup(temp_dir / 'src', backend=backend, recipients=[sample_age_key], recipients_files=[])
    backup_id = backup_result.backup_id
    (temp_dir / 'restored').mkdir()
    restore(temp_dir / 'restored', backend, [temp_dir / 'age_key'])
    assert (temp_dir / 'src/hello.txt').read_bytes() == (temp_dir / 'restored/hello.txt').read_bytes()
    assert (temp_dir / 'src/dir1/sample.txt').read_bytes() == (temp_dir / 'restored/dir1/sample.txt').read_bytes()
    assert sorted(p.name for p in (temp_dir / 'backup_target').iterdir()) == [
        f'baq.{backup_id}.data.00000',
        f'baq.{backup_id}.meta',
    ]
    meta_path = temp_dir / f'backup_target/baq.{backup_id}.meta'
    meta_content = [json.loads(line) for line in gzip.decompress(meta_path.read_bytes()).splitlines()]
    assert meta_content == [
        {
            'baq_backup': {
                'file_format_version': 'v1',
                'backup_id': backup_id,
                'date': meta_content[0]['baq_backup']['date'],
                'encryption_keys': [
                    {
                        'backup_id': backup_id,
                        'sha1': meta_content[0]['baq_backup']['encryption_keys'][0]['sha1'],
                        'age_encrypted': meta_content[0]['baq_backup']['encryption_keys'][0]['age_encrypted'],
                    }
                ]
            }
        }, {
            'directory': {
                'atime': meta_content[1]['directory']['atime'],
                'ctime': meta_content[1]['directory']['ctime'],
                'mtime': meta_content[1]['directory']['mtime'],
                'uid': meta_content[1]['directory']['uid'],
                'gid': meta_content[1]['directory']['gid'],
                'mode': meta_content[1]['directory']['mode'],
                'path': '.',
            }
        }, {
            'file': {
                'atime': meta_content[2]['file']['atime'],
                'ctime': meta_content[2]['file']['ctime'],
                'mtime': meta_content[2]['file']['mtime'],
                'uid': meta_content[2]['file']['uid'],
                'gid': meta_content[2]['file']['gid'],
                'mode': meta_content[2]['file']['mode'],
                'path': 'hello.txt',
            }
        }, {
            'content': {
                'offset': 0,
                'sha3_512': 'adb798d7b4c94952e61c5d9beed5d3bf9443460f5d5a9f17eb32def95bc23ba8608f7630ea236958602500d06f5c19c64114c06ce09f1b92301b9c3fc73f0728',
                'encryption_key_sha1': meta_content[0]['baq_backup']['encryption_keys'][0]['sha1'],
                'df_name': f'baq.{backup_id}.data.00000',
                'df_offset': 0,
                'df_size': 33,
            }
        }, {
            'file_done': {
                'sha3_512': 'adb798d7b4c94952e61c5d9beed5d3bf9443460f5d5a9f17eb32def95bc23ba8608f7630ea236958602500d06f5c19c64114c06ce09f1b92301b9c3fc73f0728',
            }
        }, {
            'directory': {
                'atime': meta_content[5]['directory']['atime'],
                'ctime': meta_content[5]['directory']['ctime'],
                'mtime': meta_content[5]['directory']['mtime'],
                'uid': meta_content[5]['directory']['uid'],
                'gid': meta_content[5]['directory']['gid'],
                'mode': meta_content[5]['directory']['mode'],
                'path': 'dir1',
            }
        }, {
            'file': {
                'atime': meta_content[6]['file']['atime'],
                'ctime': meta_content[6]['file']['ctime'],
                'mtime': meta_content[6]['file']['mtime'],
                'uid': meta_content[6]['file']['uid'],
                'gid': meta_content[6]['file']['gid'],
                'mode': meta_content[6]['file']['mode'],
                'path': 'dir1/sample.txt',
            }
        }, {
            'content': {
                'offset': 0,
                'sha3_512': 'd318a04d4a61bcb9f2f10a9523c30cfef69922fea0a3c4c1c7f5f01fed01cea9ee4a9a14e29126fadb0427eae42df1efa8a0cd18eb0d75a96241a1da432dbe8d',
                'encryption_key_sha1': meta_content[0]['baq_backup']['encryption_keys'][0]['sha1'],
                'df_name': f'baq.{backup_id}.data.00000',
                'df_offset': 33,
                'df_size': 49,
            }
        }, {
            'file_done': {
                'sha3_512': 'd318a04d4a61bcb9f2f10a9523c30cfef69922fea0a3c4c1c7f5f01fed01cea9ee4a9a14e29126fadb0427eae42df1efa8a0cd18eb0d75a96241a1da432dbe8d'
            }
        }, {
            'done': {
                'backup_id': backup_id,
                'date': meta_content[-1]['done']['date'],
            }
        }
    ]


def test_incremental_backup_and_restore(temp_dir, sample_age_key):
    (temp_dir / 'src').mkdir()
    (temp_dir / 'src/hello.txt').write_text('Hello, World!\n')
    (temp_dir / 'src/big').write_bytes(os.urandom(3 * 2**20))
    backend = FileBackend(temp_dir / 'backup_target')
    backup_result = backup(temp_dir / 'src', backend=backend, recipients=[sample_age_key], recipients_files=[])
    backup_id_1 = backup_result.backup_id
    while datetime.utcnow().strftime('%Y%m%dT%H%M%SZ') == backup_result.backup_id:
        sleep(0.05)
    with (temp_dir / 'src/big').open(mode='r+b') as f:
        f.write(os.urandom(100))
    backend = FileBackend(temp_dir / 'backup_target')
    backup_result = backup(temp_dir / 'src', backend=backend, recipients=[sample_age_key], recipients_files=[])
    backup_id_2 = backup_result.backup_id
    assert (temp_dir / 'backup_target' / f'baq.{backup_id_1}.data.00000').is_file()
    assert (temp_dir / 'backup_target' / f'baq.{backup_id_1}.data.00000').stat().st_size > 3000000
    assert (temp_dir / 'backup_target' / f'baq.{backup_id_2}.data.00000').is_file()
    assert (temp_dir / 'backup_target' / f'baq.{backup_id_2}.data.00000').stat().st_size < 1500000
    (temp_dir / 'restored').mkdir()
    restore(temp_dir / 'restored', backend, [temp_dir / 'age_key'])
    assert (temp_dir / 'src/hello.txt').read_bytes() == (temp_dir / 'restored/hello.txt').read_bytes()
    #assert (temp_dir / 'src/dir1/sample.txt').read_bytes() == (temp_dir / 'restored/dir1/sample.txt').read_bytes()
