from pytest import fixture

from baq.operations import backup, restore
from baq.backends import FileBackend


@fixture
def sample_age_key(temp_dir):
    secret_key_path = temp_dir / 'age_key'
    secret_key_path.write_text('AGE-SECRET-KEY-1MXGXH5HGD2HZGDH7AZXWQXCNHFZLA6N87WWSPE7GC5JVLML8Q57SS900C8\n')
    public_key = 'age1gjl494dmnd6u4ccpctshmrmdy9mqmuzuat76qtg0uyz68mn8es3sry0d49'
    return public_key


def test_backup_and_restore(temp_dir, sample_age_key):
    (temp_dir / 'src').mkdir()
    (temp_dir / 'src/hello.txt').write_text('Hello, World!\n')
    (temp_dir / 'src/dir1').mkdir()
    (temp_dir / 'src/dir1/sample.txt').write_text('This is dir1/sample.txt\n')
    backend = FileBackend(temp_dir / 'backup_target')
    backup(temp_dir / 'src', backend=backend, recipients=[sample_age_key], recipients_files=[])
    (temp_dir / 'restored').mkdir()
    restore(temp_dir / 'restored', backend, [temp_dir / 'age_key'])
    assert (temp_dir / 'src/hello.txt').read_bytes() == (temp_dir / 'restored/hello.txt').read_bytes()
    assert (temp_dir / 'src/dir1/sample.txt').read_bytes() == (temp_dir / 'restored/dir1/sample.txt').read_bytes()
