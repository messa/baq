from pytest import fixture
from subprocess import check_call, DEVNULL
from baq.age_wrapper import encrypt_with_age, decrypt_with_age


def test_age_available():
    check_call(['age', '--help'], stdout=DEVNULL, stderr=DEVNULL)


@fixture
def sample_age_key(temp_dir):
    secret_key_path = temp_dir / 'age_key'
    secret_key_path.write_text('AGE-SECRET-KEY-1MXGXH5HGD2HZGDH7AZXWQXCNHFZLA6N87WWSPE7GC5JVLML8Q57SS900C8\n')
    public_key = 'age1gjl494dmnd6u4ccpctshmrmdy9mqmuzuat76qtg0uyz68mn8es3sry0d49'
    return public_key


def test_run_age_directly(temp_dir, sample_age_key):
    (temp_dir / 'sample.txt').write_text('Test data')
    with (temp_dir / 'sample.txt').open(mode='rb') as f:
        check_call(['age', '-r', sample_age_key, '-o', str(temp_dir / 'sample.txt.age')], stdin=f)
    assert (temp_dir / 'sample.txt.age').is_file()
    with (temp_dir / 'sample.txt.age').open(mode='rb') as f:
        check_call(['age', '--decrypt', '-i', str(temp_dir / 'age_key'), '-o', str(temp_dir / 'verify.txt')], stdin=f)
    assert (temp_dir / 'verify.txt').read_bytes() == (temp_dir / 'sample.txt').read_bytes()


def test_encrypt_and_decrypt_with_age(temp_dir, sample_age_key):
    data = b'banana'
    encrypted = encrypt_with_age(data, [sample_age_key], [])
    assert isinstance(encrypted, str)
    # it is str (with only ASCII characters) because sometimes we are putting that into a JSON
    decrypted = decrypt_with_age(encrypted, [temp_dir / 'age_key'])
    assert decrypted == data
