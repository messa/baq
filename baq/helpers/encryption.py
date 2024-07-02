from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CTR
from logging import getLogger
from secrets import token_bytes
from subprocess import check_output


logger = getLogger(__name__)


def encrypt_gpg(src_path, dst_path, recipients):
    assert src_path.is_file()
    assert not dst_path.exists()
    gpg_cmd = ['gpg2', '--encrypt', '--sign', '--trust-model=always', '--compress-algo=none']
    for r in recipients:
        gpg_cmd.extend(['-r', r])
    gpg_cmd.extend(['-o', str(dst_path), str(src_path)])
    logger.debug('Running %s', ' '.join(gpg_cmd))
    check_output(gpg_cmd)
    assert dst_path.is_file()
    # GnuPG sometime suffers from race conditions inside GPG Agent and outputs an empty file.
    # So let's check that the output file is not empty.
    assert dst_path.stat().st_size > 0


def decrypt_gpg(src_path, dst_path):
    assert src_path.is_file()
    assert not dst_path.exists()
    gpg_cmd = ['gpg2', '--decrypt', '-o', str(dst_path), str(src_path)]
    logger.debug('Running %s', ' '.join(gpg_cmd))
    check_output(gpg_cmd)
    if not dst_path.is_file():
        raise Exception('Decryption failed: output file not found')
    if not dst_path.stat().st_size:
        raise Exception('Decryption failed: output file is empty')


def encrypt_aes(data, key):
    nonce = token_bytes(16)
    encryptor = Cipher(AES(key), CTR(nonce)).encryptor()
    return nonce + encryptor.update(data) + encryptor.finalize()


def decrypt_aes(encrypted_data, key):
    nonce = encrypted_data[:16]
    decryptor = Cipher(AES(key), CTR(nonce)).decryptor()
    return decryptor.update(encrypted_data[16:]) + decryptor.finalize()


assert decrypt_aes(encrypt_aes(b'hello', 32*b'x'), 32*b'x') == b'hello'
assert decrypt_aes(encrypt_aes(b'\x00\x01\x02'*999, 32*b'x'), 32*b'x') == b'\x00\x01\x02'*999
