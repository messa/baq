from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend
from logging import getLogger
import os


logger = getLogger(__name__)


def encrypt_aes_cbc(data, key):
    assert isinstance(data, bytes)
    assert isinstance(key, bytes)
    assert len(key) == 32
    iv = os.urandom(16)
    padder = PKCS7(128).padder()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted = iv
    encrypted += encryptor.update(padder.update(data))
    encrypted += encryptor.update(padder.finalize())
    encrypted += encryptor.finalize()
    #logger.debug('encrypt_aes_cbc %d B -> %d B', len(data), len(encrypted))
    return encrypted


def decrypt_aes_cbc(data, key):
    assert len(data) >= 16
    assert len(data) % 16 == 0, repr(data)
    iv = data[:16]
    unpadder = PKCS7(128).unpadder()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    decrypted = unpadder.update(decryptor.update(data[16:]))
    decrypted += unpadder.update(decryptor.finalize())
    decrypted += unpadder.finalize()
    return decrypted
