from baq.encryption import encrypt_aes_cbc, decrypt_aes_cbc


def test_encrypt_and_decrypt_aes_cbc():
    assert decrypt_aes_cbc(encrypt_aes_cbc(b'Hello!', 32*b'x'), 32*b'x') == b'Hello!'


def test_decrypt_aes_cbc():
    encrypted = bytes.fromhex('867d89d2c8f70305b9ad2052d1d14e877a112429e7854fc27598de745e0125e7')
    assert decrypt_aes_cbc(encrypted, 32*b'x') == b'Hello!'
