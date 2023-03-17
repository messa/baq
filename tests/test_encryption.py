from baq.backup import decrypt_aes, encrypt_aes


def test_encrypt_decrypt_aes():
    assert decrypt_aes(encrypt_aes(b'hello', 32*b'x'), 32*b'x') == b'hello'
    assert decrypt_aes(encrypt_aes(b'\x00\x01\x02'*999, 32*b'x'), 32*b'x') == b'\x00\x01\x02'*999
