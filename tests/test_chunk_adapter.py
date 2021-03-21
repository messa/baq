from baq.backends import FileBackend
from baq.chunk_adapter import ChunkAdapter


def test_chunk_adapter_with_file_backend_no_encryption(temp_dir):
    backup_dir = temp_dir / 'backup'
    backup_dir.mkdir()
    fb = FileBackend(backup_dir)
    ca = ChunkAdapter(fb)
    r1 = ca.write_data_chunk('backup1', b'Hello!', encryption_key=None)
    r2 = ca.write_data_chunk('backup1', b'banana', encryption_key=None)
    r3 = ca.write_data_chunk('backup1', b'banana'*999, encryption_key=None)
    ca.close_data_file()
    assert ca.read_data_chunk(r1.name, r1.offset, r1.size, encryption_key=None) == b'Hello!'
    assert ca.read_data_chunk(r2.name, r2.offset, r2.size, encryption_key=None) == b'banana'
    assert ca.read_data_chunk(r3.name, r3.offset, r3.size, encryption_key=None) == b'banana'*999


def test_chunk_adapter_with_file_backend_with_encryption(temp_dir):
    backup_dir = temp_dir / 'backup'
    backup_dir.mkdir()
    fb = FileBackend(backup_dir)
    ca = ChunkAdapter(fb)
    encryption_key = b'(This is the AES encryption key)'
    assert len(encryption_key) == 32
    r1 = ca.write_data_chunk('backup1', b'Hello!', encryption_key)
    r2 = ca.write_data_chunk('backup1', b'banana', encryption_key)
    r3 = ca.write_data_chunk('backup1', b'banana'*999, encryption_key)
    ca.close_data_file()
    assert ca.read_data_chunk(r1.name, r1.offset, r1.size, encryption_key) == b'Hello!'
    assert ca.read_data_chunk(r2.name, r2.offset, r2.size, encryption_key) == b'banana'
    assert ca.read_data_chunk(r3.name, r3.offset, r3.size, encryption_key) == b'banana'*999
