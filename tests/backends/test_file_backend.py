from pytest import fixture


@fixture
def file_backend(temp_dir):
    from baq.backends import FileBackend
    backup_dir = temp_dir / 'backup'
    backup_dir.mkdir()
    return FileBackend(backup_dir)


def test_file_backend_write_data_chunk(file_backend, temp_dir):
    r = file_backend.write_data_chunk('backup1', b'Hello!')
    assert r.name == 'baq.backup1.data.00000'
    assert r.offset == 0
    assert r.size == 6
    r = file_backend.write_data_chunk('backup1', b'Banana')
    assert r.name == 'baq.backup1.data.00000'
    assert r.offset == 6
    assert r.size == 6
    file_backend.close_data_file()
    data_path = temp_dir / 'backup' / r.name
    assert data_path.read_bytes() == b'Hello!Banana'


def test_file_backend_read_data_chunk(file_backend):
    r = file_backend.write_data_chunk('backup1', b'Hello!')
    file_backend.close_data_file()
    data = file_backend.read_data_chunk(r.name, r.offset, r.size)
    assert data == b'Hello!'


def test_file_backend_list_files(file_backend):
    assert file_backend.list_files() == []
    r = file_backend.write_data_chunk('backup1', b'Hello!')
    file_backend.close_data_file()
    assert file_backend.list_files() == [r.name]


def test_file_backend_store_and_retrieve_file(file_backend, temp_dir):
    (temp_dir / 'sample.txt').write_bytes(b'Some metadata')
    file_backend.store_file(temp_dir / 'sample.txt', 'testfile')
    file_backend.retrieve_file('testfile', temp_dir / 'sample2.txt')
    assert (temp_dir / 'sample2.txt').read_bytes() == b'Some metadata'
