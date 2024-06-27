from baq.util import sha1_file


def test_sha1_file(tmp_path):
    (tmp_path / 'hello.txt').write_bytes(b'Hello, world!\n\x00\xff')
    assert sha1_file(tmp_path / 'hello.txt').hexdigest() == '94ccf964443f3ec7227290fc36e3d1e159b69627'
    assert sha1_file(tmp_path / 'hello.txt', length=16).hexdigest() == '94ccf964443f3ec7227290fc36e3d1e159b69627'
    assert sha1_file(tmp_path / 'hello.txt', length=999).hexdigest() == '94ccf964443f3ec7227290fc36e3d1e159b69627'
    assert sha1_file(tmp_path / 'hello.txt', length=15).hexdigest() == '308bb9b89ce3956b429d8136b9bf1b255a943c0d'
    assert sha1_file(tmp_path / 'hello.txt', length=3).hexdigest() == 'dbc2d1fed0dc37a70aea0f376958c802eddc0559'
    assert sha1_file(tmp_path / 'hello.txt', length=0).hexdigest() == 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
