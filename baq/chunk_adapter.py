from logging import getLogger
import zlib

from .encryption import encrypt_aes_cbc, decrypt_aes_cbc


logger = getLogger(__name__)


class ChunkAdapter:
    '''
    This objects wraps a backend object and provides compression and
    encryption for writing/reading data chunks.
    '''

    def __init__(self, backend):
        self.backend = backend

    def write_data_chunk(self, backup_id, chunk, encryption_key):
        compression_flag, chunk = self._compress_chunk(chunk)
        encryption_flag, chunk = self._encrypt_chunk(chunk, encryption_key)
        flag_byte = bytes.fromhex(compression_flag + encryption_flag)
        return self.backend.write_data_chunk(backup_id, flag_byte + chunk)

    def close_data_file(self):
        self.backend.close_data_file()

    def read_data_chunk(self, name, offset, size, encryption_key):
        raw_chunk = self.backend.read_data_chunk(name, offset, size)
        compression_flag, encryption_flag = raw_chunk[0:1].hex()
        chunk = raw_chunk[1:]
        chunk = self._decrypt_chunk(encryption_flag, chunk, encryption_key)
        chunk = self._decompress_chunk(compression_flag, chunk)
        return chunk

    def _compress_chunk(self, chunk):
        chunk_z = zlib.compress(chunk)
        if len(chunk_z) < len(chunk):
            return '1', chunk_z
        return '0', chunk

    def _decompress_chunk(self, compression_flag, chunk):
        if compression_flag == '0':
            return chunk
        if compression_flag == '1':
            return zlib.decompress(chunk)
        raise Exception('Unknown compression flag: {!r}'.format(compression_flag))

    def _encrypt_chunk(self, chunk, encryption_key):
        if encryption_key is None:
            return '0', chunk
        return '1', encrypt_aes_cbc(chunk, encryption_key)

    def _decrypt_chunk(self, encryption_flag, chunk, encryption_key):
        if encryption_flag == '0':
            return chunk
        if encryption_flag == '1':
            return decrypt_aes_cbc(chunk, encryption_key)
        raise Exception('Unknown encryption flag: {!r}'.format(encryption_flag))

