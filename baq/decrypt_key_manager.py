from logging import getLogger

from .age_wrapper import decrypt_with_age


logger = getLogger(__name__)


class DecryptKeyManager:

    def __init__(self, encryption_keys, identity_files):
        self.encryption_keys = encryption_keys
        self.identity_files = identity_files
        self.open_keys = {}

    def get_key(self, key_hash):
        assert isinstance(key_hash, str)
        if not self.open_keys.get(key_hash):
            open_key = None
            for key_data in self.encryption_keys:
                if key_data['sha1'] == key_hash:
                    if key_data.get('hex'):
                        open_key = bytes.fromhex(key_data['hex'])
                    elif key_data.get('age_encrypted'):
                        open_key = decrypt_with_age(key_data['age_encrypted'], self.identity_files)
                    else:
                        # there should always be hex or age_encrypted
                        raise Exception('Unknown key data structure')
                    break
            if not open_key:
                raise Exception(f"Could not open key {key_hash}")
            self.open_keys[key_hash] = open_key
        return self.open_keys[key_hash]

