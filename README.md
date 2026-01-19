Baq – incremental backup tool with compression and encryption
=============================================================

Baq is a Python-based backup tool that backs up files and block devices to AWS S3. It provides:

- **Incremental backups** – Only changed blocks are uploaded, saving time and storage costs
- **Compression** – Data is compressed using Zstandard (zstd) for efficient storage
- **Encryption** – Backups are encrypted using GPG with recipient public keys
- **AWS S3 support** – Backups are stored in S3 with configurable storage classes


Installation
------------

```shell
pip install https://github.com/messa/baq/archive/v1.0.7.zip
```


Prerequisites
-------------

- Python 3.9 or newer
- AWS credentials configured (via environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, or `~/.aws/credentials`)
- GPG (gpg2) installed and configured with a recipient key


Usage
-----

### Backup

```shell
baq backup /path/to/directory s3://bucket-name/backup-path -r RECIPIENT_KEY_ID
```

Options:
- `-r`, `--recipient` – GPG recipient key ID for encryption (required, can be specified multiple times)
- `--s3-storage-class` – S3 storage class (default: `STANDARD_IA`)
- `-v`, `--verbose` – Enable verbose output

### Restore

```shell
baq restore s3://bucket-name/backup-path/baq.TIMESTAMP.meta /path/to/restore
```

The backup URL should point to the `.meta` file. GPG must have access to the private key for decryption.


Development
-----------

### Running E2E tests against AWS S3

You need to have AWS credentials configured.
The boto3 library can read credentials from environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
or from file `~/.aws/credentials`.
The required IAM permissions are `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`.

```
BAQ_E2E_TESTS=1 BAQ_E2E_S3_PREFIX=s3://sample-bucket/baq/tests make check
```

### Simulating a block device

See [`man losetup`](https://manpages.ubuntu.com/manpages/xenial/man8/losetup.8.html).

```shell
dd if=/dev/zero of=/tmp/test.img bs=1M count=10
sudo losetup --find /tmp/test.img
sudo losetup --associated /tmp/test.img
```

Run tests:

```shell
BAQ_E2E_TESTS=1 BAQ_E2E_S3_PREFIX=s3://.../... BAQ_E2E_TEST_BLOCK_DEVICE=/dev/loop0 make check
```
