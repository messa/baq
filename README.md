Baq – incremental backup tool with compression and encryption
=============================================================

Installation
------------

```shell
pip install https://github.com/messa/baq/archive/v1.0.5.zip
```


Development
-----------

### Running E2E tests against AWS S3

You need to have AWS credentials configured.
The boto3 library can read credentials from enviroment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
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
