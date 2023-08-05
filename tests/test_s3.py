from base64 import b64encode
import boto3
import hashlib


def test_s3_multipart_upload(e2e_s3_config):
    s3_bucket_name = e2e_s3_config.bucket_name
    s3_key = e2e_s3_config.path_prefix + 'testfile'
    print(f'{s3_bucket_name=}')
    print(f'{s3_key=}')

    client = boto3.client('s3')
    create_response = client.create_multipart_upload(
        ACL='private',
        Bucket=s3_bucket_name,
        Key=s3_key,
        ChecksumAlgorithm='SHA1',
        StorageClass='STANDARD')
    print(f'{create_response=}')
    upload_id = create_response['UploadId']
    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.complete_multipart_upload
        # Part numbers can be any number from 1 to 10,000, inclusive.
        # To ensure that data is not corrupted when traversing the network, specify the Content-MD5 header in the upload part request.
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
        # Part size: 5 MiB to 5 GiB. There is no minimum size limit on the last part of your multipart upload.
        # Maximum object size: 5 TiB
        upload_response = client.upload_part(
            Bucket=s3_bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=b'Hello World!\n',
            ChecksumSHA1=b64encode(hashlib.sha1(b'Hello World!\n').digest()).decode('ascii'))
        print(f'{upload_response=}')
        assert upload_response['ChecksumSHA1'] == b64encode(hashlib.sha1(b'Hello World!\n').digest()).decode('ascii')

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.complete_multipart_upload

        complete_response = client.complete_multipart_upload(
            Bucket=s3_bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={
                'Parts': [
                    {
                        'PartNumber': 1,
                        'ETag': upload_response['ETag'],
                        'ChecksumSHA1': b64encode(hashlib.sha1(b'Hello World!\n').digest()).decode('ascii'),
                    }
                ]
            },
            ChecksumSHA1=b64encode(hashlib.sha1(hashlib.sha1(b'Hello World!\n').digest()).digest()).decode('ascii'))
        print(f'{complete_response=}')

    finally:
        abort_response = client.abort_multipart_upload(
            Bucket=s3_bucket_name,
            Key=s3_key,
            UploadId=upload_id)
        print(f'{abort_response=}')
