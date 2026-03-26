import os
import mimetypes
from urllib.request import urlopen
import io
from hashlib import md5
from time import localtime
import boto3
from boto3.s3.transfer import TransferConfig


ALLOWED_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "application/pdf",
    "text/plain",
    "text/csv",
    "application/json",
    "application/zip",
}


def get_objects(aws_s3_client, bucket_name):
    for key in aws_s3_client.list_objects(Bucket=bucket_name)["Contents"]:
        print(f" {key['Key']}, size: {key['Size']}")


def download_file_and_upload_to_s3(
    aws_s3_client, bucket_name, url, keep_local=False
) -> str:
    file_name = f'image_file_{md5(str(localtime()).encode("utf-8")).hexdigest()}.jpg'
    with urlopen(url) as response:
        content = response.read()
        aws_s3_client.upload_fileobj(
            Fileobj=io.BytesIO(content),
            Bucket=bucket_name,
            ExtraArgs={"ContentType": "image/jpg"},
            Key=file_name,
        )
    if keep_local:
        with open(file_name, mode="wb") as jpg_file:
            jpg_file.write(content)
    return "https://s3-{0}.amazonaws.com/{1}/{2}".format(
        "us-west-2", bucket_name, file_name
    )


def upload_file(aws_s3_client, file_path, bucket_name, validate_mime=False):
    """
    Upload a small file using the standard upload_file method.
    Boto3 automatically uses multipart upload for files > 8MB,
    but this function is intended for small files with simple usage.

    Args:
        aws_s3_client: authenticated S3 client
        file_path: local path to the file to upload
        bucket_name: destination S3 bucket
        validate_mime: if True, checks the file's MIME type is in the allowed list
    """
    if not os.path.exists(file_path):
        print(f"Error: file '{file_path}' not found.")
        return False

    if validate_mime:
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type not in ALLOWED_MIME_TYPES:
            print(f"Error: MIME type '{mime_type}' is not allowed.")
            print(f"Allowed types: {', '.join(sorted(ALLOWED_MIME_TYPES))}")
            return False

    object_key = os.path.basename(file_path)

    response = aws_s3_client.upload_file(file_path, bucket_name, object_key)
    print(f"File '{object_key}' uploaded successfully.")
    return True


def upload_large_file(aws_s3_client, file_path, bucket_name, validate_mime=False):
    """
    Upload a large file using multipart upload via TransferConfig.

    Multipart upload splits the file into chunks and uploads them in parallel,
    which is much faster and more reliable for large files. If one chunk fails,
    only that chunk needs to be retried.

    TransferConfig settings used:
        multipart_threshold  - files above this size use multipart (5 MB here)
        multipart_chunksize  - size of each individual chunk (5 MB)
        max_concurrency      - how many threads upload chunks in parallel
        use_threads          - enable multithreaded transfers

    Args:
        aws_s3_client: authenticated S3 client
        file_path: local path to the file to upload
        bucket_name: destination S3 bucket
        validate_mime: if True, checks the file's MIME type is in the allowed list
    """
    if not os.path.exists(file_path):
        print(f"Error: file '{file_path}' not found.")
        return False

    if validate_mime:
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type not in ALLOWED_MIME_TYPES:
            print(f"Error: MIME type '{mime_type}' is not allowed.")
            print(f"Allowed types: {', '.join(sorted(ALLOWED_MIME_TYPES))}")
            return False

    object_key = os.path.basename(file_path)
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"Uploading '{object_key}' ({file_size_mb:.2f} MB) using multipart upload...")

    config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,  
        multipart_chunksize=5 * 1024 * 1024,  
        max_concurrency=10,                   
        use_threads=True,
    )

    aws_s3_client.upload_file(
        file_path,
        bucket_name,
        object_key,
        Config=config,
    )

    print(f"Large file '{object_key}' uploaded successfully.")
    return True


def upload_file_obj(aws_s3_client, filename, bucket_name):
    with open(filename, "rb") as file:
        aws_s3_client.upload_fileobj(file, bucket_name, "hello_obj.txt")


def upload_file_put(aws_s3_client, filename, bucket_name):
    with open(filename, "rb") as file:
        aws_s3_client.put_object(
            Bucket=bucket_name, Key="hello_put.txt", Body=file.read()
        )