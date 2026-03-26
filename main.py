import logging
from botocore.exceptions import ClientError
from auth import init_client
from bucket.crud import list_buckets, create_bucket, delete_bucket, bucket_exists
from bucket.policy import read_bucket_policy, assign_policy
from bucket.lifecycle import set_lifecycle_policy, read_lifecycle_policy
from object.crud import (
    download_file_and_upload_to_s3,
    get_objects,
    upload_file,
    upload_large_file,
)
from bucket.encryption import set_bucket_encryption, read_bucket_encryption
import argparse

parser = argparse.ArgumentParser(
    description="CLI program that helps with S3 buckets.",
    usage="""
    How to download and upload directly:
        python main.py -bn my-bucket -ol https://example.com/image.jpg -du

    How to list buckets:
        python main.py -lb

    How to create bucket:
        python main.py -bn my-bucket -cb -region us-west-2

    How to upload a small file:
        python main.py -bn my-bucket -uf ./photo.jpg

    How to upload a small file WITH mime type validation:
        python main.py -bn my-bucket -uf ./photo.jpg -vm

    How to upload a large file (multipart):
        python main.py -bn my-bucket -ulf ./bigvideo.mp4

    How to set a lifecycle policy (delete after 120 days):
        python main.py -bn my-bucket -slp

    How to set a lifecycle policy with custom number of days:
        python main.py -bn my-bucket -slp -lpd 30

    How to read the lifecycle policy:
        python main.py -bn my-bucket -rlp

    How to assign missing policy:
        python main.py -bn my-bucket -amp
    """,
    prog="main.py",
    epilog="DEMO APP FOR BTU_AWS",
)


parser.add_argument(
    "-lb", "--list_buckets",
    help="List already created buckets.",
    action="store_true",
)

parser.add_argument(
    "-cb", "--create_bucket",
    help="Flag to create bucket.",
    choices=["False", "True"], type=str, nargs="?", const="True", default="False",
)

parser.add_argument(
    "-bn", "--bucket_name",
    type=str, help="Pass bucket name.", default=None,
)

parser.add_argument(
    "-bc", "--bucket_check",
    help="Check if bucket already exists.",
    choices=["False", "True"], type=str, nargs="?", const="True", default="True",
)

parser.add_argument(
    "-region", "--region",
    type=str, help="Region variable.", default=None,
)

parser.add_argument(
    "-db", "--delete_bucket",
    help="Flag to delete bucket.",
    choices=["False", "True"], type=str, nargs="?", const="True", default="False",
)

parser.add_argument(
    "-be", "--bucket_exists",
    help="Flag to check if bucket exists.",
    choices=["False", "True"], type=str, nargs="?", const="True", default="False",
)

parser.add_argument(
    "-rp", "--read_policy",
    help="Flag to read bucket policy.",
    choices=["False", "True"], type=str, nargs="?", const="True", default="False",
)

parser.add_argument(
    "-arp", "--assign_read_policy",
    help="Flag to assign read bucket policy.",
    choices=["False", "True"], type=str, nargs="?", const="True", default="False",
)

parser.add_argument(
    "-amp", "--assign_missing_policy",
    help="Flag to assign multiple bucket policy.",
    choices=["False", "True"], type=str, nargs="?", const="True", default="False",
)

parser.add_argument(
    "-du", "--download_upload",
    help="Download from URL and upload to bucket.",
    choices=["False", "True"], type=str, nargs="?", const="True", default="False",
)

parser.add_argument(
    "-ol", "--object_link",
    type=str, help="Link to download and upload to bucket.", default=None,
)

parser.add_argument(
    "-lo", "--list_objects",
    type=str, help="List bucket objects.", nargs="?", const="True", default="False",
)

parser.add_argument(
    "-ben", "--bucket_encryption",
    type=str, help="Set bucket encryption.", nargs="?", const="True", default="False",
)

parser.add_argument(
    "-rben", "--read_bucket_encryption",
    type=str, help="Read bucket encryption settings.", nargs="?", const="True", default="False",
)


parser.add_argument(
    "-uf", "--upload_file",
    type=str,
    help="Path to a small file to upload to S3.",
    default=None,
    metavar="FILE_PATH",
)

parser.add_argument(
    "-ulf", "--upload_large_file",
    type=str,
    help="Path to a large file to upload using multipart upload.",
    default=None,
    metavar="FILE_PATH",
)

parser.add_argument(
    "-vm", "--validate_mime",
    help="Validate the file's MIME type before uploading (use with -uf or -ulf).",
    action="store_true",
)

parser.add_argument(
    "-slp", "--set_lifecycle_policy",
    help="Set a lifecycle policy that deletes objects after N days.",
    action="store_true",
)

parser.add_argument(
    "-lpd", "--lifecycle_policy_days",
    type=int,
    help="Number of days after which objects are deleted (used with -slp, default: 120).",
    default=120,
    metavar="DAYS",
)

parser.add_argument(
    "-rlp", "--read_lifecycle_policy",
    help="Read the current lifecycle policy of the bucket.",
    action="store_true",
)


def main():
    s3_client = init_client()
    args = parser.parse_args()

    if args.bucket_name:
        if args.create_bucket == "True":
            if not args.region:
                parser.error("Please provide region for bucket --region REGION_NAME")
            if (args.bucket_check == "True") and bucket_exists(s3_client, args.bucket_name):
                parser.error("Bucket already exists")
            if create_bucket(s3_client, args.bucket_name, args.region):
                print("Bucket successfully created")

        if (args.delete_bucket == "True") and delete_bucket(s3_client, args.bucket_name):
            print("Bucket successfully deleted")

        if args.bucket_exists == "True":
            print(f"Bucket exists: {bucket_exists(s3_client, args.bucket_name)}")

        if args.read_policy == "True":
            print(read_bucket_policy(s3_client, args.bucket_name))

        if args.assign_read_policy == "True":
            assign_policy(s3_client, "public_read_policy", args.bucket_name)

        if args.assign_missing_policy == "True":
            assign_policy(s3_client, "multiple_policy", args.bucket_name)

        if args.object_link:
            if args.download_upload == "True":
                print(download_file_and_upload_to_s3(s3_client, args.bucket_name, args.object_link))

        if args.bucket_encryption == "True":
            if set_bucket_encryption(s3_client, args.bucket_name):
                print("Encryption set")

        if args.read_bucket_encryption == "True":
            print(read_bucket_encryption(s3_client, args.bucket_name))

        if args.list_objects == "True":
            get_objects(s3_client, args.bucket_name)

        #upload small file 
        if args.upload_file:
            upload_file(
                s3_client,
                args.upload_file,
                args.bucket_name,
                validate_mime=args.validate_mime,
            )

        #upload large file (multipart) 
        if args.upload_large_file:
            upload_large_file(
                s3_client,
                args.upload_large_file,
                args.bucket_name,
                validate_mime=args.validate_mime,
            )

        #set lifecycle policy 
        if args.set_lifecycle_policy:
            set_lifecycle_policy(s3_client, args.bucket_name, days=args.lifecycle_policy_days)

        #read lifecycle policy 
        if args.read_lifecycle_policy:
            print(read_lifecycle_policy(s3_client, args.bucket_name))

    if args.list_buckets:
        buckets = list_buckets(s3_client)
        if buckets:
            for bucket in buckets["Buckets"]:
                print(f'  {bucket["Name"]}')


if __name__ == "__main__":
    try:
        main()
    except ClientError as e:
        logging.error(e)