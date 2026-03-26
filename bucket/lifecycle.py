import json


def set_lifecycle_policy(aws_s3_client, bucket_name, days=120):
    """
    Attach a lifecycle policy to the bucket that automatically deletes
    every object 'days' after it was created.

    How lifecycle policies work:
      - AWS S3 checks object creation dates daily.
      - When an object's age exceeds the 'Expiration.Days' value,
        S3 deletes it automatically at no extra cost.
      - The policy is bucket-wide unless you scope it with a Prefix.
      - 'Filter: {}' means the rule applies to ALL objects in the bucket.

    Args:
        aws_s3_client: authenticated S3 client
        bucket_name: the bucket to apply the policy to
        days: number of days after creation before the object is deleted (default: 120)
    """
    lifecycle_policy = {
        "Rules": [
            {
                "ID": f"delete-after-{days}-days",
                "Status": "Enabled",
                "Filter": {},
                "Expiration": {
                    "Days": days 
                },
            }
        ]
    }

    response = aws_s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle_policy,
    )

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        print(f"Lifecycle policy set: objects will be deleted after {days} days.")
        return True
    return False


def read_lifecycle_policy(aws_s3_client, bucket_name):
    """
    Read and display the current lifecycle policy for a bucket.
    """
    try:
        response = aws_s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        return json.dumps(response.get("Rules", []), indent=2, default=str)
    except aws_s3_client.exceptions.from_code("NoSuchLifecycleConfiguration"):
        return "No lifecycle policy is set for this bucket."
    except Exception as e:
        return f"Error reading lifecycle policy: {e}"