"""Live check that the bronze bucket is reachable. Skips (not fails) when the
machine has no AWS credentials, so the suite stays green offline and in CI."""
import boto3
import pytest
from botocore.exceptions import ClientError, NoCredentialsError

import config


def test_bucket_exists():
    cfg = config.load("aws")
    s3 = boto3.client("s3", region_name=cfg["region"])
    try:
        resp = s3.list_objects_v2(Bucket=cfg["bucket"], MaxKeys=1)
    except NoCredentialsError:
        pytest.skip("no AWS credentials configured")
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("InvalidAccessKeyId", "ExpiredToken"):
            pytest.skip(f"AWS credentials unusable: {exc.response['Error']['Code']}")
        raise
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
