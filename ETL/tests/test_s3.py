import boto3
import pandas as pd
from moto import mock_aws
from s3_client import s3_extract

@mock_aws
def test_s3_extract_returns_latest_jsonl():
    s3 = boto3.client("s3", region_name="eu-west-3")
    bucket = "test-bucket"
    prefix = "raw/infoclimat/"
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint":"eu-west-3"}
    )

    # ancien fichier
    old = b'{"_airbyte_data":{"x":1}}\n'
    s3.put_object(Bucket=bucket, Key=prefix+"old.jsonl", Body=old)

    # nouveau fichier
    new = b'{"_airbyte_data":{"x":2}}\n'
    s3.put_object(Bucket=bucket, Key=prefix+"new.jsonl", Body=new)

    df = s3_extract(s3, bucket, prefix)
    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, "x"] == 2