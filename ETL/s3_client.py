import boto3
import pandas as pd

def s3_connection(access_key,secret_access,region):
    
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access,
        region_name=region,
    )

    return s3

def s3_extract(s3, bucket, prefix):

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    json_objs = [
        o for o in resp.get("Contents", [])
        if o["Key"].endswith(".json") or o["Key"].endswith(".jsonl")
    ]
    if not json_objs:
        raise FileNotFoundError(f"Aucun .json/.jsonl trouvé sous prefix={prefix}")
    
    latest = max(json_objs, key=lambda o: o["LastModified"])
    key = latest["Key"]

    obj = s3.get_object(Bucket=bucket, Key=key)

    raw = pd.read_json(obj["Body"], lines=True)

    return pd.json_normalize(raw["_airbyte_data"])