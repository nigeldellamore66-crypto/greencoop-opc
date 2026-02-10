import boto3
import pandas as pd
import json
from moto import mock_aws

from s3_client import s3_extract,s3_upload
from transform import transform_infoclimat
from validate import missing_report 

@mock_aws
def test_pipeline_infoclimat_end_to_end():
    
    # Fake S3
    s3 = boto3.client("s3", region_name="eu-west-3")
    bucket = "test-bucket"
    prefix = "raw/infoclimat/"
    key_transformed = "processed/infoclimat/info.jsonl"

    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-3"}
    )

    body = b"""
{"_airbyte_data":{"id_station":"07015","dh_utc":"2026-02-05T12:00:00Z","temperature":10.5,"humidite":80,"pression":1012,"pluie_1h":0.4}}
{"_airbyte_data":{"id_station":"07015","dh_utc":"2026-02-05T13:00:00Z","temperature":11.0,"humidite":82,"pression":1011,"pluie_1h":0.0}}
"""
    s3.put_object(Bucket=bucket, Key=prefix+"data.jsonl", Body=body)

    # Extraction des données dans le dataframe
    df_raw = s3_extract(s3, bucket, prefix)
    assert len(df_raw) == 2

    # Transformation des données
    df_processed = transform_infoclimat(df_raw)
    assert "station_id" in df_processed.columns
    assert "timestamp" in df_processed.columns    
    assert len(df_processed) == 2

    # Validate minimal des valeurs manquantes
    miss = missing_report(df_processed)  
    assert miss.loc["station_id","missing"] == 0
    assert miss.loc["timestamp","missing"] == 0

    # Transformation en jsonl et upload sur S3mock
    s3_upload(s3, bucket, key_transformed, df_processed)

    # Lecture des données du jsonl stocké
    obj = s3.get_object(Bucket=bucket, Key=key_transformed)
    records = [json.loads(l) for l in obj["Body"].read().decode().splitlines()]

    assert len(records) == 2
    assert records[0]["station_id"] == "07015"