import boto3
import pandas as pd
import io

def s3_connection(access_key,secret_access,region):
    # Création d'une instance pour le client S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access,
        region_name=region,
    )

    return s3

def s3_extract(s3, bucket, prefix):
    # Liste les objets dans le chemin sélectionné
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # Selection des fichiers json/jsonl dans le répertoire
    json_objs = [
        o for o in resp.get("Contents", [])
        if o["Key"].endswith(".json") or o["Key"].endswith(".jsonl")
    ]
    if not json_objs:
        raise FileNotFoundError(f"Aucun .json/.jsonl trouvé sous prefix={prefix}")
    
    # Choisi le fichier le plus récent
    latest = max(json_objs, key=lambda o: o["LastModified"])
    key = latest["Key"]

    # Telecharge le fichier
    obj = s3.get_object(Bucket=bucket, Key=key)
    
    # Transforme le body du fichier en dataframe
    raw = pd.read_json(obj["Body"], lines=True)

    return pd.json_normalize(raw["_airbyte_data"])

def s3_upload(s3, bucket, key, df):
    # Création d'un buffer temporaire pour les données
    buf = io.StringIO()
    # Conversion du dataframe en json à l'intérieur du buffer
    df.to_json(buf, orient="records", lines=True, force_ascii=False, date_format="iso")
    # Ecriture des données sur le bucket à l'emplacement choisi
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))