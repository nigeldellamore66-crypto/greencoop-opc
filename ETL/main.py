import pandas as pd
from dotenv import load_dotenv
import os
from s3_client import s3_connection,s3_extract
from validate import validate_raw
from transform import split_infoclimat

# Chargement des variables d'environnement
load_dotenv()
access_key=os.getenv("AWS_ACCESS_KEY_ID")
secret_access=os.getenv("AWS_SECRET_ACCESS_KEY")
region=os.getenv("AWS_DEFAULT_REGION")
bucket = os.getenv("S3_BUCKET")
prefix_infoclimat = os.getenv("INFOCLIMAT")
prefix_ichtegem = os.getenv("ICHTEGEM")
prefix_madeleine = os.getenv("MADELEINE")

# Connection au bucket S3
s3=s3_connection(access_key, secret_access, region)

# Extraction des données du bucket dans 3 dataframes
df_infoclimat=s3_extract(s3,bucket,prefix_infoclimat)
df_ichtegem=s3_extract(s3,bucket,prefix_ichtegem)
df_madeleine=s3_extract(s3,bucket,prefix_madeleine)

# Séparation des données d'Infoclimat Data/Metadata
df_ic_meta,df_ic_data=split_infoclimat(df_infoclimat)

# Validation avant migration

with open("validate_report.txt", "w", encoding="utf-8") as f:
    validate_raw(df_ic_meta, "Infoclimat - Metadata", output=f)
    validate_raw(df_ic_data, "Infoclimat - Data", output=f)
    validate_raw(df_ichtegem, "Ichtegem", output=f)
    validate_raw(df_madeleine, "La Madeleine", output=f)
    print("\n Validation avant migration terminée — rapport disponible dans : validate_report.txt")
