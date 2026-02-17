import pandas as pd
from dotenv import load_dotenv
import os
from s3_client import s3_connection, s3_extract, s3_upload
from validate import validate_raw, validate_processed
from transform import split_infoclimat, transform_wu_station, transform_infoclimat, build_stations

# Chargement des variables d'environnement
load_dotenv()
access_key=os.getenv("AWS_ACCESS_KEY_ID")
secret_access=os.getenv("AWS_SECRET_ACCESS_KEY")
region=os.getenv("AWS_DEFAULT_REGION")
bucket = os.getenv("S3_BUCKET")
# Chemin des données source
infoclimat_raw = os.getenv("INFOCLIMAT_RAW")
ichtegem_raw = os.getenv("ICHTEGEM_RAW")
madeleine_raw = os.getenv("MADELEINE_RAW")
# Chemin des données cible
infoclimat_pro = os.getenv("INFOCLIMAT_PRO")
ichtegem_pro = os.getenv("ICHTEGEM_PRO")
madeleine_pro = os.getenv("MADELEINE_PRO")
stations_pro = os.getenv("STATIONS_PRO")


# Informations de configurations pour les stations amateurs
wu_stations = [
    {"station_id": "IICHTE19", "station_name": "WeerstationBS", "latitude": 51.092, "longitude": 2.999, "elevation_m": 15, "city": "Ichtegem", "state": None, "hardware":"other", "software":"EasyWeatherV1.6.6", "source": "Weather Underground"},
    {"station_id": "ILAMAD25", "station_name": "La Madeleine", "latitude": 50.659, "longitude": 3.07, "elevation_m": 23, "city": "La Madeleine", "state": None, "hardware":"other", "software":"EasyWeatherPro_V5.1.6","source": "Weather Underground"},
]

# DEBUT DU SCRIPT

# Connection au bucket S3
s3=s3_connection(access_key, secret_access, region)

# Extraction des données du bucket dans 3 dataframes
df_infoclimat=s3_extract(s3,bucket,infoclimat_raw)
df_ichtegem=s3_extract(s3,bucket,ichtegem_raw)
df_madeleine=s3_extract(s3,bucket,madeleine_raw)

# Séparation des données d'Infoclimat Data/Metadata
df_ic_meta,df_ic_data=split_infoclimat(df_infoclimat)

# Validation avant transformation

with open("/data/validate_raw.txt", "w", encoding="utf-8") as f:
    validate_raw(df_ic_meta, "Infoclimat - Metadata", output=f)
    validate_raw(df_ic_data, "Infoclimat - Data", output=f)
    validate_raw(df_ichtegem, "Ichtegem", output=f)
    validate_raw(df_madeleine, "La Madeleine", output=f)
    print("\n Validation avant transformation terminée — rapport disponible dans : validate_raw.txt")

# Transformation des données de mesures météorologiques

df_ichtegem_trans=transform_wu_station(df_ichtegem,"IICHTE19")
df_madeleine_trans=transform_wu_station(df_madeleine,"ILAMAD25")
df_infoclimat_trans=transform_infoclimat(df_ic_data)

# Construction du dataframe contenant les informations des stations

df_stations = build_stations(df_ic_meta, wu_stations)

# Validation après transformation

with open("/data/validate_processed.txt", "w", encoding="utf-8") as f:
    validate_processed(df_stations, "Infos stations", output=f)
    validate_processed(df_infoclimat_trans, "Infoclimat", output=f)
    validate_processed(df_ichtegem_trans, "Ichtegem", output=f)
    validate_processed(df_madeleine_trans, "La Madeleine", output=f)
    print("\nValidation après transformation terminée — rapport disponible dans : validate_processed.txt")

# Upload des données transformées sur le bucket S3
s3_upload(s3, bucket, infoclimat_pro, df_infoclimat_trans)
s3_upload(s3, bucket, ichtegem_pro, df_ichtegem_trans)
s3_upload(s3, bucket, madeleine_pro, df_madeleine_trans)
s3_upload(s3, bucket, stations_pro, df_stations)

print("\nTransfert des données transformées sur le bucket -", bucket, "- terminée.")