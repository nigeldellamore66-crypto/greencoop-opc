import pandas as pd
import os
from dotenv import load_dotenv
from pipeline import insert_records
from s3_client import s3_connection, s3_extract
from db import get_db, create_user, index_unique
from validate import calculer_taux_erreur

# Chargement des variables d'environnement
load_dotenv()
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_access = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION")
bucket = os.getenv("S3_BUCKET")

# Chemins des données sources
infoclimat_pro = os.getenv("INFOCLIMAT_PRO")
ichtegem_pro = os.getenv("ICHTEGEM_PRO")
madeleine_pro = os.getenv("MADELEINE_PRO")
stations_pro = os.getenv("STATIONS_PRO")

# Infos MongoDB
DB_NAME = os.getenv("DB_NAME")
MONGO_ADMIN=os.getenv("MONGO_ADMIN")
ADMIN_PWD=os.getenv("ADMIN_PWD")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PWD = os.getenv("MONGO_PWD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")

# Test si authentification nécessaire
USE_AUTH = os.getenv("MONGO_USE_AUTH", "true").lower() == "true"

if USE_AUTH:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PWD}@{MONGO_HOST}:27017/{DB_NAME}?authSource=admin"
    MONGO_ADMIN_URI = f"mongodb://{MONGO_ADMIN}:{ADMIN_PWD}@{MONGO_HOST}:27017/admin"
else:
    # Sans authentification
    MONGO_URI = f"mongodb://{MONGO_HOST}:27017/{DB_NAME}"
    MONGO_ADMIN_URI = f"mongodb://{MONGO_HOST}:27017/admin"

print(f"Mode: {'Avec authentification' if USE_AUTH else 'Sans authentification'}")

# Noms des collections
COLLECTION_METEO = os.getenv("COLLECTION_MEASURES")
COLLECTION_STATIONS = os.getenv("COLLECTION_STATIONS")

# DEBUT DU SCRIPT

# Connection au bucket S3
s3 = s3_connection(access_key, secret_access, region)

# Extraction des données du bucket dans 4 dataframes
df_infoclimat = s3_extract(s3, bucket, infoclimat_pro)
df_ichtegem = s3_extract(s3, bucket, ichtegem_pro)
df_madeleine = s3_extract(s3, bucket, madeleine_pro)
df_stations = s3_extract(s3, bucket, stations_pro)

# Connexion MONGO_ADMIN sur admin
dbadmin = get_db(MONGO_ADMIN_URI, "admin")

# Création MONGO_USER sur admin avec droits sur DB_NAME
create_user(dbadmin,MONGO_USER,MONGO_PWD,DB_NAME)

# Bascule avec MONGO_ADMIN sur DB_NAME
dbadmin = get_db(MONGO_ADMIN_URI, DB_NAME)

# Création index unique pour les deux collections avec MONGO_ADMIN
index_unique(dbadmin, COLLECTION_METEO)
index_unique(dbadmin, COLLECTION_STATIONS)

# Connexion avec MONGO_USER sur DB_NAME
db = get_db(MONGO_URI, DB_NAME)

# Insertion des données météo (3 dataframes) dans la première collection
print(f"\n=== Insertion dans {COLLECTION_METEO} ===")
dataframes_meteo = [
    ("Infoclimat", df_infoclimat),
    ("Ichtegem", df_ichtegem),
    ("Madeleine", df_madeleine)
]

total_inserted_meteo = 0
total_duplicates_meteo = 0

for name, df in dataframes_meteo:
    print(f"\nTraitement de {name}...")
    inserted, duplicates = insert_records(df, db, COLLECTION_METEO)
    print(f"  {inserted} documents insérés, {duplicates} doublons ignorés.")
    total_inserted_meteo += inserted
    total_duplicates_meteo += duplicates

print(f"\nTotal {COLLECTION_METEO}: {total_inserted_meteo} insérés, {total_duplicates_meteo} doublons")

# Insertion des données stations (1 dataframe) dans la deuxième collection
print(f"\n=== Insertion dans {COLLECTION_STATIONS} ===")
inserted_stations, duplicates_stations = insert_records(df_stations, db, COLLECTION_STATIONS)
print(f"{inserted_stations} stations insérées, {duplicates_stations} doublons ignorés.")

print("\n=== Import terminé ===")

# Validation collection météo
resultat_meteo = calculer_taux_erreur(db, COLLECTION_METEO, dataframes_meteo)

# Validation collection stations
resultat_stations = calculer_taux_erreur(
    db, 
    COLLECTION_STATIONS, 
    [("stations", df_stations)]
)

# Afficher le résultat final

print("\nRÉSUMÉ DE LA MIGRATION")
print(f"Météo - Taux d'erreur: {resultat_meteo['taux_erreur']:.2f}%")
print(f"Stations - Taux d'erreur: {resultat_stations['taux_erreur']:.2f}%")

if resultat_meteo['succes'] and resultat_stations['succes']:
    print("Migration réussie !")
else:
    print("Problèmes détectés")