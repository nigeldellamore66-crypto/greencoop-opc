# greencoop-opc

## Contexte
Ce projet s’inscrit dans le cadre de *Forecast 2.0*, dont l’objectif est d’enrichir les modèles de prévision de la demande électrique de GreenAndCoop par l’intégration de nouvelles sources de données météorologiques (stations semi-professionnelles et amateurs).

Les données sont collectées depuis plusieurs sources hétérogènes (Excel, JSON), stockées dans un bucket S3, puis préparées pour une ingestion dans MongoDB.

---

## Architecture globale
1. Collecte des données via Airbyte  
2. Stockage brut des données dans un bucket Amazon S3  
3. Transformation des données via un script Python  
4. Préparation des données pour une ingestion dans MongoDB  

---

## Ingestion des données

### Outil
- **Airbyte**

### Sources
- Réseau InfoClimat (stations : Bergues, Hazebrouck, Armentières, Lille-Lesquin)
- Weather Underground :
  - Station ILAMAD25 (La Madeleine, France)
  - Station IICHTE19 (Ichtegem, Belgique)

### Formats
- Excel (InfoClimat)
- JSON (Weather Underground)

### Destination
- **Amazon S3** (zone de données brutes)

### Organisation des données dans S3
Les données sont stockées sous forme brute, sans transformation, selon la convention suivante :

s3://green-and-coop-nigel/raw/infoclimat/fichier.json
s3://green-and-coop-nigel/raw/weather-underground-ichtegem/fichier.json
s3://green-and-coop-nigel/raw/weather-underground-madeleine/fichier.json


Cette structure permet :
- la rejouabilité des traitements
- le partitionnement temporel
- la traçabilité par source et par station

---

## Transformation des données

Les données brutes sont transformées via un script Python afin de :
- normaliser les noms de champs
- harmoniser les formats de dates
- typer correctement les variables
- enrichir les données avec les métadonnées des stations
- produire un format compatible avec MongoDB (documents JSON)

Le script ne modifie pas les données sources mais génère un nouveau jeu de données transformées.

---

## Contrôles qualité des données

Des tests automatisés sont appliqués avant et après transformation :
- présence des champs attendus
- vérification des types
- détection des doublons
- détection des valeurs manquantes

Les résultats des contrôles sont journalisés.

---

## Dépendances
Les dépendances Python nécessaires au projet sont listées dans le fichier : requirements.txt

---

## Environnement
Un fichier .env contenant les variables d'environnement suivantes est requis pour le fonctionnement du script:

AWS_ACCESS_KEY_ID=XXX

AWS_SECRET_ACCESS_KEY=XXX

AWS_DEFAULT_REGION=XXX

S3_BUCKET=green-and-coop-nigel

INFOCLIMAT_RAW="raw/infoclimat/"

ICHTEGEM_RAW="raw/weather-underground-ichtegem/"

MADELEINE_RAW="raw/weather-underground-madeleine/"

INFOCLIMAT_PRO="processed/infoclimat/measurements_infoclimat.jsonl"

ICHTEGEM_PRO="processed/ichtegem/measurements_ichtegem.jsonl"

MADELEINE_PRO="processed/lamadeleine/measurements_madeleine.jsonl"

STATIONS_PRO="processed/metadata/stations.jsonl"

---

## Exécution
1. Lancer la synchronisation Airbyte vers S3  
2. Exécuter le script de transformation Python  
3. Vérifier les logs et les résultats des tests de qualité  
4. Importer les données transformées dans MongoDB  

---

## Notes
- Les données brutes dans S3 ne doivent jamais être modifiées.
- Toute transformation doit être versionnée et traçable.

## Logique de transformation des données
### Objectif

La phase de transformation vise à :

Harmoniser des sources hétérogènes (InfoClimat / Weather Underground)

Uniformiser les unités et typages

Standardiser les noms de colonnes

Produire des données compatibles MongoDB et exploitables par les Data Scientists

### Sources traitées
Source	Format brut	Particularités
InfoClimat	JSON	Données + métadonnées imbriquées
Weather Underground	JSON (via Excel Airbyte)	Unités texte (°F, mph, in…)
### Étapes de transformation
1. Nettoyage initial

Suppression des lignes entièrement vides

Normalisation des noms de colonnes

Extraction des champs utiles uniquement

2. Harmonisation des timestamps

Conversion en datetime

Normalisation en UTC

Exemple :

2026-02-05T12:00:00Z

3. Conversion des unités

Weather Underground fournit des unités impériales :

Variable	Conversion
Température °F → °C	(F − 32) / 1.8
Pression inHg → hPa	inHg × 33.8639
Vent mph → m/s	mph × 0.44704
Pluie inches → mm	in × 25.4

5. Direction du vent

Conversion cardinal → degrés :

Direction	Degrés
N	0°
E	90°
S	180°
W	270°
...

### Schéma cible — Measurements

Exemple de structure finale :

{
  station_id: str,
  timestamp: datetime,
  temperature_c: float,
  humidity_pct: integer,
  pressure_hpa: float,
  wind_speed_ms: float,
  wind_gust_ms: float,
  wind_direction_deg: float,
  precip_accum_1h_mm: float
}

### Transformation des stations

Deux types de métadonnées sont consolidés :

Stations InfoClimat (JSON stations)

Stations WU (métadonnées fournies)

Schéma final :

{
  station_id: str,
  station_name: str,
  latitude: float,
  longitude: float,
  elevation_m: float,
  city: str,
  source: str,
  hardware: str,
  software: str
}

## Fonctionnement du script de transformation

Le script suit une logique modulaire :

1. Extract

Lecture des fichiers JSONL depuis S3 via s3_extract()
Normalisation du champ _airbyte_data.

2. Validate (raw)

Tests d’intégrité :

Colonnes présentes

Valeurs manquantes

Doublons

Typage

Rapport exporté dans validate_raw.txt.

3. Transform

Fonctions principales :

transform_infoclimat(df)

transform_wu_station(df, station_id)

build_stations(df_meta, wu_meta)

Chaque fonction :

Nettoie

Convertit unités

Standardise schéma

4. Validate (post-transform)

Vérification :

Schéma final

Tests d’intégrité

Rapport exporté dans validate_processed.txt.

5. Load (S3 processed)

Export en JSON Lines via :

s3_upload(s3, bucket, key, df)

Format :

1 document / ligne

Compatible MongoDB

## Tests

Les tests automatisés couvrent :

- Extraction S3 (mock AWS)

- Transformations

- Validation

- Pipeline end-to-end

- Framework : pytest + moto.

## Résultat

Deux jeux de données prêts à l’import MongoDB :

- measurements

- stations
  
Stockés sur S3 dans /processed/
