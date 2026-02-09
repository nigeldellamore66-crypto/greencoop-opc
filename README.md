# greencoop-opc

# Ingestion des données météorologiques

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
