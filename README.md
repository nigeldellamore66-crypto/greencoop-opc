# Pipeline ETL météo — Airbyte · S3 · MongoDB

> Collecte, transformation et ingestion de données météorologiques
> multi-sources (stations semi-professionnelles et amateurs) dans MongoDB,
> dans le cadre du projet Forecast 2.0 de GreenAndCoop.

## Contexte & objectif

GreenAndCoop cherche à enrichir ses modèles de prévision de la demande
électrique avec de nouvelles sources de données météorologiques.

Ce pipeline collecte des données depuis des stations InfoClimat et Weather
Underground, les stocke brutes sur S3, les transforme et les prépare
pour ingestion dans MongoDB.

## Stack technique

`Python` `Airbyte` `Amazon S3` `MongoDB` `Pytest` `Docker`

## Architecture
```
Stations météo (InfoClimat / Weather Underground)
        │
        ▼
    [Airbyte]  ←  collecte multi-sources (Excel, JSON)
        │
        ▼
  [Amazon S3]  ←  stockage brut (raw/)
        │
        ▼
 [Script ETL]  ←  normalisation, conversion d'unités, tests qualité
        │
        ▼
  [Amazon S3]  ←  données transformées (processed/)
        │
        ▼
 [Script MIG]  ←  migration vers MongoDB
        │
        ▼
   [MongoDB]  ←  ingestion finale
```

## Structure du repo
```
├── ETL/                    # Script de transformation et tests qualité
├── MIG/                    # Script de migration vers MongoDB
├── docker-compose.yml      # Orchestration des deux scripts
└── README.md
```

## Schéma des données produites

**Measurements** (3 jeux de données — 1 par source)

| Champ | Type |
|---|---|
| `station_id` | str |
| `timestamp` | datetime |
| `temperature_c` | float |
| `humidity_pct` | integer |
| `pressure_hpa` | float |
| `wind_speed_ms` | float |
| `wind_direction_deg` | float |
| `precip_accum_mm` | float |

**Stations** (métadonnées consolidées)

| Champ | Type |
|---|---|
| `station_id` | str |
| `station_name` | str |
| `latitude / longitude` | float |
| `elevation_m` | float |
| `source` | str |

## Installation
```bash
git clone https://github.com/nigeldellamore66-crypto/OPC-Pipeline-ETL-meteo
# Créer le fichier .env et renseigner les clés AWS et les chemins S3
```

Variables d'environnement requises dans `.env` :
```env
AWS_ACCESS_KEY_ID=XXX
AWS_SECRET_ACCESS_KEY=XXX
AWS_DEFAULT_REGION=XXX
S3_BUCKET=green-and-coop-nigel

INFOCLIMAT_RAW=raw/infoclimat/
ICHTEGEM_RAW=raw/weather-underground-ichtegem/
MADELEINE_RAW=raw/weather-underground-madeleine/

INFOCLIMAT_PRO=processed/infoclimat/measurements_infoclimat.jsonl
ICHTEGEM_PRO=processed/ichtegem/measurements_ichtegem.jsonl
MADELEINE_PRO=processed/lamadeleine/measurements_lamadeleine.jsonl
STATIONS_PRO=processed/metadata/stations.jsonl
```

## Exécution

Prérequis : Docker installé
```bash
docker compose up -d
```

Le `docker-compose.yml` orchestre automatiquement :
1. **ETL** — extraction depuis S3, transformation, tests qualité, stockage S3
2. **MIG** — migration des données transformées vers MongoDB

Les rapports de qualité sont générés à chaque exécution :
- `validate_raw.txt` — contrôles avant transformation
- `validate_processed.txt` — contrôles après transformation

## Tests
```bash
pytest ETL/tests/
```

Les tests couvrent l'extraction S3 (mock AWS), les transformations,
la validation et le pipeline end-to-end.

## Apprentissages clés

- Mise en place d'un pipeline ETL complet de bout en bout
- Intégration d'Airbyte pour la collecte multi-sources hétérogènes
- Normalisation de données météo (conversion unités impériales → métriques,
  harmonisation de schémas)
- Tests de qualité automatisés à chaque étape avec Pytest
- Orchestration de scripts via Docker Compose
- Travail avec des services cloud AWS S3 et MongoDB
