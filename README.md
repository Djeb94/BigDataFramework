# BigData Framework — Spotify Analytics

## Problématique
Quelles sont les combinaisons optimales de caractéristiques audio (danceabilité, énergie, durée) qui maximisent la popularité d'un titre selon les décennies (1920-2020) ?

## Stack technique
- **Langage** : PySpark
- **Stockage** : HDFS
- **Format** : Parquet (Snappy)
- **Base de données** : PostgreSQL
- **API** : FastAPI + JWT
- **Visualisation** : Streamlit + Plotly
- **Orchestration** : Docker Compose

## Architecture Médaillon
```
Source CSV (tracks.csv) ──┐
                           ├─→ feeder.py ─→ HDFS /raw ─→ processor.py ─→ HDFS /silver ─→ datamart.py ─→ PostgreSQL
Source PostgreSQL (artists)┘
                                                                                          API FastAPI ─→ Streamlit
```

## Dataset
- **Source** : Kaggle — Spotify Dataset 1921-2020 (600k tracks)
- **tracks.csv** : 586 672 lignes — features audio, popularité, release date
- **artists.csv** : 1 162 095 lignes — genres, followers, popularité artiste

## Structure du projet
```
Projet/
├── docker-compose.yml
├── hadoop.env
├── hadoop-hive.env
├── pipeline/
│   ├── feeder.py        # Ingestion CSV + PostgreSQL → HDFS raw
│   ├── processor.py     # Nettoyage + jointure + window function → HDFS silver
│   ├── datamart.py      # Agrégations → PostgreSQL
│   ├── jars/            # Driver PostgreSQL JDBC
│   └── logs/            # Logs d'exécution .txt
├── source/
│   └── tracks.csv
├── api/
│   ├── main.py          # API REST FastAPI + JWT
│   └── app.py           # Visualisation Streamlit
└── README.md
```

## Lancer le projet

### 1. Démarrer le cluster
```bash
docker compose up -d
```

### 2. Ingestion (feeder.py)
```bash
docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/pipeline/jars/postgresql-42.6.0.jar \
  /opt/pipeline/feeder.py \
  "hdfs://namenode:9000/data/raw/spotify/tracks.csv" \
  "jdbc:postgresql://hive-metastore-postgresql:5432/spotify" \
  "hdfs://namenode:9000/data/raw/spotify/parquet"
```

### 3. Traitement (processor.py)
```bash
docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/pipeline/jars/postgresql-42.6.0.jar \
  /opt/pipeline/processor.py \
  "hdfs://namenode:9000/data/raw/spotify/parquet" \
  "hdfs://namenode:9000/data/silver/spotify"
```

### 4. Datamarts (datamart.py)
```bash
docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/pipeline/jars/postgresql-42.6.0.jar \
  /opt/pipeline/datamart.py \
  "hdfs://namenode:9000/data/silver/spotify" \
  "jdbc:postgresql://hive-metastore-postgresql:5432/spotify"
```

### 5. API
```bash
cd api && uvicorn main:app --reload --port 8000
```

### 6. Visualisation
```bash
cd api && streamlit run app.py
```

## Datamarts
| Table | Description |
|---|---|
| `dm_audio_by_decade` | Features audio moyennes par décennie |
| `dm_top_tracks` | Top 1000 tracks popularité > 70 |
| `dm_genre_popularity` | Popularité moyenne par genre |
| `dm_top_artists` | Top 500 artistes |

## Endpoints API
| Endpoint | Description |
|---|---|
| `POST /token` | Authentification JWT |
| `GET /audio-by-decade` | Données par décennie (paginé) |
| `GET /top-tracks` | Top tracks (paginé) |
| `GET /genre-popularity` | Popularité par genre (paginé) |
| `GET /top-artists` | Top artistes (paginé) |