# -*- coding: utf-8 -*-
import sys
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/opt/pipeline/logs/datamart.txt"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("datamart")

if len(sys.argv) != 3:
    log.error("Usage: datamart.py <input_silver> <jdbc_url>")
    sys.exit(1)

input_silver = sys.argv[1]
jdbc_url     = sys.argv[2]

spark = (
    SparkSession.builder
    .appName("spotify-datamart")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def write_pg(df, table):
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", "hive")
        .option("password", "hive")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    log.info("Datamart '{}' ecrit dans PostgreSQL".format(table))

try:
    log.info("Lecture silver")
    df = spark.read.parquet("{}/tracks_enriched".format(input_silver))
    df.cache()
    log.info("Silver charge : {} lignes".format(df.count()))

    # ── Datamart 1 : features audio par décennie ──────────────────────────────
    log.info("Datamart 1 : audio par decennie")
    dm_audio = (
        df.filter(F.col("decade").isNotNull() & (F.col("decade") >= 1920))
        .groupBy("decade")
        .agg(
            F.round(F.avg("popularity"),    2).alias("avg_popularity"),
            F.round(F.avg("danceability"),  4).alias("avg_danceability"),
            F.round(F.avg("energy"),        4).alias("avg_energy"),
            F.round(F.avg("duration_ms") / 1000, 2).alias("avg_duration_sec"),
            F.round(F.avg("tempo"),         2).alias("avg_tempo"),
            F.count("id").alias("nb_tracks")
        )
        .orderBy("decade")
    )
    dm_audio.show()
    write_pg(dm_audio, "dm_audio_by_decade")

    # ── Datamart 2 : top tracks popularité > 70 ───────────────────────────────
    log.info("Datamart 2 : top tracks")
    dm_top = (
        df.filter(F.col("popularity") > 70)
        .select(
            "id", "name", "popularity", "danceability",
            "energy", "tempo", "duration_ms", "decade",
            "artist_name", "artist_followers", "release_date"
        )
        .orderBy(F.desc("popularity"))
        .limit(1000)
    )
    dm_top.show(5)
    write_pg(dm_top, "dm_top_tracks")

    # ── Datamart 3 : popularité par genre ─────────────────────────────────────
    log.info("Datamart 3 : popularite par genre")
    dm_genre = (
        df.filter(F.col("artist_genres").isNotNull() & (F.col("artist_genres") != "[]"))
        .withColumn("genre", F.explode(
            F.split(F.regexp_replace(F.col("artist_genres"), "[\\[\\]']", ""), ", ")
        ))
        .filter(F.col("genre") != "")
        .groupBy("genre")
        .agg(
            F.round(F.avg("popularity"), 2).alias("avg_popularity"),
            F.round(F.avg("danceability"), 4).alias("avg_danceability"),
            F.round(F.avg("energy"), 4).alias("avg_energy"),
            F.count("id").alias("nb_tracks")
        )
        .filter(F.col("nb_tracks") >= 50)
        .orderBy(F.desc("avg_popularity"))
        .limit(100)
    )
    dm_genre.show(5)
    write_pg(dm_genre, "dm_genre_popularity")

    # ── Datamart 4 : top artistes ─────────────────────────────────────────────
    log.info("Datamart 4 : top artistes")
    dm_artists = (
        df.filter(F.col("artist_name").isNotNull())
        .groupBy("artist_name", "artist_followers", "artist_popularity")
        .agg(
            F.round(F.avg("popularity"), 2).alias("avg_track_popularity"),
            F.round(F.avg("danceability"), 4).alias("avg_danceability"),
            F.round(F.avg("energy"), 4).alias("avg_energy"),
            F.count("id").alias("nb_tracks")
        )
        .filter(F.col("nb_tracks") >= 5)
        .orderBy(F.desc("artist_popularity"))
        .limit(500)
    )
    dm_artists.show(5)
    write_pg(dm_artists, "dm_top_artists")

except Exception as e:
    log.error("Erreur datamart : {}".format(e))
    raise

finally:
    spark.stop()
    log.info("Datamart termine.")