# -*- coding: utf-8 -*-
import sys
import logging
from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/opt/pipeline/logs/processor.txt"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("processor")

if len(sys.argv) != 3:
    log.error("Usage: processor.py <input_raw> <output_silver>")
    sys.exit(1)

input_raw     = sys.argv[1]
output_silver = sys.argv[2]

spark = (
    SparkSession.builder
    .appName("spotify-processor")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

today = date.today()

try:
    #Lecture RAW
    log.info("Lecture tracks raw")
    df_tracks = spark.read.parquet("{}/tracks".format(input_raw))

    log.info("Lecture artists raw")
    df_artists = spark.read.parquet("{}/artists".format(input_raw))

    log.info("Validation des donnees")
    before = df_tracks.count()

    df_tracks = df_tracks.filter(F.col("id").isNotNull())
    df_tracks = df_tracks.filter(F.col("name").isNotNull())
    df_tracks = df_tracks.filter(
        (F.col("popularity").cast("int") >= 0) &
        (F.col("popularity").cast("int") <= 100)
    )
    df_tracks = df_tracks.filter(F.col("duration_ms").cast("long") > 0)
    df_tracks = df_tracks.filter(
        F.col("release_date").isNotNull() &
        (F.length(F.col("release_date")) >= 4)
    )

    after = df_tracks.count()
    log.info("Lignes apres validation : {} (supprimees : {})".format(after, before - after))

    df_tracks = (
        df_tracks
        .withColumn("popularity",    F.col("popularity").cast("int"))
        .withColumn("duration_ms",   F.col("duration_ms").cast("long"))
        .withColumn("danceability",  F.col("danceability").cast("double"))
        .withColumn("energy",        F.col("energy").cast("double"))
        .withColumn("tempo",         F.col("tempo").cast("double"))
        .withColumn("release_year",  F.substring(F.col("release_date"), 1, 4).cast("int"))
        .withColumn("decade",        (F.col("release_year") / 10).cast("int") * 10)
    )

    df_artists = (
        df_artists
        .withColumn("popularity", F.col("popularity").cast("int"))
        .withColumn("followers",  F.col("followers").cast("double"))
    )

    df_tracks.cache()
    df_artists.cache()
    log.info("Cache applique sur tracks et artists")

    #ointure tracks + artists
    log.info("Jointure tracks + artists")
    df_joined = (
        df_tracks
        .withColumn("artist_id", F.explode(F.split(F.regexp_replace(F.col("id_artists"), "[\\[\\]']", ""), ", ")))
        .join(
            df_artists.select(
                F.col("id").alias("artist_id"),
                F.col("name").alias("artist_name"),
                F.col("followers").alias("artist_followers"),
                F.col("popularity").alias("artist_popularity"),
                F.col("genres").alias("artist_genres")
            ),
            on="artist_id",
            how="left"
        )
    )

    #Agrégation ppopularité moyenne par décennie 
    log.info("Agregation par decennie")
    df_agg = (
        df_joined
        .groupBy("decade")
        .agg(
            F.avg("popularity").alias("avg_popularity"),
            F.avg("danceability").alias("avg_danceability"),
            F.avg("energy").alias("avg_energy"),
            F.count("id").alias("nb_tracks")
        )
        .orderBy("decade")
    )
    df_agg.show(10)

    #rank par popularité dans chaque décennie
    log.info("Window function : rank popularite par decennie")
    window_spec = Window.partitionBy("decade").orderBy(F.desc("popularity"))
    df_joined = df_joined.withColumn("rank_in_decade", F.rank().over(window_spec))

    log.info("Ecriture silver")
    (
        df_joined
        .withColumn("year",  F.lit(today.year))
        .withColumn("month", F.lit(today.month))
        .withColumn("day",   F.lit(today.day))
        .repartition(4)
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet("{}/tracks_enriched".format(output_silver))
    )
    log.info("Silver ecrit dans {}/tracks_enriched".format(output_silver))

except Exception as e:
    log.error("Erreur processor : {}".format(e))
    raise

finally:
    spark.stop()
    log.info("Processor termine.")