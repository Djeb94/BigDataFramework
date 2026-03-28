# -*- coding: utf-8 -*-
import sys
import logging
from datetime import date
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/opt/pipeline/logs/feeder.txt"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("feeder")

if len(sys.argv) != 4:
    log.error("Usage: feeder.py <input_tracks> <jdbc_url> <output_raw>")
    sys.exit(1)

input_tracks = sys.argv[1]
jdbc_url     = sys.argv[2]
output_raw   = sys.argv[3]

spark = (
    SparkSession.builder
    .appName("spotify-feeder")
    .config("spark.jars", "/opt/pipeline/jars/postgresql-42.6.0.jar")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

today = date.today()
log.info("Ingestion date : {}".format(today))

try:
    log.info("Lecture tracks depuis HDFS : {}".format(input_tracks))
    df_tracks = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_tracks)
    )
    log.info("Tracks : {} lignes".format(df_tracks.count()))

    log.info("Lecture artists depuis PostgreSQL")
    df_artists = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "artists")
        .option("user", "hive")
        .option("password", "hive")
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    log.info("Artists : {} lignes".format(df_artists.count()))

    def add_date(df):
        return (
            df.withColumn("year",  F.lit(today.year))
              .withColumn("month", F.lit(today.month))
              .withColumn("day",   F.lit(today.day))
        )

    df_tracks  = add_date(df_tracks)
    df_artists = add_date(df_artists)

    df_tracks.cache()
    df_artists.cache()

    (
        df_tracks
        .repartition(4)
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet("{}/tracks".format(output_raw))
    )
    log.info("Tracks ecrits dans {}/tracks".format(output_raw))

    (
        df_artists
        .repartition(2)
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet("{}/artists".format(output_raw))
    )
    log.info("Artists ecrits dans {}/artists".format(output_raw))

except Exception as e:
    log.error("Erreur feeder : {}".format(e))
    raise

finally:
    spark.stop()
    log.info("Feeder termine.")