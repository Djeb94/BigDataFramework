from datetime import date
from pyspark.sql import SparkSession, functions as F
import time


spark = (
    SparkSession.builder
    .appName("preprocessor")
    .enableHiveSupport()
    .getOrCreate()
)


input_path = "hdfs://namenode:9000/data/raw/breweries_partitioned" 
df = (
    spark.read.parquet(input_path)
)


df2 = (df
       .withColumn("id", F.col("id").cast("int"))
       .select("name", "city", "state", "id"))


time.sleep(120)


(df2.write
   .mode("overwrite")
   .format("parquet")
   .partitionBy("city")
   .saveAsTable("default.breweries_curated"))



