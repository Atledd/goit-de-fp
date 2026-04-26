from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LandingToBronze") \
    .getOrCreate()

df = spark.read.csv(
    "data/landing/athlete_events.csv",
    header=True,
    inferSchema=True
)

df.write.mode("overwrite").parquet("data/bronze/athlete_events")

spark.stop()