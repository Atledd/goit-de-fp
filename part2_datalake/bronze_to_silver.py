from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

df = spark.read.parquet(
    "data/bronze/athlete_events"
)

clean_df = df.filter(
    col("height").isNotNull() &
    col("weight").isNotNull() &
    col("medal").isNotNull()
)

clean_df.write.mode("overwrite").parquet(
    "data/silver/athlete_events"
)

spark.stop()