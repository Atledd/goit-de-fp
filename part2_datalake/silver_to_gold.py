from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

df = spark.read.parquet(
    "data/silver/athlete_events"
)

agg = df.groupBy(
    "sport",
    "medal",
    "sex",
    "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
)

agg.write.mode("overwrite").parquet(
    "data/gold/athlete_stats"
)

agg.show()

spark.stop()