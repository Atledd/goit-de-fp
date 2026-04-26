from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("FinalProjectStreaming") \
    .config(
      "spark.jars.packages",
      "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


athlete_bio = spark.read \
    .format("jdbc") \
    .option(
        "url",
        "jdbc:mysql://localhost:3307/olympic_dataset"
    ) \
    .option(
        "driver",
        "com.mysql.cj.jdbc.Driver"
    ) \
    .option(
        "dbtable",
        "athlete_bio"
    ) \
    .option("user", "root") \
    .option("password", "root") \
    .load() \
    .filter(
        col("height").isNotNull() &
        col("weight").isNotNull()
    )


schema = StructType([
    StructField("athlete_id", IntegerType()),
    StructField("sport", StringType()),
    StructField("medal", StringType())
])


events = spark.readStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers","localhost:9092") \
 .option("subscribe","athlete_event_results") \
 .load()


parsed = events.selectExpr("CAST(value AS STRING)")\
 .select(
    from_json(
       col("value"),
       schema
    ).alias("data")
 ).select("data.*")\
 .withColumn("ts", current_timestamp())


joined = parsed.join(
    athlete_bio,
    "athlete_id"
)


agg = joined.groupBy(
   "sport",
   "medal",
   "sex",
   "country_noc"
).agg(
   avg("height").alias("avg_height"),
   avg("weight").alias("avg_weight"),
   current_timestamp().alias("created_at")
)


def process_batch(df, epoch_id):

    if df.count() == 0:
        return

    print("=== Batch Results ===")
    df.show(truncate=False)

    rows = df.toJSON().collect()

    from kafka import KafkaProducer
    import json

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode()
    )

    for row in rows:
        producer.send(
            "athlete_stats_enriched",
            {"data": row}
        )

    producer.flush()

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3307/olympic_dataset") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "athlete_stats_enriched") \
        .option("user", "root") \
        .option("password", "root") \
        .mode("append") \
        .save()

    print("Batch written to Kafka and MySQL")


query = agg.writeStream \
 .outputMode("complete") \
 .foreachBatch(process_batch) \
 .start()


query.awaitTermination()