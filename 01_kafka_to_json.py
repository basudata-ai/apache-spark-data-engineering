import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# 1. Start Spark with the Kafka connector
spark = (
    SparkSession.builder
    .appName("KafkaToJson")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Shape of the customer row and of the Debezium message
row_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
])

envelope_schema = StructType([
    StructField("before", row_schema),   # row before the change (null for inserts)
    StructField("after", row_schema),    # row after the change (null for deletes)
    StructField("op", StringType()),     # r=snapshot, c=insert, u=update, d=delete
    StructField("ts_ms", LongType()),    # time of the change
])

# 3. Read the stream from Kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "shop.shopdb.customers")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Convert bytes -> text -> columns
parsed = raw.select(
    from_json(col("value").cast("string"), envelope_schema).alias("e"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
)


def pick(field):
    # For deletes the data is in "before"; for everything else it is in "after"
    return (
        when(col("e.op") == "d", col(f"e.before.{field}"))
        .otherwise(col(f"e.after.{field}"))
        .alias(field)
    )


changes = parsed.where(col("e.op").isNotNull()).select(
    col("e.op").alias("op"),
    pick("id"),
    pick("name"),
    pick("email"),
    pick("city"),
    col("e.ts_ms").alias("event_ts_ms"),
    "kafka_partition",
    "kafka_offset",
)

# 5. Write the changes as JSON files into the destination folder
query = (
    changes.writeStream
    .format("json")
    .option("path", "output/json_changes")
    .option("checkpointLocation", "output/checkpoints/json_sink")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
