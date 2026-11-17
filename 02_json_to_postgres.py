import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

PG_HOST, PG_PORT = "localhost", 5433
PG_DB, PG_USER, PG_PASSWORD = "sinkdb", "sink", "sink"

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

# 1. Start Spark with the Postgres JDBC driver
spark = (
    SparkSession.builder
    .appName("JsonToPostgres")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Schema of the JSON files written by 01_kafka_to_json.py
schema = StructType([
    StructField("op", StringType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("event_ts_ms", LongType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
])

os.makedirs("output/json_changes", exist_ok=True)

# 3. Read the JSON folder as a stream (new files are picked up automatically)
stream = spark.readStream.schema(schema).json("output/json_changes")


# 4. For every micro-batch of new rows, write to Postgres
def write_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # (a) Append every change event to the history table (Spark JDBC)
    batch_df.write.jdbc(PG_URL, "customers_changes", mode="append", properties=PG_PROPS)

    # (b) Apply the changes to the replica table in the right order
    rows = batch_df.orderBy("event_ts_ms", "kafka_offset").collect()
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASSWORD)
    with conn, conn.cursor() as cur:
        for r in rows:
            if r.op == "d":
                cur.execute("DELETE FROM customers_replica WHERE id = %s", (r.id,))
            else:  # r = snapshot, c = insert, u = update
                cur.execute(
                    """
                    INSERT INTO customers_replica (id, name, email, city)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name,
                        email = EXCLUDED.email,
                        city = EXCLUDED.city
                    """,
                    (r.id, r.name, r.email, r.city),
                )
    conn.close()


query = (
    stream.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", "output/checkpoints/pg_sink")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
