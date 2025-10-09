from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, udf, window, when, round
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, LongType, IntegerType
import os
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
import uuid


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'


uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())


# Cassandra cluster/session global (khởi tạo 1 lần)
cluster = Cluster(['cassandra1'], port=9042, connect_timeout=20, control_connection_timeout=10)
session = cluster.connect()

# Creating a keyspace named event_data_view, where data will be written for kappa
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS event_data_view
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
)

session.set_keyspace("event_data_view")
session.execute("""
    CREATE TABLE IF NOT EXISTS real_time_view (
        id uuid PRIMARY KEY,
        event TEXT,
        count INT,
        time_stamp TIMESTAMP
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS real_time_event_rate (
        window_start timestamp PRIMARY KEY,
        window_end timestamp,
        n_view int,
        addtocart int,
        transaction int
    )
""", timeout=30)


def write_to_cassandra(batch_df, batch_id):
    """
    Writing every batch to Cassandra
    """

    batch_df_with_meta = batch_df \
        .withColumn("id", uuid_udf()) \
        .withColumn("time_stamp", current_timestamp())

    # Ghi vào Cassandra
    batch_df_with_meta.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="real_time_view", keyspace="event_data_view") \
        .mode("append") \
        .save()

def start_stream():
    """
    Starting the spark session. Grouping and counting items.
    """

    # Setting up Spark
    spark = SparkSession.builder \
        .appName("KafkaStreamProcessing") \
        .config("spark.cassandra.connection.host", "cassandra1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark1:7077")) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    # Defining the data structure (like columns of the table and dtypes)
    schema = StructType([
        StructField("itemid", StringType()),
        StructField("visitorid", StringType()),
        StructField("event", StringType()),
        StructField("transactionid", IntegerType()),
        StructField("timestamp", LongType())
    ])

    # Getting messages directly from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "event_message") \
        .option("startingOffsets", "latest") \
        .load()

    # Grouping and counting
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    result_df = parsed_df.groupBy("event").count()

    # Writing cassandra
    query = result_df.writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("complete") \
        .foreachBatch(write_to_cassandra) \
        .option("spark.cassandra.connection.host", "cassandra1") \
        .start()
    
    # 2️⃣ Chuyển timestamp
    df = parsed_df.withColumn("event_ts", (col("timestamp") / 1000).cast("timestamp"))

    # 3️⃣ Gom nhóm theo cửa sổ 20s và theo loại event
    event_rate_20s = df.groupBy(
        window(col("event_ts"), "15 seconds"),
        col("event")
    ).count() \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event"),
        col("count").alias("event_count")
    )

    # Pivot bảng: mỗi row là 1 window, các cột là count từng event
    pivot_df = event_rate_20s.groupBy("window_start", "window_end") \
        .pivot("event", ["view", "addtocart", "transaction"]) \
        .sum("event_count").withColumnRenamed("view", "n_view")
    pivot_df = pivot_df.na.fill(0, ["n_view", "addtocart", "transaction"])

    metrics_query = pivot_df.writeStream \
            .trigger(processingTime="20 seconds") \
            .outputMode("update") \
            .foreachBatch(lambda batch_df, batch_id: batch_df.write
                        .format("org.apache.spark.sql.cassandra")
                        .mode("append")
                        .options(keyspace="event_data_view", table="real_time_event_rate")
                        .save()) \
            .option("spark.cassandra.connection.host", "cassandra1") \
            .start()


    return query, metrics_query

query, metrics_query = start_stream()
query.awaitTermination()
metrics_query.awaitTermination()

