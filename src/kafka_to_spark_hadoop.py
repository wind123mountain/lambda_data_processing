from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, IntegerType
import os
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
import uuid


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'


uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())


# Cassandra cluster/session global (khởi tạo 1 lần)
cluster = Cluster(['cassandra1'], port=9042)
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


def start_stream():
    """
    Starting the spark session. Grouping and counting items.
    """

    # Setting up Spark
    spark = SparkSession.builder \
        .appName("KafkaBatchProcessing") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Defining the data structure (like columns of the table and dtypes)
    schema = StructType([
        StructField("itemid", StringType()),
        StructField("visitorid", StringType()),
        StructField("event", StringType()),
        StructField("transactionid", DoubleType()),
        StructField("timestamp", StringType())
    ])

    # Getting messages directly from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "event_message") \
        .option("kafka.group.id", "batch_layer") \
        .load()
    
    events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    events = events.withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))
    events = events.withWatermark("timestamp", "20 seconds").dropDuplicates(["visitorid", "timestamp"])

    raw_query = events.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "hdfs://hadoop:8020/checkpoints/retailrocket_raw") \
        .option("path", "hdfs://hadoop:8020/data/retailrocket_raw") \
        .trigger(processingTime="30 seconds") \
        .outputMode("append") \
        .start()

    return raw_query

query = start_stream()
query.awaitTermination()
