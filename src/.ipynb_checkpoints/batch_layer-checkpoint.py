from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from cassandra.cluster import Cluster
import time

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
    CREATE TABLE IF NOT EXISTS batch_view (
        itemid text,
        event text,
        total_count int,
        PRIMARY KEY (itemid, event)
    )
""")



spark = SparkSession.builder \
    .appName("Retailrocket-BatchLayer") \
    .config("spark.cassandra.connection.host", "cassandra1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

time.sleep(100)

while True:
    print("=== Running batch job ===")

    try:
        # 1️⃣ Đọc toàn bộ dữ liệu gốc từ HDFS
        df = spark.read.parquet("hdfs://hadoop:8020/data/retailrocket_raw")

        # 2️⃣ Tính toán tổng số lượt xem/mua theo sản phẩm
        agg = df.groupBy("itemid", "event").agg(count("*").alias("total_count"))

        # 3️⃣ Ghi vào Cassandra (batch_view)
        agg.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .option("confirm.truncate", "true") \
            .options(table="batch_view", keyspace="event_data_view") \
            .save()
    except Exception as e:
        print(f"Error: {e}")
    
    time.sleep(150)