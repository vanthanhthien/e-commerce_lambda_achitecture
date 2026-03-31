import os
import sys
import time

sys.path.append(os.getcwd())

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from dotenv import load_dotenv, find_dotenv

from src.common.data_quality import get_review_schema, get_validation_rules
from src.monitoring.monitor_spark_bronze import SparkBronzeMonitor
from src.streaming.dynamodb_sink import DynamoDBSink

# 1. Load Path
load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
BRONZE_PATH = os.getenv("BRONZE_LAYER_PATH")
DLQ_PATH = os.getenv("MONITOR_DLQ_PATH")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH")

print("⏳ Đang khởi tạo Spark Session (Phiên bản Local siêu tốc)...")

# 2. Khởi tạo Spark
spark = SparkSession.builder \
    .appName("Kafka_To_Bronze_Local") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()
    
jvm_timezone = spark.sparkContext._jvm.java.util.TimeZone
jvm_timezone.setDefault(jvm_timezone.getTimeZone("UTC"))
spark.sparkContext.setLogLevel("WARN")

bronze_monitor = SparkBronzeMonitor()
dynamodb_sink = DynamoDBSink()

# HÀM DUY NHẤT XỬ LÝ MẺ DỮ LIỆU
def route_data_quality(df, epoch_id):
    start_time = time.time()
    
    schema = get_review_schema()
    parsed_df = df.selectExpr("CAST(value AS STRING) as raw_json") \
                  .withColumn("data", from_json(col("raw_json"), schema)) \
                  .select("data.*", "raw_json")

    # ==========================================
    # 🛡️ LUẬT TÌM RÁC (EXPLICIT ERROR ROUTING)
    # ==========================================
    error_condition = (
        col("reviewerID").isNull() | (col("reviewerID") == "None") | (col("reviewerID") == "") |
        col("asin").isNull() | (col("asin") == "None") | (col("asin") == "") |
        col("overall").isNull() | (col("overall") < 1) | (col("overall") > 5)
    )

    # 1. Phân loại rác
    invalid_df = parsed_df.filter(error_condition)

    # 2. Phân loại hàng ngon (Drop raw_json luôn)
    valid_df = parsed_df.filter(~error_condition).drop("raw_json")
    # ==========================================

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    total_records = valid_count + invalid_count

    if valid_count > 0:
        valid_df.write.mode("append").json(BRONZE_PATH)
        dynamodb_sink.save_metrics(valid_df, epoch_id)
        
    if invalid_count > 0:
        print(f"⚠️ Phát hiện {invalid_count} dòng LỖI! Đang xả vào DLQ...")
        invalid_df.write.mode("append").json(DLQ_PATH)
            
    if total_records > 0:
        print(f"✅ Micro-batch {epoch_id}: HỢP LỆ ({valid_count}) | LỖI ({invalid_count})")
        process_time_ms = round((time.time() - start_time) * 1000, 2)
        bronze_monitor.log_batch_quality(epoch_id, total_records, valid_count, invalid_df, process_time_ms)
        
print(f"🚀 Đang đọc luồng dữ liệu từ Kafka Topic: {KAFKA_TOPIC}...")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

query = raw_stream.writeStream \
    .foreachBatch(route_data_quality) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()