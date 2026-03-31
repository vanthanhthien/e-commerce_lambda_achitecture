import os
import sys

sys.path.append(os.getcwd())

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv, find_dotenv

from src.common.data_quality import get_review_schema
from src.monitoring.monitor_silver import SilverStreamMonitor

print("⏳ Đang khởi tạo Spark Session (Bronze -> Silver Local)...")

load_dotenv(find_dotenv())

BRONZE_RAW_PATH = os.getenv("BRONZE_LAYER_PATH")
SILVER_LAYER_PATH = os.getenv("SILVER_LAYER_PATH")
SILVER_CHECKPOINT_PATH = os.getenv("SILVER_CHECKPOINT_PATH")

if not BRONZE_RAW_PATH or not SILVER_LAYER_PATH or not SILVER_CHECKPOINT_PATH:
    raise ValueError("❌ Lỗi Cấu Hình: Thiếu đường dẫn tầng Bronze/Silver!")

# Tự động tạo thư mục local (Cực kỳ đơn giản)
os.makedirs(BRONZE_RAW_PATH, exist_ok=True)
os.makedirs(SILVER_LAYER_PATH, exist_ok=True)

# Khởi tạo Spark Session với Delta Lake (Bỏ extra_packages AWS)
builder = SparkSession.builder \
    .appName("Continuous_Bronze_To_Silver_Local") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "false") \
    .config("spark.databricks.delta.autoCompact.enabled", "false") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.debug.maxToStringFields", "100")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")
jvm_timezone = spark.sparkContext._jvm.java.util.TimeZone
jvm_timezone.setDefault(jvm_timezone.getTimeZone("UTC"))
spark.streams.addListener(SilverStreamMonitor())

try:
    print(f"📥 Đang cắm vòi hút dữ liệu liên tục từ Raw Bronze: {BRONZE_RAW_PATH}...")
    schema = get_review_schema()

    bronze_stream_df = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", 100) \
        .json(BRONZE_RAW_PATH)
    
    print("🗂️ Đang áp dụng logic tạo cột phân vùng 'review_date'...")
    df_transformed = bronze_stream_df.withColumn(
        "review_date", 
        to_date(from_unixtime(col("unixReviewTime")))
    )

    print(f"💾 Đang xả dữ liệu vào Silver Layer: {SILVER_LAYER_PATH}...")
    query = df_transformed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH) \
        .option("mergeSchema", "true") \
        .partitionBy("review_date") \
        .trigger(processingTime="1 minute") \
        .start(SILVER_LAYER_PATH)

    print("🎉 HỆ THỐNG STREAMING ĐÃ CHẠY! Nhấn Ctrl+C để dừng.")
    query.awaitTermination()

except Exception as e:
    print(f"❌ Có lỗi xảy ra trong luồng Streaming: {e}")