import os
import sys
os.environ['TZ'] = 'UTC'
sys.path.append(os.getcwd())

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv, find_dotenv

print("⏳ Đang nạp cấu hình bảo mật từ .env...")
load_dotenv(find_dotenv())

# ==========================================
# 1. NẠP BIẾN MÔI TRƯỜNG (KHÔNG DÙNG FALLBACK CHO SECRETS)
# ==========================================
SILVER_LAYER_PATH = os.getenv("SILVER_LAYER_PATH")
DWH_JDBC_URL = os.getenv("DWH_JDBC_URL")
DWH_JDBC_DRIVER = os.getenv("DWH_JDBC_DRIVER", "org.postgresql.Driver") # Tên driver thì an toàn
DWH_USER = os.getenv("DWH_USER")
DWH_PASSWORD = os.getenv("DWH_PASSWORD")

# Lấy thông tin AWS để Spark có thể lên S3 đọc data Silver
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")

# THUỐC TRỊ BỆNH LỘ BẢO MẬT (Nguyên tắc Fail-Fast)
if not all([SILVER_LAYER_PATH, DWH_JDBC_URL, DWH_USER, DWH_PASSWORD, AWS_ACCESS_KEY]):
    raise ValueError("❌ Lỗi Bảo Mật/Cấu hình: Bắt buộc phải khai báo đầy đủ Database và AWS trong file .env!")

DWH_SPARK_JAR = "org.postgresql:postgresql:42.6.0"

def run_incremental_load():
    print(f"⏳ Đang khởi tạo Spark Session (Tích hợp AWS S3 & Postgres JDBC)...")
    
    # Ép Spark tải CÙNG LÚC Delta Lake, AWS S3 và Postgres JDBC
    spark = SparkSession.builder \
        .appName("Silver_To_DWH_Incremental") \
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC") \
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com") \
        .config("spark.jars.packages", f"io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,{DWH_SPARK_JAR}") \
        .getOrCreate()
        
    jvm_timezone = spark.sparkContext._jvm.java.util.TimeZone
    jvm_timezone.setDefault(jvm_timezone.getTimeZone("UTC"))
    spark.sparkContext.setLogLevel("WARN")

    # ==========================================
    # 🌟 BƯỚC 1: TÌM WATERMARK (MỐC LỊCH SỬ) TỪ POSTGRES
    # ==========================================
    max_time = 0
    try:
        query = "(SELECT MAX(unixReviewTime) as max_time FROM raw_ecommerce_reviews) as t"
        max_time_df = spark.read.format("jdbc") \
            .option("url", DWH_JDBC_URL) \
            .option("dbtable", query) \
            .option("user", DWH_USER) \
            .option("password", DWH_PASSWORD) \
            .option("driver", DWH_JDBC_DRIVER) \
            .load()
        
        result = max_time_df.collect()[0]["max_time"]
        if result is not None:
            max_time = result
            print(f"📌 Đã tìm thấy dữ liệu cũ trong Postgres. Mốc thời gian mới nhất đang có: {max_time}")
        else:
            print("📌 Bảng trong Postgres đang trống. Sẽ chạy Full Load lần đầu.")
    except Exception as e:
        print("📌 Bảng 'raw_ecommerce_reviews' chưa tồn tại trong Postgres. Sẽ tạo mới và nạp toàn bộ.")

    # ==========================================
    # 🌟 BƯỚC 2: ĐỌC VÀ LỌC DỮ LIỆU TỪ S3 SILVER
    # ==========================================
    print(f"📥 Đang đọc dữ liệu từ Cloud S3 (Silver Layer): {SILVER_LAYER_PATH}...")
    df_silver = spark.read.format("delta").load(SILVER_LAYER_PATH)
    df_new = df_silver.filter(col("unixReviewTime") > max_time)
    new_records_count = df_new.count()

    if new_records_count == 0:
        print("✅ Bỏ qua ghi: Không có đánh giá (review) nào mới ở tầng Silver trên S3.")
        return

    print(f"🚀 Tìm thấy {new_records_count} bản ghi MỚI. Bắt đầu đẩy vào Postgres...")

    # ==========================================
    # 🌟 BƯỚC 3: GHI NỐI TIẾP (APPEND) VÀO DWH
    # ==========================================
    try:
        df_new.write \
            .format("jdbc") \
            .option("url", DWH_JDBC_URL) \
            .option("dbtable", "raw_ecommerce_reviews") \
            .option("user", DWH_USER) \
            .option("password", DWH_PASSWORD) \
            .option("driver", DWH_JDBC_DRIVER) \
            .option("batchsize", "10000") \
            .option("rewriteBatchedStatements", "true") \
            .option("numPartitions", "4") \
            .mode("append") \
            .save()
            
        print("✅ THÀNH CÔNG! Dữ liệu đã được nạp an toàn từ AWS S3 vào Data Warehouse (Postgres).")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng khi đẩy dữ liệu lên DWH: {e}")
        raise e # <--- Ép Airflow báo đỏ nếu có lỗi!

if __name__ == "__main__":
    run_incremental_load()