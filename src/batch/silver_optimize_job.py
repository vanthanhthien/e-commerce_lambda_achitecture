import os
import sys

# Đảm bảo Python nhận diện được các thư mục gốc
sys.path.append(os.getcwd())

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv, find_dotenv

# ==========================================
# 1. LOAD CẤU HÌNH TỪ .ENV VÀ AWS S3
# ==========================================
load_dotenv(find_dotenv())

SILVER_LAYER_PATH = os.getenv("SILVER_LAYER_PATH")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")

if not SILVER_LAYER_PATH:
    raise ValueError("❌ Thiếu SILVER_LAYER_PATH trong file .env!")

# ==========================================
# 2. KHỞI TẠO BỘ MÁY SPARK (TRANG BỊ GIÁP AWS)
# ==========================================
print("⏳ Đang khởi tạo bộ máy dọn dẹp Delta Lake trên AWS S3...")

builder = SparkSession.builder \
    .appName("Silver_Layer_Optimization") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")

# Tích hợp thư viện AWS vào Delta Spark
extra_packages = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
]
spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # Chỉ hiện lỗi, ẩn bớt log rác

# ==========================================
# 3. THỰC THI GOM FILE VÀ DỌN RÁC (OPTIMIZE & VACUUM)
# ==========================================
def run_optimization():
    try:
        print(f"🔗 Đang kết nối vào kho dữ liệu Silver tại: {SILVER_LAYER_PATH}")
        delta_table = DeltaTable.forPath(spark, SILVER_LAYER_PATH)
        
        # --- BƯỚC A: GOM NHÓM (COMPACTION) ---
        print("📦 BƯỚC 1/2: Đang gộp các file nhỏ thành file lớn (OPTIMIZE)...")
        # Ép kích thước file chuẩn là 256MB (rất tốt cho S3 và DWH)
        spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 256 * 1024 * 1024)
        
        # Chỉ 1 dòng duy nhất, Delta tự làm mọi thứ!
        delta_table.optimize().executeCompaction()
        print("✅ Gom file hoàn tất!")

        # --- BƯỚC B: DỌN LỊCH SỬ (VACUUM) ---
        print("🧹 BƯỚC 2/2: Xóa các file rác lịch sử cũ hơn 7 ngày (VACUUM)...")
        
        # Giữ lại lịch sử 168 giờ (7 ngày) phòng trường hợp muốn Time Travel (quay ngược thời gian)
        delta_table.vacuum(168) 
        print("✅ Dọn rác S3 hoàn tất!")

        print("\n🎉 TẤT CẢ ĐÃ XONG! Tầng Silver giờ đã siêu mượt và sạch sẽ. Đã sẵn sàng đổ vào Postgres!")

    except Exception as e:
        print(f"❌ Có lỗi xảy ra trong quá trình tối ưu: {e}")

if __name__ == "__main__":
    run_optimization()