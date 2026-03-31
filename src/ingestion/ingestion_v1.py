import os
import sys

sys.path.append(os.getcwd())

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv, find_dotenv

# Load cấu hình
load_dotenv(find_dotenv())

SILVER_LAYER_PATH = os.getenv("SILVER_LAYER_PATH")

def run_vacuum_job():
    print("⏳ Đang khởi tạo Spark Session cho Job dọn dẹp (VACUUM)...")
    
    # Khởi tạo Spark với Delta Lake
    builder = SparkSession.builder \
        .appName("Delta_Lake_Vacuum_Job") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if not SILVER_LAYER_PATH or not os.path.exists(SILVER_LAYER_PATH):
        print("⚠️ Không tìm thấy thư mục Silver Layer. Có thể hệ thống chưa có dữ liệu.")
        return

    print(f"🧹 Đang tiến hành VACUUM (Dọn rác lịch sử) tại: {SILVER_LAYER_PATH}")
    
    # Kết nối vào Delta Table
    delta_table = DeltaTable.forPath(spark, SILVER_LAYER_PATH)
    
    # Chạy lệnh Vacuum để xóa các file rác cũ hơn 168 giờ (7 ngày)
    # Những file bị xóa là những version cũ không còn dùng tới, KHÔNG PHẢI xóa dữ liệu hiện tại!
    delta_table.vacuum(168)
    
    print("✅ Hoàn tất dọn dẹp! Thư mục Silver đã sạch sẽ và tối ưu dung lượng ổ cứng.")

if __name__ == "__main__":
    run_vacuum_job()