import os
import sys
import sqlite3
import time
import polars as pl
from dotenv import load_dotenv, find_dotenv

# Đảm bảo Python nhận diện đường dẫn gốc
sys.path.append(os.getcwd())

# Load cấu hình
load_dotenv(find_dotenv())

# Lấy đường dẫn ĐẦU VÀO từ .env
BRONZE_LOG = os.getenv("MONITOR_SPARK_BRONZE_PATH")
SILVER_LOG = os.getenv("MONITOR_SILVER_STREAM_PATH")

# Lấy đường dẫn DATABASE ĐẦU RA từ .env
DB_PATH = os.getenv("MONITOR_DB_PATH", "data/monitoring_logs/monitoring.db")

# Tự động tạo thư mục đầu ra nếu chưa có
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def init_db():
    """Khởi tạo Database SQLite và các bảng nếu chưa tồn tại"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bronze_metrics (
            timestamp TEXT PRIMARY KEY,
            total_received REAL,
            valid_records REAL,
            invalid_records REAL,
            error_rate_percent REAL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS silver_metrics (
            timestamp TEXT PRIMARY KEY,
            input_rows_per_second REAL,
            processed_rows_per_second REAL,
            addBatch_ms REAL
        )
    ''')
    
    conn.commit()
    conn.close()

def aggregate_bronze_logs():
    """Gom nhóm log Bronze Layer và ghi vào SQLite"""
    # FIX LỖI: Kiểm tra thêm os.path.getsize > 0 để tránh đọc file rỗng (0 bytes)
    if not BRONZE_LOG or not os.path.exists(BRONZE_LOG) or os.path.getsize(BRONZE_LOG) == 0:
        return

    try:
        df = pl.read_ndjson(BRONZE_LOG)
        if df.height == 0: return

        df = df.with_columns([
            pl.col("metrics").struct.field("total_received").cast(pl.Float64).alias("total_received"),
            pl.col("metrics").struct.field("valid_records").cast(pl.Float64).alias("valid_records"),
            pl.col("metrics").struct.field("invalid_records").cast(pl.Float64).alias("invalid_records"),
            pl.col("timestamp").str.to_datetime(strict=False)
        ])

        agg_df = (
            df.sort("timestamp")
            .group_by_dynamic("timestamp", every="1m")
            .agg([
                pl.col("total_received").sum(),
                pl.col("valid_records").sum(),
                pl.col("invalid_records").sum()
            ])
            .with_columns(
                (pl.col("invalid_records") / pl.col("total_received") * 100)
                .fill_nan(0.0).round(2).alias("error_rate_percent")
            )
        )

        agg_df = agg_df.with_columns(pl.col("timestamp").cast(pl.Utf8))

        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        
        cursor.executemany('''
            INSERT OR REPLACE INTO bronze_metrics 
            (timestamp, total_received, valid_records, invalid_records, error_rate_percent)
            VALUES (?, ?, ?, ?, ?)
        ''', agg_df.rows())
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Lỗi khi xử lý Bronze log: {e}")

def aggregate_silver_logs():
    """Gom nhóm log Silver Layer và ghi vào SQLite"""
    # FIX LỖI: Kiểm tra thêm os.path.getsize > 0 để tránh đọc file rỗng (0 bytes)
    if not SILVER_LOG or not os.path.exists(SILVER_LOG) or os.path.getsize(SILVER_LOG) == 0:
        return

    try:
        df = pl.read_ndjson(SILVER_LOG)
        if df.height == 0: return

        df = df.with_columns([
            pl.col("performance").struct.field("input_rows_per_second").cast(pl.Float64).alias("input_rows_per_second"),
            pl.col("performance").struct.field("processed_rows_per_second").cast(pl.Float64).alias("processed_rows_per_second"),
            pl.col("processing_time").struct.field("addBatch").cast(pl.Float64).alias("addBatch_ms"),
            pl.col("timestamp").str.slice(0, 19).str.to_datetime(format="%Y-%m-%dT%H:%M:%S", strict=False)
        ])

        agg_df = (
            df.sort("timestamp")
            .group_by_dynamic("timestamp", every="1m")
            .agg([
                pl.col("input_rows_per_second").mean(),
                pl.col("processed_rows_per_second").mean(),
                pl.col("addBatch_ms").mean()
            ])
        )

        agg_df = agg_df.with_columns(pl.col("timestamp").cast(pl.Utf8))

        conn = sqlite3.connect(DB_PATH, timeout=15.0)
        cursor = conn.cursor()
        
        cursor.executemany('''
            INSERT OR REPLACE INTO silver_metrics 
            (timestamp, input_rows_per_second, processed_rows_per_second, addBatch_ms)
            VALUES (?, ?, ?, ?)
        ''', agg_df.rows())
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Lỗi khi xử lý Silver log: {e}")

if __name__ == "__main__":
    print("⏳ Đang khởi tạo Database...")
    init_db()
    print("🚀 Đang khởi động luồng gom nhóm Log Real-time...")
    
    # vòng lặp để dashboard tự cập nhật liên tục
    try:
        while True:
            aggregate_bronze_logs()
            aggregate_silver_logs()
            time.sleep(3) # Cứ 3 giây gom 1 lần cho đỡ nặng máy
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng luồng gom Log.")