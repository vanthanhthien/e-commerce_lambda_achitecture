import sqlite3
import polars as pl

def load_metrics(db_path, table_name, search_kw="", limit=30):
    """Đọc dữ liệu từ SQLite bằng Polars cực nhanh"""
    try:
        query = f"SELECT * FROM {table_name} ORDER BY timestamp DESC"
        # Đọc trực tiếp vào Polars
        conn = sqlite3.connect(db_path)
        df = pl.read_database(query=query, connection=conn)
        conn.close()
        
        if not df.is_empty():
            # Xử lý timezone kiểu Polars
            df = df.with_columns(
                pl.col("timestamp").str.to_datetime().dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Ho_Chi_Minh").alias("timestamp_tz")
            ).with_columns(
                pl.col("timestamp_tz").dt.strftime('%H:%M:%S').alias("Thời Gian")
            )
            
            # Lọc từ khóa nếu có (tìm trên tất cả các cột dạng chuỗi)
            if search_kw:
                # Ép tất cả các cột về string và ghép lại để tìm kiếm
                concat_expr = pl.concat_str([pl.col(c).cast(pl.Utf8) for c in df.columns], separator=" ")
                df = df.filter(concat_expr.str.contains(f"(?i){search_kw}"))
                
            df = df.head(limit).sort("timestamp")
        return df
    except Exception as e:
        return pl.DataFrame()

def load_latest_metrics(db_path, table_name, limit=60):
    """Dành riêng cho Bottleneck Radar"""
    try:
        query = f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT {limit}"
        conn = sqlite3.connect(db_path)
        df = pl.read_database(query=query, connection=conn)
        conn.close()
        
        if not df.is_empty():
            df = df.with_columns(
                pl.col("timestamp").str.to_datetime().dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Ho_Chi_Minh").alias("timestamp_tz")
            ).with_columns(
                pl.col("timestamp_tz").dt.strftime('%H:%M:%S').alias("Thời Gian")
            ).sort("timestamp_tz")
        return df
    except Exception:
        return pl.DataFrame()

def get_total_records(db_path):
    """Lấy tổng số dòng đã nhận"""
    try:
        query = "SELECT SUM(total_received) as total FROM bronze_metrics"
        conn = sqlite3.connect(db_path)
        df = pl.read_database(query=query, connection=conn)
        conn.close()
        total = df.select("total").item()
        return total if total is not None else 0
    except Exception:
        return 0