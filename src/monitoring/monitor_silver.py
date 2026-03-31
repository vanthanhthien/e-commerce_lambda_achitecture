import os
import json
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from pyspark.sql.streaming import StreamingQueryListener

load_dotenv(find_dotenv())

# Đã sửa lại đúng tên Class để file kia có thể import được
class SilverStreamMonitor(StreamingQueryListener):
    def __init__(self): # Đã gõ lại khoảng trắng cho chuẩn
        super().__init__() 
        self.log_path = os.getenv("MONITOR_SILVER_STREAM_PATH")
        if not self.log_path:
            raise ValueError("❌ Lỗi: Thiếu MONITOR_SILVER_STREAM_PATH trong file .env!")
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        
    def onQueryStarted(self, event):
        # Đã bỏ event.name để tránh lỗi của Spark 3.5
        print(f"🚀 Silver Stream Started: {event.id}")
        
    def onQueryProgress(self, event):
        progress = event.progress # Đã sửa lỗi thiếu chữ 'r' (progess -> progress)
        duration_ms = progress.durationMs if progress.durationMs else {}
        
        log_entry = {
            "timestamp": progress.timestamp,
            "batch_id": progress.batchId,
            "performance": {
                "input_rows_per_second": progress.inputRowsPerSecond,
                "processed_rows_per_second": progress.processedRowsPerSecond,
                "total_input_rows": progress.numInputRows,
                "duration_ms": duration_ms
            },
            "processing_time": {
                "addBatch": duration_ms.get("addBatch", 0),
                "getBatch": duration_ms.get("getBatch", 0),
                "queryPlanning": duration_ms.get("queryPlanning", 0),
                "triggerExecution": duration_ms.get("triggerExecution", 0)
            }
        }
        
        # 1. Ghi âm thầm vào ổ cứng cho Dashboard đọc
        with open(self.log_path, 'a', encoding='utf-8') as f:
            json.dump(log_entry, f, ensure_ascii=False)
            f.write('\n')
            f.flush() # ép tống khứ dữ liệu xuống ổ cứng
            os.fsync(f.fileno())
            
        # 2. LOA PHÁT THANH RA MÀN HÌNH ĐEN (TERMINAL)
        num_rows = progress.numInputRows
        if num_rows > 0:
            speed = progress.processedRowsPerSecond
            latency = duration_ms.get("addBatch", 0)
            print(f"✅ [Silver] Batch {progress.batchId}: Đã dọn sạch {num_rows} dòng | Tốc độ: {speed} dòng/s | Độ trễ ghi S3: {latency} ms")
        else:
            print(f"💤 [Silver] Batch {progress.batchId}: Đang chờ dữ liệu mới từ luồng Bronze...")
            
    def onQueryIdle(self, event):
        # Hàm này bắt buộc phải có từ bản Spark 3.5 trở lên
        pass
            
    def onQueryTerminated(self, event):
        # Đã bỏ event.name để tránh lỗi
        print(f"🛑 Silver Stream Terminated: {event.id} - Reason: {event.exceptionMessage}")