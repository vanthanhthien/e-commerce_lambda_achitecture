import os
import json
from datetime import datetime
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class SparkBronzeMonitor:
    def __init__(self):
        self.log_path = os.getenv("MONITOR_SPARK_BRONZE_PATH")
        if not self.log_path:
            raise ValueError("❌ Lỗi: Thiếu MONITOR_SPARK_BRONZE_PATH trong file .env!")
        
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def log_batch_quality(self, epoch_id, total_records, valid_count, invalid_df, process_time_ms):
        invalid_count = invalid_df.count() 
        sample_errors = [] # Sửa thành số nhiều cho khớp bên dưới
        if invalid_count > 0:
            sample_errors = [row['raw_json'] for row in invalid_df.take(5)]  
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "batch_id": epoch_id,
            "metrics": {
                "total_received": total_records,
                "valid_records": valid_count,
                "invalid_records": invalid_count,
                "error_rate_percent": round((invalid_count / total_records) * 100, 2) if total_records > 0 else 0,
                "processing_time_ms": process_time_ms
            },
            "data_quality": {
                "is_acceptable": (invalid_count / total_records) < 0.2 if total_records > 0 else True, 
                "sample_error_data": sample_errors 
            }
        }
        
        with open(self.log_path, 'a', encoding='utf-8') as f:
            json.dump(log_entry, f, ensure_ascii=False)
            f.write('\n')
            f.flush() # Ép tống khứ dữ liệu xuống ổ cứng ngay lập tức
            os.fsync(f.fileno()) # Lệnh cưỡng chế Windows không được phép giữ trên RAM