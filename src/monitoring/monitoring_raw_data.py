import json
import time
import sys
import os
from datetime import datetime
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

class MonitoringRawData:
    def __init__(self):
    # lấy từ  .env, nếu ko có thì báo lỗi
        self.log_path = os.getenv("MONITOR_RAW_DATA_PATH")
        if not self.log_path:
            raise ValueError("❌ Lỗi: Thiếu MONITOR_RAW_DATA_PATH trong file .env!")
        
        # tự động tạo thư mục nếu chưa tồn tại
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def log_ingestion(self,record, start_time):   
        end_time = time.time()
        process_time_ms = round((end_time - start_time) * 1000, 2)  # thời gian xử lý tính bằng ms
        
        json_str = json.dumps(record, ensure_ascii=False)  # chuyển record thành chuỗi JSON
        size_entry = sys.getsizeof(json_str)  # tính kích thước của chuỗi JSON
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "metadata": {
                "source": "raw_data_ingestion",
                "status": "ingested",
                "process_time_ms": process_time_ms,
                "size_bytes": size_entry
            },
            "data_content" : record  # thêm nội dung dữ liệu vào log entry
        }
        
        with open(self.log_path, 'a', encoding='utf-8') as f:
            json.dump(log_entry, f, ensure_ascii=False)
            f.write('\n')
        print(f"✅ Đã ghi 1 dòng log vào đường dẫn tuyệt đối: {os.path.abspath(self.log_path)}")
    