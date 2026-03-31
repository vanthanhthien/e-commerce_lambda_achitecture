import os
import sys
import glob
from dotenv import load_dotenv, find_dotenv

sys.path.append(os.getcwd())
load_dotenv(find_dotenv())

# Lấy đường dẫn Local từ .env
RAW_DLQ_PATH = os.getenv("MONITOR_DLQ_PATH")
COMPACTED_FILENAME = os.getenv("COMPACTED_DLQ_FILENAME", "all_dead_letters.jsonl")

def run_local_compaction():
    print("🧹 [LOCAL DLQ COMPACTION] Đang khởi động tiến trình gom rác...")
    
    if not RAW_DLQ_PATH or str(RAW_DLQ_PATH).startswith("s3"):
        print("⚠️ Cấu hình DLQ đang trỏ lên S3. Vui lòng check lại .env!")
        return

    # Đảm bảo thư mục tồn tại
    os.makedirs(RAW_DLQ_PATH, exist_ok=True)
    compacted_file_path = os.path.join(RAW_DLQ_PATH, COMPACTED_FILENAME)

    try:
        # 1. Tìm tất cả các file JSONL nhỏ (ngoại trừ siêu file tổng)
        search_pattern = os.path.join(RAW_DLQ_PATH, "*.jsonl")
        all_files = glob.glob(search_pattern)
        
        small_files = [f for f in all_files if not f.endswith(COMPACTED_FILENAME)]

        if not small_files:
            print("✅ Không có file rác lẻ tẻ nào cần gom.")
            return

        print(f"📦 Tìm thấy {len(small_files)} file rác nhỏ. Đang trộn...")
        
        # 2. Đọc từng file nhỏ và ghi nối tiếp vào siêu file tổng
        with open(compacted_file_path, 'a', encoding='utf-8') as outfile:
            for file_path in small_files:
                with open(file_path, 'r', encoding='utf-8') as infile:
                    for line in infile:
                        if line.strip(): 
                            outfile.write(line)
                            
                # 3. Trộn xong thì xóa luôn file nhỏ
                os.remove(file_path)

        print("🎉 HOÀN TẤT GOM RÁC LOCAL! Ổ cứng đã được quy hoạch gọn gàng.")

    except Exception as e:
        print(f"❌ Lỗi gom rác Local: {e}")

if __name__ == "__main__":
    run_local_compaction()