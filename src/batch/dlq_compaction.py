import os
import sys
import glob
from dotenv import load_dotenv, find_dotenv

# Đảm bảo Python nhận diện thư mục gốc của project
sys.path.append(os.getcwd())

# Load cấu hình từ file .env
load_dotenv(find_dotenv())

# ==========================================
# 1. ĐỌC CẤU HÌNH TỪ .ENV (XỬ LÝ ĐƯỜNG DẪN ĐỘNG)
# ==========================================
# Lấy base path (loại bỏ dấu ngoặc kép nếu có)
BASE_PATH = os.getenv("MONITOR_BASE_PATH", "data/monitoring").strip('"')

# Lấy đường dẫn DLQ và TỰ ĐỘNG DỊCH chuỗi ${MONITOR_BASE_PATH} thành đường dẫn thật
RAW_DLQ_PATH = os.getenv("MONITOR_DLQ_PATH", f"{BASE_PATH}/dlq").strip('"')
DLQ_PATH = RAW_DLQ_PATH.replace("${MONITOR_BASE_PATH}", BASE_PATH)

COMPACTED_FILENAME = os.getenv("COMPACTED_DLQ_FILENAME", "all_dead_letters.jsonl")

# Đường dẫn tuyệt đối đến siêu file tổng
COMPACTED_FILE = os.path.join(DLQ_PATH, COMPACTED_FILENAME)

def run_compaction():
    print("🧹 [COMPACTION JOB] Đang khởi động tiến trình gom rác tối ưu RAM...")
    
    # Đảm bảo thư mục tồn tại
    os.makedirs(DLQ_PATH, exist_ok=True)
    
    # 1. Quét tìm tất cả các file JSON lẻ tẻ
    all_files = glob.glob(os.path.join(DLQ_PATH, "*.json"))
    small_files = [f for f in all_files if not f.endswith(COMPACTED_FILENAME)]
    
    if not small_files:
        print("✅ [COMPACTION JOB] Thư mục sạch sẽ, không có file lẻ tẻ nào cần gom.")
    else:
        print(f"📦 Tìm thấy {len(small_files)} file nhỏ. Đang tiến hành trộn (Line-by-Line)...")
        
        # ==========================================
        # 2. THUẬT TOÁN ĐỌC LUỒNG (KHÔNG TỐN RAM)
        # ==========================================
        # Mở siêu file ở chế độ ghi nối (append)
        with open(COMPACTED_FILE, "a", encoding="utf-8") as outfile:
            for file_path in small_files:
                # Mở từng file nhỏ ở chế độ đọc
                with open(file_path, "r", encoding="utf-8") as infile:
                    # Đọc lướt từng dòng, đẩy sang file đích rồi ném khỏi RAM ngay lập tức
                    for line in infile:
                        if line.strip(): 
                            outfile.write(line)
                
                # Đóng file và thủ tiêu file nhỏ ngay lập tức
                os.remove(file_path) 
                
        print(f"🎉 Đã gom thành công {len(small_files)} file vào '{COMPACTED_FILENAME}'.")

    # ==========================================
    # 3. CÀN QUÉT RÁC ẨN HỆ THỐNG
    # ==========================================
    print("🧽 Đang lùng sục rác ẩn (CRC, _SUCCESS, TMP) do Spark để lại...")
    try:
        for f in os.listdir(DLQ_PATH):
            if f.startswith(".") or f.endswith(".crc") or f.endswith(".tmp") or f == "_SUCCESS":
                try: 
                    os.remove(os.path.join(DLQ_PATH, f))
                except Exception: 
                    pass # Bỏ qua nếu Spark đang tạm thời khóa file
    except Exception as e: 
        print(f"⚠️ Không thể quét dọn rác ẩn: {e}")

    print("✨ Hoàn tất dọn dẹp toàn diện! Bộ nhớ RAM tiêu thụ: ~0 MB!")

if __name__ == "__main__":
    run_compaction()