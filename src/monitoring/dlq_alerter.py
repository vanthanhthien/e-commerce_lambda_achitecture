import os
import sys
import time
import polars as pl
from dotenv import load_dotenv, find_dotenv

# Đảm bảo Python nhận diện đường dẫn gốc
sys.path.append(os.getcwd())

# Load cấu hình để tìm đường đến bãi rác (DLQ)
load_dotenv(find_dotenv())
DLQ_PATH = os.getenv("MONITOR_DLQ_PATH")

def alert_dlq_status():
    """Hàm quét bãi rác DLQ và phát Cảnh báo (Alert)"""
    if not DLQ_PATH or not os.path.exists(DLQ_PATH):
        return

    try:
        # Dùng Polars quét TOÀN BỘ file JSON trong thư mục DLQ siêu tốc
        dlq_files = os.path.join(DLQ_PATH, "*.json")
        
        # Bọc try-except để nếu không có file JSON nào, nó không bị văng lỗi
        try:
            df_loi = pl.read_ndjson(dlq_files)
        except Exception:
            return # Thoát êm ái nếu không đọc được file
        
        tong_so_loi = df_loi.height

        if tong_so_loi == 0:
            return

        # --- BẮT ĐẦU PHÂN TÍCH LỖI ---
        print("\n" + "="*50)
        print("🚨🚨🚨 CẢNH BÁO CHẤT LƯỢNG DỮ LIỆU (DLQ ALERT) 🚨🚨🚨")
        print("="*50)
        print(f"📉 TỔNG SỐ LƯỢNG RÁC HIỆN TẠI: {tong_so_loi} records bị từ chối!")
        
        # --- SỬA LỖI MÙ MÀU: DÒ TÌM CỘT TRƯỚC KHI ĐẾM ---
        danh_sach_cot = df_loi.columns
        print("\n🔍 PHÂN LOẠI NGUYÊN NHÂN LỖI:")
        
        if "reviewerID" in danh_sach_cot:
            thieu_reviewer_id = df_loi.filter(pl.col("reviewerID").is_null()).height
            print(f"  - Lỗi thiếu reviewerID : {thieu_reviewer_id} vụ.")
            
        if "asin" in danh_sach_cot:
            thieu_asin = df_loi.filter(pl.col("asin").is_null()).height
            print(f"  - Lỗi thiếu asin (Sản phẩm) : {thieu_asin} vụ.")
        else:
            print("  - Data hiện tại không chứa cột 'asin' để kiểm tra.")
            
        if "overall" in danh_sach_cot:
            thieu_overall = df_loi.filter(pl.col("overall").is_null()).height
            print(f"  - Lỗi thiếu overall (Điểm) : {thieu_overall} vụ.")

        print("\n⚠️ YÊU CẦU HÀNH ĐỘNG:")
        print("  Gửi tin nhắn Slack đến: @Data_Engineering_Team, @Backend_API_Team")
        print("="*50 + "\n")

    except Exception as e:
        print(f"⚠️ [DLQ ALERTER] Lỗi ngầm khi quét thư mục DLQ: {e}")

if __name__ == "__main__":
    print("🤖 [DLQ ALERTER] Khởi động hệ thống Radar quét rác Real-time...")
    
    # VÒNG LẶP THỜI GIAN: Quét rác mỗi 10 giây
    while True:
        alert_dlq_status()
        time.sleep(10)