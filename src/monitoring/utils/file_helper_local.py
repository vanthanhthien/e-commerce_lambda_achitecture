import os
import glob
import polars as pl
from dotenv import load_dotenv, find_dotenv
import concurrent.futures
load_dotenv(find_dotenv())
COMPACTED_FILE = os.getenv("COMPACTED_DLQ_FILENAME", "all_dead_letters.jsonl")

def load_data_file(folder_or_file, is_live_session=True, search_kw="", lines=100):
    if not folder_or_file:
        return pl.DataFrame([{"Lỗi": "Đường dẫn file/thư mục không tồn tại trong .env!"}])
        
    if not is_live_session:
        return pl.DataFrame([{"Thông báo": "Dữ liệu Raw/Log chỉ hiển thị ở Phiên Live."}])
    
    try:
        file_to_read = None
        if os.path.isfile(folder_or_file):
            file_to_read = folder_or_file
        elif os.path.isdir(folder_or_file):
            compact_path = os.path.join(folder_or_file, COMPACTED_FILE)
            if os.path.exists(compact_path):
                file_to_read = compact_path
            else:
                # Nếu là thư mục nhưng không có file compact, lấy file json mới nhất
                files = sorted(glob.glob(os.path.join(folder_or_file, "*.json*")), reverse=True)
                if files: file_to_read = files[0]

        if not file_to_read or not os.path.exists(file_to_read):
            return pl.DataFrame([{"Trạng thái": "Thư mục sạch sẽ / Không tìm thấy file!"}])

        # Lazy Evaluation: Quét file mà không load hết vào RAM
        lf = pl.scan_ndjson(file_to_read, ignore_errors=True)
        
        # Nếu có từ khóa, dùng sức mạnh của Rust để filter
        if search_kw:
            # Tạo biểu thức nối chuỗi tất cả các cột
            # Lưu ý: Lọc JSON phức tạp đôi khi yêu cầu collect trước, ta làm cách an toàn
            df = lf.tail(lines * 10).collect() # Lấy dư ra chút rồi lọc
            concat_expr = pl.concat_str([pl.col(c).cast(pl.Utf8) for c in df.columns], separator=" ")
            df = df.filter(concat_expr.str.contains(f"(?i){search_kw}")).tail(lines)
            return df if not df.is_empty() else pl.DataFrame([{"Trạng thái": "Không tìm thấy từ khóa!"}])
        
        # Lấy trực tiếp N dòng cuối bằng tail()
        df = lf.tail(lines).collect()
        return df if not df.is_empty() else pl.DataFrame([{"Trạng thái": "File trống rỗng!"}])
        
    except Exception as e:
        return pl.DataFrame([{"Lỗi": f"Không thể đọc file: {e}"}])

def clear_dlq_files(dlq_path):
    if not dlq_path: return False, "Thiếu đường dẫn DLQ_PATH trong .env"
    try:
        files = glob.glob(os.path.join(dlq_path, "*.json*"))
        for f in files: os.remove(f)
        return True, len(files)
    except Exception as e:
        return False, str(e)