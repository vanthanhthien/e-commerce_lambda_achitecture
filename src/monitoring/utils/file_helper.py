import os
import glob
import polars as pl
import boto3
from urllib.parse import urlparse
from dotenv import load_dotenv, find_dotenv
import concurrent.futures

load_dotenv(find_dotenv())
COMPACTED_FILE = os.getenv("COMPACTED_DLQ_FILENAME", "all_dead_letters.jsonl")

# ==========================================
# HÀM PHỤ TRỢ: PHÂN TÍCH ĐƯỜNG DẪN S3
# ==========================================
def parse_s3_path(s3_path):
    """Tách 's3a://bucket-name/folder/file' thành 'bucket-name' và 'folder/file'"""
    if s3_path.startswith("s3a://"):
        s3_path = s3_path.replace("s3a://", "s3://")
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip('/')

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-southeast-1")
    )

# ==========================================
# HÀM ĐỌC DỮ LIỆU (HỖ TRỢ CẢ LOCAL LẪN S3)
# ==========================================
def load_data_file(folder_or_file, is_live_session=True, search_kw="", lines=100):
    if not folder_or_file:
        return pl.DataFrame([{"Lỗi": "Đường dẫn file/thư mục không tồn tại trong .env!"}])
        
    if not is_live_session:
        return pl.DataFrame([{"Thông báo": "Dữ liệu Raw/Log chỉ hiển thị ở Phiên Live."}])
    
    try:
        is_s3 = folder_or_file.startswith("s3://") or folder_or_file.startswith("s3a://")

        # ==========================================
        # ☁️ XỬ LÝ ĐỌC FILE TỪ AWS S3 (ĐỘNG CƠ MULTI-THREADING)
        # ==========================================
        if is_s3:
            bucket_name, prefix = parse_s3_path(folder_or_file)
            s3_client = get_s3_client()
            
            # Liệt kê các file trong thư mục S3
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if 'Contents' not in response:
                return pl.DataFrame([{"Trạng thái": "Thư mục S3 sạch sẽ / Không tìm thấy file rác!"}])
                
            # Lọc ra các file JSON
            json_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.json') or obj['Key'].endswith('.jsonl')]
            if not json_files:
                return pl.DataFrame([{"Trạng thái": "Thư mục S3 không chứa file JSON!"}])
                
            # Lấy TOÀN BỘ file rác (Max 500 file để an toàn RAM)
            sorted_files = sorted(json_files, key=lambda x: x['LastModified'], reverse=True)
            files_to_read = sorted_files[:500] 
            
            # TUYỆT CHIÊU: Tải 20 file cùng 1 lúc (Nhanh gấp 20 lần)
            def download_s3_obj(file_obj):
                try:
                    return s3_client.get_object(Bucket=bucket_name, Key=file_obj['Key'])['Body'].read() + b"\n"
                except:
                    return b""

            # Kích hoạt động cơ song song
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                all_bytes_list = list(executor.map(download_s3_obj, files_to_read))
                
            all_bytes = b"".join(all_bytes_list)
            
            if not all_bytes.strip():
                return pl.DataFrame([{"Trạng thái": "File trên S3 trống rỗng!"}])
                
            df = pl.read_ndjson(all_bytes)

        # ==========================================
        # 💻 XỬ LÝ ĐỌC FILE TỪ LOCAL (ỐNG HÚT ĐA FILE)
        # ==========================================
        else:
            all_dfs = []
            
            # Trường hợp 1: Truyền trực tiếp đường dẫn 1 file
            if os.path.isfile(folder_or_file):
                all_dfs.append(pl.read_ndjson(folder_or_file, ignore_errors=True))
            
            # Trường hợp 2: Truyền đường dẫn thư mục (DLQ của Spark)
            elif os.path.isdir(folder_or_file):
                # Ưu tiên đọc file đã gom (Compacted) nếu có
                compact_path = os.path.join(folder_or_file, COMPACTED_FILE)
                if os.path.exists(compact_path):
                    all_dfs.append(pl.read_ndjson(compact_path, ignore_errors=True))
                
                # QUÉT SẠCH TẤT CẢ FILE JSON CÒN LẠI (Vét máng lịch sử)
                other_files = glob.glob(os.path.join(folder_or_file, "part-*.json")) # File của Spark
                # Hoặc lấy tất cả .json/jsonl nếu sếp muốn chắc cú
                # other_files = glob.glob(os.path.join(folder_or_file, "*.json*"))
                
                # Lọc bỏ file compacted nếu nó vô tình nằm trong list này
                other_files = [f for f in other_files if COMPACTED_FILE not in f]
                
                if other_files:
                    # Đọc và gộp toàn bộ file rác lẻ tẻ
                    for f in other_files[:200]: # Giới hạn 200 file cho nhẹ Dashboard
                        try:
                            all_dfs.append(pl.read_ndjson(f, ignore_errors=True))
                        except:
                            continue

            if not all_dfs:
                return pl.DataFrame([{"Trạng thái": "Thư mục sạch sẽ / Không tìm thấy file!"}])
            
            # GỘP TẤT CẢ LẠI THÀNH 1 SIÊU DATAFRAME
            df = pl.concat(all_dfs, how="diagonal")

        # ==========================================
        # 🔍 XỬ LÝ TÌM KIẾM VÀ CẮT DÒNG
        # ==========================================
        if search_kw:
            # TUYỆT CHIÊU: Chỉ tìm kiếm trên các cột Chuỗi (Né mấy cột List/Struct ra để chống sập)
            valid_dtypes = (pl.Utf8, getattr(pl, 'String', pl.Utf8), pl.Categorical)
            string_columns = [c for c, dtype in df.schema.items() if dtype in valid_dtypes]
            
            if string_columns:
                concat_expr = pl.concat_str([pl.col(c).fill_null("") for c in string_columns], separator=" ")
                df = df.filter(concat_expr.str.contains(f"(?i){search_kw}"))
            
            if df.is_empty():
                return pl.DataFrame([{"Trạng thái": "Không tìm thấy từ khóa!"}])
        
        # Cắt lấy số dòng theo yêu cầu (tail)
        return df.tail(lines)
        
    except Exception as e:
        return pl.DataFrame([{"Lỗi": f"Không thể đọc file: {e}"}])
    
# ==========================================
# HÀM XÓA FILE DLQ (HỖ TRỢ CẢ LOCAL LẪN S3)
# ==========================================
def clear_dlq_files(dlq_path):
    if not dlq_path: return False, "Thiếu đường dẫn DLQ_PATH trong .env"
    
    try:
        is_s3 = dlq_path.startswith("s3://") or dlq_path.startswith("s3a://")
        
        # XÓA TRÊN ĐÁM MÂY S3
        if is_s3:
            bucket_name, prefix = parse_s3_path(dlq_path)
            s3_client = get_s3_client()
            
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if 'Contents' not in response:
                return True, 0 # Không có gì để xóa
                
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents'] if obj['Key'].endswith('.json') or obj['Key'].endswith('.jsonl')]
            
            if objects_to_delete:
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
            return True, len(objects_to_delete)
            
        # XÓA TRÊN Ổ CỨNG LOCAL
        else:
            files = glob.glob(os.path.join(dlq_path, "*.json*"))
            for f in files: os.remove(f)
            return True, len(files)
            
    except Exception as e:
        return False, str(e)