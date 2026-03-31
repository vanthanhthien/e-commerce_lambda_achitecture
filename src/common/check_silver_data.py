import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv, find_dotenv

print("⏳ Đang khởi tạo Spark để kiểm tra dữ liệu...")

# Tự động tìm và load file .env
load_dotenv(find_dotenv())

# Lấy đường dẫn từ biến môi trường (Đã sửa lỗi thêm dấu ngoặc kép ngầm)
SILVER_PATH = os.getenv("SILVER_LAYER_PATH")
MONITOR_PATH = os.getenv("MONITOR_SAMPLING_PATH")

if not SILVER_PATH or not MONITOR_PATH:
    raise ValueError("❌ Lỗi Cấu Hình: Thiếu SILVER_LAYER_PATH hoặc MONITOR_SAMPLING_PATH trong file .env!")

# TỰ ĐỘNG TẠO THƯ MỤC MONITORING NẾU CHƯA CÓ
os.makedirs(MONITOR_PATH, exist_ok=True)

# Khởi tạo Spark với Delta
builder = SparkSession.builder \
    .appName("CheckSilverData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    print(f"📥 Đang đọc dữ liệu từ: {SILVER_PATH}...")
    df = spark.read.format("delta").load(SILVER_PATH)

    print("\n" + "="*50)
    print("✅ 1. KIỂM TRA ĐỊNH DẠNG (SCHEMA)")
    print("="*50)
    df.printSchema()

    print("\n" + "="*50)
    print("✅ 2. KIỂM TRA TỔNG SỐ DÒNG (ĐÃ GOM NHÓM)")
    print("="*50)
    print(f"Tổng số dòng hiện có: {df.count()} dòng")

    print("\n" + "="*50)
    print("✅ 3. XEM THỬ 5 DÒNG DỮ LIỆU ĐẦU TIÊN")
    print("="*50)
    
    # Ép Spark chỉ lấy 5 dòng và chuyển sang Pandas DataFrame để hiển thị đẹp hơn
    pandas_df = df.limit(5).toPandas()
    
    # In ra Terminal với format gọn gàng hơn
    print(pandas_df.to_string(index=False))
    
    # --- XUẤT FILE CSV VÀO THƯ MỤC MONITORING ---
    sample_csv_path = os.path.join(MONITOR_PATH, "sample_silver_data.csv")
    pandas_df.to_csv(sample_csv_path, index=False, encoding='utf-8')
    
    print("\n" + "🌟"*25)
    print(f"💡 Đã xuất 5 dòng mẫu ra file: {sample_csv_path}")
    print("👉 Hãy mở cây thư mục VS Code, tìm file sample_silver_data.csv và click vào để xem siêu đẹp!")
    print("🌟"*25 + "\n")
    
except Exception as e:
    print(f"❌ Không thể đọc dữ liệu. Lỗi: {e}")