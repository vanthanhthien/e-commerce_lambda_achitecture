import streamlit as st
import os
import time
import shutil
import polars as pl
from dotenv import load_dotenv, find_dotenv

from utils.db_helper import load_metrics, get_total_records
from utils.file_helper import load_data_file, clear_dlq_files
from utils.ui_components import render_sidebar

# ==========================================
# 1. CẤU HÌNH TỪ .ENV (KHÔNG HARDCODE)
# ==========================================
load_dotenv(find_dotenv())
DB_PATH = os.getenv("MONITOR_DB_PATH")
DLQ_PATH = os.getenv("MONITOR_DLQ_PATH")
BRONZE_LOG = os.getenv("MONITOR_SPARK_BRONZE_PATH")
SILVER_LOG = os.getenv("MONITOR_SILVER_STREAM_PATH")

st.set_page_config(page_title="Data Lakehouse Monitor", page_icon="⚡", layout="wide")

if not DB_PATH or not os.path.exists(DB_PATH):
    st.error(f"⏳ Đang chờ Database tại: {DB_PATH}... Vui lòng bật luồng dữ liệu Spark!")
    st.stop()

st.title("⚡ Hệ Thống Điều Hành Real-time Lakehouse")
st.markdown("Giám sát dòng chảy dữ liệu trực tiếp theo **Giờ Việt Nam (UTC+7)**.")

# ==========================================
# 2. GỌI SIDEBAR VÀ NHẬN BIẾN
# ==========================================
ACTIVE_DB_PATH, is_live, auto_refresh, refresh_rate, LOG_DIR = render_sidebar()

# ==========================================
# 3. NÚT BẤM HÀNH ĐỘNG
# ==========================================
st.sidebar.header("🛠️ Hành Động")
if st.sidebar.button("🗑️ Dọn dẹp Bãi rác (DLQ)", use_container_width=True):
    success, result = clear_dlq_files(DLQ_PATH)
    if success:
        st.sidebar.success(f"✅ Đã dọn sạch {result} file rác!")
        time.sleep(1); st.rerun()
    else:
        st.sidebar.error(f"Lỗi: {result}")

if st.sidebar.button("💾 Lưu Phiên & Khởi động lại", use_container_width=True):
    import glob
    
    # 1. TẠO VÙNG AN TOÀN NGOÀI TẦM ĐẠN CỦA FILE .BAT
    ARCHIVE_DIR = "archive_sessions" # Lưu hẳn ra thư mục gốc dự án
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    # Đếm số lượng phiên đã lưu trong vùng an toàn
    session_files = glob.glob(os.path.join(ARCHIVE_DIR, "monitoring_session_*.db"))
    new_session_id = len(session_files) + 1
    
    # 2. COPY FILE SANG VÙNG AN TOÀN
    safe_path = os.path.join(ARCHIVE_DIR, f"monitoring_session_{new_session_id}.db")
    shutil.copy2(DB_PATH, safe_path)
    
    # 3. RESET DATA HIỆN TẠI NHƯ CŨ
    import sqlite3
    conn = sqlite3.connect(DB_PATH); cursor = conn.cursor()
    cursor.execute("DELETE FROM bronze_metrics"); cursor.execute("DELETE FROM silver_metrics")
    conn.commit(); conn.close()
    
    if BRONZE_LOG and os.path.exists(BRONZE_LOG): open(BRONZE_LOG, 'w').close()
    if SILVER_LOG and os.path.exists(SILVER_LOG): open(SILVER_LOG, 'w').close()
    
    st.sidebar.success(f"✅ Đã cất an toàn vào: {safe_path}")
    time.sleep(1.5); st.rerun()

# ==========================================
# 4. HIỂN THỊ DỮ LIỆU
# ==========================================
total_records_all_time = get_total_records(ACTIVE_DB_PATH)
raw_bronze = load_metrics(ACTIVE_DB_PATH, "bronze_metrics")
raw_silver = load_metrics(ACTIVE_DB_PATH, "silver_metrics")

# Thay thế .empty bằng .is_empty()
if raw_bronze.is_empty() and raw_silver.is_empty():
    st.info(f"Trạng thái: Trống rỗng. Đang xem file: {ACTIVE_DB_PATH}")
else:
    st.subheader("🎯 Chỉ Số Điều Hành")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🌟 Tổng Records", f"{total_records_all_time:,.0f}")
    
    if not raw_bronze.is_empty():
        # Lấy dòng cuối bằng Polars: .tail(1).to_dicts()[0]
        latest_bronze = raw_bronze.tail(1).to_dicts()[0]
        col2.metric("Tỷ lệ Rác (DLQ)", f"{latest_bronze['error_rate_percent']}%", 
                    delta=f"{latest_bronze['invalid_records']} lỗi", delta_color="inverse")
                    
    if not raw_silver.is_empty():
        latest_silver = raw_silver.tail(1).to_dicts()[0]
        col3.metric("Tốc độ chạy (Rows/sec)", f"{latest_silver['input_rows_per_second']:,.1f}")
        col4.metric("Độ trễ ghi (ms)", f"{latest_silver['addBatch_ms']:,.0f}")

    st.markdown("---")
    col_chart1, col_chart2 = st.columns(2)
    with col_chart1:
        st.subheader("📉 Bronze: Tỷ lệ Lỗi")
        if not raw_bronze.is_empty(): 
            # Đẩy về Pandas để Streamlit Native Chart vẽ mượt nhất
            st.area_chart(raw_bronze.to_pandas().set_index("Thời Gian")["error_rate_percent"], color="#FF4B4B")
            
    with col_chart2:
        st.subheader("⚡ Silver: Tốc độ xử lý")
        if not raw_silver.is_empty(): 
            st.line_chart(raw_silver.to_pandas().set_index("Thời Gian")[["input_rows_per_second", "processed_rows_per_second"]])

    st.markdown("---")
    st.subheader("🔍 Khám Phá Dữ Liệu")
    search_query = st.text_input("🔎 Nhập từ khóa:", "")
    
    # Chuyền đúng biến tĩnh từ .env vào
    df_bronze_search = load_metrics(ACTIVE_DB_PATH, "bronze_metrics", search_kw=search_query)
    df_silver_search = load_metrics(ACTIVE_DB_PATH, "silver_metrics", search_kw=search_query)
    df_dlq_search = load_data_file(DLQ_PATH, is_live, search_kw=search_query)
    df_log_search = load_data_file(BRONZE_LOG, is_live, search_kw=search_query)

    tab1, tab2, tab3, tab4 = st.tabs(["🥉 Lịch sử Bronze", "🥈 Lịch sử Silver", "🗑️ DLQ", "📝 Raw Logs"])
    
    with tab1: 
        if not df_bronze_search.is_empty():
            # Xóa cột timestamp nếu có và đảo ngược thứ tự bằng Polars (.reverse())
            cols_to_drop = ['timestamp'] if 'timestamp' in df_bronze_search.columns else []
            display_bronze = df_bronze_search.drop(cols_to_drop).reverse()
            st.dataframe(display_bronze.to_pandas(), use_container_width=True)
            
    with tab2: 
        if not df_silver_search.is_empty():
            cols_to_drop = ['timestamp'] if 'timestamp' in df_silver_search.columns else []
            display_silver = df_silver_search.drop(cols_to_drop).reverse()
            st.dataframe(display_silver.to_pandas(), use_container_width=True)
            
    with tab3: 
        st.dataframe(df_dlq_search.to_pandas(), use_container_width=True)
        
    with tab4: 
        st.dataframe(df_log_search.to_pandas(), use_container_width=True)

if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()