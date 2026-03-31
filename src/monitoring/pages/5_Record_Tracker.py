import streamlit as st
import os
import sys
import polars as pl
import json
from dotenv import load_dotenv, find_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.file_helper import load_data_file

load_dotenv(find_dotenv())
DLQ_PATH = os.getenv("MONITOR_DLQ_PATH")

DATA_DIR = os.getenv("OUTPUT_FOLDER_NAME", "data/generated_realtime")
DATA_FILE = os.getenv("OUTPUT_FILE_NAME", "realtime_reviews.jsonl")
RAW_DATA_PATH = os.path.join(DATA_DIR, DATA_FILE)

st.set_page_config(page_title="End-to-End Record Tracker", page_icon="🔬", layout="wide")
st.title("🔬 KÍNH HIỂN VI THEO DẤU DỮ LIỆU (DATA LINEAGE)")
st.markdown("Truy vết chi tiết vòng đời của một bản ghi (Review) xuyên suốt kiến trúc Lambda.")

# ==========================================
# 1. BẢNG DỮ LIỆU GỢI Ý 
# ==========================================
with st.expander("📋 Mở danh sách Review mới nhất để lấy ID (Click vào đây)"):
    st.info("💡 Mẹo: Copy một mã `reviewerID` hoặc `asin` ở bảng dưới, sau đó dán vào ô tìm kiếm.")
    if RAW_DATA_PATH and os.path.exists(RAW_DATA_PATH):
        df_recent = load_data_file(RAW_DATA_PATH, is_live_session=True, lines=50)
        # Thay thế .empty bằng .is_empty()
        if not df_recent.is_empty() and "Trạng thái" not in df_recent.columns:
            display_cols = [c for c in ['reviewerID', 'asin', 'overall', 'summary'] if c in df_recent.columns]
            if display_cols: 
                # Lọc cột bằng select và ép về Pandas để hiển thị bảng đẹp
                st.dataframe(df_recent.select(display_cols).head(10).to_pandas(), use_container_width=True)
            else:
                st.warning("⚠️ Không tìm thấy các cột ID chuẩn.")
        else:
            st.write("Chưa có dữ liệu mới.")
    else:
        st.warning(f"Chưa tìm thấy file dữ liệu tại: {RAW_DATA_PATH}")

st.markdown("---")

# ==========================================
# 2. KHUNG TÌM KIẾM VÀ QUÉT LINEAGE
# ==========================================
search_id = st.text_input("🔑 Nhập ID cần theo dõi (VD: A2XYZ123, B000123...):", placeholder="Gõ ID và nhấn Enter...")

if search_id:
    st.markdown(f"### 🎯 Kết quả truy vết cho ID: `{search_id}`")
    
    # --- SCAN DỮ LIỆU BẰNG POLARS ---
    df_raw = load_data_file(RAW_DATA_PATH, is_live_session=True, search_kw=search_id, lines=50)
    found_in_raw = not (df_raw.is_empty() or "Trạng thái" in df_raw.columns)
    
    df_dlq = load_data_file(DLQ_PATH, is_live_session=True, search_kw=search_id, lines=50)
    found_in_dlq = not (df_dlq.is_empty() or "Trạng thái" in df_dlq.columns)

    # --- VẼ BẢN ĐỒ LINEAGE (VISUAL PIPELINE) ---
    st.subheader("🗺️ Bản đồ Hành trình (Pipeline Flow)")
    
    c1, c2, c3, c4 = st.columns(4)
    
    # Khối 1: Ingestion
    with c1:
        if found_in_raw:
            st.success("🏭 **1. Kafka Ingestion**\n\n✅ Đã tiếp nhận")
        else:
            st.error("🏭 **1. Kafka Ingestion**\n\n❌ Không tồn tại")

    # Khối 2: Bronze / DLQ
    with c2:
        if not found_in_raw:
            st.info("🥉 **2. Bronze Filter**\n\n⚪ Chờ dữ liệu")
        elif found_in_dlq:
            st.error("🥉 **2. Bronze Filter**\n\n🚨 Văng vào DLQ")
        else:
            st.success("🥉 **2. Bronze Filter**\n\n✅ Đã làm sạch")

    # Khối 3: Silver
    with c3:
        if not found_in_raw:
            st.info("🥈 **3. Silver Processing**\n\n⚪ Chờ dữ liệu")
        elif found_in_dlq:
            st.error("🥈 **3. Silver Processing**\n\n❌ Bị chặn")
        else:
            st.success("🥈 **3. Silver Processing**\n\n✅ Lưu PostgreSQL")

    # Khối 4: Gold (dbt)
    with c4:
        if found_in_raw and not found_in_dlq:
            st.warning("🥇 **4. Gold (dbt)**\n\n⏳ Đợi chạy Batch")
        else:
            st.info("🥇 **4. Gold (dbt)**\n\n⚪ Chưa sẵn sàng")

    st.markdown("---")

    # --- NHẬT KÝ VÀ MỔ XẺ PAYLOAD ---
    col_log, col_payload = st.columns([1, 1])
    
    with col_log:
        st.subheader("⏱️ Nhật ký Vận đơn (Chronology)")
        if not found_in_raw:
            st.error("➖ Dữ liệu chưa từng đi vào hệ thống. Hãy kiểm tra lại file sinh dữ liệu.")
        else:
            st.markdown("- 🟢 **Hệ thống Nguồn:** Record được sinh ra và bơm vào Kafka topic.")
            st.markdown("- 🟢 **Tiếp nhận:** Cụm Spark (Bronze) xác nhận đã đọc thành công.")
            if found_in_dlq:
                st.markdown("- 🔴 **Kiểm định:** Vi phạm Data Quality (Thiếu trường hoặc sai kiểu).")
                st.markdown("- 🔴 **Cách ly:** Đã bị gắp ra và tống vào Bãi rác DLQ.")
            else:
                st.markdown("- 🟢 **Kiểm định:** Dữ liệu hoàn toàn sạch sẽ, Pass vòng DLQ.")
                st.markdown("- 🟢 **Xử lý:** Spark (Silver) đã ép kiểu và đẩy lên PostgreSQL Warehouse.")
                st.markdown("- 🟡 **Chờ đợi:** Nằm trong Database chờ Airflow gọi dbt dựng thành báo cáo.")

    with col_payload:
        st.subheader("🔬 X-Quang Payload (JSON Inspector)")
        if found_in_dlq:
            st.error("⚠️ Hiển thị Dữ liệu bị lỗi (Từ bãi rác DLQ)")
            # Ép dòng đầu tiên về dictionary bằng Polars
            error_record = df_dlq.head(1).to_dicts()[0]
            st.json(error_record)
        elif found_in_raw:
            st.success("✅ Hiển thị Dữ liệu gốc (Từ Kafka)")
            raw_record = df_raw.head(1).to_dicts()[0]
            st.json(raw_record)
        else:
            st.info("Không có payload để hiển thị.")