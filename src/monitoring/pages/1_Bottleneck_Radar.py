import streamlit as st
import os
import time
from dotenv import load_dotenv, find_dotenv
import plotly.graph_objects as go
from datetime import datetime, timezone
import polars as pl
import sys

# Import hàm từ utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db_helper import load_latest_metrics

# Bắt buộc gọi từ .env
load_dotenv(find_dotenv())
DB_PATH = os.getenv("MONITOR_DB_PATH")

st.set_page_config(page_title="Lakehouse Bottleneck Radar", page_icon="🚨", layout="wide")
st.title("🚨 HỆ THỐNG CẢNH BÁO NÚT THẮT CỔ CHAI TOÀN DIỆN")

if not DB_PATH or not os.path.exists(DB_PATH):
    st.warning(f"⏳ Radar đang dò tìm DB tại {DB_PATH}... Vui lòng bật hệ thống Kafka và Spark!")
    st.stop()

raw_bronze = load_latest_metrics(DB_PATH, "bronze_metrics")
raw_silver = load_latest_metrics(DB_PATH, "silver_metrics")

# ==========================================
# KHÔI PHỤC CÁC BIẾN BỊ THẤT LẠC BẰNG POLARS
# ==========================================
# Lấy dòng cuối cùng (mới nhất) dạng dictionary
latest_bronze = raw_bronze.tail(1).to_dicts()[0] if not raw_bronze.is_empty() else None
latest_silver = raw_silver.tail(1).to_dicts()[0] if not raw_silver.is_empty() else None

auto_refresh = st.sidebar.checkbox("Bật Radar Quét Liên Tục (Auto-refresh)", value=True)
refresh_rate = st.sidebar.slider("Tốc độ quét (giây)", 2, 30, 5)

if raw_bronze.is_empty() and raw_silver.is_empty():
    st.warning("⏳ Radar đang dò tìm tín hiệu... Vui lòng bật luồng Streaming!")
    st.stop()

# ==========================================
# TRẠM KIỂM SOÁT 4 TẦNG (PIPELINE STAGES)
# ==========================================

# --- TẦNG 1: INGESTION (Nguồn phát Kafka) ---
st.subheader("📡 TẦNG 1: INGESTION (Data Generator & Kafka)")
if latest_bronze is not None:
    # Xử lý datetime bằng Python thuần (thay thế pd.to_datetime)
    dt_str = str(latest_bronze['timestamp']).split('.')[0]
    latest_time = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    time_diff = (datetime.now(timezone.utc) - latest_time).total_seconds()
    
    if time_diff > 30:
        st.error(f"🚨 BÁO ĐỘNG ĐỎ: Đã {time_diff:.0f} giây không có dữ liệu mới chạy vào Kafka! Hãy kiểm tra script sinh dữ liệu có bị tắt không?")
    else:
        st.success(f"✅ Tín hiệu ổn định. Nhịp tim hệ thống: {time_diff:.1f} giây trước.")
else:
    st.info("Chưa có dữ liệu Ingestion.")

st.markdown("---")

# --- TẦNG 2: BRONZE (Trạm Lọc Rác) ---
st.subheader("🥉 TẦNG 2: BRONZE LAYER (Data Quality)")
col_b1, col_b2 = st.columns([1, 2])
with col_b1:
    if latest_bronze is not None:
        err_rate = latest_bronze['error_rate_percent']
        if err_rate > 30:
            st.error(f"⚠️ NÚT THẮT CHẤT LƯỢNG: Tỷ lệ rác vọt lên {err_rate}%! Bãi rác DLQ đang quá tải.")
        elif err_rate > 10:
            st.warning(f"🟡 Cảnh báo: Tỷ lệ rác đang ở mức {err_rate}%.")
        else:
            st.success(f"✅ Dữ liệu thô sạch sẽ (Lỗi: {err_rate}%).")
with col_b2:
    if not raw_bronze.is_empty():
        st.caption("📉 Biểu đồ Tỷ lệ Lỗi (DLQ Rate) - Đã được trả lại cho Cơ trưởng!")
        # Convert sang Pandas chỉ để tương thích hoàn hảo với st.area_chart
        st.area_chart(raw_bronze.to_pandas().set_index("Thời Gian")["error_rate_percent"], color="#FF4B4B")

st.markdown("---")

# --- TẦNG 3: SILVER (Động cơ Spark) ---
st.subheader("🥈 TẦNG 3: SILVER LAYER (Processing & Throughput)")
col_s1, col_s2 = st.columns(2)

with col_s1:
    if latest_silver is not None:
        batch_ms = latest_silver['addBatch_ms']
        if batch_ms > 75000:
            st.error(f"🔥 NÚT THẮT TÀI NGUYÊN (RAM/CPU): Spark đang kiệt sức! (Độ trễ: {batch_ms}ms).")
        
        else:
            st.success(f"✅ Bộ nhớ ổn định. Tốc độ ghi Delta Lake: {batch_ms}ms.")
        
        fig = go.Figure(go.Indicator(
            mode = "gauge+number", value = batch_ms, title = {'text': "RAM Stress (addBatch_ms)"},
            gauge = {
                'axis': {'range': [None, 5000]},
                'bar': {'color': "black"},
                'steps': [
                    {'range': [0, 1500], 'color': "lightgreen"},
                    {'range': [1500, 3000], 'color': "yellow"},
                    {'range': [3000, 5000], 'color': "red"}],
                'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 4000}
            }))
        fig.update_layout(height=200, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig, use_container_width=True)

with col_s2:
    if latest_silver is not None:
        in_rps = latest_silver['input_rows_per_second']
        out_rps = latest_silver['processed_rows_per_second']
        if in_rps > (out_rps * 1.5) and in_rps > 10:
            st.error(f"🌪️ NÚT THẮT ĐƯỜNG ỐNG: Dữ liệu vào ({in_rps:.1f} r/s) quá nhanh so với tốc độ xử lý ({out_rps:.1f} r/s)!")
        else:
            st.success(f"✅ Tốc độ xử lý ({out_rps:.1f} r/s) theo kịp lượng nước bơm vào.")
            
        st.caption("⚡ Biểu đồ Tốc độ (Đỏ: Nước cấp vào | Xanh: Nước xả ra)")
        st.line_chart(raw_silver.to_pandas().set_index("Thời Gian")[["input_rows_per_second", "processed_rows_per_second"]], color=["#FF4B4B", "#00FF00"])

st.markdown("---")

# --- TẦNG 4: GOLD (Batch DWH & Power BI) ---
st.subheader("🥇 TẦNG 4: GOLD LAYER (Data Warehouse & dbt)")
st.info("💡 Tầng Gold chạy theo cơ chế BATCH (Airflow định kỳ). Để phát hiện nút thắt cổ chai ở tầng này, chúng ta sẽ theo dõi thời gian thực thi (Execution Time) của DAG trên giao diện Airflow. Nếu tác vụ dbt chạy lâu hơn 5 phút -> Cần tối ưu SQL (Thêm Index hoặc Incremental Load).")

# 4. TỰ ĐỘNG LÀM MỚI
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()