import streamlit as st
import os
import sys
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
from dotenv import load_dotenv, find_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db_helper import load_metrics

load_dotenv(find_dotenv())
DB_PATH = os.getenv("MONITOR_DB_PATH")

st.set_page_config(page_title="Data Reconciliation Audit", page_icon="⚖️", layout="wide")
st.title("⚖️ TRẠM KIỂM TOÁN DỮ LIỆU (DATA RECONCILIATION)")
st.markdown("Giám sát tính toàn vẹn của dữ liệu: Phễu tổng, Độ tươi (SLA), Xu hướng và Sổ cái từng Micro-batch.")

if not DB_PATH or not os.path.exists(DB_PATH):
    st.warning("⏳ Đang chờ hệ thống thu thập đủ logs để đối soát...")
    st.stop()

# Lấy dữ liệu 1000 batch gần nhất bằng engine Polars
df_bronze = load_metrics(DB_PATH, "bronze_metrics", limit=1000)
df_silver = load_metrics(DB_PATH, "silver_metrics", limit=1000)

if df_bronze.is_empty() or df_silver.is_empty():
    st.info("Chưa đủ dữ liệu ở cả 2 tầng Bronze và Silver để hiển thị trạm kiểm toán.")
else:
    # ==========================================
    # 1. TÍNH TOÁN LOGIC ĐỐI SOÁT BẰNG POLARS
    # ==========================================
    bronze_in = df_bronze.select(pl.col('total_received').sum()).item() if 'total_received' in df_bronze.columns else 0
    dlq_out = df_bronze.select(pl.col('invalid_records').sum()).item() if 'invalid_records' in df_bronze.columns else 0
    expected_silver_in = bronze_in - dlq_out
    
    # Lấy số dòng đã xử lý ở Silver
    if 'processed_rows' in df_silver.columns:
        silver_processed = df_silver.select(pl.col('processed_rows').sum()).item()
    else:
        silver_processed = expected_silver_in  # Mock nếu chưa có cột đếm tổng

    discrepancy = expected_silver_in - silver_processed

    # ==========================================
    # VÙNG HIỂN THỊ 1: TỔNG QUAN PHỄU & SLA
    # ==========================================
    col_funnel, col_sla = st.columns([2, 1])
    
    with col_funnel:
        st.subheader("1️⃣ Hành trình Dữ liệu (All-time Funnel)")
        fig_funnel = go.Figure(go.Funnel(
            y=["1. Bơm Vào (Bronze In)", "2. Bị loại bỏ (DLQ)", "3. Hợp lệ (Expected Silver)", "4. Đã xử lý (Silver Out)"],
            x=[bronze_in, dlq_out, expected_silver_in, silver_processed],
            textinfo="value+percent initial",
            marker={"color": ["#3498db", "#e74c3c", "#f1c40f", "#2ecc71"]}
        ))
        fig_funnel.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig_funnel, use_container_width=True)
        
    with col_sla:
        st.subheader("2️⃣ Độ tươi Dữ liệu (SLA)")
        # Lấy record cuối cùng cực nhanh bằng to_dicts()
        latest_latency_ms = df_silver.tail(1).to_dicts()[0]['addBatch_ms'] if 'addBatch_ms' in df_silver.columns else 0
        latency_sec = latest_latency_ms / 1000

        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = latency_sec,
            title = {'text': "Thời gian xử lý 1 Batch (Giây)"},
            gauge = {
                'axis': {'range': [None, 60]},
                'bar': {'color': "#2c3e50"},
                'steps': [
                    {'range': [0, 10], 'color': "#2ecc71"},   # Khỏe
                    {'range': [10, 30], 'color': "#f1c40f"},  # Bình thường
                    {'range': [30, 60], 'color': "#e74c3c"}], # Nguy hiểm
                'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 30}
            }))
        fig_gauge.update_layout(height=250, margin=dict(t=40, b=20))
        st.plotly_chart(fig_gauge, use_container_width=True)
        
        st.metric("✅ Trạng Thái Khớp Sổ", "Hoàn hảo" if discrepancy == 0 else "Lệch", 
                  delta=f"Mất {discrepancy} dòng" if discrepancy != 0 else "0 thất thoát", 
                  delta_color="inverse" if discrepancy != 0 else "normal")

    st.markdown("---")

    # ==========================================
    # VÙNG HIỂN THỊ 2: TREND & SỔ CÁI
    # ==========================================
    col_trend, col_ledger = st.columns([3, 2])

    with col_trend:
        st.subheader("3️⃣ Radar Xu hướng (Reconciliation Trend)")
        st.markdown("Theo dõi sự chênh lệch giữa dòng dữ liệu đổ vào và dòng chảy ra theo thời gian thực.")
        
        if 'input_rows_per_second' in df_silver.columns and 'processed_rows_per_second' in df_silver.columns:
            # Đẩy qua Pandas chỉ để vẽ biểu đồ
            fig_trend = px.line(df_silver.to_pandas(), x='Thời Gian', y=['input_rows_per_second', 'processed_rows_per_second'],
                                labels={'value': 'Rows / Giây', 'variable': 'Luồng Dữ liệu'},
                                color_discrete_map={
                                    'input_rows_per_second': '#3498db',     # Xanh dương (Vào)
                                    'processed_rows_per_second': '#2ecc71'  # Xanh lá (Ra)
                                })
            fig_trend.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("Chưa có đủ metric tốc độ để vẽ biểu đồ.")

    with col_ledger:
        st.subheader("4️⃣ Sổ cái Micro-batch (Ledger)")
        st.markdown("Chi tiết 20 mẻ dữ liệu (batch) gần nhất chạy qua tầng Silver.")
        
        # Sắp xếp và lấy 20 dòng siêu tốc bằng Polars
        ledger_df = df_silver.sort("timestamp", descending=True).head(20)
        
        in_col = 'input_rows' if 'input_rows' in ledger_df.columns else 'input_rows_per_second'
        out_col = 'processed_rows' if 'processed_rows' in ledger_df.columns else 'processed_rows_per_second'
        
        if in_col in ledger_df.columns and out_col in ledger_df.columns:
            # Đánh giá trạng thái bằng logic when/then cực kỳ mạnh mẽ của Polars
            ledger_df = ledger_df.with_columns(
                pl.when((pl.col(in_col) == 0) & (pl.col(out_col) == 0)).then(pl.lit("💤 Idle"))
                .when(pl.col(in_col) <= pl.col(out_col)).then(pl.lit("✅ Khớp"))
                .otherwise(pl.lit("⚠️ Tắc nghẽn")).alias("Trạng Thái")
            )
            
            # Chọn các cột cần thiết và đổi tên
            display_df = ledger_df.select(['Thời Gian', in_col, out_col, 'Trạng Thái']).rename({
                in_col: 'Input',
                out_col: 'Processed'
            })
            st.dataframe(display_df.to_pandas(), use_container_width=True, height=300)
        else:
            st.info("Chưa có đủ metric để lập sổ cái.")