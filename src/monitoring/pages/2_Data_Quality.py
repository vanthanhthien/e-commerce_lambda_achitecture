import streamlit as st
import os
import sys
import polars as pl
import plotly.express as px
from dotenv import load_dotenv, find_dotenv

# Gọi các hàm từ thư mục utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.file_helper import load_data_file
from utils.db_helper import load_latest_metrics

# Load cấu hình
load_dotenv(find_dotenv())
DLQ_PATH = os.getenv("MONITOR_DLQ_PATH")
DB_PATH = os.getenv("MONITOR_DB_PATH")

st.set_page_config(page_title="Data Quality Inspector", page_icon="🕵️‍♂️", layout="wide")
st.title("🕵️‍♂️ PHÒNG KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU (DLQ)")
st.markdown("Phân tích chuyên sâu các bản ghi bị từ chối (Dead Letter Queue) tại tầng Bronze.")

if not DLQ_PATH:
    st.error("🚨 Chưa cấu hình MONITOR_DLQ_PATH trong file .env!")
    st.stop()

# 1. Đọc dữ liệu lỗi (Tăng lên 10000 dòng để vét sạch lịch sử trên S3)
st.info(f"Đang phân tích bãi rác tại: `{DLQ_PATH}`")
df_dlq = load_data_file(DLQ_PATH, is_live_session=True, lines=10000)

# Kiểm tra xem có rác không (Thay df.empty bằng df.is_empty())
if "Trạng thái" in df_dlq.columns or df_dlq.is_empty():
    st.success("🎉 TUYỆT VỜI! Bãi rác trống rỗng. Dữ liệu đang sạch 100% không có lỗi nào phát sinh.")
    st.balloons()
else:
    st.warning(f"⚠️ Phát hiện tổng cộng {df_dlq.height} bản ghi lỗi. Đang tiến hành giải phẫu...")
    
    st.subheader("📊 Thống kê Cột bị thiếu/Lỗi (Tổng quan)")
    
    # Đếm số lượng giá trị null siêu tốc bằng Polars
    null_counts = df_dlq.null_count().melt()
    null_counts.columns = ['Tên Cột', 'Số bản ghi bị Null']
    
    # Lọc ra các cột có lỗi (> 0)
    null_counts = null_counts.filter(pl.col('Số bản ghi bị Null') > 0)
    
    if not null_counts.is_empty():
        # Chuyển nhẹ sang Pandas để Plotly Express vẽ cho chuẩn
        fig = px.bar(null_counts.to_pandas(), x='Tên Cột', y='Số bản ghi bị Null', color='Tên Cột',
                     title="Các trường dữ liệu hay bị thiếu nhất trong toàn bộ lịch sử")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Không phát hiện cột Null, có thể lỗi do sai định dạng kiểu dữ liệu (Data Type Mismatch).")

    st.markdown("---")
    st.subheader("📋 BẢNG THEO DÕI RÁC DLQ CHI TIẾT")
    
    # Tạo 2 Tab theo đúng ý sếp
    tab_live, tab_history = st.tabs(["⚡ Lỗi Phiên Hiện Tại (Mới nhất)", "🗄️ Toàn Bộ Lịch Sử DLQ (All-time)"])
    
    with tab_live:
        st.markdown("Hiển thị **20 bản ghi lỗi mới nhất** vừa bị văng vào bãi rác.")
        # Lấy 20 dòng cuối (mới nhất)
        df_live = df_dlq.tail(20)
        st.dataframe(df_live.to_pandas(), use_container_width=True, hide_index=True)
        
    with tab_history:
        st.markdown(f"Hiển thị **Toàn bộ {df_dlq.height} bản ghi lỗi** có trong kho S3. (Cuộn để xem tiếp)")
        # Hiển thị toàn bộ với thanh cuộn (height=400)
        st.dataframe(df_dlq.to_pandas(), use_container_width=True, height=400, hide_index=True)

    st.markdown("---")
    st.subheader("💡 Khuyến nghị cho Data Engineer:")
    st.markdown("""
    * **Thiếu `asin` hoặc `reviewerID`:** Cần kiểm tra lại code trên hệ thống nguồn (Kafka Producer).
    * **Sai định dạng ngày tháng:** Cập nhật lại hàm `to_timestamp()` trong file Spark Streaming của Tầng Bronze.
    * **Dữ liệu rác chứa ký tự lạ:** Thêm Regex Filter vào luồng dọn dẹp.
    """)