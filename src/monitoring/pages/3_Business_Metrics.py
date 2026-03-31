import streamlit as st
import os
import sys
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv, find_dotenv

# Gọi các hàm từ thư mục utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.file_helper import load_data_file

# Load cấu hình từ .env
load_dotenv(find_dotenv())

DATA_DIR = os.getenv("OUTPUT_FOLDER_NAME", "data/generated_realtime")
DATA_FILE = os.getenv("OUTPUT_FILE_NAME", "realtime_reviews.jsonl")
RAW_DATA_PATH = os.path.join(DATA_DIR, DATA_FILE)

st.set_page_config(page_title="Real-time Business", page_icon="📈", layout="wide")
st.title("📈 BÁO CÁO KINH DOANH REAL-TIME (SILVER LAYER)")
st.markdown("Giám sát sức khỏe thương hiệu, xu hướng đánh giá và xử lý khủng hoảng truyền thông tức thời.")

if not RAW_DATA_PATH or not os.path.exists(RAW_DATA_PATH):
    st.warning(f"⏳ Đang chờ dữ liệu chảy vào ống dẫn (Chưa tìm thấy file {RAW_DATA_PATH})...")
    st.stop()

# Đọc 1000 reviews mới nhất để phân tích nóng bằng Polars
df_raw = load_data_file(RAW_DATA_PATH, is_live_session=True, lines=1000)

# Thay thế .empty bằng .is_empty()
if "Trạng thái" in df_raw.columns or df_raw.is_empty():
    st.info("Hệ thống chưa nhận được lượt đánh giá (review) nào mới.")
else:
    # ==========================================
    # 🔍 BẬT CHẾ ĐỘ X-RAY ĐỂ BẮT BỆNH SCHEMA
    # ==========================================
    if 'overall' not in df_raw.columns:
        st.error("🚨 X-RAY SCANNER: Không tìm thấy cột 'overall'. Hãy xem dữ liệu thực tế bên dưới đang có những cột gì!")
        st.info(f"👉 Danh sách các cột hiện có: {', '.join(df_raw.columns)}")
        st.write("📦 Dữ liệu thô (3 dòng đầu tiên) mà Polars đọc được:")
        st.dataframe(df_raw.head(3).to_pandas(), use_container_width=True)
        st.stop() # Dừng chương trình tại đây để sếp đọc lỗi
    # ==========================================

    # Ép kiểu dữ liệu an toàn và lọc null bằng Polars
    df_raw = df_raw.with_columns(
        pl.col('overall').cast(pl.Float64, strict=False)
    ).drop_nulls(subset=['overall'])
    
    # Phân loại cảm xúc siêu tốc bằng pl.when().then()
    df_raw = df_raw.with_columns(
        pl.when(pl.col('overall') >= 4).then(pl.lit("Tích cực (4-5⭐)"))
        .when(pl.col('overall') == 3).then(pl.lit("Trung lập (3⭐)"))
        .otherwise(pl.lit("Tiêu cực (1-2⭐)")).alias("Sentiment")
    )
    
    # ==========================================
    # 1. BỘ CHỈ SỐ KPI ĐIỀU HÀNH TỔNG
    # ==========================================
    total_reviews = df_raw.height
    avg_rating = df_raw.select(pl.col('overall').mean()).item()
    positive_pct = df_raw.filter(pl.col('overall') >= 4).height / total_reviews * 100 if total_reviews > 0 else 0
    negative_count = df_raw.filter(pl.col('overall') <= 2).height
    
    st.subheader("🎯 Bảng Chỉ số Tổng quan (1000 lượt gần nhất)")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("🛒 Tổng Lượng Review", f"{total_reviews:,}")
    kpi2.metric("⭐ Điểm Trung Bình", f"{avg_rating:.2f} / 5.0" if avg_rating else "0 / 5.0", 
                delta=f"{avg_rating - 3.5:.2f} so với mốc chuẩn" if avg_rating else None, 
                delta_color="normal" if avg_rating and avg_rating >= 3.5 else "inverse")
    kpi3.metric("😍 Tỷ lệ Hài lòng", f"{positive_pct:.1f}%", help="Tỷ lệ đánh giá 4 và 5 sao")
    kpi4.metric("🚨 Báo động Tiêu cực", f"{negative_count} lượt", 
                delta="Cần CSKH xử lý ngay", delta_color="inverse")
    
    st.markdown("---")
    
    # ==========================================
    # 2. KHU VỰC BIỂU ĐỒ TRỰC QUAN (CHARTS)
    # ==========================================
    c_chart1, c_chart2 = st.columns(2)
    
    with c_chart1:
        st.subheader("📊 Cấu trúc Cảm xúc (Sentiment Share)")
        
        # Đếm số lượng theo nhóm (tương đương value_counts của pandas)
        sentiment_counts = df_raw.group_by('Sentiment').agg(pl.len().alias('Số Lượng'))
        
        # Cố định màu sắc cho chuẩn: Tích cực (Xanh), Tiêu cực (Đỏ), Trung lập (Xám/Vàng)
        color_map = {"Tích cực (4-5⭐)": "#2ecc71", "Trung lập (3⭐)": "#f1c40f", "Tiêu cực (1-2⭐)": "#e74c3c"}
        
        # Đẩy về Pandas để Plotly Express vẽ ngon lành
        fig_donut = px.pie(sentiment_counts.to_pandas(), names='Sentiment', values='Số Lượng', hole=0.5,
                           color='Sentiment', color_discrete_map=color_map)
        fig_donut.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_donut, use_container_width=True)
        
    with c_chart2:
        st.subheader("📈 Nhịp đập Cảm xúc (Rolling Trend)")
        st.markdown("Xu hướng trung bình của 20 đánh giá gần nhất (Càng cao càng tốt).")
        
        # Tính moving average để vẽ đường xu hướng cho mượt bằng Polars
        df_trend = df_raw.clone()
        df_trend = df_trend.with_row_index("Lượt", offset=1)
        df_trend = df_trend.with_columns(
            pl.col('overall').rolling_mean(window_size=20, min_periods=1).alias('Rolling_Avg')
        )
        
        fig_trend = px.line(df_trend.to_pandas(), x='Lượt', y='Rolling_Avg', 
                            labels={'Lượt': 'Dòng thời gian (Các review đổ về)', 'Rolling_Avg': 'Điểm TB (20 lượt)'})
        fig_trend.add_hrect(y0=1, y1=3, line_width=0, fillcolor="red", opacity=0.1, annotation_text="Vùng nguy hiểm")
        fig_trend.add_hrect(y0=4, y1=5, line_width=0, fillcolor="green", opacity=0.1, annotation_text="Vùng an toàn")
        fig_trend.update_yaxes(range=[1, 5.2])
        st.plotly_chart(fig_trend, use_container_width=True)

    st.markdown("---")
    
    # ==========================================
    # 3. TRẠM LẮNG NGHE KHÁCH HÀNG & XỬ LÝ KHỦNG HOẢNG
    # ==========================================
    st.subheader("🎧 Trạm Lắng Nghe Tiếng Nói Khách Hàng (VOC)")
    
    tab_all, tab_bad, tab_good, tab_hot = st.tabs(["📝 Tất cả Feed", "🚨 XỬ LÝ KHỦNG HOẢNG (1-2⭐)", "💖 Khen ngợi (4-5⭐)", "🔥 Sản phẩm HOT"])
    
    cols_to_show = [c for c in ['asin', 'overall', 'reviewerName', 'summary', 'reviewText'] if c in df_raw.columns]
    
    with tab_all:
        st.dataframe(df_raw.select(cols_to_show).head(30).to_pandas(), use_container_width=True)
        
    with tab_bad:
        st.error("Danh sách các khách hàng đang nổi giận. Cần phản hồi ngay lập tức!")
        df_bad = df_raw.filter(pl.col('overall') <= 2)
        if not df_bad.is_empty():
            st.dataframe(df_bad.select(cols_to_show).to_pandas(), use_container_width=True)
        else:
            st.success("Không có đánh giá tiêu cực nào gần đây! Quá tuyệt vời!")
            
    with tab_good:
        st.success("Những lời khen có cánh từ khách hàng.")
        df_good = df_raw.filter(pl.col('overall') >= 4)
        if not df_good.is_empty():
            st.dataframe(df_good.select(cols_to_show).head(30).to_pandas(), use_container_width=True)
        else:
            st.info("Chưa có đánh giá 4-5 sao nào.")
            
    with tab_hot:
        if 'asin' in df_raw.columns:
            # Group by, count và sort cực nhanh
            top_products = df_raw.group_by('asin').agg(pl.len().alias('Lượt Nhắc Đến')).sort('Lượt Nhắc Đến', descending=True).head(10)
            top_products = top_products.rename({'asin': 'Mã Sản Phẩm (ASIN)'})
            
            fig_bar = px.bar(top_products.to_pandas(), x='Lượt Nhắc Đến', y='Mã Sản Phẩm (ASIN)', orientation='h', text_auto=True)
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}) # Xếp từ cao xuống thấp
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.write("Không tìm thấy trường 'asin' trong dữ liệu.")