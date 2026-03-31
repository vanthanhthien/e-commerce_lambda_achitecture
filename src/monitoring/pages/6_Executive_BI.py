import streamlit as st
import os
import polars as pl
import plotly.express as px
from dotenv import load_dotenv, find_dotenv

# Load cấu hình từ .env
load_dotenv(find_dotenv())

# ==========================================
# BẢO MẬT: CHỈ LẤY TỪ .ENV, KHÔNG HARDCODE PASSWORD
# ==========================================
DWH_HOST = os.getenv("DWH_HOST")
DWH_PORT = os.getenv("DWH_PORT")
DWH_DB = os.getenv("DWH_DB")
DWH_USER = os.getenv("DWH_USER")
DWH_PASSWORD = os.getenv("DWH_PASSWORD")

st.set_page_config(page_title="Executive BI Dashboard", page_icon="💎", layout="wide")
st.title("💎 TỔNG LÃNH ĐẠO KINH DOANH (PLATINUM LAYER)")
st.markdown("Báo cáo được trích xuất trực tiếp từ Data Warehouse bằng **Polars Engine** siêu tốc.")

# Kiểm tra an toàn bảo mật trước khi cho chạy tiếp
if not all([DWH_HOST, DWH_PORT, DWH_DB, DWH_USER, DWH_PASSWORD]):
    st.error("🚨 LỖI BẢO MẬT: Chưa cấu hình đủ thông tin đăng nhập Data Warehouse (`DWH_HOST`, `DWH_USER`, `DWH_PASSWORD`...) trong file `.env`!")
    st.stop()

# Chuỗi kết nối Database siêu tốc cho Polars
DB_URI = f"postgresql://{DWH_USER}:{DWH_PASSWORD}@{DWH_HOST}:{DWH_PORT}/{DWH_DB}"

# ==========================================
# 1. KẾT NỐI DATA WAREHOUSE (POLARS)
# ==========================================
@st.cache_data(ttl=60) # Cập nhật mỗi 60 giây
def load_dwh_data(query):
    try:
        # Ép engine="connectorx" để tối đa hóa tốc độ đọc từ PostgreSQL
        df = pl.read_database_uri(query=query, uri=DB_URI, engine="connectorx")
        return df
    except Exception as e:
        st.error(f"❌ Lỗi kết nối DWH: {e}")
        return pl.DataFrame()

# ==========================================
# 2. KIỂM TRA BẢNG PLATINUM / GOLD
# ==========================================
# Thử lấy data từ bảng Platinum
query_platinum = "SELECT * FROM gold_layer_platinum.fact_sales_incremental;"
df_fact = load_dwh_data(query_platinum)

# Nếu bảng Platinum chưa có, thử lấy từ bảng Gold
if df_fact.is_empty():
    st.info("Chưa tìm thấy dữ liệu ở tầng Platinum, đang thử đọc từ tầng Gold...")
    query_gold = "SELECT * FROM gold_layer.fact_sales_reviews;"
    df_fact = load_dwh_data(query_gold)

# ==========================================
# 3. VẼ BIỂU ĐỒ BI (BUSINESS INTELLIGENCE)
# ==========================================
if not df_fact.is_empty():
    st.success("✅ Đã kết nối thành công Data Warehouse bằng Polars!")
    
    # Ép kiểu dữ liệu an toàn trong Polars
    if 'total_amount' in df_fact.columns:
        df_fact = df_fact.with_columns(pl.col('total_amount').cast(pl.Float64, strict=False))
    if 'rating_score' in df_fact.columns:
        df_fact = df_fact.with_columns(pl.col('rating_score').cast(pl.Float64, strict=False))

    # Bảng số liệu Tổng (KPIs) sử dụng Polars API
    total_revenue = df_fact.select(pl.col('total_amount').sum()).item() if 'total_amount' in df_fact.columns else 0
    total_orders = df_fact.height # Lấy số dòng cực nhanh
    avg_rating = df_fact.select(pl.col('rating_score').mean()).item() if 'rating_score' in df_fact.columns else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("💰 Tổng Doanh Thu", f"${total_revenue:,.2f}" if total_revenue else "$0")
    col2.metric("📦 Tổng Số Đơn Hàng", f"{total_orders:,}")
    col3.metric("⭐ Điểm Đánh Giá TB", f"{avg_rating:.2f}/5.0" if avg_rating else "0/5.0")
    
    st.markdown("---")
    
    # Khu vực Biểu đồ
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("🛒 Doanh Thu Theo Mã Cửa Hàng (Store ID)")
        if 'store_id' in df_fact.columns and 'total_amount' in df_fact.columns:
            # Groupby siêu tốc bằng Polars
            df_store = df_fact.group_by('store_id').agg(
                pl.col('total_amount').sum().alias('Doanh_Thu')
            ).sort('Doanh_Thu', descending=True)
            
            # Truyền TRỰC TIẾP Polars DataFrame vào Plotly
            fig_store = px.bar(df_store, x='store_id', y='Doanh_Thu', 
                               title="Doanh thu đóng góp từ các Cửa hàng",
                               color='Doanh_Thu', color_continuous_scale='Viridis')
            st.plotly_chart(fig_store, use_container_width=True)
        else:
            st.warning("Thiếu cột `store_id` hoặc `total_amount` để vẽ.")

    with c2:
        st.subheader("⭐ Phân Bổ Đánh Giá (Rating Distribution)")
        if 'rating_score' in df_fact.columns:
            # Tạo cột làm tròn điểm số và đếm số lượng
            df_rating = df_fact.with_columns(
                pl.col('rating_score').round(0).alias('Rounded_Rating')
            ).group_by('Rounded_Rating').agg(
                pl.count().alias('So_Luong')
            )
            
            # Truyền TRỰC TIẾP Polars DataFrame vào Plotly
            fig_pie = px.pie(df_rating, names='Rounded_Rating', values='So_Luong', hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.warning("Thiếu cột `rating_score` để vẽ.")
            
    # Hiển thị dữ liệu mẫu
    with st.expander("🔍 Xem dữ liệu gốc (Raw DWH Data - Polars Engine)"):
        st.dataframe(df_fact.head(50), use_container_width=True)
else:
    st.warning("⚠️ Không thể tải dữ liệu từ Data Warehouse. Hãy đảm bảo Airflow đã chạy dbt thành công để nạp data vào Postgres!")