import streamlit as st
import glob
import os
from dotenv import load_dotenv, find_dotenv

def render_sidebar():
    load_dotenv(find_dotenv())
    
    # 100% Lấy từ .env
    db_path = os.getenv("MONITOR_DB_PATH")
    
    # Tự động suy ra thư mục chứa DB (data/monitoring_logs)
    log_dir = os.path.dirname(db_path) if db_path else "data/monitoring_logs"

    st.sidebar.header("⚙️ Bảng Điều Khiển")
    refresh_rate = st.sidebar.slider("Tốc độ làm mới (giây)", 5, 60, 10)

    st.sidebar.markdown("---")
    st.sidebar.header("📂 Lịch Sử Biểu Đồ")
    
    session_files = sorted(glob.glob(os.path.join(log_dir, "monitoring_session_*.db")))
    session_names = ["Phiên hiện tại (Live)"] + [f"Phiên {i+1}" for i in range(len(session_files))]
    selected_session = st.sidebar.selectbox("Chọn phiên để xem:", session_names)

    is_live = selected_session == "Phiên hiện tại (Live)"
    
    if is_live:
        active_db_path = db_path
        auto_refresh = st.sidebar.checkbox("Bật tự động làm mới", value=True)
    else:
        session_index = session_names.index(selected_session) - 1
        active_db_path = session_files[session_index]
        auto_refresh = False
        st.sidebar.warning("Ngừng làm mới vì đang xem dữ liệu quá khứ.")

    # Trả về thêm log_dir để Home.py dùng khi muốn lưu phiên bản
    return active_db_path, is_live, auto_refresh, refresh_rate, log_dir