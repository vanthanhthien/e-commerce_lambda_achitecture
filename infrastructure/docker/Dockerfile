# Sử dụng hình ảnh Airflow chính thức (chạy trên lõi Linux Debian)
FROM apache/airflow:2.8.1

# Đổi sang quyền root (Admin của Linux) để cài đặt phần mềm hệ thống
USER root

# Cài đặt Java (vũ khí bắt buộc để Spark có thể chạy được)
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    apt-get clean

# Thiết lập biến môi trường cho Java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Đổi lại quyền cho user airflow để cài thư viện Python an toàn
USER airflow

# Cài đặt PySpark, Delta Lake và dbt-postgres
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    dbt-postgres==1.7.0 \
    python-dotenv