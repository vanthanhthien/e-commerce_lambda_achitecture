@echo off
echo ===================================================
echo 🚀 KHOI DONG HE THONG REAL-TIME (CO KIEM SOAT) 🚀
echo ===================================================

echo [0/6] Dang danh thuc Docker (Kafka, Postgres, Airflow)...
docker-compose -f infrastructure\docker\docker-compose.yml up -d
echo ⏳ Vui long doi 5 giay de Kafka khoi dong hoan toan...
timeout /t 5 /nobreak

echo [1/6] Khoi dong Data Producer (Ban data vao Kafka)...
start "Data Producer" cmd /k "python src/ingestion/create_data.py --mode realtime"
echo ⏳ Doi 5 giay de co nhung dong data dau tien trong Kafka...
timeout /t 5 /nobreak

echo [2/6] Khoi dong Spark Streaming (Bronze Layer)...
start "Spark Bronze" cmd /k "python src/streaming/spark_streaming.py"
echo ⏳ Doi 15 giay de Spark khoi tao JVM va ket noi len AWS S3...
timeout /t 15 /nobreak

echo [3/6] Khoi dong Spark Streaming (Silver Layer)...
start "Spark Silver" cmd /k "python -m src.streaming.bronze_to_silver_stream"
echo ⏳ Doi 5 giay de Silver doc data tu Bronze...
timeout /t 5 /nobreak

echo [4/6] Khoi dong Log Aggregator va DLQ Alerter...
start "Log Aggregator" cmd /k "python -m src.monitoring.log_aggregator"
start "DLQ Alerter" cmd /k "python src/monitoring/dlq_alerter.py"
echo ⏳ Doi 3 giay de gom log...
timeout /t 3 /nobreak

echo [5/6] Khoi dong Streamlit Dashboard...
start "Streamlit Dashboard" cmd /k "streamlit run src/monitoring/Home.py"

echo.
echo ✅ TAT CA HE THONG DA ON DINH!
pause