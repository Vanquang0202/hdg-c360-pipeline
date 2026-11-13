FROM apache/airflow:2.10.5-python3.11
# Giữ Airflow cố định; psycopg dùng đúng job chỉ đọc hiện có.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "psycopg[binary]==3.3.8" && pip check
COPY --chown=airflow:root init_local.py /opt/airflow/init_local.py
