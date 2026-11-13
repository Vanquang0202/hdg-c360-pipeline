FROM apache/superset:6.1.0
# Image chính thức bản lean: chỉ bổ sung driver PostgreSQL, không dùng source DataHub.
USER root
RUN uv pip install --python /app/.venv/bin/python psycopg2-binary==2.9.11
USER superset
