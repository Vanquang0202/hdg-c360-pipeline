# Mục đích: Tái sử dụng mapping v2 và cố định phạm vi PostgreSQL test cho Sink.
# Đầu vào: schema_contract.py và byte gốc của DDL Oracle.
# Đầu ra: TABLES, CONTRACT_HASH, database/port/schema test; không trỏ Mirror live.
"""Read-only reuse of the Full Load v2 Oracle contract."""
import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs"))
from schema_contract import source_schema, pg_catalog_type  # noqa: E402

TABLES = source_schema()
CONTRACT_HASH = hashlib.sha256((ROOT / "sql/oracle/01_create_source_tables.sql").read_bytes()).hexdigest()
DATABASE = "hdg_cdc_sink_test"
PORT = 55433
MIRROR_SCHEMA = "mirror_test"
META_SCHEMA = "sink_meta"
