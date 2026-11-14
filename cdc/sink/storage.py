# Mục đích: Tạo/kiểm schema và thực hiện SQL chỉ trong PostgreSQL test riêng.
# Đầu vào: Kết nối đúng endpoint test, hợp đồng kiểu và giá trị đã giải mã.
# Đầu ra: Bảng test cùng metadata; mọi kết nối sai đích bị chặn trước khi ghi.
"""Default test-only storage; public cutover requires an explicit verified scope."""
import os
import re
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
import psycopg
from psycopg import sql
from contract import TABLES, CONTRACT_HASH, DATABASE, PORT, MIRROR_SCHEMA, pg_catalog_type
from protocol import Rejected

TOKEN = "hdg-cdc-sink-prototype-test-only-v1"
HERE = Path(__file__).resolve().parent


_CUTOVER = ContextVar("hdg_cutover_target", default=None)
CUTOVER_TOKEN = "hdg-c360-cutover-v1"


def cutover_profile():
    return _CUTOVER.get()


@contextmanager
def cutover_target(connection, database, confirmation, purpose="clone", live_checked=False):
    # Phạm vi tường minh; mặc định prototype vẫn chỉ vào cổng 15433/mirror_test.
    clone = bool(re.fullmatch(r"hdg_cutover_clone_[0-9a-f]{8}_test", database))
    live = database == "hdg_mirror" and purpose == "live" and live_checked
    if confirmation != database or not ((clone and purpose == "clone") or live):
        raise ValueError("Explicit validated cutover endpoint required")
    if (connection.info.host != "127.0.0.1" or connection.info.port != 15432
            or connection.info.dbname != database):
        raise ValueError("Refusing cutover endpoint")
    token = _CUTOVER.set(dict(database=database, user=connection.info.user, purpose=purpose))
    try:
        yield
    finally:
        _CUTOVER.reset(token)


def mirror_namespace():
    return "public" if cutover_profile() else MIRROR_SCHEMA


# Mật khẩu chỉ lấy từ môi trường hoặc .env.test local; không đọc cấu hình Mirror live.
def password():
    supplied = os.environ.get("CDC_TEST_PASSWORD")
    if supplied:
        return supplied
    path = HERE / ".env.test"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("CDC_TEST_PASSWORD="):
                return line.split("=", 1)[1].strip()
    raise ValueError("Set CDC_TEST_PASSWORD or create cdc/sink/.env.test")


def connect_test():
    # BƯỚC 1: Kết nối cố định test DB/cổng 15433; không có tùy chọn rơi về PostgreSQL live.
    connection = psycopg.connect(host="127.0.0.1", port=PORT, dbname=DATABASE,
                                 user="sink_test_owner", password=password(), autocommit=True,
                                 connect_timeout=5, options="-c search_path=pg_catalog -c timezone=UTC")
    identity = connection.execute("SELECT current_database(),current_user,current_setting('server_encoding')").fetchone()
    if identity != (DATABASE, "sink_test_owner", "UTF8"):
        connection.close()
        raise ValueError("Refusing non-test PostgreSQL target")
    connection.execute("SET statement_timeout='30s'")
    connection.execute("SET lock_timeout='5s'")
    return connection


def ddl(metadata_only=False):
    # Dựng bảng test từ contract v2; giữ PK đúng hai danh mục và byte-check cho chuỗi.
    parts = [
        "CREATE SCHEMA mirror_test", "CREATE SCHEMA sink_meta",
        """CREATE TABLE sink_meta.guard(
            singleton boolean PRIMARY KEY CHECK(singleton), token text NOT NULL, contract_hash text NOT NULL)""",
        """CREATE TABLE sink_meta.epochs(
            epoch uuid PRIMARY KEY, manifest_hash text NOT NULL, manifest jsonb NOT NULL,
            state text NOT NULL CHECK(state IN ('ACTIVE','BLOCKED')),
            next_frame integer NOT NULL, last_commit_scn numeric NOT NULL)""",
        """CREATE TABLE sink_meta.checkpoints(
            epoch uuid REFERENCES sink_meta.epochs, topic text NOT NULL, topic_id text NOT NULL,
            partition integer NOT NULL, next_offset bigint NOT NULL,
            PRIMARY KEY(epoch,topic,partition))""",
        """CREATE TABLE sink_meta.inbox(
            epoch uuid REFERENCES sink_meta.epochs, topic_id text NOT NULL, partition integer NOT NULL,
            record_offset bigint NOT NULL, wire_hash text NOT NULL, raw_record jsonb NOT NULL,
            outcome text NOT NULL, PRIMARY KEY(epoch,topic_id,partition,record_offset))""",
        """CREATE TABLE sink_meta.event_ledger(
            epoch uuid REFERENCES sink_meta.epochs, event_id text NOT NULL, fingerprint text NOT NULL,
            PRIMARY KEY(epoch,event_id))""",
        """CREATE TABLE sink_meta.transaction_ledger(
            epoch uuid REFERENCES sink_meta.epochs, source_tx text NOT NULL, fingerprint text NOT NULL,
            PRIMARY KEY(epoch,source_tx))""",
        """CREATE TABLE sink_meta.frames(
            epoch uuid REFERENCES sink_meta.epochs, frame_no integer NOT NULL,
            fingerprint text NOT NULL, outcome text NOT NULL, PRIMARY KEY(epoch,frame_no))""",
        """CREATE TABLE sink_meta.quarantine(
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            epoch uuid NOT NULL, frame_no integer NOT NULL, fingerprint text NOT NULL,
            reason text NOT NULL, raw_records jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp())""",
    ]
    if metadata_only:
        return ";\n".join(parts[1:]) + ";\n"
    for table in TABLES.values():
        columns = [f"{c['name']} {c['postgres']}" + (" NOT NULL" if c["required"] else "")
                   for c in table["columns"]]
        columns += [f"CONSTRAINT ck_{c['name']}_bytes CHECK(octet_length({c['name']})<={c['length']})"
                    for c in table["columns"] if c["family"] == "string" and c["unit"] == "BYTE"]
        if table["primary_key"]:
            columns.append("PRIMARY KEY (" + ",".join(table["primary_key"]) + ")")
        parts.append("CREATE TABLE mirror_test." + table["mirror"] + "(" + ",".join(columns) + ")")
    return ";\n".join(parts) + ";\n"


def assert_endpoint(connection):
    profile = cutover_profile()
    if profile:
        if (connection.info.dbname != profile["database"] or connection.info.host != "127.0.0.1"
                or connection.info.port != 15432 or connection.info.user != profile["user"]):
            raise ValueError("Refusing cutover endpoint")
    elif (connection.info.dbname!=DATABASE or connection.info.host!="127.0.0.1"
            or connection.info.port!=PORT or connection.info.user!="sink_test_owner"):
        raise ValueError("Refusing non-test endpoint")


def bootstrap(connection):
    # BƯỚC 2: Chỉ tạo schema khi chưa có và không chiếm cấu trúc lạ; marker/hash giữ đúng môi trường test.
    assert_endpoint(connection)
    if cutover_profile():
        raise ValueError("Use cutover preparation, never test bootstrap")
    if connection.execute("SELECT to_regclass('sink_meta.guard')").fetchone()[0] is not None:
        assert_target(connection)
        return False
    if connection.execute("SELECT count(*) FROM pg_namespace WHERE nspname IN ('sink_meta','mirror_test')").fetchone()[0]:
        raise ValueError("Unknown test schemas already exist; refusing overwrite")
    with connection.transaction():
        connection.execute(ddl())
        connection.execute("INSERT INTO sink_meta.guard VALUES(true,%s,%s)", (TOKEN, CONTRACT_HASH))
    assert_target(connection)
    return True


def allowed_c360_trigger(connection,relation_name,observer=False):
    from c360_full_load import checked_queue_trigger, Blocked
    try:
        return checked_queue_trigger(connection,relation_name,observer=observer)
    except Blocked as error:
        raise Rejected("UNAPPROVED_C360_TRIGGER",str(error)) from error


def assert_target(connection,observer=False):
    # BƯỚC 3: Kiểm marker, đủ kiểu/NULL/PK/byte-check và các dependency không được phép.
    assert_endpoint(connection)
    if cutover_profile():
        identity=connection.execute("SELECT current_database(),current_user,current_setting('server_encoding')").fetchone()
        if identity != (cutover_profile()["database"],cutover_profile()["user"],"UTF8"):
            raise ValueError("Cutover identity/encoding differs")
    if not connection.autocommit or connection.info.dbname != (cutover_profile() or {}).get("database", DATABASE):
        raise ValueError("Test connection must use autocommit and the dedicated test database")
    marker = connection.execute("SELECT token,contract_hash FROM sink_meta.guard WHERE singleton").fetchone()
    if marker != (CUTOVER_TOKEN if cutover_profile() else TOKEN, CONTRACT_HASH):
        raise ValueError("Test-only marker or schema contract differs")
    for table in TABLES.values():
        actual = connection.execute(
            "SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull "
            "FROM pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped "
            "ORDER BY a.attnum", (mirror_namespace() + "." + table["mirror"],)).fetchall()
        expected = [(c["name"], pg_catalog_type(c), c["required"]) for c in table["columns"]]
        if actual != expected:
            raise Rejected("TARGET_SCHEMA", table["mirror"])
        constraints = connection.execute(
            "SELECT contype::text,pg_get_constraintdef(oid),convalidated FROM pg_constraint "
            "WHERE conrelid=%s::regclass", (mirror_namespace() + "." + table["mirror"],)).fetchall()
        pk = [definition for kind,definition,valid in constraints if kind == "p" and valid]
        if pk != (["PRIMARY KEY (id)"] if table["primary_key"] else []):
            raise Rejected("TARGET_PRIMARY_KEY", table["mirror"])
        for c in table["columns"]:
            if c["family"] == "string" and c["unit"] == "BYTE":
                valid = connection.execute(
                    "SELECT count(*) FROM pg_constraint WHERE conrelid=%s::regclass AND contype='c' "
                    "AND convalidated AND replace(regexp_replace(pg_get_expr(conbin,conrelid),"
                    "'[[:space:]()]','','g'),'::text','')=%s",
                    (mirror_namespace()+"."+table["mirror"], f"octet_length{c['name']}<={c['length']}")).fetchone()[0]
                if valid != 1:
                    raise Rejected("TARGET_BYTE_CONSTRAINT", c["name"])
        unsafe = connection.execute(
            "SELECT (SELECT count(*) FROM pg_trigger WHERE tgrelid=%s::regclass AND NOT tgisinternal)"
            "+(SELECT count(*) FROM pg_policy WHERE polrelid=%s::regclass)"
            "+(SELECT count(*) FROM pg_constraint WHERE conrelid=%s::regclass AND contype='f')",
            (mirror_namespace()+"."+table["mirror"],)*3).fetchone()[0]
        if cutover_profile():
            unsafe += connection.execute(
                "SELECT (SELECT count(*) FROM pg_constraint WHERE contype='f' AND confrelid=%s::regclass)"
                "+(SELECT count(*) FROM pg_class WHERE oid=%s::regclass AND (relrowsecurity OR relforcerowsecurity))",
                (mirror_namespace()+"."+table["mirror"],)*2).fetchone()[0]
        unsafe -= allowed_c360_trigger(connection,mirror_namespace()+"."+table["mirror"],observer=observer)
        if unsafe:
            raise Rejected("TEST_TARGET_HAS_EXTERNAL_POLICIES_OR_TRIGGERS")


def relation(table):
    return sql.Identifier(mirror_namespace(), table["mirror"])


def insert_row(connection, table, values):
    names = [c["name"] for c in table["columns"]]
    connection.execute(sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        relation(table), sql.SQL(",").join(map(sql.Identifier, names)),
        sql.SQL(",").join(sql.Placeholder() for _ in names)), [values[n] for n in names])


def matching_where(table, image, names=None):
    # So NULL-safe trên mọi cột before; collation C giữ khác biệt chuỗi/khoảng trắng.
    columns = [c for c in table["columns"] if names is None or c["name"] in names]
    expressions = []
    parameters = []
    for c in columns:
        expression = sql.Identifier(c["name"])
        if c["family"] == "string":
            expression = sql.SQL('{} COLLATE "C"').format(expression)
        expressions.append(sql.SQL("{} IS NOT DISTINCT FROM %s").format(expression))
        parameters.append(image[c["name"]])
    return sql.SQL(" AND ").join(expressions), parameters


def reset_test_fixture(connection):
    """Explicitly destroy ONLY prototype fixtures after verifying the fixed test endpoint."""
    # Chỉ reset bảng test được liệt kê sau khi xác nhận endpoint/marker; không dùng để sửa lỗi live.
    if cutover_profile():
        raise ValueError("Test reset forbidden for cutover targets")
    assert_target(connection)
    names=[("sink_meta",name) for name in (
        "quarantine","frames","transaction_ledger","event_ledger","inbox","checkpoints","epochs")]
    names += [("mirror_test",t["mirror"]) for t in TABLES.values()]
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(631874,1)")
        connection.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY").format(
            sql.SQL(",").join(sql.Identifier(*name) for name in names)))
