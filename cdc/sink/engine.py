# Mục đích: Kiểm manifest/baseline rồi áp giao dịch nguồn hoàn chỉnh vào Mirror test.
# Đầu vào: Record đã gom đủ, epoch/baseline và kết nối PostgreSQL test.
# Đầu ra: Mirror, inbox, ledger và checkpoint commit nguyên khối; lỗi dữ liệu được quarantine.
"""Atomic transaction applier; default test target or explicitly guarded cutover scope."""
import uuid
import re
from collections import Counter
from psycopg import sql
from psycopg.types.json import Jsonb
import psycopg
from contract import TABLES, CONTRACT_HASH
from protocol import decode, digest, canonical, integer, row, Rejected, loads
from storage import assert_target, insert_row, relation, matching_where


# BƯỚC 1: Kiểm baseline, contract và offset đã khai báo trước khi áp giao dịch.


def validate(document):
    # BƯỚC 1: Baseline phải được khai báo cho đủ bốn bảng, cùng range hữu hạn; không suy baseline từ offset.
    if not isinstance(document, dict) or document.get("format") != "hdg-sink-prototype-v1":
        raise Rejected("MANIFEST_FORMAT")
    from storage import cutover_profile
    live = document.get("mode") == "cutover-live" and (cutover_profile() or {}).get("purpose") == "live"
    if document.get("mode") not in ("synthetic-test", "captured-fixture-test") and not live:
        raise Rejected("TEST_MANIFEST_REQUIRED", "explicit verified cutover scope required")
    if document.get("identity_policy") != "experimental-redo-v1":
        raise Rejected("IDENTITY_POLICY")
    uuid.UUID(document["epoch"])
    if document.get("contract_hash") != CONTRACT_HASH:
        raise Rejected("MANIFEST_CONTRACT_HASH")
    if not isinstance(document.get("source_incarnation"), str) or not document["source_incarnation"]:
        raise Rejected("SOURCE_INCARCATION_REQUIRED")
    prefix = document.get("topic_prefix")
    if prefix not in ("hdg_sink_test", "hdg_observe"):
        raise Rejected("TOPIC_PREFIX")
    baseline = document.get("baseline")
    if not isinstance(baseline, dict) or baseline.get("assertion") != ("verified-source-baseline" if live else "verified-test-baseline"):
        raise Rejected("EXPLICIT_BASELINE_REQUIRED")
    integer(baseline.get("scn"), "test baseline SCN", 0)
    tables = baseline.get("tables", {})
    if set(tables) != set(TABLES):
        raise Rejected("BASELINE_ALL_FOUR_TABLES")
    baseline_kind = baseline.get("kind", "inline")
    if baseline_kind not in ("inline", "spark-stage-v2"):
        raise Rejected("BASELINE_KIND")
    if baseline_kind == "spark-stage-v2":
        uuid.UUID(baseline["run_id"])
    for source, spec in tables.items():
        from protocol import descriptors
        descriptors(spec.get("schema"), TABLES[source])
        if baseline_kind == "spark-stage-v2":
            if "rows" in spec:
                raise Rejected("STAGE_BASELINE_MUST_REFERENCE_AUDIT")
            integer(spec.get("row_count"), "stage row count", 0)
            if not isinstance(spec.get("stage_digest"), str) or not re.fullmatch(r"[0-9a-f]{64}", spec["stage_digest"]):
                raise Rejected("STAGE_BASELINE_DIGEST")
        else:
            if not isinstance(spec.get("rows"), list):
                raise Rejected("BASELINE_ROWS")
            for raw in spec["rows"]:
                row(raw, spec["schema"], TABLES[source])
    streams = {}
    allowed = {prefix + ".VSS." + source for source in TABLES} | {prefix + ".transaction"}
    for spec in document.get("streams", []):
        if spec.get("topic") not in allowed or integer(spec.get("partition"), "partition", 0) != 0:
            raise Rejected("PROTOTYPE_ONE_PARTITION")
        if not isinstance(spec.get("topic_id"), str) or not spec["topic_id"]:
            raise Rejected("TOPIC_ID_REQUIRED")
        key = spec["topic"], spec["partition"]
        if key in streams:
            raise Rejected("DUPLICATE_STREAM")
        start = integer(spec.get("start"), "start offset", 0)
        end = integer(spec.get("end"), "end offset", 0)
        if end < start or end - start > 100000:
            raise Rejected("BOUNDED_RANGE_REQUIRED")
        streams[key] = spec
    if {topic for topic, _ in streams} != allowed:
        raise Rejected("ALL_STREAM_RANGES_REQUIRED")
    if sum(s["end"]-s["start"] for s in streams.values())>100000:
        raise Rejected("PROTOTYPE_RECORD_LIMIT")
    coordinates = []
    for frame in document.get("frames", []):
        integer(frame.get("event_count"), "frame event_count", 0)
        if not frame.get("records"):
            raise Rejected("EMPTY_FRAME")
        for coordinate in frame["records"]:
            if not isinstance(coordinate, list) or len(coordinate) != 3:
                raise Rejected("FRAME_COORDINATE")
            topic, partition, offset = coordinate
            key = topic, partition
            if key not in streams or not streams[key]["start"] <= offset < streams[key]["end"]:
                raise Rejected("FRAME_RANGE")
            coordinates.append(tuple(coordinate))
    expected = sum(s["end"]-s["start"] for s in streams.values())
    if len(coordinates) != expected or len(set(coordinates)) != expected:
        raise Rejected("FINITE_RANGE_NOT_FULLY_COVERED")
    return document


def manifest_hash(document):
    return digest(document)


def load(path):
    return validate(loads(path.read_bytes()))


def check_records(document, records):
    # BƯỚC 2: Pin topic ID và kiểm đủ tọa độ/range; thiếu một record cũng phải dừng.
    spec = {(s["topic"],s["partition"]):s for s in document["streams"]}
    found = {}
    total_bytes = 0
    for record in records:
        coordinate = record.coordinate
        stream = spec.get(coordinate[:2])
        if stream is None or record.topic_id != stream["topic_id"]:
            raise Rejected("TOPIC_ID_OR_STREAM")
        if not stream["start"] <= record.offset < stream["end"] or coordinate in found:
            raise Rejected("RECORD_OUTSIDE_FINITE_RANGE")
        found[coordinate] = record
        total_bytes += len(record.value or b"") + len(record.key or b"")
        if total_bytes > 64*1024*1024:
            raise Rejected("PROTOTYPE_BATCH_MEMORY_LIMIT")
    expected = {tuple(c) for frame in document["frames"] for c in frame["records"]}
    if set(found) != expected:
        raise Rejected("INCOMPLETE_FIXTURE")
    return found


class SimulatedCrash(RuntimeError):
    pass


def initialize(connection, document):
    # BƯỚC 2: Khóa epoch và nạp baseline đã khai báo; không ghi đè trạng thái test đang có.
    validate(document)
    assert_target(connection)
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(631874,1)")
        if connection.execute("SELECT count(*) FROM sink_meta.epochs").fetchone()[0]:
            raise Rejected("TEST_BASELINE_ALREADY_INITIALIZED", "never overwrite an epoch/baseline")
        for table in TABLES.values():
            if connection.execute(sql.SQL("SELECT count(*) FROM {}").format(relation(table))).fetchone()[0]:
                raise Rejected("TEST_MIRROR_NOT_EMPTY")
        _register_epoch(connection, document)
        if document["baseline"].get("kind") == "spark-stage-v2":
            _initialize_stage(connection, document)
            return
        for name, spec in document["baseline"]["tables"].items():
            table = TABLES[name]
            for raw in spec["rows"]:
                insert_row(connection, table, row(raw,spec["schema"],table))



def _register_epoch(connection, document):
    # Chỉ gọi trong transaction đã kiểm schema/đích và khóa advisory.
    connection.execute(
        "INSERT INTO sink_meta.epochs VALUES(%s,%s,%s,'ACTIVE',0,%s)",
        (document["epoch"],manifest_hash(document),Jsonb(canonical(document)),document["baseline"]["scn"]))
    for stream in document["streams"]:
        connection.execute(
            "INSERT INTO sink_meta.checkpoints VALUES(%s,%s,%s,%s,%s)",
            (document["epoch"],stream["topic"],stream["topic_id"],stream["partition"],stream["start"]))

def _initialize_stage(connection, document, destination=None):
    """Copy an audited Stage run within the same explicitly guarded database."""
    # BƯỚC 1: Khóa run và Stage, kiểm SCN/schema/audit; không đọc từ database live.
    from schema_contract import checked_expression, validate_pg_columns
    # Baseline lớn cần ngân sách dài hơn DML CDC; LOCAL tự khôi phục sau transaction.
    connection.execute("SET LOCAL statement_timeout='180s'")
    if destination is not None and not re.fullmatch(r"hdg_cutover_[0-9a-f]{32}", destination):
        raise Rejected("INVALID_STAGE_DESTINATION")
    baseline = document["baseline"]
    run_id = baseline["run_id"]
    run = connection.execute(
        "SELECT source_scn,status FROM hdg_load.runs WHERE run_id=%s FOR UPDATE", (run_id,)).fetchone()
    if run != (baseline["scn"], "VALIDATED"):
        raise Rejected("STAGE_BASELINE_RUN")
    for table in TABLES.values():
        connection.execute(sql.SQL("LOCK TABLE {} IN SHARE MODE").format(
            sql.Identifier("hdg_stage", table["mirror"])))
    for name, table in TABLES.items():
        expected = baseline["tables"][name]
        actual_schema = connection.execute(
            "SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),"
            "CASE WHEN a.attnotnull THEN 'true' ELSE 'false' END "
            "FROM pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
            ("hdg_stage." + table["mirror"],)).fetchall()
        validate_pg_columns(actual_schema,
            [(c["name"], "text", "true" if c["required"] else "false") for c in table["columns"]]
            + [("load_run_id", "character varying(36)", "true")], "Stage baseline " + name)
        audit = connection.execute(
            "SELECT source_rows,stage_rows,content_equal,stage_digest FROM hdg_load.table_runs "
            "WHERE run_id=%s AND source_table=%s FOR SHARE", (run_id, name)).fetchone()
        if audit != (expected["row_count"], expected["row_count"], True, expected["stage_digest"]):
            raise Rejected("STAGE_BASELINE_AUDIT", name)
        current_digest = connection.execute("SELECT hdg_load.stage_digest(%s,%s)",
                                            (table["mirror"], run_id)).fetchone()[0]
        stage = sql.Identifier("hdg_stage", table["mirror"])
        count = connection.execute(sql.SQL("SELECT count(*) FROM {} WHERE load_run_id=%s").format(stage),
                                   (run_id,)).fetchone()[0]
        if current_digest != expected["stage_digest"] or count != expected["row_count"]:
            raise Rejected("STAGE_BASELINE_CONTENT", name)
    # Chỉ bắt đầu copy sau khi cả bốn audit/schema/digest đều đã hợp lệ.
    for name, table in TABLES.items():
        stage = sql.Identifier("hdg_stage", table["mirror"])
        target = sql.Identifier(destination, table["mirror"]) if destination else relation(table)
        # BƯỚC 2: Chuyển kiểu bằng guard v2; sai độ dài/precision phải lỗi, không cắt/làm tròn.
        columns = sql.SQL(",").join(sql.Identifier(c["name"]) for c in table["columns"])
        typed = sql.SQL(",").join(sql.SQL(checked_expression(c)) for c in table["columns"])
        connection.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {} WHERE load_run_id=%s").format(
            target, columns, typed, stage), (run_id,))
        # BƯỚC 3: So hai chiều, giữ cả dòng trùng; lỗi rollback cả baseline và epoch/checkpoint.
        difference = connection.execute(sql.SQL(
            "WITH typed AS MATERIALIZED (SELECT {} FROM {} WHERE load_run_id=%s) "
            "SELECT count(*) FROM ((SELECT {} FROM {} EXCEPT ALL SELECT * FROM typed) "
            "UNION ALL (SELECT * FROM typed EXCEPT ALL SELECT {} FROM {})) d").format(
                typed, stage, columns, target, columns, target),
                (run_id,)).fetchone()[0]
        if difference:
            raise Rejected("STAGE_BASELINE_COPY_MISMATCH", name)

def transaction(events, spec, incarnation):
    # BƯỚC 3: Gom đúng giao dịch, kiểm END/count/order; thiếu sự kiện thì chưa được áp vào Mirror.
    dml = [event for event in events if event["kind"] == "dml"]
    controls = [event["payload"] for event in events if event["kind"] == "control"]
    if not dml:
        if controls or spec.get("txid") is not None or spec["event_count"] != 0:
            raise Rejected("CONTROL_WITHOUT_COMPLETE_TRANSACTION")
        return None
    txids = {e["txid"] for e in dml}
    commits = {e["commit_scn"] for e in dml}
    if len(txids)!=1 or len(commits)!=1 or spec.get("txid") not in txids:
        raise Rejected("MIXED_OR_WRONG_TRANSACTION")
    if len(dml)!=integer(spec["event_count"],"event_count",1):
        raise Rejected("TRANSACTION_EVENT_COUNT")
    if [e["order"] for e in dml] != list(range(1,len(dml)+1)):
        raise Rejected("TRANSACTION_ORDER")
    orders = Counter()
    counts = Counter()
    for event in dml:
        orders[event["table"]]+=1
        counts[event["table"]]+=1
        if event["collection_order"] != orders[event["table"]]:
            raise Rejected("COLLECTION_ORDER")
    if len({e["identity"] for e in dml}) != len(dml):
        raise Rejected("AMBIGUOUS_SOURCE_IDENTITY_WITHIN_TRANSACTION")
    ends = [c for c in controls if c["status"]=="END"]
    begins = [c for c in controls if c["status"]=="BEGIN"]
    if len(ends)!=1 or len(begins)>1 or any(c["id"]!=spec["txid"] for c in controls):
        raise Rejected("MISSING_OR_INVALID_TRANSACTION_END")
    end=ends[0]
    if integer(end.get("event_count"),"END event_count",1)!=len(dml):
        raise Rejected("END_EVENT_COUNT")
    collections={}
    if not isinstance(end.get("data_collections"),list):
        raise Rejected("END_COLLECTIONS")
    for collection in end["data_collections"]:
        qualified=collection.get("data_collection","")
        name=qualified.rsplit(".",1)[-1]
        if name not in TABLES or not qualified.endswith(".VSS."+name) or name in collections:
            raise Rejected("END_COLLECTION_NAME")
        collections[name]=integer(collection.get("event_count"),"collection event_count",1)
    if collections!=dict(counts):
        raise Rejected("END_COLLECTION_COUNTS")
    commit=next(iter(commits))
    return dict(commit=commit,key=digest([incarnation,spec["txid"],commit]),
                fingerprint=digest([(e["identity"],e["fingerprint"]) for e in dml]),dml=dml)


def apply_dml(connection, event):
    # BƯỚC 5: INSERT mới hoặc match toàn bộ before; UPDATE/DELETE chỉ tác động một bản sao, kể cả dòng trùng/NULL.
    table=TABLES[event["table"]]
    if event["op"]=="c":
        if table["primary_key"]:
            where,params=matching_where(table,event["after"],table["primary_key"])
            if connection.execute(sql.SQL("SELECT 1 FROM {} WHERE {} FOR UPDATE").format(
                    relation(table),where),params).fetchone():
                raise Rejected("PK_INSERT_CONFLICT")
        insert_row(connection,table,event["after"])
        return
    where,params=matching_where(table,event["before"])
    # ctid is a locked, transaction-local tuple locator only, never durable identity.
    target=connection.execute(sql.SQL(
        "SELECT ctid::text FROM {} WHERE {} ORDER BY ctid LIMIT 1 FOR UPDATE").format(
            relation(table),where),params).fetchone()
    if target is None:
        raise Rejected("BEFORE_IMAGE_NOT_FOUND",event["table"])
    if event["op"]=="d":
        affected=connection.execute(sql.SQL("DELETE FROM {} WHERE ctid=%s::tid").format(
            relation(table)),(target[0],)).rowcount
    else:
        names=[c["name"] for c in table["columns"]]
        affected=connection.execute(sql.SQL("UPDATE {} SET {} WHERE ctid=%s::tid").format(
            relation(table),sql.SQL(",").join(sql.SQL("{}=%s").format(sql.Identifier(n)) for n in names)),
            [event["after"][n] for n in names]+[target[0]]).rowcount
    if affected!=1:
        raise Rejected("ROW_EFFECT_NOT_ONE")


def _quarantine(connection,epoch,frame_no,records,error):
    # Lưu bằng chứng lỗi bằng transaction riêng sau rollback; BLOCKED ngăn job bỏ qua lỗi và tiếp tục checkpoint.
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(631874,1)")
        connection.execute(
            "INSERT INTO sink_meta.quarantine(epoch,frame_no,fingerprint,reason,raw_records) "
            "VALUES(%s,%s,%s,%s,%s)",
            (epoch,frame_no,digest([r.wire() for r in records]),error.code,
             Jsonb([r.wire() for r in records])))
        connection.execute("UPDATE sink_meta.epochs SET state='BLOCKED' WHERE epoch=%s",(epoch,))


def apply_frame(connection, document, frame_no, records, failpoint=None, before_commit=None, before_apply=None):
    """Each complete finite source transaction + its metadata commits atomically."""
    # BƯỚC 4: Kiểm manifest/đích, rồi giữ Mirror, inbox, dedup và checkpoint trong cùng một transaction.
    assert_target(connection)
    validate(document)
    spec=document["frames"][frame_no]
    epoch=document["epoch"]
    wire_hash=digest([r.wire() for r in records])
    try:
        with connection.transaction():
            connection.execute("SELECT pg_advisory_xact_lock(631874,1)")
            state=connection.execute(
                "SELECT manifest_hash,state,next_frame,last_commit_scn FROM sink_meta.epochs "
                "WHERE epoch=%s FOR UPDATE",(epoch,)).fetchone()
            if state is None or state[0]!=manifest_hash(document):
                raise Rejected("BASELINE_MANIFEST_MISMATCH")
            if state[1]!="ACTIVE":
                raise Rejected("EPOCH_BLOCKED")
            if [list(r.coordinate) for r in records]!=spec["records"]:
                raise Rejected("FRAME_RECORDS")
            # Frame đã commit chỉ được nhận lại khi raw fingerprint trùng; không áp DML lần hai.
            if frame_no<state[2]:
                old=connection.execute("SELECT fingerprint FROM sink_meta.frames WHERE epoch=%s AND frame_no=%s",
                                       (epoch,frame_no)).fetchone()
                if old is None or old[0]!=wire_hash:
                    raise Rejected("REPLAY_FRAME_CONTENT_CONFLICT")
                return "REPLAY_FRAME"
            if frame_no!=state[2]:
                raise Rejected("FRAME_ORDER")
            if before_apply:
                before_apply(connection)
            # BƯỚC 5: next_offset phải liên tục và đúng topic ID; mọi cập nhật ở đây còn nằm trong transaction.
            stream_specs={(s["topic"],s["partition"]):s for s in document["streams"]}
            for record in records:
                stream=stream_specs.get((record.topic,record.partition))
                if stream is None or record.topic_id!=stream["topic_id"]:
                    raise Rejected("TOPIC_ID_CHANGED")
                checkpoint=connection.execute(
                    "SELECT topic_id,next_offset FROM sink_meta.checkpoints WHERE epoch=%s AND topic=%s "
                    "AND partition=%s FOR UPDATE",(epoch,record.topic,record.partition)).fetchone()
                if checkpoint!=(record.topic_id,record.offset):
                    raise Rejected("OFFSET_ORDER_OR_GAP")
                connection.execute(
                    "UPDATE sink_meta.checkpoints SET next_offset=next_offset+1 WHERE epoch=%s AND topic=%s "
                    "AND partition=%s",(epoch,record.topic,record.partition))
            # BƯỚC 6: Kiểm đủ schema, ảnh dòng và END giao dịch trước khi thay đổi Mirror test.
            events=[decode(r,document["topic_prefix"],document["source_incarnation"]) for r in records]
            tx=transaction(events,spec,document["source_incarnation"])
            outcome="TOMBSTONE_ONLY"
            if tx is not None:
                if tx["commit"]<=document["baseline"]["scn"]:
                    raise Rejected("EVENT_NOT_AFTER_TEST_BASELINE", "no automatic baseline skip/cutover")
                # Chống replay nguồn ở offset mới bằng ledger; row_id không phải khóa dòng lâu dài.
                old=connection.execute(
                    "SELECT fingerprint FROM sink_meta.transaction_ledger WHERE epoch=%s AND source_tx=%s",
                    (epoch,tx["key"])).fetchone()
                seen=[]
                for event in tx["dml"]:
                    prior=connection.execute(
                        "SELECT fingerprint FROM sink_meta.event_ledger WHERE epoch=%s AND event_id=%s",
                        (epoch,event["identity"])).fetchone()
                    if prior and prior[0]!=event["fingerprint"]:
                        raise Rejected("SOURCE_EVENT_CONTENT_CONFLICT")
                    seen.append(prior is not None)
                if old is not None:
                    if old[0]!=tx["fingerprint"] or not all(seen):
                        raise Rejected("SOURCE_TRANSACTION_REPLAY_CONFLICT")
                    outcome="REPLAY_SOURCE"
                else:
                    if any(seen):
                        raise Rejected("PARTIAL_SOURCE_REPLAY")
                    if tx["commit"]<state[3]:
                        raise Rejected("OUT_OF_ORDER_COMMIT")
                    for event in tx["dml"]:
                        apply_dml(connection,event)
                        connection.execute(
                            "INSERT INTO sink_meta.event_ledger VALUES(%s,%s,%s)",
                            (epoch,event["identity"],event["fingerprint"]))
                    connection.execute("INSERT INTO sink_meta.transaction_ledger VALUES(%s,%s,%s)",
                                       (epoch,tx["key"],tx["fingerprint"]))
                    connection.execute("UPDATE sink_meta.epochs SET last_commit_scn=%s WHERE epoch=%s",
                                       (tx["commit"],epoch))
                    outcome="APPLIED"
            if failpoint:
                failpoint("after_dml")
            # BƯỚC 7: Lưu inbox/kết quả cùng ledger và checkpoint; lỗi trước commit sẽ rollback tất cả.
            for record,event in zip(records,events):
                record_outcome=("TOMBSTONE" if event["kind"]=="tombstone" else
                                "CONTROL" if event["kind"]=="control" else outcome)
                connection.execute(
                    "INSERT INTO sink_meta.inbox VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (epoch,record.topic_id,record.partition,record.offset,digest(record.wire()),
                     Jsonb(record.wire()),record_outcome))
            connection.execute("INSERT INTO sink_meta.frames VALUES(%s,%s,%s,%s)",
                               (epoch,frame_no,wire_hash,outcome))
            connection.execute("UPDATE sink_meta.epochs SET next_frame=next_frame+1 WHERE epoch=%s",(epoch,))
            if before_commit:
                before_commit(connection)
            if failpoint:
                failpoint("before_commit")
        if failpoint:
            failpoint("after_commit")
        return outcome
    except Rejected as error:
        # Failure evidence commits separately; target DML/ledger/checkpoints were rolled back.
        if error.code not in ("EPOCH_BLOCKED","BASELINE_MANIFEST_MISMATCH"):
            _quarantine(connection,epoch,frame_no,records,error)
        raise
    except psycopg.Error as error:
        if error.sqlstate and error.sqlstate[:2] in ("22","23"):
            rejected=Rejected("POSTGRES_"+error.sqlstate)
            _quarantine(connection,epoch,frame_no,records,rejected)
            raise rejected from error
        # Connection failures, serialization/deadlock etc. stay retryable, without skipped offsets.
        raise
