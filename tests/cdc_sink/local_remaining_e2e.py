# Mục đích: E2E local cho QUOC_TICH/NHANKHAU, không reset các bài test đã PASS.
# Đầu vào: Docker local, Source đang RUNNING và Kafka localhost:29092.
# Đầu ra: Database test mới và bằng chứng trong test-results; không cutover Mirror.
import argparse
import json
import queue
import re
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "cdc/sink"))
import contract
import storage
from psycopg import sql
import psycopg
from confluent_kafka import Consumer, TopicPartition, TopicCollection, KafkaException
from confluent_kafka.admin import AdminClient
from protocol import loads, decode, canonical, digest
from engine import validate, check_records, initialize, apply_frame
from kafka_reader import read_ranges
from fixtures import row_schema
from urllib.request import urlopen

TEST_DB = "hdg_cdc_remaining_1111_" + uuid.uuid4().hex[:8] + "_test"
PREFIX = "hdg_observe"
TARGETS = ["VSS_DM_QUOC_TICH", "VSS_HO_GD_NHANKHAU"]


def oracle(text):
    # Chỉ vào container local cố định; lỗi SQL phải rollback và dừng, không tiếp tục DML.
    result = subprocess.run(
        ["docker", "exec", "-i", "hdg-c360-oracle", "sqlplus", "-L", "-s", "/", "as", "sysdba"],
        input="whenever sqlerror exit failure rollback\nset heading off feedback off pages 0 lines 300 trimspool on\nalter session set container=FREEPDB1;\n" + text + "\nexit\n",
        capture_output=True, text=True, timeout=35)
    if result.returncode or "ORA-" in result.stdout or "SP2-" in result.stdout:
        raise RuntimeError("Oracle command failed: " + result.stdout[:700])
    return result.stdout.strip()


def metadata(admin, consumer):
    found = admin.list_topics(timeout=10)
    names = [PREFIX + ".VSS." + name for name in contract.TABLES] + [PREFIX + ".transaction"]
    existing = [name for name in names if name in found.topics]
    futures = admin.describe_topics(TopicCollection(existing))
    result = {}
    for name in names:
        if name not in found.topics:
            result[name] = dict(topic=name, topic_id="UNUSED-EMPTY-TEST-STREAM",
                                partition=0, start=0, end=0, exists=False)
            continue
        assert set(found.topics[name].partitions) == {0}, "Only partition 0 supported"
        # Topic mới có thể chưa có leader ngay; chỉ retry metadata, không tạo lại DML.
        for retry in range(5):
            try:
                low, high = consumer.get_watermark_offsets(TopicPartition(name, 0), timeout=10)
                break
            except KafkaException:
                if retry == 4:
                    raise
                time.sleep(1)
        result[name] = dict(topic=name, topic_id=str(futures[name].result(timeout=10).topic_id),
                            partition=0, start=low, end=high, exists=True)
    return result


def dump(path, data):
    path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute-local", action="store_true")
    parser.add_argument("--resume", type=Path, help="Resume captured run after source fixtures were safely deleted")
    args = parser.parse_args()
    if not args.execute_local:
        parser.error("Explicit --execute-local is required: creates/deletes only new Oracle fixtures")
    if args.resume:
        resume(args.resume)
        return
    report_dir = ROOT / "tests/cdc_sink/test-results" / ("remaining-" + uuid.uuid4().hex)
    report_dir.mkdir(parents=True)
    report = {"status": "STARTED", "database": TEST_DB, "scope": TARGETS,
              "old_snapshot_scn": 2438031, "live_cutover": "BLOCKED"}
    dump(report_dir / "result.json", report)
    print("Evidence:", report_dir.relative_to(ROOT), flush=True)

    # BƯỚC 1: Xác minh container, connector và user/database test trước mọi thao tác ghi.
    for name, image in [("hdg-c360-oracle", "gvenzl/oracle-free:23-slim"),
                        ("hdg-cdc-sink-test", "postgres:16")]:
        raw = subprocess.check_output(["docker", "inspect", "--format",
                                      "{{.Config.Image}}|{{.State.Running}}", name], text=True).strip()
        assert raw == image + "|true", "Unexpected local container identity"
    cfg = json.load(urlopen("http://localhost:8083/connectors/hdg-oracle-observe/config", timeout=10))
    state = json.load(urlopen("http://localhost:8083/connectors/hdg-oracle-observe/status", timeout=10))
    assert cfg["database.hostname"] == "hdg-c360-oracle"
    assert cfg["database.pdb.name"] == "FREEPDB1"
    assert cfg["provide.transaction.metadata"] == "true"
    assert all(name in cfg["table.include.list"] for name in TARGETS)
    assert state["connector"]["state"] == "RUNNING"
    assert state["tasks"] and all(t["state"] == "RUNNING" for t in state["tasks"])
    identity = oracle("select 'IDENTITY='||(select name from v$database)||':'||sys_context('USERENV','CON_NAME') from dual;")
    assert "IDENTITY=FREE:FREEPDB1" in identity
    incarnation = oracle("select 'INC='||dbid||':'||resetlogs_change# from v$database;").split("INC=")[-1].strip()
    report["source_incarnation"] = incarnation
    report["source_counts_before"] = oracle(
        "select 'COUNTS='||(select count(*) from VSS.VSS_DM_QUOC_TICH)||':'||(select count(*) from VSS.VSS_HO_GD_NHANKHAU) from dual;")

    # Kết nối gốc chỉ đọc danh sách DB. Không reset hoặc ghi vào DB test đang có.
    with storage.connect_test() as connection:
        assert not connection.execute("SELECT 1 FROM pg_database WHERE datname=%s", (TEST_DB,)).fetchone(), (
            "Test database already exists; retain its evidence and choose a reviewed fresh run")
        connection.execute(sql.SQL("CREATE DATABASE {} OWNER sink_test_owner").format(sql.Identifier(TEST_DB)))

    # Bộ test đổi tên đích chỉ trong tiến trình này. Host/port/user và marker/schema guard vẫn giữ.
    # Không sửa contract.py, không cho phép cấu hình đích Mirror hoặc database bất kỳ.
    contract.DATABASE = TEST_DB
    storage.DATABASE = TEST_DB
    connection = storage.connect_test()
    storage.bootstrap(connection)
    admin = AdminClient({"bootstrap.servers": "localhost:29092", "broker.address.family": "v4"})
    consumer = Consumer({"bootstrap.servers": "localhost:29092", "broker.address.family": "v4",
                         "group.id": "hdg-remaining-readonly-" + uuid.uuid4().hex,
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "allow.auto.create.topics": False})
    initial = metadata(admin, consumer)
    report["offsets_before"] = initial

    token = uuid.uuid4().hex[:16]
    marker = "E2E1111-" + token
    nid = marker
    code = "E2E-" + token
    qid = -(int(token[:12], 16) + 1000000000000000)
    report["fixture"] = {"marker": marker, "nationality_id": qid}
    dump(report_dir / "result.json", report)
    predicate = f"ID='{nid}' AND MA_CD='{code}' AND GHI_CHU='{marker}'"
    assert oracle(f"select count(*) from VSS.VSS_HO_GD_NHANKHAU where ID='{nid}' or MA_CD='{code}';") == "0"
    assert oracle(f"select count(*) from VSS.VSS_DM_QUOC_TICH where ID={qid};") == "0"
    dml = f"""
insert into VSS.VSS_DM_QUOC_TICH(ID,MA,TEN) values({qid},'ZZ','{marker}');
insert into VSS.VSS_HO_GD_NHANKHAU(ID,MA_CD,MA_HO_GD,HO_TEN,GHI_CHU,TYPEID,TRANG_THAI_DUYET,NGAY_SINH,CREATED_DATE)
values('{nid}','{code}',NULL,'E2E fixture initial','{marker}','CD',2,12345678901234567890.123456789,TO_DATE('2026-11-11 12:34:56','YYYY-MM-DD HH24:MI:SS'));
select 'PENDING_SCN='||DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER from dual;
"""
    # BƯỚC 2: Giữ INSERT chưa commit; một session khác xác nhận snapshot chưa thấy fixture.
    process = subprocess.Popen(
        ["docker", "exec", "-i", "hdg-c360-oracle", "sqlplus", "-L", "-s", "/", "as", "sysdba"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    lines = queue.Queue()
    def collect():
        for line in process.stdout:
            lines.put(line.strip())
    threading.Thread(target=collect, daemon=True).start()
    try:
        process.stdin.write("whenever sqlerror exit failure rollback\nset heading off feedback off pages 0 lines 300\nalter session set container=FREEPDB1;\n" + dml)
        process.stdin.flush()
        deadline = time.monotonic() + 40
        baseline_scn = None
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=1)
            except queue.Empty:
                continue
            if "ORA-" in line or "SP2-" in line:
                raise RuntimeError("Pending fixture SQL failed: " + line)
            if line.startswith("PENDING_SCN="):
                baseline_scn = int(line.split("=")[1]); break
        if baseline_scn is None:
            process.stdin.close()
            raise RuntimeError("No pending transaction SCN; no cutover inference allowed")
        assert oracle(f"select count(*) from VSS.VSS_DM_QUOC_TICH AS OF SCN {baseline_scn} where ID={qid};") == "0"
        assert oracle(f"select count(*) from VSS.VSS_HO_GD_NHANKHAU AS OF SCN {baseline_scn} where {predicate};") == "0"
        report["fixture_snapshot_scn"] = baseline_scn
        report["pending_insert_absent_at_snapshot"] = True
        dump(report_dir / "result.json", report)
        process.stdin.write("commit;\nselect 'INSERT_COMMITTED' from dual;\nexit\n")
        process.stdin.flush()
        process.stdin.close()
        assert process.wait(timeout=30) == 0
    except BaseException:
        if process.poll() is None:
            try:
                if not process.stdin.closed:
                    process.stdin.write("rollback;\nexit\n")
                    process.stdin.flush()
                    process.stdin.close()
                process.wait(timeout=15)
            except (BrokenPipeError, OSError, subprocess.TimeoutExpired):
                process.terminate()
        raise
    print("Fixture INSERT committed; snapshot SCN", baseline_scn, flush=True)

    # BƯỚC 3: Chỉ UPDATE/DELETE hai fixture mới, kiểm số dòng bằng 1 trước mỗi commit.
    def guarded_statements(statements):
        block = "begin\n"
        for statement in statements:
            block += statement + "\nif SQL%ROWCOUNT<>1 then raise_application_error(-20001,'Fixture row count mismatch'); end if;\n"
        return oracle(block + "end;\n/\ncommit;")
    guarded_statements([
        f"update VSS.VSS_DM_QUOC_TICH set TEN='{marker}-updated' where ID={qid} AND MA='ZZ' AND TEN='{marker}';",
        f"update VSS.VSS_HO_GD_NHANKHAU set HO_TEN='E2E fixture updated',MODIFY_DATE=TO_DATE('2026-11-11 12:35:57','YYYY-MM-DD HH24:MI:SS') where {predicate} AND HO_TEN='E2E fixture initial';"])
    guarded_statements([
        f"delete from VSS.VSS_DM_QUOC_TICH where ID={qid} AND MA='ZZ' AND TEN='{marker}-updated';",
        f"delete from VSS.VSS_HO_GD_NHANKHAU where {predicate} AND HO_TEN='E2E fixture updated';"])
    assert oracle(f"select count(*) from VSS.VSS_DM_QUOC_TICH where ID={qid};") == "0"
    assert oracle(f"select count(*) from VSS.VSS_HO_GD_NHANKHAU where ID='{nid}' or MA_CD='{code}';") == "0"
    report["source_counts_after"] = oracle(
        "select 'COUNTS='||(select count(*) from VSS.VSS_DM_QUOC_TICH)||':'||(select count(*) from VSS.VSS_HO_GD_NHANKHAU) from dual;")
    assert report["source_counts_before"] == report["source_counts_after"]
    report["fixture_deleted_only"] = True
    dump(report_dir / "result.json", report)

    capture_and_apply(report_dir, report, connection, admin, consumer, initial, baseline_scn, incarnation)


def resume(report_dir):
    # Chỉ tiếp tục đợt đã dọn fixture; không chạy lại INSERT/UPDATE/DELETE hoặc reset DB.
    report_dir = report_dir.resolve()
    assert report_dir.is_relative_to((ROOT / "tests/cdc_sink/test-results").resolve())
    report = json.loads((report_dir / "result.json").read_text(encoding="utf-8"))
    assert report["fixture_deleted_only"] is True
    database = report["database"]
    assert re.fullmatch(r"hdg_cdc_remaining_1111_[0-9a-f]{8}_test", database)
    marker = report["fixture"]["marker"]
    assert re.fullmatch(r"E2E1111-[0-9a-f]{16}", marker)
    qid = report["fixture"]["nationality_id"]
    assert isinstance(qid, int) and qid < 0
    assert oracle(f"select count(*) from VSS.VSS_HO_GD_NHANKHAU where ID='{marker}';") == "0"
    assert oracle(f"select count(*) from VSS.VSS_DM_QUOC_TICH where ID={qid};") == "0"
    assert report["source_counts_before"] == report["source_counts_after"]
    contract.DATABASE = database
    storage.DATABASE = database
    connection = storage.connect_test()
    storage.assert_target(connection)
    epochs = connection.execute("SELECT count(*) FROM sink_meta.epochs").fetchone()[0]
    if epochs:
        # Khởi động tiến trình mới chỉ replay manifest đã PASS; không nạp baseline hoặc reset.
        assert epochs == 1 and report["status"] == "PASS"
        document = json.loads((report_dir / "manifest.json").read_text(encoding="utf-8"))
        validate(document)
        records = read_ranges(document, "localhost:29092")
        found = check_records(document, records)
        before = digest(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints ORDER BY topic").fetchall())
        ledger_before = connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0]
        for number, frame in enumerate(document["frames"]):
            batch = [found[tuple(c)] for c in frame["records"]]
            assert apply_frame(connection, document, number, batch) == "REPLAY_FRAME"
        assert digest(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints ORDER BY topic").fetchall()) == before
        assert connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0] == ledger_before == 6
        for table in contract.TABLES.values():
            assert connection.execute(sql.SQL("SELECT count(*) FROM {}").format(storage.relation(table))).fetchone()[0] == 0
        report["separate_process_replay"] = "PASS"
        dump(report_dir / "result.json", report)
        connection.close()
        print("PASS: separate process replay, unchanged checkpoint/ledger and empty test Mirror", flush=True)
        return
    admin = AdminClient({"bootstrap.servers": "localhost:29092", "broker.address.family": "v4"})
    consumer = Consumer({"bootstrap.servers": "localhost:29092", "broker.address.family": "v4",
                         "group.id": "hdg-remaining-readonly-" + uuid.uuid4().hex,
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "allow.auto.create.topics": False})
    capture_and_apply(report_dir, report, connection, admin, consumer,
                      report["offsets_before"], report["fixture_snapshot_scn"],
                      report["source_incarnation"])


def capture_and_apply(report_dir, report, connection, admin, consumer, initial, baseline_scn, incarnation):
    # BƯỚC 4: Đợi đúng 3 giao dịch/2 tombstone; đọc range hữu hạn không commit Kafka offset.
    end = None
    for attempt in range(40):
        end = metadata(admin, consumer)
        increments = {name: end[name]["end"] - initial[name]["end"] for name in end}
        if increments[PREFIX + ".transaction"] >= 6 and all(
            increments[PREFIX + ".VSS." + name] >= 4 for name in TARGETS):
            break
        time.sleep(1)
    assert increments[PREFIX + ".transaction"] == 6, ("Concurrent/incomplete transaction range", increments)
    assert all(increments[PREFIX + ".VSS." + name] == 4 for name in TARGETS), increments
    assert all(increments[PREFIX + ".VSS." + name] == 0 for name in ["VSS_DM_DANTOC", "VSS_DM_DBHC"]), increments
    streams = [dict(topic=name, topic_id=end[name]["topic_id"], partition=0,
                    start=initial[name]["end"], end=end[name]["end"]) for name in end]
    records = read_ranges({"streams": streams}, "localhost:29092")
    document = dict(format="hdg-sink-prototype-v1", mode="captured-fixture-test",
                    identity_policy="experimental-redo-v1", epoch=str(uuid.uuid4()),
                    contract_hash=contract.CONTRACT_HASH, source_incarnation=incarnation,
                    topic_prefix=PREFIX,
                    baseline=dict(assertion="verified-test-baseline", scn=baseline_scn,
                        tables={name: dict(schema=row_schema(name), rows=[]) for name in contract.TABLES}),
                    streams=streams, frames=[])
    dml_records = [r for r in records if ".VSS." in r.topic and r.value is not None]
    controls = [r for r in records if r.topic == PREFIX + ".transaction"]
    decoded = {r.coordinate: decode(r, PREFIX, incarnation) for r in dml_records}
    assert len(dml_records) == 6
    transactions = {}
    for r in dml_records:
        e = decoded[r.coordinate]
        transactions.setdefault(e["txid"], []).append(r)
    commits = []
    for txid, batch in sorted(transactions.items(), key=lambda item: decoded[item[1][0].coordinate]["commit_scn"]):
        batch.sort(key=lambda r: decoded[r.coordinate]["order"])
        ctl = [r for r in controls if loads(r.value)["payload"]["id"] == txid]
        begin = [r for r in ctl if loads(r.value)["payload"]["status"] == "BEGIN"]
        finish = [r for r in ctl if loads(r.value)["payload"]["status"] == "END"]
        assert len(begin) == len(finish) == 1
        extra = []
        for r in batch:
            if decoded[r.coordinate]["op"] == "d":
                extra += [t for t in records if t.topic == r.topic and t.offset == r.offset + 1 and t.value is None]
        frame_records = begin + batch + finish + extra
        document["frames"].append(dict(txid=txid, event_count=len(batch),
                                       records=[list(r.coordinate) for r in frame_records]))
        commits.append(decoded[batch[0].coordinate]["commit_scn"])
    assert len(transactions) == 3 and commits == sorted(commits)
    inserts = [e for e in decoded.values() if e["op"] == "c"]
    for r in dml_records:
        e = decoded[r.coordinate]
        for image in [e["before"], e["after"]]:
            if image is not None:
                assert len(image) == len(contract.TABLES[e["table"]]["columns"])
    for r in dml_records:
        e = decoded[r.coordinate]
        if e["op"] == "c":
            source = loads(r.value)["payload"]["source"]
            assert int(source["scn"]) <= baseline_scn < e["commit_scn"]
    report["cross_snapshot_transaction_verified"] = True
    validate(document)
    found = check_records(document, records)
    dump(report_dir / "manifest.json", document)
    (report_dir / "records.jsonl").write_text("\n".join(json.dumps(r.wire()) for r in records)+"\n", encoding="utf-8")
    report["events"] = [dict(table=e["table"], op=e["op"], txid=e["txid"],
                             commit_scn=e["commit_scn"], offset=coordinate[2],
                             before_fields=0 if e["before"] is None else len(e["before"]),
                             after_fields=0 if e["after"] is None else len(e["after"]))
                        for coordinate, e in decoded.items()]
    report["offsets_after"] = end
    report["commits"] = commits
    dump(report_dir / "result.json", report)

    # BƯỚC 5: Áp transaction qua code Sink thật; mỗi bước so toàn dòng và kiểm replay.
    initialize(connection, document)
    results = []
    for number, spec in enumerate(document["frames"]):
        batch = [found[tuple(c)] for c in spec["records"]]
        failpoint_used = False
        if number == 0:
            class RetryProbe(RuntimeError):
                pass
            def failpoint(phase):
                if phase == "before_commit":
                    raise RetryProbe("Rollback probe before commit")
            try:
                apply_frame(connection, document, number, batch, failpoint=failpoint)
            except RetryProbe:
                pass
            else:
                raise AssertionError("Rollback probe did not fail")
            assert connection.execute("SELECT count(*) FROM sink_meta.inbox").fetchone()[0] == 0
            assert connection.execute("SELECT next_frame FROM sink_meta.epochs").fetchone()[0] == 0
            for table in contract.TABLES.values():
                assert connection.execute(sql.SQL("SELECT count(*) FROM {}").format(storage.relation(table))).fetchone()[0] == 0
            checkpoints = dict(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints").fetchall())
            assert all(checkpoints[s["topic"]] == s["start"] for s in streams)
            failpoint_used = True
        outcome = apply_frame(connection, document, number, batch)
        assert outcome == "APPLIED"
        for r in batch:
            event = decode(r, PREFIX, incarnation)
            if event["kind"] != "dml":
                continue
            table = contract.TABLES[event["table"]]
            cols = [c["name"] for c in table["columns"]]
            rows = connection.execute(sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(",").join(map(sql.Identifier, cols)), storage.relation(table))).fetchall()
            expected = event["after"]
            assert ([] if expected is None else [canonical(expected)]) == [
                canonical(dict(zip(cols, row))) for row in rows]
        snapshot = digest(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints ORDER BY topic").fetchall())
        connection.close()
        connection = storage.connect_test()
        assert apply_frame(connection, document, number, batch) == "REPLAY_FRAME"
        assert digest(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints ORDER BY topic").fetchall()) == snapshot
        results.append(dict(frame=number, outcome=outcome, restart_replay="REPLAY_FRAME",
                            rollback_before_commit=failpoint_used))
    checkpoint = dict(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints").fetchall())
    assert all(checkpoint[s["topic"]] == s["end"] for s in streams)
    assert connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0] == 6
    assert connection.execute("SELECT count(*) FROM sink_meta.transaction_ledger").fetchone()[0] == 3
    assert connection.execute("SELECT count(*) FROM sink_meta.inbox WHERE outcome='TOMBSTONE'").fetchone()[0] == 2
    assert connection.execute("SELECT count(*) FROM sink_meta.quarantine").fetchone()[0] == 0
    for name in TARGETS:
        assert connection.execute(sql.SQL("SELECT count(*) FROM {}").format(storage.relation(contract.TABLES[name]))).fetchone()[0] == 0
    report.update(status="PASS", results=results, checkpoints=checkpoint,
                  ledger_events=6, ledger_transactions=3, tombstones=2,
                  fixture_scope_only=True,
                  cutover_reason="SCN 2438031 has no evidenced historical writer fence/offset barrier")
    dump(report_dir / "result.json", report)
    connection.close()
    consumer.close()
    print(json.dumps({"status": "PASS", "database": report["database"], "snapshot_scn": baseline_scn,
                      "commit_scns": commits, "results": results}), flush=True)


if __name__ == "__main__":
    main()
