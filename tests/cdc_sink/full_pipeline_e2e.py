# Mục đích: Demo local Full Load Spark → baseline Stage → Kafka CDC → Sink → đối soát.
# Đầu vào: Dịch vụ local có sẵn, credential môi trường/.env; không nhận endpoint live tùy ý.
# Đầu ra: Database test mới và bằng chứng riêng; không reset test cũ hoặc Kafka offsets.
import argparse
import copy
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "cdc/sink/.venv/Lib/site-packages"))
sys.path.insert(0, str(ROOT / "cdc/sink"))
sys.path.insert(0, str(ROOT / "jobs"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import contract
import storage
from engine import initialize, validate, check_records, apply_frame, transaction
from protocol import loads, decode, digest, Rejected
from fixtures import row_schema
from kafka_reader import read_ranges
from local_remaining_e2e import metadata, dump
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from psycopg import sql
from dotenv import dotenv_values

PREFIX = "hdg_observe"
PYTHON = r"C:\Python311\python.exe"


def api(suffix):
    return json.load(urlopen("http://localhost:8083/connectors/hdg-oracle-observe/" + suffix, timeout=10))


class Fence:
    # Giữ khóa bằng cùng session Oracle; mọi lỗi/thoát phải rollback và giải phóng khóa.
    def __init__(self):
        self.process = subprocess.Popen(
            ["docker", "exec", "-i", "hdg-c360-oracle", "sqlplus", "-L", "-s", "/", "as", "sysdba"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.lines = queue.Queue()
        def collect():
            for line in self.process.stdout:
                self.lines.put(line.strip())
        threading.Thread(target=collect, daemon=True).start()
        self.send("whenever sqlerror exit failure rollback\nset heading off feedback off pages 0 lines 400 trimspool on\nalter session set container=FREEPDB1;")

    def send(self, query, timeout=45):
        marker = "DONE_" + uuid.uuid4().hex
        self.process.stdin.write(query + "\nselect '" + marker + "' from dual;\n")
        self.process.stdin.flush()
        rows = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                line = self.lines.get(timeout=1)
            except queue.Empty:
                if self.process.poll() is not None:
                    raise RuntimeError("Oracle fence session exited")
                continue
            if "ORA-" in line or "SP2-" in line:
                raise RuntimeError("Oracle fence/DML failed: " + line)
            if line == marker:
                return rows
            if line:
                rows.append(line)
        raise RuntimeError("Oracle fence reply timed out")

    def value(self, expression):
        rows = self.send("select 'VALUE='||(" + expression + ") from dual;")
        return next(line.split("=", 1)[1] for line in rows if line.startswith("VALUE="))

    def lock(self):
        self.send("\n".join("lock table VSS." + name + " in exclusive mode nowait;" for name in contract.TABLES))

    def close(self):
        if self.process.poll() is None:
            try:
                self.process.stdin.write("rollback;\nexit\n")
                self.process.stdin.flush()
                self.process.stdin.close()
                self.process.wait(timeout=15)
            except (OSError, subprocess.TimeoutExpired):
                self.process.terminate()


def prepare_metrics_reader(output):
    # Helper chỉ đọc JMX agent đã bật; binary nằm trong test-results được Git ignore.
    java_source = "import com.sun.tools.attach.VirtualMachine;\nimport javax.management.*;\nimport javax.management.remote.*;\npublic class ReadCaptureMetrics {\n public static void main(String[] args) throws Exception {\n  VirtualMachine vm=VirtualMachine.attach(args[0]);\n  String address=vm.getAgentProperties().getProperty(\"com.sun.management.jmxremote.localConnectorAddress\");\n  vm.detach();\n  if(address==null) throw new IllegalStateException(\"Existing local JMX agent required\");\n  try(JMXConnector connector=JMXConnectorFactory.connect(new JMXServiceURL(address))) {\n   MBeanServerConnection server=connector.getMBeanServerConnection();\n   for(ObjectName bean:server.queryNames(new ObjectName(\"debezium.oracle:*\"),null)) {\n    if(!bean.toString().contains(\"context=streaming\")) continue;\n    System.out.println(\"BEAN=\"+bean);\n    for(MBeanAttributeInfo info:server.getMBeanInfo(bean).getAttributes()) {\n     String name=info.getName();\n     if(name.matches(\"(?i).*(scn|transaction|connected|error|eventposition|millisecondbehind|capturedtables|queue).*\")) {\n      try { System.out.println(name+\"=\"+String.valueOf(server.getAttribute(bean,name))); }\n      catch(Exception e) { System.out.println(name+\"=UNREADABLE\"); }\n     }\n    }\n   }\n  }\n }\n}\n"
    source = output / "ReadCaptureMetrics.java"
    source.write_text(java_source, encoding="utf-8")
    subprocess.run(["javac","--release","17",str(source)],check=True,timeout=40)
    subprocess.run(["docker","cp",str(source.with_suffix(".class")),
                    "hdg-cdc-connect-1:/tmp/ReadCaptureMetrics.class"],check=True,timeout=20)


def metrics():
    # JMX agent đã có sẵn; helper chỉ đọc, không bật agent/restart Connect.
    r = subprocess.run(["docker", "exec", "hdg-cdc-connect-1", "java", "--add-modules", "jdk.attach",
                        "-cp", "/tmp", "ReadCaptureMetrics", "1"], capture_output=True, text=True, timeout=20)
    if r.returncode:
        raise RuntimeError("Existing read-only JMX helper unavailable")
    return dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line)


def capture_barrier(scn, admin, consumer, incarnation):
    # Không dùng Source offset đơn lẻ: kết hợp khóa writer, miner không còn transaction,
    # queue rỗng, Source offset đã flush và vector topic ổn định qua hai lần quan sát.
    samples = []
    previous = None
    for attempt in range(45):
        state = api("status")
        assert state["connector"]["state"] == "RUNNING" and all(t["state"] == "RUNNING" for t in state["tasks"])
        m = metrics()
        offset = api("offsets")["offsets"][0]["offset"]
        vector = metadata(admin, consumer)
        current = {name: (v["topic_id"], v["end"]) for name, v in vector.items()}
        ready = (m.get("Connected") == "true" and m.get("NumberOfActiveTransactions") == "0"
                 and m.get("QueueRemainingCapacity") == m.get("QueueTotalCapacity")
                 and m.get("CurrentQueueSizeInBytes") == "0"
                 and m.get("AbandonedTransactionCount") == "0" and m.get("ErrorCount") == "0"
                 and int(m.get("OffsetScn", "0")) >= scn and int(offset["scn"]) >= scn)
        if ready:
            samples.append(dict(metrics=m, source_offset=offset,
                                vector={name: v["end"] for name, v in vector.items()}))
            if previous == current:
                assert all(v["exists"] and v["start"] == 0 for v in vector.values()), "Incomplete retained history"
                records = read_ranges({"streams": [dict(v) for v in vector.values()]}, "localhost:29092")
                events = [decode(r, PREFIX, incarnation) for r in records]
                dml = [e for e in events if e["kind"] == "dml"]
                controls = [e["payload"] for e in events if e["kind"] == "control"]
                ids = {e["txid"] for e in dml}
                assert {c["id"] for c in controls if c["status"] == "END"} == ids
                for txid in ids:
                    group = sorted([e for e in dml if e["txid"] == txid], key=lambda e:e["order"])
                    ctrl = [e for e in events if e["kind"] == "control" and e["payload"]["id"] == txid]
                    tx = transaction(ctrl + group, {"txid":txid, "event_count":len(group)}, incarnation)
                    assert tx["commit"] <= scn, "Unexpected post-fence transaction"
                return vector, samples, len(ids)
            previous = current
        else:
            previous = None
        time.sleep(1)
    raise RuntimeError("BLOCKED: capture completeness/barrier conditions not proven")


def spark_env(database):
    env = os.environ.copy()
    secrets = dotenv_values(ROOT / "cdc/.env")
    password = env.get("ORACLE_CDC_PASSWORD") or secrets.get("ORACLE_CDC_PASSWORD")
    assert password, "Missing read-only Oracle CDC credential"
    env.update(ORACLE_USER="C##HDG_CDC", ORACLE_PASSWORD=password,
               ORACLE_JDBC_URL="jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
               PG_USER="sink_test_owner", PG_PASSWORD=storage.password(),
               PG_JDBC_URL="jdbc:postgresql://localhost:15433/" + database,
               PYSPARK_PYTHON=PYTHON, PYSPARK_DRIVER_PYTHON=PYTHON)
    import pyspark
    env["SPARK_HOME"] = str(Path(pyspark.__file__).parent)
    jars = [ROOT / "jobs/jars/ojdbc11.jar", ROOT / "jobs/jars/postgresql-42.7.14.jar"]
    assert all(p.is_file() for p in jars)
    return env


def run_spark(arguments, env, log):
    # Giống launcher hiện có: gọi spark-submit trực tiếp, tránh gateway .cmd lồng nhau trên Windows.
    import pyspark
    submit = Path(pyspark.__file__).parent / "bin/spark-submit.cmd"
    jars = ";".join((ROOT/p).as_posix() for p in ["jobs/jars/ojdbc11.jar","jobs/jars/postgresql-42.7.14.jar"])
    options = ["--master","local[2]","--driver-memory","1g","--driver-class-path",jars,
               "--conf","spark.executor.extraClassPath="+jars,"--conf","spark.jars=",
               "--conf","spark.driver.host=127.0.0.1","--conf","spark.driver.bindAddress=127.0.0.1"]
    child = env.copy()
    child.pop("PYSPARK_SUBMIT_ARGS", None)
    child["JAVA_HOME"] = str(Path(child["JAVA_HOME"]).resolve())
    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"
    command = "& " + literal(submit) + " " + " ".join(literal(v) for v in options+arguments)
    with log.open("w", encoding="utf-8") as handle:
        result = subprocess.run(["powershell.exe","-NoProfile","-Command",command],env=child,cwd=ROOT,
                                stdout=handle,stderr=subprocess.STDOUT,timeout=720)
    if result.returncode:
        raise RuntimeError("Spark failed; inspect private log " + str(log.resolve().relative_to(ROOT)))


def spark_check(scn, output):
    # Dùng lại projection/canonical comparison của reconcile; namespace test cố định,
    # không nới điều kiện namespace hoặc schema guard của job production.
    from spark_common import create_spark, settings, source_schema, read_source, read_jdbc, transport_projection, reconcile_frames
    from schema_preflight import assert_source_schema
    from spark_common import jdbc_connection
    config = settings()
    assert re.fullmatch(r"jdbc:postgresql://localhost:15433/hdg_full_e2e_[0-9a-f]{8}_test", config["pg_url"])
    database = config["pg_url"].rsplit("/", 1)[1]
    contract.DATABASE = storage.DATABASE = database
    with storage.connect_test() as connection:
        storage.assert_target(connection)
    spark = create_spark("HDG complete test Mirror reconciliation")
    try:
        with jdbc_connection(spark, config, "oracle") as oracle:
            assert_source_schema(oracle, source_schema())
        results = {}
        for name, table in source_schema().items():
            left = read_source(spark, config, name, table, scn)
            right = read_jdbc(spark, config, "pg",
                "SELECT " + transport_projection(table, "pg") + " FROM mirror_test." + table["mirror"])
            results[name] = reconcile_frames(left, right, table)
        dump(output, results)
        assert all(r["equal"] for r in results.values()), "Full-row reconciliation failed"
    finally:
        spark.stop()


def resume_run(output):
    # Tiếp tục bằng snapshot/offset đã chốt; không khóa writer, không phát DML hoặc reset DB.
    output = output.resolve()
    assert output.is_relative_to((ROOT / "tests/cdc_sink/test-results").resolve())
    report = json.loads((output/"result.json").read_text(encoding="utf-8"))
    document = json.loads((output/"manifest.json").read_text(encoding="utf-8"))
    assert report["barrier"] == report["full_load"] == report["stage_reconcile"] == "PASS"
    database = report["database"]
    assert re.fullmatch(r"hdg_full_e2e_[0-9a-f]{8}_test", database)
    assert document["baseline"]["run_id"] == report["run_id"]
    assert document["baseline"]["scn"] == report["snapshot_scn"]
    assert report["end_scn"] >= document["baseline"]["scn"]
    validate(document)
    if report.get("error"):
        report.setdefault("previous_errors", []).append(report.pop("error"))
    report["status"] = "RESUMING"
    dump(output/"result.json", report)
    contract.DATABASE = storage.DATABASE = database
    env = spark_env(database)
    connection = storage.connect_test()
    try:
        storage.assert_target(connection)
        records = read_ranges(document, "localhost:29092")
        found = check_records(document, records)
        epochs = connection.execute("SELECT count(*) FROM sink_meta.epochs").fetchone()[0]
        if epochs == 0:
            assert all(connection.execute(sql.SQL("SELECT count(*) FROM {}").format(storage.relation(t))).fetchone()[0] == 0 for t in contract.TABLES.values())
            wrong = copy.deepcopy(document)
            next(iter(wrong["baseline"]["tables"].values()))["stage_digest"] = "0"*64
            try:
                initialize(connection, wrong)
            except Rejected as error:
                assert error.code == "STAGE_BASELINE_AUDIT"
            else:
                raise AssertionError("Bad audit accepted")
            assert connection.execute("SELECT count(*) FROM sink_meta.epochs").fetchone()[0] == 0
            initialize(connection, document)
            report["bridge_bad_digest_rollback"] = "PASS"
        else:
            assert epochs == 1
        report["baseline_bridge"] = "PASS"
        dump(output/"result.json", report)
        print("Resume: Stage baseline bridge PASS", flush=True)
        # Tại run này fixture I/U/D net-zero; trước CDC vẫn kiểm toàn bộ snapshot S.
        if connection.execute("SELECT next_frame FROM sink_meta.epochs").fetchone()[0] == 0:
            run_spark([str(Path(__file__).relative_to(ROOT)), "--spark-check", str(report["snapshot_scn"]),
                       "--output", str(output/"baseline_reconcile.json")], env, output/"spark_baseline_reconcile.log")
        for number, frame in enumerate(document["frames"]):
            batch = [found[tuple(c)] for c in frame["records"]]
            outcome = apply_frame(connection, document, number, batch)
            assert outcome in ("APPLIED","REPLAY_FRAME")
        checkpoint = dict(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints").fetchall())
        assert all(checkpoint[s["topic"]] == s["end"] for s in document["streams"])
        fingerprint = digest(checkpoint)
        connection.close()
        connection = storage.connect_test()
        for number, frame in enumerate(document["frames"]):
            batch = [found[tuple(c)] for c in frame["records"]]
            assert apply_frame(connection, document, number, batch) == "REPLAY_FRAME"
        assert fingerprint == digest(dict(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints").fetchall()))
        assert connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0] == 12
        assert connection.execute("SELECT count(*) FROM sink_meta.transaction_ledger").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM sink_meta.inbox WHERE outcome='TOMBSTONE'").fetchone()[0] == 4
        assert connection.execute("SELECT count(*) FROM sink_meta.quarantine").fetchone()[0] == 0
        print("Resume: CDC apply and replay PASS; running full final reconciliation", flush=True)
        run_spark([str(Path(__file__).relative_to(ROOT)), "--spark-check", str(report["end_scn"]),
                   "--output", str(output/"final_reconcile.json")], env, output/"spark_final_reconcile.log")
        report.update(status="PASS", cdc_apply="PASS", replay="PASS", checkpoint=checkpoint,
            baseline_reconcile=json.loads((output/"baseline_reconcile.json").read_text()),
            final_reconcile=json.loads((output/"final_reconcile.json").read_text()),
            fixture_cleanup="PASS",scope_limit="Controlled local window, one net-zero fixture transaction; not live cutover")
        dump(output/"result.json", report)
        print(json.dumps({k:report[k] for k in ["status","database","run_id","snapshot_scn","end_scn","checkpoint"]}),flush=True)
    except BaseException as error:
        report.update(status="FAIL_OR_BLOCKED",error=type(error).__name__+": "+str(error))
        dump(output/"result.json", report)
        raise
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute-local", action="store_true")
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--spark-check", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.spark_check:
        spark_check(args.spark_check, args.output)
        return
    if not args.execute_local:
        parser.error("--execute-local required: holds Oracle table locks, creates fresh test DB and fixture-only DML")
    if args.resume:
        resume_run(args.resume)
        return
    stamp = uuid.uuid4().hex[:8]
    database = "hdg_full_e2e_" + stamp + "_test"
    out = ROOT / "tests/cdc_sink/test-results" / ("full-" + stamp)
    out.mkdir(parents=True)
    report = dict(status="STARTED", database=database, scope="local integration only", live_cutover=False)
    dump(out/"result.json", report)
    print("Evidence:", out.relative_to(ROOT), flush=True)
    fence = None
    connection = None
    consumer = None
    try:
        # BƯỚC 1: Kiểm endpoint, tạo DB mới; SQL/Stage/publisher chỉ cài trong DB này.
        prepare_metrics_reader(out)
        cfg = api("config")
        assert cfg["database.hostname"] == "hdg-c360-oracle" and cfg["database.pdb.name"] == "FREEPDB1"
        assert cfg["snapshot.mode"] == "no_data" and cfg["provide.transaction.metadata"] == "true"
        with storage.connect_test() as admin_connection:
            admin_connection.execute(sql.SQL("CREATE DATABASE {} OWNER sink_test_owner").format(sql.Identifier(database)))
        contract.DATABASE = storage.DATABASE = database
        connection = storage.connect_test()
        for path in ["sql/postgres/01_create_mirror_tables.sql", "sql/postgres/02_publish_full_load.sql"]:
            connection.execute((ROOT/path).read_text(encoding="utf-8"))
        storage.bootstrap(connection)
        env = spark_env(database)
        admin = AdminClient({"bootstrap.servers":"localhost:29092","broker.address.family":"v4"})
        consumer = Consumer({"bootstrap.servers":"localhost:29092","broker.address.family":"v4",
            "group.id":"hdg-full-readonly-"+stamp,"enable.auto.commit":False,"enable.auto.offset.store":False,
            "allow.auto.create.topics":False})
        # BƯỚC 2: Khóa độc quyền cả bốn bảng NOWAIT chứng minh không có writer chưa commit
        # trên chúng và ngăn writer khác trong thời gian chốt snapshot/Full Load.
        fence = Fence()
        identity = fence.value("(select name from v$database)||':'||sys_context('USERENV','CON_NAME')")
        assert identity == "FREE:FREEPDB1"
        fence.lock()
        report["writer_fence"] = fence.send(
            "select 'LOCK='||o.object_name||':'||lo.locked_mode from v$locked_object lo join dba_objects o on o.object_id=lo.object_id "
            "where lo.session_id=sys_context('USERENV','SID') and o.owner='VSS';")
        assert len([r for r in report["writer_fence"] if r.endswith(":6")]) == 4
        assert int(fence.value("(select count(*) from dba_triggers where table_owner='VSS' and status='ENABLED')")) == 0
        scn = int(fence.value("DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER"))
        incarnation = fence.value("(select dbid||':'||resetlogs_change# from v$database)")
        report["snapshot_scn"] = scn
        vector, samples, historical_transactions = capture_barrier(scn, admin, consumer, incarnation)
        report.update(barrier_samples=samples, start_vector=vector,
                      historical_transactions_verified=historical_transactions, barrier="PASS")
        dump(out/"result.json", report)
        print("Writer fence and capture barrier PASS, SCN", scn, flush=True)
        # BƯỚC 3: Spark/JDBC nạp snapshot mới; run/Stage cũ không được sử dụng hoặc sửa.
        run_spark(["jobs/full_load.py","--stage-only","--source-scn",str(scn)],env,out/"spark_full_load.log")
        runs = connection.execute("SELECT run_id,source_scn,status FROM hdg_load.runs").fetchall()
        assert len(runs)==1 and runs[0][1:]==(scn,"VALIDATED")
        run_id = runs[0][0]
        run_spark(["jobs/reconcile.py","--run-id",run_id,"--target","stage","--source","snapshot"],
                  env,out/"spark_stage_reconcile.log")
        audits = connection.execute(
            "SELECT source_table,source_rows,stage_digest FROM hdg_load.table_runs WHERE run_id=%s",(run_id,)).fetchall()
        assert len(audits)==4
        baseline = dict(assertion="verified-test-baseline",kind="spark-stage-v2",scn=scn,run_id=run_id,
                        tables={name:dict(schema=row_schema(name),row_count=count,stage_digest=h)
                                for name,count,h in audits})
        report.update(run_id=run_id,baseline_counts={n:c for n,c,h in audits},full_load="PASS",stage_reconcile="PASS")
        dump(out/"result.json",report)
        print("Spark Full Load and Stage reconcile PASS", report["baseline_counts"],flush=True)
        # BƯỚC 4: Chỉ tạo fixture mới, I/U/D cả bốn bảng trong một transaction có kiểm đếm.
        marker = "E2E-FULL-" + stamp
        ident = -(1000000000000000 + int(stamp,16))
        assert all(int(fence.value("(select count(*) from VSS."+name+" where ID="+(
            "'"+marker+"'" if name=="VSS_HO_GD_NHANKHAU" else str(ident))+")"))==0 for name in contract.TABLES)
        operations = [
            f"insert into VSS.VSS_HO_GD_NHANKHAU(ID,MA_CD,MA_HO_GD,HO_TEN,GHI_CHU,TYPEID,TRANG_THAI_DUYET) values('{marker}','{marker}',NULL,'{marker}','{marker}','CD',2)",
            f"update VSS.VSS_HO_GD_NHANKHAU set HO_TEN='{marker}-U' where ID='{marker}' and MA_CD='{marker}' and GHI_CHU='{marker}'",
            f"delete from VSS.VSS_HO_GD_NHANKHAU where ID='{marker}' and MA_CD='{marker}' and GHI_CHU='{marker}' and HO_TEN='{marker}-U'",
            f"insert into VSS.VSS_DM_DANTOC(ID,CAN_CU,MA,NGAY_HL,TEN) values({ident},'{marker}','ZX',TO_DATE('2026-11-11 12:00:01','YYYY-MM-DD HH24:MI:SS'),'{marker}')",
            f"update VSS.VSS_DM_DANTOC set TEN='{marker}-U' where ID={ident} and CAN_CU='{marker}' and TEN='{marker}'",
            f"delete from VSS.VSS_DM_DANTOC where ID={ident} and CAN_CU='{marker}' and TEN='{marker}-U'",
            f"insert into VSS.VSS_DM_QUOC_TICH(ID,MA,TEN) values({ident},'ZX','{marker}')",
            f"update VSS.VSS_DM_QUOC_TICH set TEN='{marker}-U' where ID={ident} and MA='ZX' and TEN='{marker}'",
            f"delete from VSS.VSS_DM_QUOC_TICH where ID={ident} and MA='ZX' and TEN='{marker}-U'",
            f"insert into VSS.VSS_DM_DBHC(ID,MA_DBHC,TEN,NGAY_HIEU_LUC) values({ident},'99998','{marker}',TIMESTAMP '2026-11-11 12:00:01.123456')",
            f"update VSS.VSS_DM_DBHC set TEN='{marker}-U' where ID={ident} and MA_DBHC='99998' and TEN='{marker}'",
            f"delete from VSS.VSS_DM_DBHC where ID={ident} and MA_DBHC='99998' and TEN='{marker}-U'"]
        block = "begin\n" + "\n".join(op+";\nif SQL%ROWCOUNT<>1 then raise_application_error(-20001,'Fixture row count'); end if;" for op in operations)+"\nend;\n/"
        fence.send(block)
        # Không commit nếu có writer khác đang chờ khóa: không để họ chen vào cửa sổ demo.
        waiters = int(fence.value("(select count(*) from v$session where blocking_session=sys_context('USERENV','SID'))"))
        assert waiters == 0, "BLOCKED: another writer is waiting"
        fence.send("commit;")
        fence.lock()
        end_scn = int(fence.value("DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER"))
        final_vector, end_samples, _ = capture_barrier(end_scn, admin, consumer, incarnation)
        # capture_barrier ở đầu/cuối xác nhận trọn lịch sử <= SCN tương ứng; không reset offsets.
        report.update(end_scn=end_scn,end_vector=final_vector,end_barrier_samples=end_samples)
        streams = [dict(topic=n,topic_id=final_vector[n]["topic_id"],partition=0,start=v["end"],
                        end=final_vector[n]["end"]) for n,v in vector.items()]
        assert all(s["end"]-s["start"]==(2 if s["topic"].endswith(".transaction") else 4) for s in streams)
        records = read_ranges({"streams":streams},"localhost:29092")
        decoded = [(r,decode(r,PREFIX,incarnation)) for r in records]
        dml = sorted([(r,e) for r,e in decoded if e["kind"]=="dml"],key=lambda pair:pair[1]["order"])
        assert len(dml)==12 and len({e["txid"] for r,e in dml})==1
        assert all(e["commit_scn"]>scn for r,e in dml)
        begin = [r for r,e in decoded if e["kind"]=="control" and e["payload"]["status"]=="BEGIN"]
        finish = [r for r,e in decoded if e["kind"]=="control" and e["payload"]["status"]=="END"]
        tombstones = [r for r,e in decoded if e["kind"]=="tombstone"]
        assert len(begin)==len(finish)==1 and len(tombstones)==4
        frame_records = begin+[r for r,e in dml]+finish+tombstones
        document = dict(format="hdg-sink-prototype-v1",mode="captured-fixture-test",
            identity_policy="experimental-redo-v1",epoch=str(uuid.uuid4()),contract_hash=contract.CONTRACT_HASH,
            source_incarnation=incarnation,topic_prefix=PREFIX,baseline=baseline,streams=streams,
            frames=[dict(txid=dml[0][1]["txid"],event_count=12,records=[list(r.coordinate) for r in frame_records])])
        validate(document);check_records(document,records)
        dump(out/"manifest.json",document)
        (out/"records.jsonl").write_text("\n".join(json.dumps(r.wire()) for r in records)+"\n",encoding="utf-8")
        report["cdc_transaction"] = dict(txid=dml[0][1]["txid"],commit_scn=dml[0][1]["commit_scn"],events=12,tombstones=4)
        # BƯỚC 5: Negative test digest phải rollback; sau đó nạp baseline thật bằng cầu nối atomic.
        wrong = copy.deepcopy(document)
        next(iter(wrong["baseline"]["tables"].values()))["stage_digest"]="0"*64
        try:
            initialize(connection,wrong)
        except Rejected as error:
            assert error.code=="STAGE_BASELINE_AUDIT"
        else:
            raise AssertionError("Tampered Stage audit accepted")
        assert connection.execute("SELECT count(*) FROM sink_meta.epochs").fetchone()[0]==0
        assert all(connection.execute(sql.SQL("SELECT count(*) FROM {}").format(storage.relation(t))).fetchone()[0]==0 for t in contract.TABLES.values())
        initialize(connection,document)
        report["baseline_bridge"]="PASS"
        run_spark([str(Path(__file__).relative_to(ROOT)),"--spark-check",str(scn),"--output",str(out/"baseline_reconcile.json")],
                  env,out/"spark_baseline_reconcile.log")
        print("Audited baseline bridge and full baseline reconciliation PASS",flush=True)
        # BƯỚC 6: Áp DML, replay sau reconnect; so toàn bộ dữ liệu Oracle tại SCN cuối.
        assert apply_frame(connection,document,0,frame_records)=="APPLIED"
        before = digest(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints ORDER BY topic").fetchall())
        connection.close();connection=storage.connect_test()
        assert apply_frame(connection,document,0,frame_records)=="REPLAY_FRAME"
        assert before==digest(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints ORDER BY topic").fetchall())
        assert connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0]==12
        assert connection.execute("SELECT count(*) FROM sink_meta.transaction_ledger").fetchone()[0]==1
        assert connection.execute("SELECT count(*) FROM sink_meta.quarantine").fetchone()[0]==0
        checks = dict(connection.execute("SELECT topic,next_offset FROM sink_meta.checkpoints").fetchall())
        assert all(checks[s["topic"]]==s["end"] for s in streams)
        run_spark([str(Path(__file__).relative_to(ROOT)),"--spark-check",str(end_scn),"--output",str(out/"final_reconcile.json")],
                  env,out/"spark_final_reconcile.log")
        report.update(status="PASS",cdc_apply="PASS",replay="PASS",checkpoint=checks,
            baseline_reconcile=json.loads((out/"baseline_reconcile.json").read_text()),
            final_reconcile=json.loads((out/"final_reconcile.json").read_text()),
            bridge_bad_digest_rollback="PASS",fixture_cleanup="PASS",scope_limit="Single controlled local transaction, not live cutover")
        dump(out/"result.json",report)
        print(json.dumps(dict(status="PASS",database=database,run_id=run_id,snapshot_scn=scn,
                              end_scn=end_scn,cdc_transaction=report["cdc_transaction"],checkpoint=checks)),flush=True)
    except BaseException as error:
        report.update(status="FAIL_OR_BLOCKED",error=type(error).__name__+": "+str(error))
        dump(out/"result.json",report)
        raise
    finally:
        if fence: fence.close()
        if connection: connection.close()
        if consumer: consumer.close()


if __name__=="__main__":
    main()
