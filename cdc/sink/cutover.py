# Mục đích: Chuẩn bị, publish và rollback bốn Mirror cùng checkpoint; mặc định chỉ diễn tập clone.
# Đầu vào: plan đích tường minh, Stage đã audit, manifest hữu hạn và credential môi trường.
# Đầu ra: shadow/rollback giữ dữ liệu cũ; thay dữ liệu + epoch/control trong một transaction.
import argparse
import copy
import hashlib
import json
import os
import re
import subprocess
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import storage
import engine
from contract import TABLES, CONTRACT_HASH
from protocol import canonical, digest, Rejected, decode
from kafka_reader import read_ranges


def require(condition, reason):
    if not condition:
        raise Rejected(reason)


def validate_plan(plan):
    require(plan.get("format") == "hdg-cutover-v1", "CUTOVER_PLAN_FORMAT")
    uuid.UUID(plan["id"])
    target = plan["target"]
    clone = bool(re.fullmatch(r"hdg_cutover_clone_[0-9a-f]{8}_test", target["database"]))
    live = target["database"] == "hdg_mirror" and plan.get("purpose") == "live-recovery"
    require(target.get("host") == "127.0.0.1" and target.get("port") == 15432, "CUTOVER_TARGET")
    require((clone and plan.get("purpose") == "clone-rehearsal") or live, "CUTOVER_PURPOSE")
    require(plan["manifest"]["baseline"].get("kind") == "spark-stage-v2", "CUTOVER_STAGE_REQUIRED")
    require(plan["manifest"]["baseline"]["run_id"] == plan["run_id"], "CUTOVER_RUN")
    require(plan["manifest"]["mode"] == ("cutover-live" if live else "captured-fixture-test"), "CUTOVER_MODE")
    if live:
        require(plan.get("barrier", {}).get("kind") == "live-writer-fence-v1", "FRESH_LIVE_BARRIER_REQUIRED")
        require(plan["barrier"].get("destination") == "hdg_mirror", "LIVE_BARRIER_DESTINATION")
        require(plan["manifest"]["baseline"]["assertion"] == "verified-source-baseline", "LIVE_BASELINE_ASSERTION")
        expected={"hdg_observe.VSS."+name for name in TABLES}|{"hdg_observe.transaction"}
        require(plan["manifest"].get("topic_prefix")=="hdg_observe"
                and {s["topic"] for s in plan["manifest"]["streams"]}==expected, "LIVE_TOPIC_SCOPE")
    return "live" if live else "clone"


def live_gate(connection, plan, rollback_proof=None):
    # Không nhận barrier test: session Oracle hiện tại phải đang giữ đủ bốn khóa,
    # mang mã plan/SCN mới, và run live được tạo sau lúc session fence mở.
    require(validate_plan(plan) == "live", "LIVE_PLAN_REQUIRED")
    b = rollback_proof if rollback_proof is not None else plan["barrier"]
    if rollback_proof is not None:
        require(b.get("kind") == "live-rollback-fence-v1" and b.get("destination") == "hdg_mirror",
                "LIVE_ROLLBACK_FENCE_REQUIRED")
    scn = int(b["scn"]) if rollback_proof is not None else plan["manifest"]["baseline"]["scn"]
    sid, serial = int(b["sid"]), int(b["serial"])
    marker = ("HDG_ROLLBACK:" if rollback_proof is not None else "HDG_CUTOVER:") + plan["id"] + ":" + str(scn)
    query = f"""whenever sqlerror exit failure rollback
set heading off feedback off pages 0 lines 32767 trimspool on
select json_object('name' value o.object_name,'mode' value lo.locked_mode,'client_info' value s.client_info,
 'logon' value to_char(s.logon_time,'YYYY-MM-DD HH24:MI:SS'),
 'fresh_scn' value case when scn_to_timestamp({int(scn)})>=cast(s.logon_time as timestamp)-interval '30' second and scn_to_timestamp({int(scn)})>=cast(systimestamp at time zone 'UTC' as timestamp)-interval '30' minute then 1 else 0 end,
 'incarnation' value (select dbid||':'||resetlogs_change# from v$database))
from gv$locked_object lo join gv$session s on s.inst_id=lo.inst_id and s.sid=lo.session_id
join cdb_objects o on o.object_id=lo.object_id and o.con_id=lo.con_id
where s.sid={sid} and s.serial#={serial} and o.owner='VSS'
and o.object_name in ('VSS_HO_GD_NHANKHAU','VSS_DM_DANTOC','VSS_DM_QUOC_TICH','VSS_DM_DBHC');
exit
"""
    result = subprocess.run(["docker","exec","-i","hdg-c360-oracle","sqlplus","-L","-s","/","as","sysdba"],
                            input=query,text=True,capture_output=True,timeout=30)
    require(result.returncode == 0, "LIVE_FENCE_QUERY")
    rows = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    require(len(rows) == 4 and {r["name"] for r in rows} == set(TABLES), "LIVE_WRITER_FENCE_MISSING")
    require(all(r["mode"] == 6 and r["client_info"] == marker and r["fresh_scn"] == 1 for r in rows), "LIVE_FENCE_NOT_CURRENT")
    require(all(r["incarnation"] == plan["manifest"]["source_incarnation"] for r in rows), "SOURCE_INCARNATION")
    started = connection.execute("SELECT started_at FROM hdg_load.runs WHERE run_id=%s",(plan["run_id"],)).fetchone()
    require(rollback_proof is not None or (started and started[0] >= datetime.strptime(rows[0]["logon"],"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)),
            "LIVE_RUN_MUST_BE_CREATED_UNDER_CURRENT_FENCE")
    m = subprocess.run(["docker","exec","hdg-cdc-connect-1","java","--add-modules","jdk.attach","-cp","/tmp","ReadCaptureMetrics","1"],
                       capture_output=True,text=True,timeout=20)
    require(m.returncode == 0, "LIVE_CAPTURE_METRICS")
    metrics = dict(line.split("=",1) for line in m.stdout.splitlines() if "=" in line)
    require(metrics.get("Connected") == "true" and metrics.get("NumberOfActiveTransactions") == "0"
            and metrics.get("CurrentQueueSizeInBytes") == "0"
            and metrics.get("QueueRemainingCapacity") == metrics.get("QueueTotalCapacity")
            and metrics.get("AbandonedTransactionCount") == "0" and metrics.get("ErrorCount") == "0"
            and metrics.get("NumberOfOversizedTransactions") == "0" and int(metrics.get("OffsetScn","0")) >= scn,
            "LIVE_CAPTURE_NOT_DRAINED")
    status = json.load(urlopen("http://localhost:8083/connectors/hdg-oracle-observe/status",timeout=10))
    require(status["connector"]["state"]=="RUNNING" and status["tasks"]
            and all(t["state"]=="RUNNING" for t in status["tasks"]), "LIVE_CONNECTOR_NOT_RUNNING")
    config = json.load(urlopen("http://localhost:8083/connectors/hdg-oracle-observe/config",timeout=10))
    require(config.get("database.hostname")=="hdg-c360-oracle" and config.get("database.pdb.name")=="FREEPDB1"
            and config.get("provide.transaction.metadata")=="true"
            and config.get("topic.prefix")=="hdg_observe" and config.get("snapshot.mode")=="no_data"
            and all(name in config.get("table.include.list","") for name in TABLES), "LIVE_CAPTURE_CONFIG")
    offset = json.load(urlopen("http://localhost:8083/connectors/hdg-oracle-observe/offsets",timeout=10))
    require(int(offset["offsets"][0]["offset"]["scn"]) >= scn, "LIVE_SOURCE_OFFSET_NOT_FLUSHED")
    # Lúc publish live chỉ nhận frontier rỗng sau snapshot; dữ liệu sau đó được append có kiểm soát.
    require(rollback_proof is not None or not plan["manifest"]["frames"], "LIVE_INITIAL_HANDOFF_MUST_BE_EMPTY")
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import Consumer, TopicPartition, TopicCollection
    admin = AdminClient({"bootstrap.servers":"localhost:29092","broker.address.family":"v4"})
    streams = b["streams"] if rollback_proof is not None else plan["manifest"]["streams"]
    require({(s["topic"],s["partition"],s["topic_id"]) for s in streams} ==
            {(s["topic"],s["partition"],s["topic_id"]) for s in plan["manifest"]["streams"]}, "LIVE_VECTOR_INCOMPLETE")
    require(scn>=plan["manifest"]["baseline"]["scn"], "LIVE_FENCE_SCN_REGRESSION")
    topics = admin.describe_topics(TopicCollection([s["topic"] for s in streams]))
    consumer = Consumer({"bootstrap.servers":"localhost:29092","broker.address.family":"v4","group.id":"hdg-live-gate-"+uuid.uuid4().hex,
                         "enable.auto.commit":False,"enable.auto.offset.store":False,"allow.auto.create.topics":False})
    try:
        for s in streams:
            require(s["start"] == s["end"], "LIVE_VECTOR_NOT_EMPTY")
            lo,hi = consumer.get_watermark_offsets(TopicPartition(s["topic"],s["partition"]),timeout=10)
            require(hi == s["start"] and lo <= hi and str(topics[s["topic"]].result(timeout=10).topic_id) == s["topic_id"],
                    "LIVE_FRONTIER_CHANGED")
    finally:
        consumer.close()
    proof = json.loads(Path(plan["rehearsal_proof"]).read_text(encoding="utf-8"))
    checks=("crash_mid_publish_rollback","crash_after_commit_retry","ordering_blocks_without_data_change",
            "rollback_restores_original_data_acl_oid","sink_crash_rollback","append_failure_rollback",
            "append_handoff","replay_idempotent")
    require(proof.get("status") == "PASS" and proof.get("owner_acl_verified") is True
            and proof.get("column_and_schema_acl_verified") is True
            and all(proof.get(k)=="PASS" for k in checks), "REHEARSAL_REQUIRED")
    backup = Path(plan["backup"])
    require(hashlib.sha256(backup.read_bytes()).hexdigest() == proof.get("backup_sha256"), "REHEARSAL_BACKUP_MISMATCH")
    if rollback_proof is None:
        require(table_hashes(connection) == proof["original_data"], "LIVE_BACKUP_STALE")
    return dict(locks=rows,metrics=metrics,source_offset=offset,
                observed_at=datetime.now(timezone.utc).isoformat())


@contextmanager
def target(connection, plan, confirmation, command, rollback_proof=None):
    purpose = validate_plan(plan)
    checked = False
    if purpose == "live":
        if command in ("prepare","apply","rollback"):
            require(command != "rollback" or rollback_proof is not None, "LIVE_ROLLBACK_FENCE_REQUIRED")
            live_gate(connection,plan,rollback_proof)
        else:
            # Replay chỉ được vào epoch đã cutover; không dùng nó để tự arm live.
            row = connection.execute("SELECT state FROM hdg_load.cutovers WHERE id=%s",(plan["id"],)).fetchone()
            require(row and (row[0] == "ACTIVE" or command == "status"), "LIVE_NOT_ACTIVATED")
        checked = True
    with storage.cutover_target(connection,plan["target"]["database"],confirmation,purpose,checked):
        engine.validate(plan["manifest"])
        yield


def locks(connection):
    connection.execute("SET LOCAL lock_timeout='10s'")
    connection.execute("SET LOCAL statement_timeout='180s'")
    connection.execute("SELECT pg_advisory_xact_lock(784027,1)")
    connection.execute("SELECT pg_advisory_xact_lock(631874,1)")
    # Control trước Mirror, cùng thứ tự worker C360, tránh deadlock khi cutover chờ NRT.
    if connection.execute("SELECT to_regclass('c360.c360_sync_control')").fetchone()[0] is not None:
        connection.execute("SELECT control_key FROM c360.c360_sync_control WHERE control_key='NORMALIZE' FOR UPDATE")
    for t in TABLES.values():
        connection.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(storage.relation(t)))


def table_hashes(connection, namespace="public"):
    require(namespace == "public" or bool(re.fullmatch(r"hdg_(?:cutover|rollback)_[0-9a-f]{32}",namespace)), "HASH_NAMESPACE")
    result = {}
    for t in TABLES.values():
        count,h = connection.execute(sql.SQL(
            "SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(h,'' ORDER BY h),''),'UTF8')),'hex') "
            "FROM (SELECT encode(sha256(convert_to(row_to_json(t)::text,'UTF8')),'hex') h FROM {} t) q"
            ).format(sql.Identifier(namespace,t["mirror"]))).fetchone()
        result[t["mirror"]] = dict(rows=count,sha256=h)
    return result


def ownership(connection):
    return connection.execute(
        "SELECT c.relname,c.oid::bigint,pg_get_userbyid(c.relowner),c.relacl::text,c.relrowsecurity,c.relforcerowsecurity "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='public' AND c.relname=ANY(%s) ORDER BY c.relname",
        ([t["mirror"] for t in TABLES.values()],)).fetchall()


def safe_schema(connection):
    connection.execute("SELECT hdg_load.assert_mirror_schema()")
    for t in TABLES.values():
        unsafe = connection.execute(
            "SELECT (SELECT count(*) FROM pg_trigger WHERE tgrelid=%s::regclass AND NOT tgisinternal)"
            "+(SELECT count(*) FROM pg_constraint WHERE contype='f' AND (conrelid=%s::regclass OR confrelid=%s::regclass))"
            "+(SELECT count(*) FROM pg_class WHERE oid=%s::regclass AND (relrowsecurity OR relforcerowsecurity))",
            ("public."+t["mirror"],)*4).fetchone()[0]
        unsafe -= storage.allowed_c360_trigger(connection,"public."+t["mirror"])
        require(unsafe == 0,"CUTOVER_HAS_UNSUPPORTED_TRIGGER_FK_RLS")


def bootstrap(connection):
    # Chỉ gọi trong đích đã được arm; không chiếm schema không rõ nguồn gốc.
    exists = connection.execute("SELECT to_regclass('sink_meta.guard')").fetchone()[0]
    if exists is None:
        require(connection.execute("SELECT count(*) FROM pg_namespace WHERE nspname='sink_meta'").fetchone()[0] == 0,
                "UNKNOWN_SINK_SCHEMA")
        connection.execute(storage.ddl(metadata_only=True))
        connection.execute("INSERT INTO sink_meta.guard VALUES(true,%s,%s)",(storage.CUTOVER_TOKEN,CONTRACT_HASH))
    storage.assert_target(connection)
    connection.execute("""CREATE TABLE IF NOT EXISTS hdg_load.cutovers(
        id uuid PRIMARY KEY, plan jsonb NOT NULL, state text NOT NULL CHECK(state IN ('PREPARED','ACTIVE','ROLLED_BACK')),
        shadow_schema text NOT NULL, old_schema text NOT NULL, before_data jsonb NOT NULL,
        before_ownership jsonb NOT NULL, before_control jsonb NOT NULL, shadow_data jsonb NOT NULL,
        last_data jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp())""")


def copy_acl(connection, src, dst, table):
    # Public table không bị ALTER OWNER/GRANT. Bản giữ dữ liệu cũ nhận cùng owner và ACL hiệu lực.
    owner = connection.execute("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=%s::regclass",
                               (src+"."+table,)).fetchone()[0]
    connection.execute(sql.SQL("ALTER TABLE {} OWNER TO {}").format(sql.Identifier(dst,table),sql.Identifier(owner)))
    grants = connection.execute(
        "SELECT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,a.privilege_type,a.is_grantable "
        "FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a "
        "WHERE c.oid=%s::regclass",(src+"."+table,)).fetchall()
    grantees = connection.execute(
        "SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END "
        "FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a "
        "WHERE c.oid=%s::regclass",(dst+"."+table,)).fetchall()
    for (role,) in grantees:
        connection.execute(sql.SQL("REVOKE ALL ON {} FROM {}").format(sql.Identifier(dst,table),
                           sql.SQL("PUBLIC") if role=="PUBLIC" else sql.Identifier(role)))
    for role,priv,grantable in grants:
        require(priv in ("SELECT","INSERT","UPDATE","DELETE","TRUNCATE","REFERENCES","TRIGGER"),"ACL_PRIVILEGE")
        connection.execute(sql.SQL("GRANT {} ON {} TO {}{}").format(sql.SQL(priv),sql.Identifier(dst,table),
                           sql.SQL("PUBLIC") if role=="PUBLIC" else sql.Identifier(role),
                           sql.SQL(" WITH GRANT OPTION") if grantable else sql.SQL("")))


def prepare(connection, plan):
    with connection.transaction():
        locks(connection)
        safe_schema(connection)
        bootstrap(connection)
        require(not connection.execute("SELECT 1 FROM hdg_load.cutovers WHERE state='ACTIVE'").fetchone(),"CUTOVER_ALREADY_ACTIVE")
        require(not connection.execute("SELECT 1 FROM sink_meta.epochs WHERE state='ACTIVE'").fetchone(),"SINK_EPOCH_ACTIVE")
        require(not connection.execute("SELECT 1 FROM hdg_load.cutovers WHERE id=%s",(plan["id"],)).fetchone(),"PLAN_EXISTS")
        old = "hdg_rollback_"+uuid.UUID(plan["id"]).hex
        shadow = "hdg_cutover_"+uuid.UUID(plan["id"]).hex
        before = table_hashes(connection)
        meta = ownership(connection)
        control = connection.execute("SELECT row_to_json(c) FROM hdg_load.control c WHERE id=1 FOR UPDATE").fetchone()[0]
        require(not control["cdc_enabled"], "EXISTING_CDC_WRITER_REQUIRES_HANDOFF")
        for namespace in (old,shadow):
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
            for t in TABLES.values():
                connection.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
                    sql.Identifier(namespace,t["mirror"]),storage.relation(t)))
                copy_acl(connection,"public",namespace,t["mirror"])
        for t in TABLES.values():
            connection.execute(sql.SQL("INSERT INTO {} SELECT * FROM {}").format(sql.Identifier(old,t["mirror"]),storage.relation(t)))
        require(table_hashes(connection,old) == before,"ROLLBACK_COPY_MISMATCH")
        engine._initialize_stage(connection,plan["manifest"],destination=shadow)
        seeded = table_hashes(connection,shadow)
        connection.execute("INSERT INTO hdg_load.cutovers(id,plan,state,shadow_schema,old_schema,before_data,before_ownership,"
            "before_control,shadow_data,last_data) VALUES(%s,%s,'PREPARED',%s,%s,%s,%s,%s,%s,%s)",
            (plan["id"],Jsonb(plan),shadow,old,Jsonb(before),Jsonb(canonical(meta)),Jsonb(control),Jsonb(seeded),Jsonb(before)))
    return "PREPARED"


def record(connection, plan):
    row = connection.execute("SELECT plan,state,shadow_schema,old_schema,before_data,before_ownership,before_control,"
                             "shadow_data,last_data FROM hdg_load.cutovers WHERE id=%s FOR UPDATE",(plan["id"],)).fetchone()
    require(row is not None and digest(row[0]) == digest(plan),"CUTOVER_PLAN_MISMATCH")
    return row



def assert_handoff(connection, plan, allow_blocked=False):
    document = plan["manifest"]
    control=connection.execute("SELECT cdc_enabled,last_run_id FROM hdg_load.control WHERE id=1 FOR SHARE").fetchone()
    require(control and control[1]==plan["run_id"] and (control[0] or allow_blocked), "HANDOFF_CONTROL")
    row = connection.execute("SELECT state,next_frame,manifest_hash FROM sink_meta.epochs WHERE epoch=%s",
                             (document["epoch"],)).fetchone()
    require(row and (row[0]=="ACTIVE" or allow_blocked and row[0]=="BLOCKED")
            and row[2]==engine.manifest_hash(document), "HANDOFF_EPOCH")
    require(0<=row[1]<=len(document["frames"]), "HANDOFF_FRAME")
    expected={(s["topic"],s["partition"]):[s["topic_id"],s["start"]] for s in document["streams"]}
    for frame in document["frames"][:row[1]]:
        for topic, partition, offset in frame["records"]:
            item=expected[(topic,partition)]
            require(item[1]==offset, "HANDOFF_PREFIX_GAP")
            item[1]+=1
    actual={(t,p):[tid,n] for t,tid,p,n in checkpoints(connection,document["epoch"])}
    require(actual==expected, "HANDOFF_CHECKPOINT")
    return row

def apply(connection, plan, failpoint=None):
    with connection.transaction():
        locks(connection);safe_schema(connection);storage.assert_target(connection)
        row = record(connection,plan)
        if row[1] == "ACTIVE":
            assert_handoff(connection,plan)
            require(canonical(ownership(connection))==row[5],"OWNERSHIP_ACL_OR_OID_CHANGED")
            control=connection.execute("SELECT cdc_enabled,last_run_id FROM hdg_load.control WHERE id=1").fetchone()
            require(control==(True,plan["run_id"]), "HANDOFF_CONTROL")
            require(table_hashes(connection)==row[8],"ACTIVE_MIRROR_CHANGED")
            return "ALREADY_ACTIVE"
        require(row[1]=="PREPARED","CUTOVER_STATE")
        require(canonical(ownership(connection))==row[5],"OWNERSHIP_ACL_OR_OID_CHANGED")
        require(table_hashes(connection)==row[4],"MIRROR_CHANGED_SINCE_PREPARE")
        require(table_hashes(connection,row[2])==row[7] and table_hashes(connection,row[3])==row[4],"PREPARED_DATA_CHANGED")
        require(not connection.execute("SELECT 1 FROM sink_meta.epochs WHERE state='ACTIVE'").fetchone(),"SINK_EPOCH_ACTIVE")
        for i,t in enumerate(TABLES.values()):
            connection.execute(sql.SQL("DELETE FROM {}").format(storage.relation(t)))
            if failpoint and i==0: failpoint("after_first_delete")
            connection.execute(sql.SQL("INSERT INTO {} SELECT * FROM {}").format(storage.relation(t),sql.Identifier(row[2],t["mirror"])))
        engine._register_epoch(connection,plan["manifest"])
        connection.execute("UPDATE hdg_load.control SET cdc_enabled=true,bootstrap_published=true,last_run_id=%s WHERE id=1",(plan["run_id"],))
        require(table_hashes(connection)==row[7] and canonical(ownership(connection))==row[5],"PUBLISH_RECONCILE")
        connection.execute("UPDATE hdg_load.cutovers SET state='ACTIVE',last_data=shadow_data WHERE id=%s",(plan["id"],))
        if failpoint: failpoint("before_commit")
    if failpoint: failpoint("after_commit")
    return "ACTIVE"


def checkpoints(connection, epoch):
    return connection.execute("SELECT topic,topic_id,partition,next_offset FROM sink_meta.checkpoints WHERE epoch=%s "
                              "ORDER BY topic,partition",(epoch,)).fetchall()



def begin_frame(connection, plan):
    # Khóa writer khác trước khi băm/ghi; không được nhận thay đổi ngoài Sink vào mốc rollback.
    assert_handoff(connection,plan)
    for t in TABLES.values():
        connection.execute(sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(storage.relation(t)))
    row=record(connection,plan)
    require(row[1]=="ACTIVE" and table_hashes(connection)==row[8], "MIRROR_EXTERNAL_CHANGE")
    require(canonical(ownership(connection))==row[5], "OWNERSHIP_CHANGED")

def replay(connection, plan, frame=None, failpoint=None):
    storage.assert_target(connection)
    row = connection.execute("SELECT plan,state FROM hdg_load.cutovers WHERE id=%s",(plan["id"],)).fetchone()
    require(row and row[1]=="ACTIVE" and digest(row[0])==digest(plan),"CUTOVER_NOT_ACTIVE")
    document = plan["manifest"]
    assert_handoff(connection,plan)
    records = read_ranges(document,"localhost:29092")
    found = engine.check_records(document,records)
    outcomes = []
    for i in (range(len(document["frames"])) if frame is None else [frame]):
        require(0<=i<len(document["frames"]),"FRAME_INDEX")
        batch = [found[tuple(c)] for c in document["frames"][i]["records"]]
        def seal(c):
            assert_handoff(c,plan)
            require(ownership(c)==[tuple(x) for x in connection.execute(
                "SELECT before_ownership FROM hdg_load.cutovers WHERE id=%s",(plan["id"],)).fetchone()[0]],"OWNERSHIP_CHANGED")
            c.execute("UPDATE hdg_load.cutovers SET last_data=%s WHERE id=%s",(Jsonb(table_hashes(c)),plan["id"]))
        outcomes.append(engine.apply_frame(connection,document,i,batch,failpoint=failpoint,before_commit=seal,
                                            before_apply=lambda c: begin_frame(c,plan)))
    return outcomes


def extend(connection, old, new, failpoint=None):
    require(old["id"]==new["id"] and old["target"]==new["target"] and old["purpose"]==new["purpose"],"EXTENSION_TARGET")
    a,b=old["manifest"],new["manifest"]
    require({k:v for k,v in a.items() if k not in ("frames","streams")}==
            {k:v for k,v in b.items() if k not in ("frames","streams")},"EXTENSION_BASELINE")
    require(len(b["frames"])>len(a["frames"]) and b["frames"][:len(a["frames"])]==a["frames"],"EXTENSION_PREFIX")
    require(len(a["streams"])==len(b["streams"]),"EXTENSION_STREAMS")
    for x,y in zip(a["streams"],b["streams"]):
        require({k:v for k,v in x.items() if k!="end"}=={k:v for k,v in y.items() if k!="end"} and y["end"]>=x["end"],"EXTENSION_RANGE")
    require({k:v for k,v in old.items() if k!="manifest"}=={k:v for k,v in new.items() if k!="manifest"},"EXTENSION_PLAN")
    engine.validate(b)
    found=engine.check_records(b,read_ranges(b,"localhost:29092"))
    # Kiểm đủ transaction mới trước khi đổi manifest; không làm epoch mắc vào frame lỗi đã biết.
    for spec in b["frames"][len(a["frames"]):]:
        events=[decode(found[tuple(c)],b["topic_prefix"],b["source_incarnation"]) for c in spec["records"]]
        tx=engine.transaction(events,spec,b["source_incarnation"])
        require(tx is None or tx["commit"]>b["baseline"]["scn"], "EVENT_NOT_AFTER_TEST_BASELINE")
    with connection.transaction():
        locks(connection);row=record(connection,old)
        assert_handoff(connection,old)
        require(row[1]=="ACTIVE","CUTOVER_NOT_ACTIVE")
        epoch=connection.execute("SELECT state FROM sink_meta.epochs WHERE epoch=%s FOR UPDATE",(a["epoch"],)).fetchone()
        require(epoch and epoch[0]=="ACTIVE","EPOCH_BLOCKED")
        cp=checkpoints(connection,a["epoch"])
        require({(t,p):(tid,n) for t,tid,p,n in cp}==
                {(s["topic"],s["partition"]):(s["topic_id"],s["end"]) for s in a["streams"]},"EXTENSION_NOT_AT_FRONTIER")
        require(table_hashes(connection)==row[8],"MIRROR_EXTERNAL_CHANGE")
        connection.execute("UPDATE sink_meta.epochs SET manifest_hash=%s,manifest=%s WHERE epoch=%s",
                           (engine.manifest_hash(b),Jsonb(canonical(b)),a["epoch"]))
        connection.execute("UPDATE hdg_load.cutovers SET plan=%s WHERE id=%s",(Jsonb(new),new["id"]))
        if failpoint:failpoint("before_commit")
    return "EXTENDED"


def rollback(connection, plan, failpoint=None):
    with connection.transaction():
        locks(connection);safe_schema(connection);storage.assert_target(connection);row=record(connection,plan)
        require(row[1]=="ACTIVE","ROLLBACK_STATE")
        assert_handoff(connection,plan,allow_blocked=True)
        require(table_hashes(connection)==row[8],"ROLLBACK_EXTERNAL_WRITES")
        require(canonical(ownership(connection))==row[5] and table_hashes(connection,row[3])==row[4],"ROLLBACK_EVIDENCE_CHANGED")
        for t in TABLES.values():
            connection.execute(sql.SQL("DELETE FROM {}").format(storage.relation(t)))
            connection.execute(sql.SQL("INSERT INTO {} SELECT * FROM {}").format(storage.relation(t),sql.Identifier(row[3],t["mirror"])))
        connection.execute("UPDATE sink_meta.epochs SET state='BLOCKED' WHERE epoch=%s",(plan["manifest"]["epoch"],))
        c=row[6]
        connection.execute("UPDATE hdg_load.control SET cdc_enabled=%s,bootstrap_published=%s,last_run_id=%s WHERE id=1",
                           (c["cdc_enabled"],c["bootstrap_published"],c["last_run_id"]))
        require(table_hashes(connection)==row[4] and canonical(ownership(connection))==row[5],"ROLLBACK_RECONCILE")
        connection.execute("UPDATE hdg_load.cutovers SET state='ROLLED_BACK',last_data=before_data WHERE id=%s",(plan["id"],))
        if failpoint:failpoint("before_commit")
    return "ROLLED_BACK"



def make_live_plan(connection, run_id, barrier, rehearsal_proof, backup):
    # Chỉ SELECT; lấy mapping/audit từ run trong hdg_mirror, không copy plan/SCN test.
    from fixtures import row_schema
    require(connection.info.host=="127.0.0.1" and connection.info.port==15432
            and connection.info.dbname=="hdg_mirror", "LIVE_PLAN_ENDPOINT")
    uuid.UUID(barrier["plan_id"])
    run=connection.execute("SELECT source_scn,status FROM hdg_load.runs WHERE run_id=%s",(run_id,)).fetchone()
    require(run and run[1]=="VALIDATED" and run[0]==barrier["scn"], "LIVE_PLAN_RUN")
    audits=connection.execute("SELECT source_table,source_rows,stage_rows,content_equal,stage_digest "
                             "FROM hdg_load.table_runs WHERE run_id=%s",(run_id,)).fetchall()
    require({a[0] for a in audits}==set(TABLES) and all(a[1]==a[2] and a[3] and a[4] for a in audits),"LIVE_PLAN_AUDIT")
    from confluent_kafka import Consumer,TopicPartition,TopicCollection
    from confluent_kafka.admin import AdminClient
    topics=["hdg_observe.VSS."+name for name in TABLES]+["hdg_observe.transaction"]
    admin=AdminClient({"bootstrap.servers":"localhost:29092","broker.address.family":"v4"})
    descriptions=admin.describe_topics(TopicCollection(topics))
    consumer=Consumer({"bootstrap.servers":"localhost:29092","broker.address.family":"v4",
        "group.id":"hdg-plan-readonly-"+uuid.uuid4().hex,"enable.auto.commit":False,
        "enable.auto.offset.store":False,"allow.auto.create.topics":False})
    streams=[]
    try:
        for topic in topics:
            description=descriptions[topic].result(timeout=10)
            require([p.id for p in description.partitions]==[0],"LIVE_PLAN_PARTITIONS")
            low,high=consumer.get_watermark_offsets(TopicPartition(topic,0),timeout=10)
            streams.append(dict(topic=topic,topic_id=str(description.topic_id),partition=0,start=high,end=high))
    finally:consumer.close()
    document=dict(format="hdg-sink-prototype-v1",mode="cutover-live",identity_policy="experimental-redo-v1",
        epoch=str(uuid.uuid4()),contract_hash=CONTRACT_HASH,source_incarnation=barrier["source_incarnation"],
        topic_prefix="hdg_observe",baseline=dict(assertion="verified-source-baseline",kind="spark-stage-v2",
            run_id=run_id,scn=int(run[0]),tables={a[0]:dict(schema=row_schema(a[0]),row_count=a[1],stage_digest=a[4]) for a in audits}),
        streams=streams,frames=[])
    plan=dict(format="hdg-cutover-v1",id=barrier["plan_id"],purpose="live-recovery",
        target=dict(host="127.0.0.1",port=15432,database="hdg_mirror"),run_id=run_id,manifest=document,
        barrier=barrier,rehearsal_proof=str(rehearsal_proof),backup=str(backup))
    validate_plan(plan)
    plan["barrier"]["evidence"]=live_gate(connection,plan)
    # Kiểm lại vector/fence lần hai; Source vẫn đứng yên nhờ khóa, không chọn end offset tùy tiện.
    live_gate(connection,plan)
    with storage.cutover_target(connection,"hdg_mirror","hdg_mirror","live",True):
        engine.validate(document)
    return plan

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command",choices=["make-plan","prepare","apply","replay","extend","rollback","status"])
    parser.add_argument("--plan",type=Path,required=True)
    parser.add_argument("--extension",type=Path)
    parser.add_argument("--confirm-database",required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--barrier",type=Path)
    parser.add_argument("--rehearsal-proof",type=Path)
    parser.add_argument("--backup",type=Path)
    parser.add_argument("--frame",type=int)
    parser.add_argument("--rollback-proof",type=Path)
    parser.add_argument("--failure-point")
    parser.add_argument("--crash",action="store_true")
    args=parser.parse_args()
    if args.command=="make-plan":
        require(args.confirm_database=="hdg_mirror" and args.run_id and args.barrier
                and args.rehearsal_proof and args.backup, "LIVE_PLAN_INPUTS")
        require(not args.plan.exists(), "PLAN_FILE_EXISTS")
        with psycopg.connect(host="127.0.0.1",port=15432,dbname="hdg_mirror",user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],autocommit=True,connect_timeout=5,
            options="-c search_path=pg_catalog -c timezone=UTC -c statement_timeout=180s -c lock_timeout=10s") as connection:
            plan=make_live_plan(connection,args.run_id,json.loads(args.barrier.read_text(encoding="utf-8")),
                                args.rehearsal_proof,args.backup)
        with args.plan.open("x",encoding="utf-8") as handle:
            handle.write(json.dumps(plan,indent=2))
        print("Live plan verified and written; no publish executed")
        return
    plan=json.loads(args.plan.read_text(encoding="utf-8"))
    purpose=validate_plan(plan)
    require(not args.failure_point or purpose=="clone","FAILPOINT_CLONE_ONLY")
    connection=psycopg.connect(host="127.0.0.1",port=15432,dbname=plan["target"]["database"],
        user=os.environ["PG_USER"],password=os.environ["PG_PASSWORD"],autocommit=True,
        connect_timeout=5,options="-c search_path=pg_catalog -c timezone=UTC -c statement_timeout=180s -c lock_timeout=10s")
    def failure(point):
        if point==args.failure_point:
            if args.crash:os._exit(92 if point=="after_commit" else 91)
            raise RuntimeError("Injected cutover failure: "+point)
    try:
        with target(connection,plan,args.confirm_database,args.command,
                    json.loads(args.rollback_proof.read_text(encoding="utf-8")) if args.rollback_proof else None):
            if args.command=="prepare":result=prepare(connection,plan)
            elif args.command=="apply":result=apply(connection,plan,failure)
            elif args.command=="replay":result=replay(connection,plan,args.frame,failure)
            elif args.command=="rollback":result=rollback(connection,plan,failure)
            elif args.command=="extend":
                require(args.extension is not None,"EXTENSION_REQUIRED")
                result=extend(connection,plan,json.loads(args.extension.read_text(encoding="utf-8")),failure)
            else:
                storage.assert_target(connection)
                result=connection.execute("SELECT id,state,shadow_schema,old_schema FROM hdg_load.cutovers WHERE id=%s",(plan["id"],)).fetchall()
            print(json.dumps(dict(command=args.command,result=result,database=plan["target"]["database"]),default=str))
    finally:connection.close()


if __name__=="__main__":
    main()
