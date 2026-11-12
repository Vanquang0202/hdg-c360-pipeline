# Mục đích: Diễn tập cutover trên clone mới; không sửa Oracle/Kafka hoặc Mirror chính.
# Đầu vào: backup hiện có và hai range CDC thực đã kiểm chứng, chỉ dùng làm fixture clone.
# Đầu ra: evidence PASS/FAIL, không xóa clone kể cả khi test lỗi.
import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"cdc/sink/.venv/Lib/site-packages"))
sys.path.insert(0,str(ROOT/"cdc/sink"))
import psycopg
from psycopg import sql
import cutover
import engine
from contract import TABLES
from protocol import digest,canonical,Rejected


def save(path,data):
    path.write_text(json.dumps(data,indent=2,default=str),encoding="utf-8")


def env_secret(name):
    # Dùng credential container local trong bộ nhớ; không in hoặc ghi ra artifact.
    return subprocess.check_output(["docker","exec","hdg-c360-postgres","printenv",name],text=True).strip()



def all_acl(connection):
    schema=connection.execute("SELECT pg_get_userbyid(nspowner),nspacl::text FROM pg_namespace WHERE nspname='public'").fetchone()
    columns=connection.execute("SELECT c.relname,a.attname,a.attacl::text FROM pg_attribute a "
        "JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='public' AND c.relname=ANY(%s) AND a.attnum>0 AND NOT a.attisdropped "
        "ORDER BY c.relname,a.attnum",([t["mirror"] for t in TABLES.values()],)).fetchall()
    return schema,columns

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-clone",action="store_true")
    parser.add_argument("--backup",type=Path,required=True)
    parser.add_argument("--baseline",type=Path,required=True)
    parser.add_argument("--next",type=Path,required=True)
    args=parser.parse_args()
    if not args.run_clone:parser.error("--run-clone required")
    stamp=uuid.uuid4().hex[:8]
    db="hdg_cutover_clone_"+stamp+"_test"
    out=ROOT/"tests/cdc_sink/test-results"/("cutover-"+stamp)
    out.mkdir(parents=True)
    report=dict(status="STARTED",database=db,live_cutover=False,backup_sha256=hashlib.sha256(args.backup.read_bytes()).hexdigest())
    save(out/"result.json",report)
    print("Evidence:",out.relative_to(ROOT),flush=True)
    env=os.environ.copy();env["PG_USER"]=env_secret("POSTGRES_USER");env["PG_PASSWORD"]=env_secret("POSTGRES_PASSWORD")
    def connect(name):
        return psycopg.connect(host="127.0.0.1",port=15432,dbname=name,user=env["PG_USER"],password=env["PG_PASSWORD"],
                                autocommit=True,options="-c search_path=pg_catalog -c timezone=UTC",connect_timeout=5)
    original=connect("hdg_mirror")
    original_ownership=cutover.ownership(original)
    original_acl=all_acl(original)
    original_data=cutover.table_hashes(original)
    report["original_data"]=original_data
    original.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(db),sql.Identifier(env["PG_USER"])))
    original.close()
    with args.backup.open("rb") as f:
        r=subprocess.run(["docker","exec","-i","hdg-c360-postgres","pg_restore","--single-transaction","--exit-on-error",
                          "-U",env["PG_USER"],"-d",db],stdin=f,capture_output=True,timeout=180)
    (out/"restore.log").write_bytes(r.stdout+r.stderr)
    assert r.returncode==0,r.stderr[:800]
    c=connect(db)
    before_meta=cutover.ownership(c)
    assert all_acl(c)==original_acl
    # OID của clone khác nguồn; owner/ACL còn lại phải trùng nguồn. Sau cutover chính OID clone phải giữ.
    assert [[v for i,v in enumerate(row) if i!=1] for row in before_meta]==[
            [v for i,v in enumerate(row) if i!=1] for row in original_ownership]
    assert cutover.table_hashes(c)==original_data
    report["owner_acl_verified"]=True
    report["column_and_schema_acl_verified"]=True
    print("Backup clone restored with original ownership/ACL PASS",flush=True)
    base=json.loads((args.baseline/"manifest.json").read_text())
    following=json.loads((args.next/"manifest.json").read_text())
    # Import bổ sung duy nhất Stage/run đã audit; không thay hoặc xóa run/Stage hiện có trong clone.
    stage_dump=out/"stage_append.dump"
    tables=["hdg_load.runs","hdg_load.table_runs"]+["hdg_stage."+t["mirror"] for t in TABLES.values()]
    command=["docker","exec","hdg-cdc-sink-test","pg_dump","-Fc","--data-only","-U","sink_test_owner","-d","hdg_full_e2e_fa5d5786_test"]
    for table in tables:command+=["-t",table]
    with stage_dump.open("xb") as f:
        r=subprocess.run(command,stdout=f,stderr=subprocess.PIPE,timeout=100)
    assert r.returncode==0,r.stderr[:500]
    with stage_dump.open("rb") as f:
        r=subprocess.run(["docker","exec","-i","hdg-c360-postgres","pg_restore","--single-transaction","--exit-on-error",
                          "--data-only","-U",env["PG_USER"],"-d",db],stdin=f,capture_output=True,timeout=120)
    assert r.returncode==0,r.stderr[:500]
    assert base["source_incarnation"]==following["source_incarnation"]
    extended=copy.deepcopy(base)
    for a,b in zip(extended["streams"],following["streams"]):
        assert a["topic"]==b["topic"] and a["topic_id"]==b["topic_id"] and a["end"]==b["start"]
        a["end"]=b["end"]
    extended["frames"]+=following["frames"]
    engine.validate(extended)
    def plan(document):
        p=dict(format="hdg-cutover-v1",id=str(uuid.uuid4()),purpose="clone-rehearsal",
            target=dict(host="127.0.0.1",port=15432,database=db),
            run_id=document["baseline"]["run_id"],manifest=copy.deepcopy(document),
            barrier=dict(kind="recorded-fixture-for-clone-only",live_eligible=False))
        p["manifest"]["epoch"]=str(uuid.uuid4())
        return p
    def cli(command,p,path,extra=None,expected=0):
        save(path,p)
        argv=[str(ROOT/"cdc/sink/.venv/Scripts/python.exe"),"-B",str(ROOT/"cdc/sink/cutover.py"),
              command,"--plan",str(path),"--confirm-database",db]+(extra or [])
        r=subprocess.run(argv,env=env,cwd=ROOT,capture_output=True,text=True,timeout=360)
        with (out/"commands.log").open("a",encoding="utf-8") as f:
            f.write(" ".join(argv)+"\nexit="+str(r.returncode)+"\n"+r.stdout+r.stderr+"\n")
        assert r.returncode==expected,(command,r.returncode,r.stdout[-500:],r.stderr[-1600:])
        return r.stdout
    first=plan(extended);file_a=out/"plan_failure.json"
    cli("prepare",first,file_a)
    with cutover.target(c,first,db,"status"):
        assert cutover.table_hashes(c)==original_data and cutover.ownership(c)==before_meta
        assert c.execute("SELECT count(*) FROM sink_meta.epochs").fetchone()[0]==0
    # Process thật chết sau DELETE bảng đầu: PostgreSQL phải rollback cả dữ liệu/control/checkpoint.
    cli("apply",first,file_a,["--failure-point","after_first_delete","--crash"],91)
    assert cutover.table_hashes(c)==original_data and cutover.ownership(c)==before_meta
    assert c.execute("SELECT state FROM hdg_load.cutovers WHERE id=%s",(first["id"],)).fetchone()[0]=="PREPARED"
    assert c.execute("SELECT count(*) FROM sink_meta.epochs").fetchone()[0]==0
    report["crash_mid_publish_rollback"]="PASS"
    # Chết sau commit: retry phải nhận ACTIVE, không seed/reset checkpoint lần hai.
    cli("apply",first,file_a,["--failure-point","after_commit","--crash"],92)
    assert "ALREADY_ACTIVE" in cli("apply",first,file_a)
    with cutover.target(c,first,db,"status"):
        assert cutover.ownership(c)==before_meta
        seeded=cutover.table_hashes(c)
        assert seeded!=original_data
        assert seeded==cutover.table_hashes(c,"hdg_cutover_"+uuid.UUID(first["id"]).hex)
        assert all(cp[3]==s["start"] for cp,s in zip(
            sorted(cutover.checkpoints(c,first["manifest"]["epoch"])),sorted(first["manifest"]["streams"],key=lambda s:s["topic"])))
    report["crash_after_commit_retry"]="PASS"
    # Frame thứ hai đi trước: phải BLOCKED, không vượt checkpoint hoặc ghi dữ liệu.
    cli("replay",first,file_a,["--frame","1"],1)
    assert c.execute("SELECT state FROM sink_meta.epochs WHERE epoch=%s",(first["manifest"]["epoch"],)).fetchone()[0]=="BLOCKED"
    assert cutover.table_hashes(c)==seeded
    cp_blocked=cutover.checkpoints(c,first["manifest"]["epoch"])
    cli("rollback",first,file_a)
    assert cutover.table_hashes(c)==original_data and cutover.ownership(c)==before_meta
    assert all_acl(c)==original_acl
    assert cutover.checkpoints(c,first["manifest"]["epoch"])==cp_blocked
    report.update(ordering_blocks_without_data_change="PASS",rollback_restores_original_data_acl_oid="PASS")
    print("Crash, ordering rejection and rollback PASS",flush=True)
    # Đợt phục hồi mới dùng epoch mới; không xóa ledger/quarantine/checkpoint cũ.
    second=plan(base);file_b=out/"plan_active.json"
    cli("prepare",second,file_b)
    cli("apply",second,file_b)
    cli("replay",second,file_b,["--failure-point","before_commit","--crash"],91)
    assert c.execute("SELECT count(*) FROM sink_meta.event_ledger WHERE epoch=%s",(second["manifest"]["epoch"],)).fetchone()[0]==0
    assert c.execute("SELECT next_frame FROM sink_meta.epochs WHERE epoch=%s",(second["manifest"]["epoch"],)).fetchone()[0]==0
    cli("replay",second,file_b)
    first_cp=cutover.checkpoints(c,second["manifest"]["epoch"])
    extra=copy.deepcopy(second)
    extra["manifest"]["frames"]=copy.deepcopy(extended["frames"])
    extra["manifest"]["streams"]=copy.deepcopy(extended["streams"])
    file_extra=out/"plan_extended.json";save(file_extra,extra)
    # Thử lỗi giữa cập nhật manifest: hash/checkpoint phải giữ như cũ, sau đó extend thật.
    cli("extend",second,file_b,["--extension",str(file_extra),"--failure-point","before_commit","--crash"],91)
    assert cutover.checkpoints(c,second["manifest"]["epoch"])==first_cp
    assert c.execute("SELECT manifest_hash FROM sink_meta.epochs WHERE epoch=%s",(second["manifest"]["epoch"],)).fetchone()[0]==engine.manifest_hash(second["manifest"])
    cli("extend",second,file_b,["--extension",str(file_extra)])
    cli("replay",extra,file_extra)
    last_cp=cutover.checkpoints(c,extra["manifest"]["epoch"])
    hashes=cutover.table_hashes(c)
    again=cli("replay",extra,file_extra)
    assert again.count("REPLAY_FRAME")==2
    assert cutover.checkpoints(c,extra["manifest"]["epoch"])==last_cp and cutover.table_hashes(c)==hashes
    assert cutover.ownership(c)==before_meta
    assert all_acl(c)==original_acl
    assert c.execute("SELECT count(*) FROM sink_meta.event_ledger WHERE epoch=%s",(extra["manifest"]["epoch"],)).fetchone()[0]==24
    assert c.execute("SELECT count(*) FROM sink_meta.transaction_ledger WHERE epoch=%s",(extra["manifest"]["epoch"],)).fetchone()[0]==2
    assert c.execute("SELECT count(*) FROM sink_meta.inbox WHERE epoch=%s",(extra["manifest"]["epoch"],)).fetchone()[0]==36
    assert c.execute("SELECT count(*) FROM sink_meta.quarantine WHERE epoch=%s",(extra["manifest"]["epoch"],)).fetchone()[0]==0
    # Đối soát đủ 132 cột với cả shadow đã audit và Candidate phục hồi trong clone.
    comparisons={}
    for name,t in TABLES.items():
        columns=sql.SQL(",").join(sql.Identifier(col["name"]) for col in t["columns"])
        for namespace in ["hdg_cutover_"+uuid.UUID(second["id"]).hex,"hdg_recovery_verified"]:
            difference=c.execute(sql.SQL("SELECT count(*) FROM ((SELECT {} FROM public.{} EXCEPT ALL SELECT {} FROM {}) "
                    "UNION ALL (SELECT {} FROM {} EXCEPT ALL SELECT {} FROM public.{})) d").format(
                    columns,sql.Identifier(t["mirror"]),columns,sql.Identifier(namespace,t["mirror"]),
                    columns,sql.Identifier(namespace,t["mirror"]),columns,sql.Identifier(t["mirror"]))).fetchone()[0]
            assert difference==0
        comparisons[name]=hashes[t["mirror"]]["rows"]
    c.execute("SELECT hdg_load.assert_mirror_schema()")
    # Clone đã sửa, nhưng chính database live phải giữ đúng dữ liệu cũ và OID/ACL.
    live=connect("hdg_mirror")
    assert cutover.table_hashes(live)==original_data and cutover.ownership(live)==original_ownership
    assert all_acl(live)==original_acl
    live.close()
    report.update(status="PASS",row_counts=comparisons,final_checkpoint=canonical(last_cp),
        actual_cdc_transactions=2,ledger_events=24,inbox_records=36,
        sink_crash_rollback="PASS",append_failure_rollback="PASS",append_handoff="PASS",replay_idempotent="PASS",
        live_unchanged=True,owner_acl_verified=True,plan=str(file_extra.relative_to(ROOT)),
        limits="Clone fixture barrier only; live requires new writer-fence plan. Bounded append/replay, not streaming assembler.")
    save(out/"result.json",report)
    c.close()
    print(json.dumps(report),flush=True)


if __name__=="__main__":
    main()
