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



def continuous_clone():
    import sys,json,copy,uuid,subprocess
    from pathlib import Path
    from unittest.mock import patch
    from types import SimpleNamespace
    ROOT=Path.cwd();sys.path[:0]=[str(ROOT/'jobs'),str(ROOT/'cdc/sink')]
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb
    import cutover,continuous,engine
    from c360_full_load import connect
    from fixtures import Builder,image
    from kafka_reader import read_ranges
    from protocol import Rejected
    from confluent_kafka import KafkaException,KafkaError
    out=ROOT/'test-results/continuous-20261113'
    with connect() as live:
     before=cutover.table_hashes(live)
     original=live.execute("SELECT plan FROM hdg_load.cutovers WHERE state='ACTIVE'").fetchone()[0]
     history=read_ranges(original['manifest'],'127.0.0.1:29092')
     db='hdg_cutover_clone_'+uuid.uuid4().hex[:8]+'_test'
     live.execute(sql.SQL('CREATE DATABASE {} OWNER hdg_user').format(sql.Identifier(db)))
     dump=subprocess.run(['docker','exec','hdg-c360-postgres','pg_dump','-U','hdg_user','-d','hdg_mirror','-Fc'],capture_output=True,check=True).stdout
     restore=subprocess.run(['docker','exec','-i','hdg-c360-postgres','pg_restore','-U','hdg_user','-d',db],input=dump,capture_output=True)
     if restore.returncode:raise RuntimeError('Clone restore failed; no live writes')
     cfg=live.info.get_parameters();cfg.update(dbname=db,password=__import__('os').environ['PG_PASSWORD']);cfg.pop('options',None)
     c=psycopg.connect(**cfg,autocommit=True)
    plan=copy.deepcopy(original);plan['purpose']='clone-rehearsal';plan['target']['database']=db;plan['manifest']['mode']='captured-fixture-test';plan['manifest']['baseline']['assertion']='verified-test-baseline'
    c.execute('UPDATE hdg_load.cutovers SET plan=%s,before_ownership=%s WHERE id=%s',(Jsonb(plan),Jsonb(cutover.ownership(c)),plan['id']))
    c.execute('UPDATE sink_meta.epochs SET manifest=%s,manifest_hash=%s WHERE epoch=%s',(Jsonb(plan['manifest']),engine.manifest_hash(plan['manifest']),plan['manifest']['epoch']))
    starts={s['topic']:s['end'] for s in plan['manifest']['streams']};b=Builder();b.prefix='hdg_observe';b.starts=dict(starts);b.next=dict(starts);b.ids={s['topic']:s['topic_id'] for s in plan['manifest']['streams']}
    a=image('VSS_HO_GD_NHANKHAU',id='WATCHCLONE00000001',ma_cd='WATCHCLONE00000001',ho_ten='Clone',typeid='CD',trang_thai_duyet=2)
    u=dict(a,ho_ten='Clone updated')
    base=int(c.execute('SELECT last_commit_scn FROM sink_meta.epochs WHERE epoch=%s',(plan['manifest']['epoch'],)).fetchone()[0])+100
    b.transaction([('VSS_HO_GD_NHANKHAU','c',None,a),('VSS_HO_GD_NHANKHAU','u',a,u)],commit=base)
    b.transaction([('VSS_HO_GD_NHANKHAU','d',u,None)],commit=base+100,tombstone=True)
    all_records=history+b.records
    visible=dict(starts)
    class Future:
     def __init__(self,t):self.t=t
     def result(self,timeout=None):return SimpleNamespace(topic_id=b.ids[self.t],partitions=[SimpleNamespace(id=0)])
    class Admin:
     def describe_topics(self,topics):return {t:Future(t) for t in starts}
    class Consumer:
     broken=False
     def commit(self,offsets,asynchronous=False):
      assert {(tp.topic,tp.partition):tp.offset for tp in offsets}=={(t,p):n for t,tid,p,n in cutover.checkpoints(c,plan['manifest']['epoch'])}
      if self.broken:raise KafkaException(KafkaError(KafkaError._TRANSPORT))
     def get_watermark_offsets(self,tp,timeout=None):return (0,visible[tp.topic])
    consumer=Consumer();admin=Admin();passed=[]
    def bounded(doc,bootstrap):
     return [r for r in all_records if any(r.topic==s['topic'] and s['start']<=r.offset<s['end'] for s in doc['streams'])]
    def current():return c.execute('SELECT plan FROM hdg_load.cutovers WHERE id=%s',(plan['id'],)).fetchone()[0]
    def cycle(fail=None):
     nonlocal plan
     plan=current();plan,state=continuous.cycle(c,plan,admin,consumer,'unused',fail);return state
    def ok(name):passed.append(name);print('PASS '+name,flush=True)
    with patch.object(continuous,'read_ranges',side_effect=bounded),patch.object(cutover,'read_ranges',side_effect=bounded),cutover.target(c,plan,db,'watch'):
     # Metadata END đầu tiên đã đến nhưng thiếu một DML: không được cập nhật manifest/checkpoint.
     first=[r for r in b.records if r.coordinate in {tuple(x) for x in b.frames[0]['records']}]
     for r in first:visible[r.topic]=max(visible[r.topic],r.offset+1)
     old_cp=cutover.checkpoints(c,plan['manifest']['epoch']);old_hash=cutover.table_hashes(c)
     missing=first[2];all_records.remove(missing)
     assert cycle()=='WAITING_TRANSACTION';assert old_cp==cutover.checkpoints(c,plan['manifest']['epoch']);all_records.append(missing);ok('incomplete_transaction_no_checkpoint')
     def fail(point):
      if point=='before_commit':raise RuntimeError('CRASH_BEFORE_COMMIT')
     try:cycle(fail);raise AssertionError('No crash')
     except RuntimeError as ex:assert str(ex)=='CRASH_BEFORE_COMMIT'
     assert cutover.table_hashes(c)==old_hash and cutover.checkpoints(c,plan['manifest']['epoch'])==old_cp;ok('crash_before_commit_atomic_rollback')
     # Restart/resume pending manifest, rồi crash sau commit trước Kafka ACK.
     def after(point):
      if point=='after_commit':raise RuntimeError('CRASH_AFTER_COMMIT')
     try:cycle(after);raise AssertionError('No crash')
     except RuntimeError as ex:assert str(ex)=='CRASH_AFTER_COMMIT'
     assert c.execute('SELECT count(*) FROM public.mirror_hdg_nhankhau WHERE id=%s',(a['id'],)).fetchone()[0]==1;ok('multi_event_transaction_atomic_apply')
     cp=cutover.checkpoints(c,plan['manifest']['epoch']);assert cp!=old_cp
     assert cycle()=='IDLE' and cutover.checkpoints(c,plan['manifest']['epoch'])==cp;ok('restart_after_commit_idempotent')
     consumer.broken=True
     try:cycle();raise AssertionError('Expected Kafka outage')
     except KafkaException:pass
     assert cutover.checkpoints(c,plan['manifest']['epoch'])==cp;consumer.broken=False
     assert cycle()=='IDLE';ok('kafka_ack_failure_recovery')
     other=psycopg.connect(**cfg,autocommit=True)
     try:
      try:
       with cutover.target(other,plan,db,'replay'):raise AssertionError('Second writer admitted')
      except Rejected as ex:assert ex.code=='SINK_WRITER_ALREADY_RUNNING'
     finally:other.close()
     ok('two_writer_sessions_blocked')
     for statement,label in [('GRANT UPDATE ON public.mirror_hdg_nhankhau TO hdg_airflow_reader','ownership'),('ALTER TABLE public.mirror_hdg_nhankhau ALTER COLUMN id TYPE varchar(200)','schema')]:
      with c.transaction(force_rollback=True):
       c.execute(statement)
       try:continuous.inspect(c,current());raise AssertionError('Guard accepted')
       except (Rejected,psycopg.Error):pass
      ok(label+'_guard_blocks')
     bad=copy.deepcopy(current());bad['manifest']['epoch']=str(uuid.uuid4())
     try:continuous.inspect(c,bad);raise AssertionError('Bad epoch accepted')
     except Rejected:pass
     ok('epoch_guard_blocks')
     original_id=b.ids[next(iter(starts))];topic=next(iter(starts));b.ids[topic]='wrong-topic'
     try:cycle();raise AssertionError('Wrong topic accepted')
     except Rejected as ex:assert ex.code=='KAFKA_TOPIC_ID_MISMATCH'
     b.ids[topic]=original_id;ok('topic_id_guard_blocks')
     for r in b.records:visible[r.topic]=max(visible[r.topic],r.offset+1)
     assert cycle()=='APPLIED';assert cutover.table_hashes(c)==old_hash;ok('automatic_delete_tombstone')
     cp=cutover.checkpoints(c,plan['manifest']['epoch']);assert cycle()=='IDLE' and cutover.checkpoints(c,plan['manifest']['epoch'])==cp;ok('idle_restart_dedup')
     assert c.execute('SELECT count(*) FROM sink_meta.quarantine').fetchone()[0]==0
    with connect() as live:assert cutover.table_hashes(live)==before
    result=dict(status='PASS',clone=db,tests=passed,passed=len(passed),transport='Injected records and transport errors; real PostgreSQL clone and guards',checkpoint=cutover.checkpoints(c,plan['manifest']['epoch']),live_unchanged=True)
    (out/'clone-result.json').write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result));c.close()


if __name__=="__main__":
    if "--continuous-clone" in sys.argv:
        continuous_clone()
    else:
        main()
