"""NRT integration trên DB clone mới, không Full Load/reset/Oracle/Kafka."""
import os
import sys
import json
import uuid
import unittest
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"jobs"),str(ROOT/"cdc/sink")]
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import c360_full_load as nrt
import storage
import cutover
import engine
from fixtures import Builder,image
from contract import TABLES
from protocol import Rejected,canonical

SOURCE="VSS_HO_GD_NHANKHAU"
MIRROR="public.mirror_hdg_nhankhau"


@unittest.skipUnless(os.environ.get("C360_NRT_INTEGRATION")=="1","New isolated DB opt-in required")
class NearRealtime(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database="hdg_cutover_clone_"+uuid.uuid4().hex[:8]+"_test"
        cls.out=ROOT/"tests/test-results"/("c360-nrt-"+cls.database.split("_")[-2])
        cls.out.mkdir(parents=True)
        print("NRT_TEST_DATABASE",cls.database,"EVIDENCE",cls.out,flush=True)
        cls.report={"database":cls.database,"status":"RUNNING","scope":"synthetic independent DB; no Oracle/Kafka"}
        (cls.out/"result.json").write_text(json.dumps(cls.report,indent=2))
        with nrt.connect() as live:
            # Chỉ đọc ba danh mục ở live; CREATE DATABASE tên mới, không thay Mirror live.
            baseline={}
            with live.cursor(row_factory=dict_row) as cur:
                for name,table in TABLES.items():
                    if name!=SOURCE:
                        baseline[name]=cur.execute(sql.SQL("SELECT * FROM public.{}").format(sql.Identifier(table["mirror"]))).fetchall()
            live.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.c=cls.connection()
        for file in ("01_create_mirror_tables.sql","02_publish_full_load.sql"):
            cls.c.execute((ROOT/"sql/postgres"/file).read_text(encoding="utf-8"))
        cls.base=cls.person("BASE","BASE-ID")
        baseline[SOURCE]=[cls.base]
        cls.builder=Builder(baseline=baseline)
        a=cls.person("SINK-OLD","SINK-ID")
        b=dict(a,ma_cd="SINK-NEW",ma_cd_old="SINK-OLD",ho_ten="Tên sau cập nhật")
        cls.builder.transaction([(SOURCE,"c",None,a)])
        cls.builder.transaction([(SOURCE,"u",a,b)])
        cls.builder.transaction([(SOURCE,"d",b,None)],tombstone=True)
        cls.doc=cls.builder.document()
        cls.found=engine.check_records(cls.doc,cls.builder.records)
        with cls.scope(cls.c):
            cutover.bootstrap(cls.c)
            engine.initialize(cls.c,cls.doc)
        cls.c.execute((ROOT/"sql/postgres/05_c360.sql").read_text(encoding="utf-8"))
        version=str(uuid.uuid4())
        with cls.c.transaction():
            nrt.refresh_maps(cls.c,version)
            cls.c.execute("UPDATE c360.c360_sync_control SET map_version=%s WHERE control_key='NORMALIZE'",(version,))
        installed=nrt.install_incremental(cls.c)
        assert installed["bootstrap_queued"]==1,installed
        first=nrt.incremental(cls.c)
        assert first["upserted"]==1 and first["unknown_rows"]==1
        cls.latencies=[first["seconds"]]
        cls.report["bootstrap"]=installed

    @classmethod
    def connection(cls):
        return psycopg.connect(host="127.0.0.1",port=15432,dbname=cls.database,
          user=os.environ["PG_USER"],password=os.environ["PG_PASSWORD"],autocommit=True,connect_timeout=5)

    @classmethod
    @contextmanager
    def scope(cls,c):
        with storage.cutover_target(c,cls.database,cls.database,"clone"):
            yield

    @classmethod
    def person(cls,code,ident,house="H1",month=1):
        return image(SOURCE,ma_cd=code,id=ident,ma_ho_gd=house,ho_ten=" Nguyễn  Văn An ",
          typeid="CD",trang_thai_duyet=2,created_date=datetime(2026,1,1),modify_date=datetime(2026,month,1),
          ngay_sinh=19950815,so_cccd="001095012345",phone_number="0912345678",
          dm_dan_toc_id=1,quoc_tich="VN",ma_tinh="01TTT",ma_xa="00004",
          ma_tinh_lh="01TTT",ma_xa_lh="00004",ma_tinh_ks="01TTT",ma_xa_ks="00004")

    def setUp(self):
        self.prefix="NRT-"+uuid.uuid4().hex[:8]
        self.ids=[]

    def insert(self,code=None,house="H1",month=1,c=None,**changes):
        c=c or self.c
        code=code or self.prefix
        ident=self.prefix+"-"+str(len(self.ids))
        self.assertEqual(c.execute("SELECT count(*) FROM "+MIRROR+" WHERE id=%s",(ident,)).fetchone()[0],0)
        data=self.person(code,ident,house,month);data.update(changes)
        self.ids.append(ident)
        with self.scope(c):storage.insert_row(c,TABLES[SOURCE],data)
        return ident

    def worker(self,**kwargs):
        result=nrt.incremental(self.c,**kwargs)
        if "seconds" in result:self.latencies.append(result["seconds"])
        return result

    def citizen(self,code):
        return self.c.execute("SELECT to_jsonb(t) FROM c360.c360_thong_tin_ca_nhan t WHERE ma_so_bhxh=%s",(code,)).fetchone()

    def cleanup(self):
        if self.ids:
            rows=self.c.execute("SELECT id FROM "+MIRROR+" WHERE id=ANY(%s)",(self.ids,)).fetchall()
            self.assertTrue(all(r[0].startswith(self.prefix+"-") for r in rows))
            self.c.execute("DELETE FROM "+MIRROR+" WHERE id=ANY(%s)",(self.ids,))
            self.worker()
            self.assertEqual(self.c.execute("SELECT count(*) FROM "+MIRROR+" WHERE id=ANY(%s)",(self.ids,)).fetchone()[0],0)
        self.assertEqual(self.c.execute("SELECT count(*) FROM c360.c360_change_queue").fetchone()[0],0)

    def state(self):
        return self.c.execute("""SELECT
          (SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]') FROM c360.c360_thong_tin_ca_nhan t),
          (SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]') FROM c360.c360_change_queue t),
          (SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY topic),'[]') FROM sink_meta.checkpoints t)
        """).fetchone()

    def test_01_sink_trigger_atomicity_and_replay(self):
        with self.scope(self.c):
            storage.assert_target(self.c);cutover.safe_schema(self.c)
            batch=lambda i:[self.found[tuple(coord)] for coord in self.doc["frames"][i]["records"]]
            self.assertEqual(engine.apply_frame(self.c,self.doc,0,batch(0)),"APPLIED")
            self.worker();self.assertIsNotNone(self.citizen("SINK-OLD"))
            before=self.state()
            def crash(point):
                if point=="before_commit":raise engine.SimulatedCrash("NRT_TEST_SINK_CRASH")
            with self.assertRaises(engine.SimulatedCrash):
                engine.apply_frame(self.c,self.doc,1,batch(1),crash)
            self.assertEqual(self.state(),before)
            self.assertEqual(engine.apply_frame(self.c,self.doc,1,batch(1)),"APPLIED")
            queued={r[0] for r in self.c.execute("SELECT ma_cd FROM c360.c360_change_queue")}
            self.assertEqual(queued,{"SINK-OLD","SINK-NEW"})
            self.worker()
            self.assertIsNone(self.citizen("SINK-OLD"));self.assertIsNotNone(self.citizen("SINK-NEW"))
            before=self.state()
            self.assertEqual(engine.apply_frame(self.c,self.doc,1,batch(1)),"REPLAY_FRAME")
            self.assertEqual(self.state(),before)
            self.assertEqual(engine.apply_frame(self.c,self.doc,2,batch(2)),"APPLIED")
            self.worker();self.assertIsNone(self.citizen("SINK-NEW"))
            self.assertEqual(self.c.execute("SELECT count(*) FROM sink_meta.quarantine").fetchone()[0],0)

    def test_02_insert_update_delete_and_dq(self):
        ident=self.insert(so_cccd=None)
        self.assertEqual(self.worker()["citizens"],1)
        self.assertIn("KHONG_SO_DINH_DANH",self.citizen(self.prefix)[0]["dq_flags"])
        self.c.execute("UPDATE "+MIRROR+" SET so_cccd='001095012345',ho_ten=' Đỗ  Văn An ' WHERE id=%s",(ident,))
        self.worker()
        r=self.citizen(self.prefix)[0]
        self.assertNotIn("KHONG_SO_DINH_DANH",r["dq_flags"]);self.assertEqual(r["ho_ten_unsigned"],"DO VAN AN")
        self.assertIsNone(r["ngay_sinh_chuan"]);self.assertIn("BIRTH_YEAR_RANGE",r["unknown_rules"])
        self.cleanup();self.assertIsNone(self.citizen(self.prefix))

    def test_03_multiple_households_rename_and_filter(self):
        first=self.insert(month=1)
        latest=self.insert(house="H2",month=2)
        self.worker();self.assertEqual(self.citizen(self.prefix)[0]["source_id"],latest)
        new=self.prefix+"-NEW"
        self.c.execute("UPDATE "+MIRROR+" SET ma_cd=%s,ma_cd_old=%s WHERE id=%s",(new,self.prefix,latest))
        result=self.worker();self.assertEqual(result["citizens"],2)
        self.assertEqual(self.citizen(self.prefix)[0]["source_id"],first)
        self.assertEqual(self.citizen(new)[0]["source_id"],latest)
        self.c.execute("UPDATE "+MIRROR+" SET typeid='GG' WHERE id=%s",(first,));self.worker()
        self.assertIsNone(self.citizen(self.prefix))
        self.c.execute("UPDATE "+MIRROR+" SET trang_thai_duyet=1 WHERE id=%s",(latest,));self.worker()
        self.assertIsNone(self.citizen(new))
        self.c.execute("UPDATE "+MIRROR+" SET trang_thai_duyet=2 WHERE id=%s",(latest,));self.worker()
        self.assertIsNotNone(self.citizen(new))
        self.cleanup();self.assertIsNone(self.citizen(new))

    def test_04_delete_newest_falls_back(self):
        first=self.insert(month=1);latest=self.insert(house="H2",month=2)
        self.worker()
        self.c.execute("DELETE FROM "+MIRROR+" WHERE id=%s",(latest,));self.worker()
        self.assertEqual(self.citizen(self.prefix)[0]["source_id"],first)
        self.cleanup()

    def test_05_rollback_after_ack_and_retry(self):
        self.insert();before=self.state()
        def fail(point):
            if point=="after_ack":raise RuntimeError("INJECTED_NRT_AFTER_ACK")
        with self.assertRaisesRegex(RuntimeError,"INJECTED_NRT_AFTER_ACK"):
            self.worker(failpoint=fail)
        self.assertEqual(self.state(),before)
        self.assertIsNotNone(self.worker()["run_id"])
        self.assertIsNotNone(self.citizen(self.prefix))
        self.cleanup()

    def test_06_duplicate_queue_re_evaluates_without_duplication(self):
        self.insert();self.worker()
        before=self.citizen(self.prefix)[0]
        self.c.execute("INSERT INTO c360.c360_change_queue(ma_cd) VALUES(%s),(%s)",(self.prefix,self.prefix))
        result=self.worker();self.assertEqual((result["claimed"],result["citizens"]),(2,1))
        after=self.citizen(self.prefix)[0]
        for key in ("run_id","sync_date"):before.pop(key);after.pop(key)
        self.assertEqual(before,after);self.cleanup()

    def test_07_commit_order_not_identity_order(self):
        late=self.connection();early=self.connection()
        try:
            late.execute("BEGIN");self.insert(code=self.prefix+"L",c=late)
            low=late.execute("SELECT max(id) FROM c360.c360_change_queue").fetchone()[0]
            early.execute("BEGIN");self.insert(code=self.prefix+"E",c=early)
            high=early.execute("SELECT max(id) FROM c360.c360_change_queue").fetchone()[0];early.execute("COMMIT")
            self.assertLess(low,high)
            result=self.worker();self.assertEqual(result["claimed"],1)
            self.assertIsNone(self.citizen(self.prefix+"L"));self.assertIsNotNone(self.citizen(self.prefix+"E"))
            late.execute("COMMIT")
            self.assertEqual(self.c.execute("SELECT id FROM c360.c360_change_queue").fetchone()[0],low)
            self.worker();self.assertIsNotNone(self.citizen(self.prefix+"L"))
        finally:
            late.close();early.close()
        self.cleanup()

    def test_08_control_lock_preserves_queue(self):
        self.insert();blocker=self.connection();other=self.connection()
        try:
            blocker.execute("BEGIN")
            blocker.execute("SELECT control_key FROM c360.c360_sync_control WHERE control_key='NORMALIZE' FOR UPDATE")
            before=self.state()
            self.assertEqual(nrt.incremental(other)["status"],"BUSY")
            self.assertEqual(self.state(),before)
            blocker.execute("ROLLBACK")
            self.worker()
        finally:blocker.close();other.close()
        with self.scope(self.c),self.c.transaction():
            cutover.locks(self.c);cutover.safe_schema(self.c)
        self.cleanup()

    def test_09_unknown_representative_preserves_work(self):
        self.insert(month=1);ident=self.insert(house="H2",modify_date=None)
        before=self.state()
        with self.assertRaisesRegex(nrt.Blocked,"REPRESENTATIVE_NULL_ORDER_UNKNOWN"):self.worker()
        self.assertEqual(self.state(),before)
        self.c.execute("UPDATE "+MIRROR+" SET modify_date=timestamp '2026-02-01' WHERE id=%s",(ident,))
        self.worker();self.cleanup()

    def test_10_guard_rejects_unknown_trigger_body_enabled_queue_and_fk(self):
        class Undo(Exception):pass
        alterations=[
          "ALTER TABLE "+MIRROR+" DISABLE TRIGGER c360_enqueue_nhankhau",
          "CREATE OR REPLACE FUNCTION c360.enqueue_mirror_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$",
          "CREATE TRIGGER unexpected AFTER INSERT ON "+MIRROR+" FOR EACH ROW EXECUTE FUNCTION c360.enqueue_mirror_change()",
          "ALTER TABLE c360.c360_change_queue ENABLE ROW LEVEL SECURITY",
          "ALTER TABLE "+MIRROR+" ENABLE ROW LEVEL SECURITY",
          "ALTER FUNCTION c360.enqueue_mirror_change() SECURITY DEFINER",
          "CREATE TRIGGER unexpected_catalog AFTER INSERT ON public.mirror_dm_dbhc FOR EACH ROW EXECUTE FUNCTION c360.enqueue_mirror_change()",
          "ALTER TABLE "+MIRROR+" ADD CONSTRAINT unexpected_fk FOREIGN KEY(dm_dan_toc_id) REFERENCES public.mirror_dm_dantoc(id)"]
        for statement in alterations:
            try:
                with self.scope(self.c),self.c.transaction():
                    self.c.execute(statement)
                    with self.assertRaises(Rejected):storage.assert_target(self.c)
                    with self.assertRaises(Rejected):cutover.safe_schema(self.c)
                    raise Undo()
            except Undo:pass
        with self.scope(self.c):
            storage.assert_target(self.c);cutover.safe_schema(self.c)

    def test_11_catalog_change_blocks_without_ack(self):
        self.insert();original=self.c.execute("SELECT ten FROM public.mirror_dm_dantoc WHERE id=1").fetchone()[0]
        self.c.execute("UPDATE public.mirror_dm_dantoc SET ten='NRT-ONLY-CATALOG-TEST' WHERE id=1")
        before=self.state()
        with self.assertRaisesRegex(nrt.Blocked,"CATALOG_REFRESH_REQUIRED"):self.worker()
        self.assertEqual(self.state(),before)
        self.c.execute("UPDATE public.mirror_dm_dantoc SET ten=%s WHERE id=1 AND ten='NRT-ONLY-CATALOG-TEST'",(original,))
        self.worker();self.cleanup()

    def test_12_change_during_batch_remains_in_queue(self):
        ident=self.insert()
        writer=self.connection()
        changed=False
        def concurrent(point):
            nonlocal changed
            if point=="after_publish" and not changed:
                writer.execute("UPDATE "+MIRROR+" SET ho_ten='Tên đến sau snapshot' WHERE id=%s",(ident,))
                changed=True
        try:
            self.worker(failpoint=concurrent)
            self.assertEqual(self.c.execute("SELECT count(*) FROM c360.c360_change_queue").fetchone()[0],1)
            self.assertNotEqual(self.citizen(self.prefix)[0]["ho_ten"],"Tên đến sau snapshot")
            self.worker()
            self.assertEqual(self.citizen(self.prefix)[0]["ho_ten"],"Tên đến sau snapshot")
        finally:writer.close()
        self.cleanup()

    def test_13_restart_connection_and_singleton_watch(self):
        self.insert()
        with self.connection() as restarted:
            result=nrt.incremental(restarted)
            self.assertEqual(result["citizens"],1)
        self.assertIsNotNone(self.citizen(self.prefix))
        with self.connection() as blocker:
            self.assertTrue(blocker.execute("SELECT pg_try_advisory_lock(36012026,1)").fetchone()[0])
            with self.assertRaisesRegex(nrt.Blocked,"NRT_WATCHER_ALREADY_RUNNING"):
                nrt.watch(self.c)
        self.cleanup()

    @classmethod
    def tearDownClass(cls):
        try:
            eligible=cls.c.execute("""SELECT count(distinct ma_cd) FROM public.mirror_hdg_nhankhau
              WHERE coalesce(typeid,'ZZ')<>ALL(%s) AND coalesce(trang_thai_duyet,0)<>ALL(%s)""",
              (list(nrt.BAD_TYPES),list(nrt.BAD_APPROVALS))).fetchone()[0]
            count=cls.c.execute("SELECT count(*) FROM c360.c360_thong_tin_ca_nhan").fetchone()[0]
            pending=cls.c.execute("SELECT count(*) FROM c360.c360_change_queue").fetchone()[0]
            cls.report.update(citizens=count,eligible_citizens=eligible,queue_pending=pending,
                              max_worker_seconds=max(cls.latencies),unknown_rules_preserved=True)
            assert eligible==count==1 and pending==0 and max(cls.latencies)<60
        finally:
            cls.c.close()
            (cls.out/"result.json").write_text(json.dumps(cls.report,indent=2))


if __name__=="__main__":
    runner=unittest.TextTestRunner(verbosity=2,failfast=True)
    result=runner.run(unittest.defaultTestLoader.loadTestsFromTestCase(NearRealtime))
    if hasattr(NearRealtime,"report"):
        NearRealtime.report.update(status="PASS" if result.wasSuccessful() else "FAIL",
          tests_run=result.testsRun,failures=len(result.failures),errors=len(result.errors))
        (NearRealtime.out/"result.json").write_text(json.dumps(NearRealtime.report,indent=2))
    sys.exit(0 if result.wasSuccessful() else 1)
