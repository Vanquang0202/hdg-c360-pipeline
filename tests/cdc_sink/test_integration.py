# Mục đích: Kiểm tính nguyên tử/replay trên database test cố định cổng 55433.
# Đầu vào: PostgreSQL test và synthetic events, không phải Mirror live.
# Đầu ra: Kết quả integration; mỗi ca chỉ reset các bảng fixture riêng của prototype.
"""Real PostgreSQL integration suite. Fixed separate container/database/port only."""
import json
import os
import subprocess
import sys
import unittest
import uuid
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from psycopg import sql

HERE=Path(__file__).resolve().parents[2] / "cdc/sink"
sys.path.insert(0,str(HERE))
from contract import TABLES, DATABASE
from protocol import Rejected, loads
from fixtures import Builder, image
from engine import validate, check_records
from storage import connect_test, bootstrap, assert_target, relation, reset_test_fixture
from engine import initialize, apply_frame, SimulatedCrash


class PostgresTests(unittest.TestCase):
    @classmethod
    # Chỉ chạy integration khi được phép reset fixture trong DB test; không dùng làm lệnh chạy hàng ngày.
    def setUpClass(cls):
        with connect_test() as connection:
            bootstrap(connection)

    def setUp(self):
        self.connection=connect_test()
        assert_target(self.connection)
        reset_test_fixture(self.connection)
        self.addCleanup(self.connection.close)

    def prepare(self,builder):
        self.builder=builder
        self.doc=validate(builder.document())
        self.found=check_records(self.doc,builder.records)
        initialize(self.connection,self.doc)

    def batch(self,index):
        return [self.found[tuple(c)] for c in self.doc["frames"][index]["records"]]

    def apply(self,index,failpoint=None):
        return apply_frame(self.connection,self.doc,index,self.batch(index),failpoint)

    def count(self,source):
        return self.connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
            relation(TABLES[source]))).fetchone()[0]

    def checkpoints(self):
        return self.connection.execute(
            "SELECT topic,next_offset FROM sink_meta.checkpoints ORDER BY topic").fetchall()

    def assert_blocked_without_apply(self,reason,checkpoint):
        self.assertEqual(self.checkpoints(),checkpoint)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.inbox").fetchone()[0],0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0],0)
        self.assertEqual(self.connection.execute("SELECT state FROM sink_meta.epochs").fetchone()[0],"BLOCKED")
        self.assertEqual(self.connection.execute("SELECT reason FROM sink_meta.quarantine").fetchone()[0],reason)

    def test_01_dbhc_c_u_d_tombstone_and_source_replay(self):
        from fixtures import demo
        b=demo(); self.prepare(b)
        self.assertTrue(all(r.key is None for r in b.records if ".VSS." in r.topic))
        self.assertEqual(self.apply(0),"APPLIED"); self.assertEqual(self.count("VSS_DM_DBHC"),1)
        self.assertEqual(self.apply(1),"APPLIED"); self.assertEqual(self.count("VSS_DM_DBHC"),1)
        self.assertEqual(self.apply(2),"APPLIED"); self.assertEqual(self.count("VSS_DM_DBHC"),0)
        self.assertEqual(self.apply(3),"REPLAY_SOURCE"); self.assertEqual(self.count("VSS_DM_DBHC"),0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0],3)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.inbox WHERE outcome='TOMBSTONE'").fetchone()[0],1)

    def test_02_all_four_tables_exact_mapping_single_transaction(self):
        b=Builder()
        nh=image("VSS_HO_GD_NHANKHAU",ma_cd="0120456789",ma_ho_gd=None,id="HDG00000000000001",
                 ho_ten="Nguyễn Văn An ",dia_chi="A"*500,so_cccd="001095012345",
                 ngay_sinh=Decimal("1E99"),lock_data=Decimal("1.2345678901234567890123456789012345678"),
                 dm_dan_toc_id=Decimal("9999999999999999999"),user_id=9999999999,
                 created_date=datetime(2026,11,11,23,59,59),typeid="GG",trang_thai_duyet=4)
        db=image("VSS_DM_DBHC",id=Decimal("99999999999999999999"),ten="Hà Nội",
                 ngay_hieu_luc=datetime(2026,11,11,12,34,56,123456),is_cap_xa=Decimal("-1E-99"))
        ethnic=image("VSS_DM_DANTOC",id=Decimal(1),ma="01",can_cu="Quyết định kiểm thử",ten="Dân tộc thử")
        nationality=image("VSS_DM_QUOC_TICH",id=Decimal(233),ma="VN",ten="Việt Nam")
        values={"VSS_HO_GD_NHANKHAU":nh,"VSS_DM_DANTOC":ethnic,"VSS_DM_QUOC_TICH":nationality,"VSS_DM_DBHC":db}
        b.transaction([(name,"c",None,data) for name,data in values.items()])
        self.prepare(b); self.apply(0)
        for source,expected in values.items():
            table=TABLES[source]
            actual=self.connection.execute(sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(",").join(sql.Identifier(c["name"]) for c in table["columns"]),
                relation(table))).fetchone()
            self.assertEqual(dict(zip([c["name"] for c in table["columns"]],actual)),expected,source)

    def test_03_both_pk_tables_update_key_change_delete(self):
        b=Builder()
        old={s:image(s,id=Decimal(1),ma="01",ten="Trước") for s in ("VSS_DM_DANTOC","VSS_DM_QUOC_TICH")}
        changed={s:dict(v,ten="Sau") for s,v in old.items()}
        newkey={s:dict(v,id=Decimal(2)) for s,v in changed.items()}
        b.transaction([(s,"c",None,v) for s,v in old.items()])
        b.transaction([(s,"u",old[s],v) for s,v in changed.items()])
        b.transaction([(s,op,before,after) for s in old for op,before,after in (
            ("d",changed[s],None),("c",None,newkey[s]))],tombstone=True)
        b.clone(2)
        b.transaction([(s,"d",v,None) for s,v in newkey.items()],tombstone=True)
        self.prepare(b)
        for index in range(len(b.frames)): self.apply(index)
        for source in old: self.assertEqual(self.count(source),0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0],10)

    def test_04_no_pk_duplicate_null_before_changes_one_copy(self):
        db=image("VSS_DM_DBHC")  # All 12 fields NULL.
        nh=image("VSS_HO_GD_NHANKHAU",ma_cd="0120456789",id=None,ma_ho_gd=None)
        b=Builder(baseline={"VSS_DM_DBHC":[db,dict(db)],"VSS_HO_GD_NHANKHAU":[nh,dict(nh)]})
        changed_db=dict(db,ten="New",id=Decimal(7))
        changed_nh=dict(nh,ho_ten="New",ma_cd="0987654321",id="Changed")
        b.transaction([("VSS_DM_DBHC","u",db,changed_db),("VSS_HO_GD_NHANKHAU","u",nh,changed_nh)])
        b.transaction([("VSS_DM_DBHC","d",db,None),("VSS_HO_GD_NHANKHAU","d",nh,None)],tombstone=True)
        b.clone(1)
        self.prepare(b); self.apply(0)
        self.assertEqual(self.count("VSS_DM_DBHC"),2); self.assertEqual(self.count("VSS_HO_GD_NHANKHAU"),2)
        self.apply(1); self.apply(2)
        self.assertEqual(self.count("VSS_DM_DBHC"),1); self.assertEqual(self.count("VSS_HO_GD_NHANKHAU"),1)
        self.assertEqual(self.connection.execute("SELECT ma_cd,id,ma_ho_gd FROM mirror_test.mirror_hdg_nhankhau").fetchone(),
                         ("0987654321","Changed",None))

    def test_05_no_pk_same_payload_different_events_insert_twice(self):
        b=Builder(); data=image("VSS_DM_DBHC",id=Decimal(1),ten="Giống nhau")
        b.transaction([("VSS_DM_DBHC","c",None,data),("VSS_DM_DBHC","c",None,data)])
        b.clone(0)
        self.prepare(b); self.apply(0); self.apply(1)
        self.assertEqual(self.count("VSS_DM_DBHC"),2)

    def test_06_same_id_different_rows_do_not_merge(self):
        for_source={"VSS_DM_DBHC":("ten",Decimal(10)),"VSS_HO_GD_NHANKHAU":("ho_ten","dup-id")}
        baseline={}
        operations=[]
        for source,(name,id_) in for_source.items():
            first=image(source,id=id_,**{name:"A"})
            second=image(source,id=id_,**{name:"B"})
            baseline[source]=[first,second]
            operations.append((source,"u",first,dict(first,id=None,**{name:"C"})))
        b=Builder(baseline=baseline); b.transaction(operations)
        self.prepare(b); self.apply(0)
        for source,(name,id_) in for_source.items():
            self.assertEqual(self.count(source),2)
            self.assertEqual(self.connection.execute(sql.SQL("SELECT count(*) FROM {} WHERE {}=%s AND id=%s").format(
                relation(TABLES[source]),sql.Identifier(name)),("B",id_)).fetchone()[0],1)

    def test_07_same_offset_replay_and_restart_are_idempotent(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        self.prepare(b); self.apply(0); checkpoint=self.checkpoints()
        self.connection.close(); self.connection=connect_test(); self.addCleanup(self.connection.close)
        self.assertEqual(self.apply(0),"REPLAY_FRAME")
        self.assertEqual(self.count("VSS_DM_DBHC"),1); self.assertEqual(self.checkpoints(),checkpoint)

    def test_08_atomic_rollback_after_partial_dml_and_retry(self):
        b=Builder(); b.transaction([
            ("VSS_DM_DANTOC","c",None,image("VSS_DM_DANTOC")),
            ("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        self.prepare(b); checkpoint=self.checkpoints()
        def fail(phase):
            if phase=="before_commit": raise SimulatedCrash("before commit")
        with self.assertRaises(SimulatedCrash): self.apply(0,fail)
        self.assertEqual(self.count("VSS_DM_DANTOC"),0); self.assertEqual(self.count("VSS_DM_DBHC"),0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.inbox").fetchone()[0],0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.event_ledger").fetchone()[0],0)
        self.assertEqual(self.checkpoints(),checkpoint)
        self.assertEqual(self.apply(0),"APPLIED")

    def test_09_missing_before_rolls_back_other_table_and_quarantines(self):
        b=Builder(); absent=image("VSS_DM_DBHC",id=Decimal(1))
        b.transaction([("VSS_DM_DANTOC","c",None,image("VSS_DM_DANTOC")),
                       ("VSS_DM_DBHC","u",absent,dict(absent,ten="new"))])
        self.prepare(b); checkpoint=self.checkpoints()
        with self.assertRaisesRegex(Rejected,"BEFORE_IMAGE_NOT_FOUND"): self.apply(0)
        self.assertEqual(self.count("VSS_DM_DANTOC"),0)
        self.assert_blocked_without_apply("BEFORE_IMAGE_NOT_FOUND",checkpoint)
        with self.assertRaisesRegex(Rejected,"EPOCH_BLOCKED"): self.apply(0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.quarantine").fetchone()[0],1)

    def mutate_data(self,transform,frame=0):
        for coordinate in self.doc["frames"][frame]["records"]:
            r=self.found[tuple(coordinate)]
            if ".VSS." in r.topic and r.value is not None:
                raw=loads(r.value); transform(raw)
                self.found[tuple(coordinate)]=replace(r,value=json.dumps(raw).encode())
                return

    def test_10_malformed_message_does_not_advance(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        self.prepare(b); checkpoint=self.checkpoints()
        r=next(r for r in self.batch(0) if ".VSS." in r.topic)
        self.found[r.coordinate]=replace(r,value=b"not JSON")
        with self.assertRaisesRegex(Rejected,"MALFORMED_JSON"): self.apply(0)
        self.assert_blocked_without_apply("MALFORMED_JSON",checkpoint)

    def test_11_schema_mismatch_event_is_quarantined(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        self.prepare(b); checkpoint=self.checkpoints()
        def bad(raw):
            next(f for f in raw["schema"]["fields"] if f["field"]=="after")["fields"].pop()
        self.mutate_data(bad)
        with self.assertRaisesRegex(Rejected,"SCHEMA_COLUMNS"): self.apply(0)
        self.assert_blocked_without_apply("SCHEMA_COLUMNS",checkpoint)

    def test_12_missing_image_field_not_assumed_null(self):
        source="VSS_DM_DBHC"; old=image(source)
        b=Builder(baseline={source:[old]}); b.transaction([(source,"u",old,dict(old,ten="new"))])
        self.prepare(b); checkpoint=self.checkpoints()
        self.mutate_data(lambda raw:raw["payload"]["before"].pop("ID"))
        with self.assertRaisesRegex(Rejected,"MISSING_OR_EXTRA"): self.apply(0)
        self.assertEqual(self.count(source),1)
        self.assert_blocked_without_apply("MISSING_OR_EXTRA_ROW_FIELD",checkpoint)

    def test_13_out_of_order_frame_and_commit_are_blocked(self):
        b=Builder(); old=image("VSS_DM_DBHC")
        b.transaction([("VSS_DM_DBHC","c",None,old)],commit=200)
        b.transaction([("VSS_DM_DBHC","u",old,dict(old,ten="new"))],commit=100)
        self.prepare(b); self.apply(0); checkpoint=self.checkpoints()
        with self.assertRaisesRegex(Rejected,"OUT_OF_ORDER_COMMIT"): self.apply(1)
        self.assertEqual(self.checkpoints(),checkpoint); self.assertEqual(self.count("VSS_DM_DBHC"),1)

    def test_14_cannot_jump_over_transaction(self):
        b=Builder(); old=image("VSS_DM_DBHC")
        b.transaction([("VSS_DM_DBHC","c",None,old)])
        b.transaction([("VSS_DM_DBHC","u",old,dict(old,ten="new"))])
        self.prepare(b); checkpoint=self.checkpoints()
        with self.assertRaisesRegex(Rejected,"FRAME_ORDER"): self.apply(1)
        self.assert_blocked_without_apply("FRAME_ORDER",checkpoint)

    def test_15_source_identity_conflict_new_offsets_is_blocked(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))]); b.clone(0)
        self.prepare(b); self.apply(0); checkpoint=self.checkpoints()
        self.mutate_data(lambda raw:raw["payload"]["after"].update(TEN="different"),frame=1)
        with self.assertRaisesRegex(Rejected,"SOURCE_EVENT_CONTENT_CONFLICT"): self.apply(1)
        self.assertEqual(self.checkpoints(),checkpoint); self.assertEqual(self.count("VSS_DM_DBHC"),1)

    def test_16_same_offset_changed_payload_is_not_a_replay(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        self.prepare(b); self.apply(0); checkpoint=self.checkpoints()
        self.mutate_data(lambda raw:raw["payload"].update(ts_ms=99))
        with self.assertRaisesRegex(Rejected,"REPLAY_FRAME_CONTENT_CONFLICT"): self.apply(0)
        self.assertEqual(self.checkpoints(),checkpoint); self.assertEqual(self.count("VSS_DM_DBHC"),1)

    def test_17_missing_end_cannot_partially_apply(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        self.prepare(b); checkpoint=self.checkpoints()
        for r in self.batch(0):
            if r.topic.endswith(".transaction"):
                raw=loads(r.value)
                if raw["payload"]["status"]=="END":
                    raw["payload"]["status"]="BEGIN"
                    self.found[r.coordinate]=replace(r,value=json.dumps(raw).encode())
        with self.assertRaisesRegex(Rejected,"TRANSACTION_END"): self.apply(0)
        self.assert_blocked_without_apply("MISSING_OR_INVALID_TRANSACTION_END",checkpoint)

    def test_18_target_typmod_drift_is_refused(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC",ten="long name"))])
        self.prepare(b); checkpoint=self.checkpoints()
        self.connection.execute("ALTER TABLE mirror_test.mirror_dm_dbhc ALTER COLUMN ten TYPE varchar(2)")
        try:
            with self.assertRaisesRegex(Rejected,"TARGET_SCHEMA"): self.apply(0)
            self.assertEqual(self.count("VSS_DM_DBHC"),0); self.assertEqual(self.checkpoints(),checkpoint)
        finally:
            self.connection.execute("ALTER TABLE mirror_test.mirror_dm_dbhc ALTER COLUMN ten TYPE varchar(200)")

    def test_19_explicit_nonzero_offset_does_not_imply_baseline(self):
        b=Builder(starts={"hdg_sink_test.VSS.VSS_DM_DBHC":17})
        b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        self.prepare(b); self.apply(0)
        value=self.connection.execute("SELECT next_offset FROM sink_meta.checkpoints WHERE topic=%s",
            (b.prefix+".VSS.VSS_DM_DBHC",)).fetchone()[0]
        self.assertEqual(value,18)

    def crash(self,phase,expected_exit):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        self.prepare(b)
        directory=HERE/"test-results"/("crash-"+str(uuid.uuid4()))
        directory.mkdir(parents=True)
        manifest=directory/"manifest.json"; records=directory/"records.jsonl"
        manifest.write_text(json.dumps(self.doc),encoding="utf-8")
        records.write_text("\n".join(json.dumps(r.wire()) for r in b.records),encoding="utf-8")
        result=subprocess.run([sys.executable,"-B",str(Path(__file__).resolve().parent/"crash_worker.py"),
                               str(manifest),str(records),phase],capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,expected_exit,result.stderr)
        return b

    def test_20_real_process_crash_before_commit_then_restart(self):
        self.crash("before_commit",91)
        self.assertEqual(self.count("VSS_DM_DBHC"),0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sink_meta.inbox").fetchone()[0],0)
        self.assertEqual(self.connection.execute("SELECT next_frame FROM sink_meta.epochs").fetchone()[0],0)
        self.assertEqual(self.apply(0),"APPLIED")
        self.assertEqual(self.count("VSS_DM_DBHC"),1)

    def test_21_real_process_crash_after_commit_then_restart(self):
        self.crash("after_commit",92)
        self.assertEqual(self.count("VSS_DM_DBHC"),1)
        self.assertEqual(self.apply(0),"REPLAY_FRAME")
        self.assertEqual(self.count("VSS_DM_DBHC"),1)

    def test_22_new_duplicate_pk_event_is_not_blind_upsert(self):
        b=Builder(); original=image("VSS_DM_DANTOC",id=Decimal(1),ten="Original")
        b.transaction([("VSS_DM_DANTOC","c",None,original)])
        b.transaction([("VSS_DM_DANTOC","c",None,dict(original,ten="Overwrite"))])
        self.prepare(b); self.apply(0); checkpoint=self.checkpoints()
        with self.assertRaisesRegex(Rejected,"PK_INSERT_CONFLICT"): self.apply(1)
        self.assertEqual(self.connection.execute("SELECT ten FROM mirror_test.mirror_dm_dantoc").fetchone()[0],"Original")
        self.assertEqual(self.checkpoints(),checkpoint)

    def test_23_partition_offset_gap_inside_frame_rejected(self):
        b=Builder(); b.transaction([
            ("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC",ten="A")),
            ("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC",ten="B"))])
        coordinates=b.frames[0]["records"]
        coordinates[1],coordinates[2]=coordinates[2],coordinates[1]
        self.prepare(b); checkpoint=self.checkpoints()
        with self.assertRaisesRegex(Rejected,"OFFSET_ORDER_OR_GAP"): self.apply(0)
        self.assert_blocked_without_apply("OFFSET_ORDER_OR_GAP",checkpoint)

    def test_24_required_ma_cd_null_rejected(self):
        b=Builder(); b.transaction([("VSS_HO_GD_NHANKHAU","c",None,image("VSS_HO_GD_NHANKHAU",ma_cd=None))])
        self.prepare(b); checkpoint=self.checkpoints()
        with self.assertRaisesRegex(Rejected,"REQUIRED_NULL"): self.apply(0)
        self.assertEqual(self.count("VSS_HO_GD_NHANKHAU"),0)
        self.assert_blocked_without_apply("REQUIRED_NULL",checkpoint)


if __name__=="__main__":
    unittest.main()
