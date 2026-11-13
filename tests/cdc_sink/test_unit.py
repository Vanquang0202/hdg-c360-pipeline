# Mục đích: Bảo vệ decoder, baseline, guard đích và Kafka reader không ghi offset.
# Đầu vào: Fixture và mock, không cần DB/Kafka đang chạy.
# Đầu ra: Kết quả unit test; Setup-Test.ps1 và lệnh unittest vẫn dùng file này.
import copy
import json
import sys
import unittest
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

# Nạp module runtime ở vị trí gốc; test không chứa bản sao code Sink.
sys.path.insert(0,str(Path(__file__).resolve().parents[2] / "cdc/sink"))
from contract import TABLES
from schema_contract import parse_type
from protocol import (decode, row, value, descriptors, Rejected, loads, exact_decimal,
                      canonical, EPOCH, Record)
from fixtures import Builder, image, row_schema, encoded_row, field, binary_integer
from engine import validate, check_records
from storage import bootstrap, assert_endpoint
from engine import transaction


def number_column(oracle="NUMBER"):
    return dict(name="amount",required=False,oracle=oracle,**parse_type(oracle))


class ProtocolTests(unittest.TestCase):
    # Kiểm decoder, replay, guard và Kafka reader giả lập; không cần kết nối hệ thống.
    def stage_document(self):
        b = Builder()
        document = b.document()
        document["baseline"].update(kind="spark-stage-v2", run_id="11111111-1111-4111-8111-111111111111")
        for spec in document["baseline"]["tables"].values():
            spec.pop("rows")
            spec.update(row_count=0, stage_digest="0"*64)
        return document

    def test_stage_baseline_reference_validates_without_embedding_rows(self):
        self.assertEqual(validate(self.stage_document())["baseline"]["kind"], "spark-stage-v2")

    def test_stage_baseline_rejects_bad_digest_or_count(self):
        for field, value in [("stage_digest", "bad"), ("row_count", -1)]:
            document = self.stage_document()
            next(iter(document["baseline"]["tables"].values()))[field] = value
            with self.assertRaises(Rejected):
                validate(document)

    def test_stage_baseline_cannot_also_embed_unverified_rows(self):
        document = self.stage_document()
        next(iter(document["baseline"]["tables"].values()))["rows"] = []
        with self.assertRaisesRegex(Rejected, "STAGE_BASELINE_MUST_REFERENCE_AUDIT"):
            validate(document)

    def test_cutover_scope_keeps_default_live_endpoint_blocked(self):
        from storage import cutover_target, assert_endpoint
        connection = SimpleNamespace(info=SimpleNamespace(dbname="hdg_mirror", host="127.0.0.1",
                                                           port=15432, user="hdg_user"))
        with self.assertRaises(ValueError):
            assert_endpoint(connection)
        with self.assertRaises(ValueError):
            with cutover_target(connection, "hdg_mirror", "hdg_mirror", "live"):
                pass

    def test_clone_scope_never_allows_test_reset(self):
        from storage import cutover_target, reset_test_fixture
        connection = SimpleNamespace(info=SimpleNamespace(dbname="hdg_cutover_clone_1234abcd_test",
                                       host="127.0.0.1", port=15432, user="hdg_user"), execute=Mock())
        with cutover_target(connection, connection.info.dbname, connection.info.dbname):
            with self.assertRaises(ValueError):
                reset_test_fixture(connection)
        connection.execute.assert_not_called()

    def test_cutover_plan_rejects_relabelled_test_as_live(self):
        from cutover import validate_plan
        plan = dict(format="hdg-cutover-v1", id="11111111-1111-4111-8111-111111111111",
                    purpose="live-recovery", target=dict(database="hdg_mirror",host="127.0.0.1",port=15432),
                    run_id="11111111-1111-4111-8111-111111111111",
                    manifest=dict(mode="cutover-live",baseline=dict(kind="spark-stage-v2",
                                  run_id="11111111-1111-4111-8111-111111111111")))
        with self.assertRaisesRegex(Rejected, "FRESH_LIVE_BARRIER_REQUIRED"):
            validate_plan(plan)

    def test_cutover_append_cannot_replace_applied_frames(self):
        from cutover import extend
        old=dict(id="x",target={},purpose="clone",manifest=dict(baseline={},streams=[],frames=[{"old":1}]))
        new=copy.deepcopy(old)
        new["manifest"]["frames"]=[{"changed":1},{"new":2}]
        with self.assertRaisesRegex(Rejected,"EXTENSION_PREFIX"):
            extend(Mock(),old,new)

    def test_cutover_append_validates_complete_transaction_before_metadata(self):
        from cutover import extend
        b=Builder()
        b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        old=dict(id="x",target={},purpose="clone",manifest=b.document())
        b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC",id=Decimal(2)))])
        new=copy.deepcopy(old)
        new["manifest"]=b.document()
        new["manifest"]["frames"][-1]["event_count"]=2
        connection=Mock()
        with patch("cutover.read_ranges",return_value=b.records):
            with self.assertRaisesRegex(Rejected,"TRANSACTION_EVENT_COUNT"):
                extend(connection,old,new)
        connection.execute.assert_not_called()

    def test_all_132_contract_columns_and_full_schemas(self):
        self.assertEqual(sum(len(t["columns"]) for t in TABLES.values()),132)
        for source,table in TABLES.items():
            original=image(source)
            decoded=row(encoded_row(source,original),row_schema(source),table)
            self.assertEqual(decoded,original)
            self.assertEqual(len(decoded),len(table["columns"]))

    def test_exact_variable_decimal_large_small_negative_and_zero(self):
        c=number_column(); f=field(c)
        examples=["0","1E99","1E-99","-0.0012300","99999999999999999999999999999999999999",
                  "1.2345678901234567890123456789012345678"]
        for text in examples:
            n=Decimal(text); sign,digits,exponent=n.as_tuple()
            i=int("".join(map(str,digits)))*(-1 if sign else 1)
            decoded=value(dict(scale=-exponent,value=binary_integer(i)),f,c)
            self.assertEqual(decoded,n,text)
        self.assertEqual(exact_decimal(-128,2),Decimal("-1.28"))

    def test_fixed_decimal_no_rounding_or_precision_loss(self):
        c=number_column("NUMBER(19,0)")
        f=field(c)
        self.assertEqual(value(binary_integer(9999999999999999999),f,c),
                         Decimal("9999999999999999999"))
        with self.assertRaises(Rejected):
            value(binary_integer(10000000000000000000),f,c)
        c=number_column("NUMBER(5,2)"); f=field(c)
        self.assertEqual(value(Decimal("1.2300"),f,c),Decimal("1.23"))
        with self.assertRaisesRegex(Rejected,"NUMBER_WOULD_ROUND"):
            value(Decimal("1.001"),f,c)
        c=number_column("NUMBER(4,-2)"); f=field(c)
        self.assertEqual(value(binary_integer(12),f,c),Decimal("1200"))

    def test_wire_integer_range_float_and_boolean_rejected(self):
        c=number_column("NUMBER(2)")
        with self.assertRaises(Rejected):
            value(128,field(c),c)
        for bad in [True,"1",1.0]:
            with self.assertRaises(Rejected):
                value(bad,field(c),c)
        c=number_column()
        with self.assertRaises(Rejected):
            descriptors(dict(type="struct",fields=[dict(field="AMOUNT",type="float64")]),
                        dict(mirror="fixture",columns=[c]))

    def test_string_bytes_char_semantics_and_trailing_spaces(self):
        table=TABLES["VSS_HO_GD_NHANKHAU"]
        cols={c["name"]:c for c in table["columns"]}
        self.assertEqual(value("HDG00000000000001 ",field(cols["id"]),cols["id"]),
                         "HDG00000000000001 ")
        self.assertEqual(value("á"*30,field(cols["user_ngoai_tinh"]),cols["user_ngoai_tinh"]),"á"*30)
        for bad in ["á"*126,"","a\x00b","__debezium_unavailable_value"]:
            with self.assertRaises(Rejected):
                value(bad,field(cols["ho_ten"]),cols["ho_ten"])

    def test_date_and_microtimestamp_no_timezone_or_rounding(self):
        cols={c["name"]:c for c in TABLES["VSS_DM_DBHC"]["columns"]}
        self.assertEqual(value(1000,field(cols["created_date"]),cols["created_date"]),
                         datetime(1970,1,1,0,0,1))
        self.assertEqual(value(-1,field(cols["ngay_hieu_luc"]),cols["ngay_hieu_luc"]),
                         datetime(1969,12,31,23,59,59,999999))
        for raw in [1,1500]:
            with self.assertRaisesRegex(Rejected,"TIMESTAMP_WOULD_ROUND"):
                value(raw,field(cols["created_date"]),cols["created_date"])
        nano=dict(field(cols["ngay_hieu_luc"]),name="io.debezium.time.NanoTimestamp")
        self.assertEqual(value(123456000,nano,cols["ngay_hieu_luc"]),datetime(1970,1,1,0,0,0,123456))
        with self.assertRaises(Rejected):
            value(123456001,nano,cols["ngay_hieu_luc"])
        with self.assertRaisesRegex(Rejected,"PYTHON_RANGE"):
            value(-10**18,field(cols["ngay_hieu_luc"]),cols["ngay_hieu_luc"])

    def test_missing_field_differs_from_null_and_schema_drift_rejected(self):
        source="VSS_DM_DBHC"; raw=encoded_row(source,image(source))
        raw.pop("ID")
        with self.assertRaisesRegex(Rejected,"MISSING_OR_EXTRA"):
            row(raw,row_schema(source),TABLES[source])
        raw=encoded_row(source,image(source))
        self.assertIsNone(row(raw,row_schema(source),TABLES[source])["id"])
        schema=row_schema(source); schema["fields"].reverse()
        with self.assertRaisesRegex(Rejected,"SCHEMA_COLUMNS"):
            row(raw,schema,TABLES[source])

    def test_json_duplicates_nan_and_schema_payload_required(self):
        for raw in [b'{"x":1,"x":2}',b'{"n":NaN}',b'not-json']:
            with self.assertRaises(Rejected):
                loads(raw)
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        event=next(r for r in b.records if ".VSS." in r.topic)
        for bad in [b'{"payload":{}}',b'{"schema":null,"payload":{}}',b'{"schema":{"type":"struct"},"payload":null}']:
            with self.assertRaises(Rejected):
                decode(replace(event,value=bad),b.prefix,"fixture")

    def test_key_null_allowed_no_pk_but_required_for_pk(self):
        for source in TABLES:
            b=Builder(); b.transaction([(source,"c",None,image(source))])
            record=next(r for r in b.records if ".VSS." in r.topic)
            decoded=decode(record,b.prefix,"fixture")
            self.assertEqual(decoded["kind"],"dml")
            if TABLES[source]["primary_key"]:
                with self.assertRaisesRegex(Rejected,"PRIMARY_KEY_REQUIRED"):
                    decode(replace(record,key=None),b.prefix,"fixture")
            else:
                self.assertIsNone(record.key)

    def test_tombstone_not_json_payload_null_and_rowid_not_identity(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        r=next(r for r in b.records if ".VSS." in r.topic)
        self.assertEqual(decode(replace(r,value=None),b.prefix,"fixture")["kind"],"tombstone")
        first=decode(r,b.prefix,"fixture")
        wire=loads(r.value); wire["payload"]["source"]["row_id"]="different-physical-rowid"
        changed=decode(replace(r,value=json.dumps(wire).encode()),b.prefix,"fixture")
        self.assertEqual((first["identity"],first["fingerprint"]),(changed["identity"],changed["fingerprint"]))

    def test_complete_transaction_end_counts_required(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        events=[decode(r,b.prefix,"fixture") for r in b.records]
        self.assertIsNotNone(transaction(events,b.frames[0],"fixture"))
        with self.assertRaisesRegex(Rejected,"TRANSACTION_END"):
            transaction(events[:-1],b.frames[0],"fixture")

    def test_manifest_never_assumes_zero_latest_or_source_baseline(self):
        b=Builder(starts={"hdg_sink_test.VSS.VSS_DM_DBHC":17})
        b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        doc=validate(b.document())
        self.assertEqual(len(check_records(doc,b.records)),len(b.records))
        for mutate in [
            lambda d:d.pop("baseline"),
            lambda d:d.update(mode="live"),
            lambda d:d["frames"][0]["records"].pop(),
            lambda d:d["baseline"]["tables"].pop("VSS_DM_DBHC")]:
            d=copy.deepcopy(doc); mutate(d)
            with self.assertRaises(Rejected):
                validate(d)
        changed=list(b.records); changed[1]=replace(changed[1],topic_id="recreated")
        with self.assertRaisesRegex(Rejected,"TOPIC_ID"):
            check_records(doc,changed)

    def test_endpoint_guard_rejects_live_before_any_sql(self):
        connection=MagicMock()
        connection.info=SimpleNamespace(dbname="hdg_mirror",host="127.0.0.1",port=15432,user="postgres")
        with self.assertRaises(ValueError):
            bootstrap(connection)
        connection.execute.assert_not_called()

    def test_bounded_kafka_reader_does_not_commit_or_reset(self):
        import kafka_reader
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        doc=validate(b.document())
        admin=MagicMock()
        admin.describe_topics.return_value={
            topic:SimpleNamespace(result=lambda timeout,topic=topic:SimpleNamespace(topic_id=b.ids[topic]))
            for topic in b.ids}
        consumer=MagicMock()
        consumer.get_watermark_offsets.side_effect=lambda tp,timeout:(b.starts[tp.topic],b.next[tp.topic])
        messages=[]
        for r in b.records:
            messages.append(SimpleNamespace(topic=lambda r=r:r.topic,partition=lambda r=r:r.partition,
                offset=lambda r=r:r.offset,key=lambda r=r:r.key,value=lambda r=r:r.value,
                headers=lambda:[],error=lambda:None))
        consumer.poll.side_effect=messages
        with patch.object(kafka_reader,"AdminClient",return_value=admin),patch.object(kafka_reader,"native_metadata",return_value=("test-cluster",b.ids,{t:SimpleNamespace(partitions=[SimpleNamespace(id=0)]) for t in b.ids})),patch.object(
                kafka_reader,"Consumer",return_value=consumer) as constructor:
            actual=kafka_reader.read_ranges(doc,"test-broker")
        self.assertEqual(len(actual),len(b.records))
        self.assertFalse(constructor.call_args.args[0]["enable.auto.commit"])
        self.assertFalse(constructor.call_args.args[0]["enable.auto.offset.store"])
        consumer.commit.assert_not_called()
        consumer.subscribe.assert_not_called()
        consumer.close.assert_called_once()

    def test_root_field_schema_mismatch_rejected(self):
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        record=next(r for r in b.records if ".VSS." in r.topic)
        wire=loads(record.value)
        next(f for f in wire["schema"]["fields"] if f["field"]=="source")["type"]="string"
        with self.assertRaisesRegex(Rejected,"ENVELOPE_FIELD_TYPE"):
            decode(replace(record,value=json.dumps(wire).encode()),b.prefix,"fixture")

    def test_retry_reconnect_closes_replacement_connection(self):
        import run
        import psycopg
        b=Builder(); b.transaction([("VSS_DM_DBHC","c",None,image("VSS_DM_DBHC"))])
        first=MagicMock(); replacement=MagicMock()
        with patch.object(run,"apply_frame",side_effect=[psycopg.errors.DeadlockDetected(),"APPLIED"]) as apply, \
             patch.object(run,"connect_test",return_value=replacement),patch.object(run.time,"sleep"), \
             patch("builtins.print"):
            self.assertEqual(run.replay(validate(b.document()),b.records,first),["APPLIED"])
        self.assertEqual(apply.call_count,2)
        first.close.assert_called_once()
        replacement.close.assert_called_once()

    def test_reset_requires_explicit_test_database_confirmation(self):
        import run
        with patch.object(sys,"argv",["run.py","reset-test","--confirm-test-db","hdg_mirror"]), \
             patch.object(run,"connect_test") as connect,patch("sys.stderr"):
            with self.assertRaises(SystemExit): run.main()
        connect.assert_not_called()



class OwnershipAclCloneTests(unittest.TestCase):
    # Chỉ chạy khi chỉ định clone riêng; mọi thay đổi ACL nằm trong transaction rollback.
    @unittest.skipUnless(__import__('os').environ.get('HDG_ACL_TEST_DSN'), 'Requires isolated ACL clone')
    def test_acl_allowlist_and_forbidden_changes(self):
        import os
        import psycopg
        import cutover
        from psycopg import sql
        with psycopg.connect(os.environ['HDG_ACL_TEST_DSN'],autocommit=True) as c:
            self.assertRegex(c.info.dbname,r'^hdg_acl_[0-9a-f]{8}_test$')
            self.assertEqual(c.info.port,15433)
            expected=[list(r) for r in cutover.ownership(c)]
            for r in expected:r[3]=None
            self.assertTrue(cutover.ownership_matches(c,expected))
            for row in expected:
                for statement in [
                    *['GRANT '+p+' ON public.{} TO hdg_airflow_reader' for p in ('INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER')],
                    'GRANT SELECT ON public.{} TO hdg_airflow_reader WITH GRANT OPTION',
                    'GRANT SELECT ON public.{} TO hdg_acl_outsider',
                    'ALTER TABLE public.{} OWNER TO hdg_acl_outsider',
                    'ALTER TABLE public.{} ENABLE ROW LEVEL SECURITY',
                    'GRANT UPDATE(id) ON public.{} TO hdg_airflow_reader']:
                    with self.subTest(table=row[0],statement=statement), c.transaction(force_rollback=True):
                        c.execute(sql.SQL(statement).format(sql.Identifier(row[0])))
                        self.assertFalse(cutover.ownership_matches(c,expected))
            for statement in ('GRANT hdg_acl_outsider TO hdg_airflow_reader','ALTER ROLE hdg_airflow_reader CREATEDB'):
                with self.subTest(statement=statement), c.transaction(force_rollback=True):
                    c.execute(statement)
                    self.assertFalse(cutover.ownership_matches(c,expected))


class ContinuousAssemblyTests(unittest.TestCase):
    def test_multi_event_transaction_and_tombstone(self):
        from continuous import assemble
        from fixtures import demo
        b=demo(); doc=b.document()
        frames=assemble(doc,list(reversed(b.records)))
        self.assertEqual([f['event_count'] for f in frames],[1,1,1,1])
        check_records(dict(doc,frames=frames),b.records)
        b=Builder(); a=image('VSS_DM_DANTOC',id=901,ma='ZX',ten='Test')
        b.transaction([('VSS_DM_DANTOC','c',None,a),('VSS_DM_DANTOC','u',a,dict(a,ten='Changed'))])
        self.assertEqual(assemble(b.document(),b.records)[0]['event_count'],2)

    def test_incomplete_transaction_waits_without_frame(self):
        from continuous import assemble
        b=Builder();a=image('VSS_DM_QUOC_TICH',id=901,ma='ZX',ten='Test')
        b.transaction([('VSS_DM_QUOC_TICH','c',None,a)])
        self.assertIsNone(assemble(b.document(),b.records[:-1]))
        self.assertIsNone(assemble(b.document(),[b.records[0],b.records[-1]]))

    def test_new_partition_blocks_before_kafka_ack_or_dml(self):
        import continuous
        plan={'manifest':{'epoch':'test','frames':[],'streams':[{'topic':'topic'}]}}
        future=Mock();future.result.return_value=SimpleNamespace(topic_id='id',partitions=[SimpleNamespace(id=0),SimpleNamespace(id=1)])
        admin=Mock();admin.describe_topics.return_value={'topic':future}
        consumer=Mock()
        with patch.object(continuous,'native_metadata',return_value=('cluster',{'topic':'id'},{'topic':future.result.return_value})),patch.object(continuous,'inspect'),patch.object(continuous.cutover,'assert_handoff',return_value=('ACTIVE',0)),patch.object(continuous.cutover,'checkpoints',return_value=[('topic','id',0,5)]),patch.object(continuous.cutover,'replay') as replay:
            with self.assertRaises(Rejected) as ex:continuous.cycle(Mock(),plan,admin,consumer,'unused')
            self.assertEqual(ex.exception.code,'WATCH_PARTITION_LAYOUT_CHANGED')
            consumer.commit.assert_not_called();replay.assert_not_called()

    def test_late_tombstone_has_no_dml(self):
        from continuous import assemble
        b=Builder();b.emit(b.prefix+'.VSS.VSS_DM_DBHC',None,None)
        self.assertEqual(assemble(b.document(),b.records)[0]['event_count'],0)

    def test_schema_source_and_topic_mismatch_block(self):
        from continuous import assemble
        b=Builder();a=image('VSS_DM_QUOC_TICH',id=901,ma='ZX',ten='Test')
        b.transaction([('VSS_DM_QUOC_TICH','c',None,a)])
        with self.assertRaises(Rejected):assemble(dict(b.document(),topic_prefix='wrong'),b.records)
        records=list(b.records);records[1]=replace(records[1],value=b'{invalid')
        with self.assertRaises(Rejected):assemble(b.document(),records)

class ReadOnlyHandoffTests(unittest.TestCase):
    def test_only_observer_omits_share_lock(self):
        import cutover,engine
        document=dict(epoch='test',frames=[],streams=[]);plan=dict(manifest=document,run_id='r')
        for read_only in (False,True):
            c=Mock();c.execute.side_effect=[Mock(fetchone=lambda:(True,'r')),Mock(fetchone=lambda:('ACTIVE',0,engine.manifest_hash(document)))]
            with patch.object(cutover,'checkpoints',return_value=[]):cutover.assert_handoff(c,plan,read_only=read_only)
            query=c.execute.call_args_list[0].args[0]
            self.assertEqual('FOR SHARE' in query,not read_only)

class NativeTopicIdentityTests(unittest.TestCase):
    def test_lossless_base64_and_cross_platform_encoding(self):
        from kafka_reader import canonical_uuid
        self.assertEqual(canonical_uuid('ETTRSiX+Rty5xgCu2aAjjg'),canonical_uuid('ETTRSiX-Rty5xgCu2aAjjg'))
        with self.assertRaises(Rejected):canonical_uuid('not-a-uuid')
    def test_legacy_requires_verified_binding_not_truncation(self):
        from kafka_reader import assert_topic
        legacy='AAAAABQQSP7/////nSDEag';native='uEcVHRQQSP6vcF2gnSDEag'
        with self.assertRaises(Rejected):assert_topic('t',legacy,native,'cluster')
        binding={'cluster_id':'cluster','topics':{'t':dict(legacy_id=legacy,native_id=native)}}
        assert_topic('t',legacy,native,'cluster',binding)
        with self.assertRaises(Rejected):assert_topic('t',legacy,'uUcVHRQQSP6vcF2gnSDEag','cluster',binding)
        with self.assertRaises(Rejected):assert_topic('t',legacy,native,'another-cluster',binding)

if __name__=="__main__":
    unittest.main()
