import copy
import json
import sys
import unittest
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from contract import TABLES
from schema_contract import parse_type
from protocol import (decode, row, value, descriptors, Rejected, loads, exact_decimal,
                      canonical, EPOCH, Record)
from fixtures import Builder, image, row_schema, encoded_row, field, binary_integer
from manifest import validate, check_records
from storage import bootstrap, assert_endpoint
from engine import transaction


def number_column(oracle="NUMBER"):
    return dict(name="amount",required=False,oracle=oracle,**parse_type(oracle))


class ProtocolTests(unittest.TestCase):
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
        connection.info=SimpleNamespace(dbname="hdg_mirror",host="127.0.0.1",port=55432,user="postgres")
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
        with patch.object(kafka_reader,"AdminClient",return_value=admin),patch.object(
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


if __name__=="__main__":
    unittest.main()
