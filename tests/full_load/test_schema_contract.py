# Mục đích: Bảo vệ mapping và SQL sinh ra khỏi lỗi cắt chuỗi/lệch schema.
# Đầu vào: DDL và source hiện có, metadata giả lập.
# Đầu ra: Kết quả unit test offline; không kết nối Oracle/PostgreSQL.
"""Offline schema/SQL regression tests. Never imports JDBC drivers or connects to a database."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs"))
import re
import unittest
from unittest.mock import patch
from schema_contract import source_schema, parse_type, checked_expression, pg_catalog_type, validate_pg_columns
from schema_preflight import validate_oracle_columns
from build_full_load_sql import generated_files
from pathlib import Path


class ContractTests(unittest.TestCase):
    # Giữ các ca mapping/typmod/SQL sinh ra để ngăn tái diễn mất dữ liệu.
    def test_all_132_columns_have_independent_type_lengths(self):
        schema=source_schema()
        self.assertEqual([len(t["columns"]) for t in schema.values()],[111,6,3,12])
        for table in schema.values():
            for c in table["columns"]:
                if c["family"]=="string":
                    inside=c["oracle"].split("(",1)[1].split(")",1)[0]
                    length=int(inside.split()[0])
                    self.assertEqual(c["postgres"],f"varchar({length})")
        population={c["name"]:c for c in schema["VSS_HO_GD_NHANKHAU"]["columns"]}
        for name,size in {"id":40,"ma_cd":30,"ma_ho_gd":30,"ho_ten":250,
                          "dia_chi":500,"so_cccd":50,"ma_offline":100,"user_ngoai_tinh":30,
                          "ly_do_phe_duyet":1000,"key_sl":250,"ma_the":2000}.items():
            self.assertEqual(population[name]["length"],size)
        self.assertFalse(population["ma_ho_gd"]["required"])
        self.assertEqual(schema["VSS_DM_DBHC"]["primary_key"],[])
        self.assertEqual(schema["VSS_DM_DANTOC"]["primary_key"],["id"])
        self.assertEqual(schema["VSS_DM_QUOC_TICH"]["columns"][-1]["length"],100)

    def test_explicit_type_arguments_and_rounding_limits(self):
        for oracle,expected in [
            ("VARCHAR2(500 BYTE)","varchar(500)"),("NVARCHAR2(100)","varchar(100)"),
            ("CHAR(1 BYTE)","varchar(1)"),("NUMBER","numeric"),
            ("NUMBER(19)","numeric(19,0)"),("NUMBER(10, 3)","numeric(10,3)"),
            ("NUMBER(4,-2)","numeric(4,-2)"),("DATE","timestamp(0) without time zone"),
            ("TIMESTAMP(6)","timestamp(6) without time zone")]:
            self.assertEqual(parse_type(oracle)["postgres"],expected)
        for unsupported in ["VARCHAR2(30)","FLOAT","TIMESTAMP(9)","NUMBER(39)"]:
            with self.assertRaises(ValueError):
                parse_type(unsupported)

    def test_incomplete_or_unrecognized_source_definition_fails_closed(self):
        from schema_contract import ROOT
        ddl=(ROOT/"sql/oracle/01_create_source_tables.sql").read_text(encoding="utf-8")
        with self.assertRaises(ValueError):
            source_schema(ddl.replace("ID                       VARCHAR2(40 BYTE)", "ID                       FLOAT"))

    def test_typmod_nullability_and_order_are_checked(self):
        expected=[("id","character varying(40)","false")]
        validate_pg_columns(expected,expected,"fixture")
        for wrong in [
            [("id","character varying(2)","false")],
            [("id","character varying(40)","true")],
            [("id","character varying(40)","false"),("extra","text","false")]]:
            with self.assertRaises(ValueError):
                validate_pg_columns(wrong,expected,"fixture")
        for typ in ["numeric(19,0)","timestamp(6) without time zone"]:
            with self.assertRaises(ValueError):
                validate_pg_columns([("v","numeric(2,0)","false")],[("v",typ,"false")],"fixture")

    def test_actual_oracle_metadata_must_match(self):
        table=dict(columns=[dict(name="id",oracle="VARCHAR2(40 BYTE)",required=False,**parse_type("VARCHAR2(40 BYTE)"))])
        good=[("ID","VARCHAR2","40","40","B",None,None,"Y")]
        validate_oracle_columns(good,table,"fixture")
        for actual in [
            [("ID","VARCHAR2","2","2","B",None,None,"Y")],
            [("ID","VARCHAR2","40","40","C",None,None,"Y")],
            [("ID","VARCHAR2","40","40","B",None,None,"N")]]:
            with self.assertRaises(ValueError):
                validate_oracle_columns(actual,table,"fixture")

    def test_all_generated_assets_are_current(self):
        from schema_contract import ROOT
        for path,content in generated_files().items():
            self.assertEqual((ROOT/path).read_text(encoding="utf-8"),content,path)

    def test_no_lossy_typmod_cast_in_publish_or_recovery(self):
        assets=generated_files()
        for name in ["02_publish_full_load.sql","04_build_recovery_candidate.sql"]:
            sql=assets["sql/postgres/"+name]
            self.assertNotRegex(sql,r"(?i)CAST\([^;]*? AS (?:var)?char\(")
            self.assertNotRegex(sql,r"::(?:var)?char\(\d+\)")
            self.assertIn("checked_text(id, 40, true)",sql)
            self.assertIn("expected_digest IS NULL",sql)
        sql=assets["sql/postgres/02_publish_full_load.sql"]
        self.assertIn("EXCEPT ALL",sql)
        self.assertIn("COLLATE \"C\"",sql)
        self.assertIn("octet_length",sql)
        migration=assets["sql/postgres/03_fix_mirror_contract.sql"]
        self.assertNotIn("UPDATE hdg_load.runs",migration[:migration.index("CREATE OR REPLACE FUNCTION")])
        self.assertNotIn("TRUNCATE",migration)

    def test_transport_projects_raw_values_and_temporal_precision(self):
        from spark_common import transport_projection, date_format
        schema=source_schema()
        for table in schema.values():
            oracle=transport_projection(table,"oracle")
            pg=transport_projection(table,"pg")
            self.assertNotIn("varchar(",oracle.lower())
            self.assertNotIn("varchar(",pg.lower())
            for c in table["columns"]:
                if c["family"]=="number":
                    self.assertIn("TO_CHAR("+c["name"].upper()+", 'TM9'",oracle)
                    self.assertIn(c["name"]+"::text",pg)
        for p in [0,3,6]:
            column=dict(oracle=f"TIMESTAMP({p})",temporal_precision=p)
            self.assertEqual(date_format(column,True),date_format(column,False))
            self.assertEqual(".FF" in date_format(column,True),p>0)

    def test_pg_preflight_rejects_two_character_id_before_any_write(self):
        from schema_preflight import assert_pg_schema
        schema=source_schema()
        def fake_rows(connection,sql,parameters=()):
            if "server_encoding" in sql:
                return [("UTF8",)]
            table=next(t for t in schema.values() if t["mirror"]==parameters[1])
            result=[(c["name"],pg_catalog_type(c),"true" if c["required"] else "false")
                    for c in table["columns"]]
            return [(name,"character varying(2)" if name=="id" else typ,nn) for name,typ,nn in result]
        with patch("schema_preflight.rows",side_effect=fake_rows):
            with self.assertRaisesRegex(ValueError,"Schema mismatch"):
                assert_pg_schema(object(),schema)



# Catalog legacy/v2 và nhánh stage-only dùng cùng nhóm kiểm schema.
import types
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock
from schema_preflight import assert_pg_schema
from schema_preflight import RUN_COLUMNS, TABLE_RUN_COLUMNS, DIGEST_COLUMN
import full_load
import spark_common

class Catalog:
    def __init__(self, digest=False, function=None, version=None):
        self.digest = digest
        self.function = digest if function is None else function
        self.version = version
        self.queries = []
        self.columns = {
            ("hdg_load","runs"): list(RUN_COLUMNS),
            ("hdg_load","table_runs"): list(TABLE_RUN_COLUMNS)+([DIGEST_COLUMN] if digest else []),
        }
        self.keys = {
            ("hdg_load.runs","p"): [("PRIMARY KEY (run_id)",)],
            ("hdg_load.table_runs","p"): [("PRIMARY KEY (run_id, source_table)",)],
            ("hdg_load.table_runs","f"): [("FOREIGN KEY (run_id) REFERENCES hdg_load.runs(run_id)",)],
        }
        for table in source_schema().values():
            self.columns[("hdg_stage",table["mirror"])] = [
                (c["name"],"text","true" if c["required"] else "false") for c in table["columns"]
            ]+[("load_run_id","character varying(36)","true")]
            self.columns[("public",table["mirror"])] = [
                (c["name"],pg_catalog_type(c),"true" if c["required"] else "false") for c in table["columns"]
            ]
            self.keys[("hdg_stage."+table["mirror"],"f")] = [
                ("FOREIGN KEY (load_run_id) REFERENCES hdg_load.runs(run_id)",)
            ]

    def rows(self, connection, sql, parameters=()):
        self.queries.append((sql, parameters))
        if "server_encoding" in sql:
            return [("UTF8",)]
        if "pg_attribute a JOIN pg_class" in sql:
            return self.columns.get(tuple(parameters),[])
        if "pg_attrdef" in sql:
            return [("clock_timestamp()",)]
        if "FROM pg_constraint" in sql:
            return self.keys.get(tuple(parameters),[])
        if "to_regprocedure('hdg_load.stage_digest" in sql:
            return [("hdg_load.stage_digest(text,text)" if self.function else None,)]
        if "to_regprocedure('hdg_load.contract_version" in sql:
            return [("hdg_load.contract_version()" if self.version is not None else None,)]
        if "SELECT hdg_load.contract_version()" in sql:
            return [(str(self.version),)]
        if "SELECT hdg_load.assert_mirror_schema()" in sql:
            return [("",)]
        raise AssertionError("Unexpected SQL in catalog fixture: "+sql)

    def preflight(self, **kwargs):
        with patch("schema_preflight.rows",side_effect=self.rows):
            return assert_pg_schema(object(),source_schema(),**kwargs)

class StageOnlyTests(unittest.TestCase):
    # Kiểm nhánh legacy/v2 bằng mock, gồm cả lỗi xảy ra trước khi ghi metadata.
    def test_legacy_staging_does_not_probe_publisher_or_mirror(self):
        catalog=Catalog()
        self.assertEqual(catalog.preflight(include_mirror=False),{"stage_digest":False})
        self.assertFalse(any("contract_version" in sql or "assert_mirror_schema" in sql
                             or (params and params[0]=="public") for sql,params in catalog.queries))
        checked=sum(len(catalog.columns[params])-1 for sql,params in catalog.queries
                    if "pg_attribute a JOIN pg_class" in sql and params[0]=="hdg_stage")
        self.assertEqual(checked,132)

    def test_v2_staging_uses_digest_without_needing_version_function(self):
        self.assertEqual(Catalog(digest=True).preflight(include_mirror=False),
                         {"stage_digest":True})

    def test_v2_publish_keeps_strict_checks(self):
        catalog=Catalog(digest=True,version=2)
        self.assertTrue(catalog.preflight()["stage_digest"])
        self.assertTrue(any("assert_mirror_schema()" in sql for sql,_ in catalog.queries))
        for version in [None,1,3]:
            with self.subTest(version=version), self.assertRaisesRegex(ValueError,"contract v2"):
                Catalog(digest=True,version=version).preflight()

    def test_legacy_publish_is_still_blocked(self):
        with self.assertRaisesRegex(ValueError,"contract v2"):
            Catalog().preflight()

    def test_record_audit_needs_both_digest_components_but_not_publisher(self):
        with self.assertRaisesRegex(ValueError,"Digest audit requires"):
            Catalog().preflight(include_mirror=False,require_digest=True)
        self.assertTrue(Catalog(digest=True).preflight(include_mirror=False,require_digest=True)["stage_digest"])

    def test_partial_digest_installation_fails_closed(self):
        for column,function in [(True,False),(False,True)]:
            with self.subTest(column=column,function=function):
                with self.assertRaisesRegex(ValueError,"Incomplete staging digest"):
                    Catalog(digest=column,function=function).preflight(include_mirror=False)

    def test_staging_types_nullability_and_all_four_tables_are_required(self):
        for table in source_schema().values():
            with self.subTest(table=table["mirror"]):
                catalog=Catalog()
                key=("hdg_stage",table["mirror"])
                name,_,required=catalog.columns[key][0]
                catalog.columns[key][0]=(name,"character varying(2)",required)
                with self.assertRaisesRegex(ValueError,"Schema mismatch"):
                    catalog.preflight(include_mirror=False)
        catalog=Catalog()
        catalog.columns[("hdg_stage","mirror_dm_dbhc")].pop()
        with self.assertRaisesRegex(ValueError,"Schema mismatch"):
            catalog.preflight(include_mirror=False)

    def test_metadata_typmod_nullability_and_keys_are_required(self):
        for key,index,value in [
            (("hdg_load","runs"),1,("source_scn","numeric(2,0)","true")),
            (("hdg_load","table_runs"),3,("stage_rows","bigint","false")),
            (("hdg_load","table_runs"),5,("content_equal","text","true")),
        ]:
            catalog=Catalog()
            catalog.columns[key][index]=value
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,"Schema mismatch"):
                catalog.preflight(include_mirror=False)
        catalog=Catalog()
        catalog.keys[("hdg_load.runs","p")]=[]
        with self.assertRaisesRegex(ValueError,"Missing metadata key"):
            catalog.preflight(include_mirror=False)

    def test_digest_column_has_exact_type_and_nullable_contract(self):
        for typ,required in [("character varying(2)","false"),("character varying(64)","true")]:
            catalog=Catalog(digest=True)
            catalog.columns[("hdg_load","table_runs")][-1]=("stage_digest",typ,required)
            with self.assertRaisesRegex(ValueError,"Schema mismatch"):
                catalog.preflight(include_mirror=False)

    def test_audit_writer_does_not_call_digest_on_legacy(self):
        for digest in [False,True]:
            with self.subTest(digest=digest),patch("spark_common.execute",return_value=1) as execute:
                spark_common.write_table_audit(object(),"run","source",{"mirror":"mirror_dm_dbhc"},
                                              8,8,1.5,with_digest=digest)
                sql,params=execute.call_args.args[1:]
                self.assertEqual("stage_digest" in sql,digest)
                self.assertEqual(len(params),7 if digest else 5)

    def mocked_full_load(self, catalog, mismatch=False):
        connection=MagicMock()
        spark=MagicMock()
        writes=Mock(return_value=1)
        logger=Mock()
        frames=[]
        counts=[50000,3,2,8]
        for count in counts:
            frame=MagicMock()
            frame.count.return_value=count
            frame.withColumn.return_value=frame
            frames.append(frame)

        @contextmanager
        def jdbc(*args):
            yield connection

        def scalar(*args):
            if "pg_try_advisory_lock" in args[1]:
                return "locked"
            raise AssertionError("stage-only tried to use unexpected SQL: "+args[1])

        comparisons=[dict(source_rows=n,target_rows=n,equal=not mismatch,content_equal=not mismatch)
                     for n in counts]
        fake_sql=types.ModuleType("pyspark.sql")
        fake_sql.functions=types.SimpleNamespace(lit=lambda value:value)
        with patch.dict(sys.modules,{"pyspark.sql":fake_sql}), \
             patch.object(sys,"argv",["full_load.py","--stage-only","--source-scn","2438031"]), \
             patch("full_load.settings",return_value={"pg_url":"jdbc:fixture","pg_user":"fixture","pg_password":"fixture"}), \
             patch("full_load.create_spark",return_value=spark), \
             patch("full_load.jdbc_connection",side_effect=jdbc), \
             patch("full_load.scalar",side_effect=scalar), \
             patch("full_load.assert_source_schema") as oracle_check, \
             patch("schema_preflight.rows",side_effect=catalog.rows), \
             patch("full_load.read_source",side_effect=frames) as read_source, \
             patch("full_load.read_target",return_value=MagicMock()), \
             patch("full_load.reconcile_frames",side_effect=comparisons), \
             patch("full_load.execute",writes), patch("spark_common.execute",writes), \
             patch("full_load.log",logger):
            if mismatch:
                with self.assertRaisesRegex(RuntimeError,"Stage reconciliation failed"):
                    full_load.main()
            else:
                full_load.main()
                self.assertEqual(read_source.call_count,4)
                self.assertTrue(all(call.args[-1]==2438031 for call in read_source.call_args_list))
                oracle_check.assert_called_once()
        spark.stop.assert_called_once()
        return writes,logger

    def test_stage_only_full_flow_legacy_and_v2_with_mocked_io(self):
        for digest in [False,True]:
            with self.subTest(digest=digest):
                writes,logger=self.mocked_full_load(Catalog(digest=digest))
                sqls=[call.args[1] for call in writes.call_args_list]
                audits=[sql for sql in sqls if "INSERT INTO hdg_load.table_runs" in sql]
                self.assertEqual(len(audits),4)
                self.assertTrue(all(("hdg_load.stage_digest(" in sql)==digest for sql in audits))
                self.assertFalse(any("public." in sql or "UPDATE hdg_load.control" in sql
                                     or "publish_full_load" in sql for sql in sqls))
                self.assertTrue(any("status='VALIDATED'" in sql for sql in sqls))
                staged=[call for call in logger.call_args_list if call.args[0]=="run_staged"]
                self.assertEqual(len(staged),1)
                self.assertEqual(staged[0].kwargs["audit_mode"],"SHA256" if digest else "DEFERRED_LEGACY")

    def test_stage_mismatch_cannot_be_marked_validated(self):
        writes,logger=self.mocked_full_load(Catalog(),mismatch=True)
        sqls=[call.args[1] for call in writes.call_args_list]
        self.assertTrue(any("status='FAILED'" in sql for sql in sqls))
        self.assertFalse(any("status='VALIDATED'" in sql or "INSERT INTO hdg_load.table_runs" in sql for sql in sqls))
        self.assertFalse(any(call.args[0]=="run_staged" for call in logger.call_args_list))

if __name__=="__main__":
    unittest.main()
