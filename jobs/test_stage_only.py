"""Offline regressions with legacy/v2 catalog fixtures and mocked Spark/JDBC actions."""
import sys
import types
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock, patch
from schema_contract import source_schema, pg_catalog_type
from schema_preflight import assert_pg_schema
from staging_metadata import RUN_COLUMNS, TABLE_RUN_COLUMNS, DIGEST_COLUMN
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
