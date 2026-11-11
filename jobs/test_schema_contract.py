"""Offline schema/SQL regression tests. Never imports JDBC drivers or connects to a database."""
import re
import unittest
from unittest.mock import patch
from schema_contract import source_schema, parse_type, checked_expression, pg_catalog_type, validate_pg_columns
from schema_preflight import validate_oracle_columns
from build_full_load_sql import generated_files
from pathlib import Path


class ContractTests(unittest.TestCase):
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


if __name__=="__main__":
    unittest.main()
