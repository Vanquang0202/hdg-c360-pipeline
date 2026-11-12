"""Offline tests: không kết nối database; dữ liệu đều giả lập."""
import sys
import os
import unittest
from pathlib import Path
from datetime import datetime
from decimal import Decimal

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"jobs"))
from c360_full_load import (eligible,representative,clean_name,birth,identity,phone,
                            build_maps,address,normalize,Blocked)

def row(**changes):
    r=dict(ma_cd="CD-TEST",id="A",modify_date=datetime(2026,1,1),created_date=datetime(2025,1,1),
           typeid=None,trang_thai_duyet=None,ngay_sinh=Decimal("19950815"),
           ho_ten=" Nguyễn  Văn An ",so_cccd="001095012345",phone_number="0912 345 678",
           _dt_ma="01",_dt_ten="Kinh",_qt_n=1,_qt_ma="VN",_qt_ten="Việt Nam")
    r.update(changes);return r

def geo(code,parent=None,old=None,name=None,active="1"):
    return dict(ma_dbhc=code,ma_cha=parent,ma_dbhc_sn=old,ten=name or code,tinh_trang=active,status=active)

class Rules(unittest.TestCase):
    def test_filter_exact_and_null(self):
        for value in ("GT","GM","GG","GK"):
            self.assertFalse(eligible(row(typeid=value)))
        for value in (1,3,4):
            self.assertFalse(eligible(row(trang_thai_duyet=value)))
        for value in (None,"CD","GZ"):
            self.assertTrue(eligible(row(typeid=value)))
        for value in (None,0,2,9):
            self.assertTrue(eligible(row(trang_thai_duyet=value)))
    def test_filter_before_rank(self):
        old=row()
        newest=row(id="Z",modify_date=datetime(2026,2,1),typeid="GG")
        self.assertEqual(representative([old,newest])["id"],"A")
        self.assertIsNone(representative([newest]))
    def test_order_modify_created_id(self):
        a=row();b=row(id="B")
        self.assertEqual(representative([a,b])["id"],"B")
        a["created_date"]=datetime(2025,2,1)
        self.assertEqual(representative([a,b])["id"],"A")
        b["modify_date"]=datetime(2026,2,1)
        self.assertEqual(representative([a,b])["id"],"B")
    def test_tie_and_null_unknown(self):
        with self.assertRaises(Blocked):representative([row(),row()])
        with self.assertRaises(Blocked):representative([row(),row(id="B",modify_date=None)])
        self.assertIsNotNone(representative([row(id=None,modify_date=None)]))
    def test_name(self):
        self.assertEqual(clean_name(" Đỗ   thị  Ánh "),("Đỗ thị Ánh","DO THI ANH"))
        self.assertEqual(clean_name(None),(None,None))
    def test_partial_dates(self):
        for raw,expected in [(19950000,"1995"),(1995,"1995"),(19950800,"199508"),(199508,"199508"),(19950815,"19950815")]:
            self.assertEqual(birth(raw,(1900,2026))[0],expected)
    def test_invalid_dates(self):
        for raw in [None,19950230,19950015,19951300,199500,Decimal("1995.5"),17500101]:
            self.assertEqual(birth(raw,(1900,2026))[2],"NGAY_SINH_LOI")
        self.assertEqual(birth(20000229,(1900,2026))[0],"20000229")
        self.assertIsNone(birth(19000229,(1900,2026))[0])
    def test_integral_numeric_birth(self):
        self.assertEqual(birth(Decimal("19950815.000"),(1900,2026))[0],"19950815")
        self.assertIsNone(birth(Decimal("19950815.1"),(1900,2026))[0])
    def test_catalog_ambiguous_not_arbitrarily_selected(self):
        result=normalize(row(_qt_n=2),"r",datetime(2026,1,1))
        self.assertIsNone(result["ma_quoc_tich"])
        self.assertIn("QUOC_TICH_MO_HO",result["dq_flags"])
        self.assertIn("NATIONALITY_AMBIGUOUS",result["unknown_rules"])
    def test_address_roles_independent(self):
        r=row(_p_lh=dict(ma_tinh="079",ten_tinh="HCM"),
              _x_lh=dict(ma_tinh="001",ten_tinh="HN",ma_xa="00004",ten_xa="X",loai_map="HIEN_HANH"))
        result=normalize(r,"r",datetime(2026,1,1))
        self.assertIn("TINH_LECH_XA_LH",result["dq_flags"])
        self.assertNotIn("TINH_LECH_XA;",result["dq_flags"])
        self.assertEqual(result["ma_tinh_lh"],"001")
    def test_year_range_unknown(self):
        self.assertEqual(birth(19950815),(None,"19950815","NGAY_SINH_NAM_UNKNOWN","BIRTH_YEAR_RANGE"))
        self.assertIsNone(birth(17500101)[0])
    def test_identity(self):
        self.assertEqual(identity("bad","000123456789"),"000123456789")
        self.assertIsNone(identity(None,"123456789"))
        self.assertIsNone(identity("１２３４５６７８９０１２",None))
    def test_phone(self):
        self.assertEqual(phone("0912 345 678"),("0912345678",None,None))
        self.assertEqual(phone("01234567890")[0],"01234567890")
        for value in ["84912345678","840912345678","912345678"]:
            self.assertEqual(phone(value)[2],"PHONE_CONVERSION")
        self.assertEqual(phone("12345")[1],"SDT_LOI")
    def test_map_precedence_ambiguous_same_province(self):
        p,x=build_maps([geo("01TTT",old="01"),geo("33TTT",old="33;34"),
                       geo("00004","01TTT","00013;00004;00019"),
                       geo("00008","01TTT","00019"),geo("00013","01TTT",active="0")])
        self.assertEqual(p["034"]["ma_tinh"],"033")
        self.assertEqual(x["00004"]["loai_map"],"HIEN_HANH")
        self.assertEqual(x["00013"]["ma_xa"],"00004")
        self.assertIsNone(x["00019"]["ma_xa"])
        self.assertEqual(x["00019"]["ma_tinh"],"001")
    def test_current_beats_old_alias(self):
        _,x=build_maps([geo("01TTT"),geo("00004","01TTT","00008"),geo("00008","01TTT")])
        self.assertEqual(x["00008"]["ma_xa"],"00008")
        self.assertEqual(x["00008"]["loai_map"],"HIEN_HANH")
    def test_map_duplicate_and_cross_province(self):
        rows=[geo("01TTT"),geo("33TTT"),geo("00004","01TTT","00019"),geo("00008","33TTT","00019")]
        _,x=build_maps(rows+[rows[2]])
        self.assertIsNone(x["00019"]["ma_tinh"])
        self.assertEqual(x["00019"]["so_ung_vien"],2)
    def test_map_conflict_blocked(self):
        with self.assertRaises(Blocked):
            build_maps([geo("01TTT"),geo("00004","01TTT"),geo("00004","01TTT",name="Other")])
    def test_province_from_commune(self):
        mapped,flags=address(dict(ma_tinh="079",ten_tinh="HCM"),
          dict(ma_tinh="001",ten_tinh="HN",ma_xa="00004",ten_xa="X",loai_map="HIEN_HANH"))
        self.assertEqual(mapped["ma_tinh"],"001");self.assertIn("TINH_LECH_XA",flags)
    def test_unknown_geo_retains_citizen(self):
        out=normalize(row(),"test",datetime(2026,1,1))
        self.assertEqual(out["ma_so_bhxh"],"CD-TEST")
        self.assertIn("XA_KHONG_MAP",out["dq_flags"])
        self.assertEqual(out["normalization_status"],"UNKNOWN")
    def test_flags_recomputed_and_no_cross_household_fill(self):
        a=row(id="A",modify_date=datetime(2025,1,1))
        b=row(id="B",modify_date=datetime(2026,1,1),so_cccd=None)
        result=normalize(representative([a,b]),"r",datetime(2026,1,1),(1900,2026))
        self.assertIn("KHONG_SO_DINH_DANH",result["dq_flags"])
        b["so_cccd"]="001095012345"
        self.assertNotIn("KHONG_SO_DINH_DANH",normalize(b,"r",datetime(2026,1,1),(1900,2026))["dq_flags"])

@unittest.skipUnless(os.environ.get("C360_INTEGRATION")=="1", "Explicit C360-only integration opt-in required")
class DatabaseRules(unittest.TestCase):
    # Chỉ dữ liệu/schema C360 được ghi; không seed hoặc reset Mirror/sink_meta.
    def setUp(self):
        from c360_full_load import connect
        self.conn=connect()
    def tearDown(self):
        self.conn.close()
    def fingerprint(self,table,business=False):
        from psycopg import sql
        expression="to_jsonb(t)-'run_id'-'sync_date'" if business else "to_jsonb(t)"
        query=sql.SQL("SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(h,'' ORDER BY h),''),'UTF8')),'hex') FROM (SELECT encode(sha256(convert_to(("+expression+")::text,'UTF8')),'hex') h FROM {} t) q").format(sql.Identifier("c360",table))
        return self.conn.execute(query).fetchone()
    def test_a_database_key_mapping_and_dq(self):
        for raw,expected in [("01TTT","001"),("34TTT","034"),("034","034"),("1","001"),("000",None),("1234",None)]:
            self.assertEqual(self.conn.execute("SELECT c360.province_key(%s)",(raw,)).fetchone()[0],expected)
        for raw,expected in [("13","00013"),("4","00004"),("00000",None),("123456",None)]:
            self.assertEqual(self.conn.execute("SELECT c360.commune_key(%s)",(raw,)).fetchone()[0],expected)
        self.assertEqual(self.conn.execute("SELECT ma_tinh,loai_map FROM c360.c360_map_dbhc_tinh WHERE ma_tra_cuu='034'").fetchone(),("033","SAP_NHAP"))
        self.assertEqual(self.conn.execute("SELECT ma_xa,ma_tinh,loai_map,so_ung_vien FROM c360.c360_map_dbhc_xa WHERE ma_tra_cuu='00019'").fetchone(),(None,"001","MO_HO",2))
        bad=self.conn.execute("""SELECT count(*) FROM c360.c360_thong_tin_ca_nhan WHERE
          (so_dinh_danh IS NULL) <> ('KHONG_SO_DINH_DANH'=ANY(string_to_array(dq_flags,';')))
          OR (normalization_status='UNKNOWN') <> (jsonb_array_length(unknown_rules)>0)
          OR (ma_xa_goc='00019' AND (ma_xa IS NOT NULL OR ma_tinh IS DISTINCT FROM '001'
            OR NOT ('XA_MO_HO'=ANY(string_to_array(dq_flags,';')))))""").fetchone()[0]
        self.assertEqual(bad,0)
        self.assertGreater(self.conn.execute("SELECT count(*) FROM c360.c360_thong_tin_ca_nhan WHERE source_row_goc->>'quoc_tich'='233' AND ma_quoc_tich='VN'").fetchone()[0],0)
    def test_b_failed_publish_rolls_back_all_c360_data(self):
        from c360_full_load import full_load
        tables=["c360_thong_tin_ca_nhan","c360_stage","c360_map_dbhc_tinh","c360_map_dbhc_xa","c360_sync_control","c360_change_queue"]
        before={t:self.fingerprint(t) for t in tables}
        class FailAfterPublish:
            def __init__(self,connection):self.connection=connection
            def __getattr__(self,name):return getattr(self.connection,name)
            def execute(self,query,params=None):
                result=self.connection.execute(query,params)
                text=query.as_string(self.connection) if hasattr(query,"as_string") else query
                if text.lstrip().startswith("INSERT INTO c360.c360_thong_tin_ca_nhan"):
                    raise RuntimeError("INJECTED_AFTER_PUBLISH_BEFORE_COMMIT")
                return result
        with self.assertRaisesRegex(RuntimeError,"INJECTED_AFTER_PUBLISH"):
            full_load(FailAfterPublish(self.conn))
        self.assertEqual(before,{t:self.fingerprint(t) for t in tables})
        status,error=self.conn.execute("SELECT status,error FROM c360.c360_sync_log ORDER BY started_at DESC LIMIT 1").fetchone()
        self.assertEqual(status,"FAILED");self.assertIn("INJECTED_AFTER_PUBLISH",error)
    def test_c_rerun_keeps_identity_and_business_data(self):
        from c360_full_load import full_load
        before=self.fingerprint("c360_thong_tin_ca_nhan",business=True)
        result=full_load(self.conn)
        self.assertEqual(result["c360_rows"],result["expected_citizens"])
        self.assertEqual(before,self.fingerprint("c360_thong_tin_ca_nhan",business=True))

if __name__=="__main__":
    unittest.main()
