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



class ReconcileRules(unittest.TestCase):
    # Chỉ fixture trong bộ nhớ: không bật C360_INTEGRATION và không kết nối live.
    def pair(self, code='A'):
        import json
        payload=normalize(row(ma_cd=code),'expected-run',datetime(2026,1,1))
        actual=dict(payload);actual['run_id']='different-run';actual['sync_date']='different-date'
        actual['ngay_sinh_goc']=19950815
        return (payload,'{"id": "A"}'),dict(ma_so_bhxh=code,_payload=actual,_raw='{"id": "A"}')
    def test_equal_with_different_technical_metadata(self):
        from c360_reconcile import compare_streams
        e,a=self.pair();r=compare_streams([e],[a])
        self.assertEqual(r['compared_citizens'],1)
        self.assertEqual(r['normalized_mismatches'],0)
        self.assertEqual(r['representative_mismatches'],0)
    def test_missing_and_extra_even_when_counts_equal(self):
        from c360_reconcile import compare_streams
        e,_=self.pair('A');_,a=self.pair('B');r=compare_streams([e],[a])
        self.assertEqual((r['missing_citizens'],r['extra_citizens']),(1,1))
    def test_raw_and_normalized_mismatches(self):
        from c360_reconcile import compare_streams
        e,a=self.pair();a['_raw']='{"id": "B"}';a['_payload']['ho_ten']='Khác'
        r=compare_streams([e],[a])
        self.assertEqual(r['representative_mismatches'],1)
        self.assertEqual(r['normalized_mismatches'],1)
        self.assertEqual(r['mismatched_fields'],{'ho_ten':1})
    def test_dq_mismatch_detected(self):
        from c360_reconcile import compare_streams
        e,a=self.pair();a['_payload']['dq_flags']=''
        self.assertEqual(compare_streams([e],[a])['mismatched_fields'],{'dq_flags':1})
    def test_missing_null_column_detected(self):
        from c360_reconcile import compare_streams
        e,a=self.pair();self.assertIsNone(e[0]['ma_so_bhxh_cu'])
        del a['_payload']['ma_so_bhxh_cu']
        self.assertEqual(compare_streams([e],[a])['mismatched_fields'],{'ma_so_bhxh_cu':1})
    def test_null_not_equal_to_blank(self):
        from c360_reconcile import same_value
        self.assertFalse(same_value('ho_ten',None,''))
        self.assertTrue(same_value('ho_ten',None,None))
    def test_numeric_and_timestamp_without_precision_loss(self):
        from c360_reconcile import same_value
        self.assertTrue(same_value('ngay_sinh_goc','19950815.000',19950815))
        self.assertFalse(same_value('ngay_sinh_goc','19950815.001',19950815))
        self.assertTrue(same_value('source_modify_date','2026-01-01T00:00:00','2026-01-01T00:00:00'))
    def test_empty_and_ordered_streams(self):
        from c360_reconcile import compare_streams
        self.assertEqual(compare_streams([],[])['compared_citizens'],0)
        pairs=[self.pair(x) for x in ['A','B','C']]
        r=compare_streams([x[0] for x in pairs],[pairs[1][1]])
        self.assertEqual((r['missing_citizens'],r['compared_citizens']),(2,1))
    def test_unknown_queue_and_failure_are_distinct(self):
        from c360_reconcile import outcome
        m=dict(missing_citizens=0,extra_citizens=0,representative_mismatches=0,
               normalized_mismatches=0,map_mismatches=0,expected_citizens=1,c360_rows=1,
               queue_rows=0,full_running=False,unknown_rows=0)
        self.assertEqual(outcome(m),'PASS')
        m['unknown_rows']=1;self.assertEqual(outcome(m),'PASS_WITH_UNKNOWN')
        m['queue_rows']=1;self.assertEqual(outcome(m),'BLOCKED')
        m['missing_citizens']=1;self.assertEqual(outcome(m),'BLOCKED')
        m['queue_rows']=0;self.assertEqual(outcome(m),'FAIL')
    def test_filter_dedup_not_dq_exclusion(self):
        from c360_reconcile import compare_streams
        # Một dòng mới nhất bị loại nghiệp vụ; dòng cũ có DQ vẫn phải được giữ.
        old=row();new=row(id='Z',typeid='GG',modify_date=datetime(2026,2,1))
        winner=representative([old,new]);self.assertEqual(winner['id'],'A')
        self.assertTrue(normalize(winner,'r',datetime(2026,1,1))['dq_flags'])
        e,a=self.pair();self.assertEqual(compare_streams([e],[a])['compared_citizens'],1)



@unittest.skipUnless(os.environ.get('C360_BI_READ_ONLY')=='1','Explicit SELECT-only report test opt-in')
class ReadOnlyReports(unittest.TestCase):
    # Chạy được trên local hiện tại: chỉ SELECT trong transaction READ ONLY, không fixture DML.
    def setUp(self):
        from c360_full_load import connect
        self.conn=connect();self.tx=self.conn.transaction();self.tx.__enter__()
        self.conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
    def tearDown(self):
        self.tx.__exit__(None,None,None);self.conn.close()
    def test_counts_exclusions_and_duplicate_rows(self):
        r=self.conn.execute('SELECT source_rows,eligible_rows,expected_citizens,excluded_business_rows,duplicate_eligible_rows,citizens FROM hdg_bi.reconcile_counts').fetchone()
        total,valid,expected,excluded,duplicates,actual=r
        self.assertEqual(total,valid+excluded);self.assertEqual(valid,expected+duplicates)
        self.assertEqual(expected,actual)
        self.assertEqual(self.conn.execute('SELECT sum(citizens) FROM hdg_bi.population').fetchone()[0],actual)
    def test_identity_and_quality_denominators(self):
        self.assertEqual(self.conn.execute('SELECT count(*) FROM hdg_bi.identity_by_province WHERE valid_identity+missing_identity<>citizens OR valid_identity_pct NOT BETWEEN 0 AND 100').fetchone()[0],0)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM hdg_bi.quality_by_province WHERE flagged_citizens+dq_empty_citizens<>citizens OR clean_complete_citizens>dq_empty_citizens OR unknown_citizens>citizens').fetchone()[0],0)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM hdg_bi.dq_flags WHERE citizens_pct NOT BETWEEN 0 AND 100').fetchone()[0],0)
        want=dict(self.conn.execute("SELECT f,count(*) FROM c360.c360_thong_tin_ca_nhan t CROSS JOIN LATERAL (SELECT DISTINCT f FROM unnest(string_to_array(t.dq_flags,';')) f WHERE f<>'') s GROUP BY f").fetchall())
        self.assertEqual(want,dict(self.conn.execute('SELECT flag,citizens FROM hdg_bi.dq_flags').fetchall()))
    def test_age_unknown_and_failed_history_retained(self):
        unknown=self.conn.execute("SELECT count(*) FROM c360.c360_thong_tin_ca_nhan WHERE ngay_sinh_chuan IS NULL OR ngay_sinh_chuan !~ '^[0-9]{8}$'").fetchone()[0]
        self.assertEqual(self.conn.execute('SELECT coalesce(sum(citizens),0) FROM hdg_bi.population WHERE age_years IS NULL').fetchone()[0],unknown)
        for mode,status,n in self.conn.execute('SELECT mode,status,count(*) FROM c360.c360_sync_log GROUP BY 1,2').fetchall():
            self.assertEqual(self.conn.execute('SELECT count(*) FROM hdg_bi.sync_batches WHERE mode=%s AND status=%s',(mode,status)).fetchone()[0],n)
    def test_reader_privileges_are_limited_to_aggregate_views(self):
        grants=self.conn.execute("SELECT has_table_privilege('hdg_bi_reader','hdg_bi.population','SELECT'),has_table_privilege('hdg_bi_reader','public.mirror_hdg_nhankhau','SELECT'),has_table_privilege('hdg_bi_reader','c360.c360_thong_tin_ca_nhan','SELECT'),has_table_privilege('hdg_bi_reader','c360.c360_thong_tin_ca_nhan','UPDATE')").fetchone()
        self.assertEqual(grants,(True,False,False,False))
        self.assertEqual(self.conn.execute('SHOW transaction_read_only').fetchone()[0],'on')



class BISafety(unittest.TestCase):
    # Fixture metadata trong RAM; không gọi Docker hoặc thay đổi Superset.
    def info(self):
        return dict(Name='/hdg-c360-superset',Config=dict(Labels={
          'com.docker.compose.project':'hdg-c360-bi','com.docker.compose.service':'superset'},
          Env=['HDG_BI_INSTANCE=hdg-c360','SUPERSET_CONFIG_PATH=/app/hdg_superset_config.py']),NetworkSettings=dict(Ports={
          '8088/tcp':[dict(HostIp='127.0.0.1',HostPort='8089')]}),Mounts=[])
    def test_only_local_8089_allowed(self):
        from c360_bi import assert_url
        self.assertEqual(assert_url('http://localhost:8089/'),'http://localhost:8089')
        for url in ['http://localhost:8088','http://remote:8089','http://localhost:8089/?target=8088',
                    'http://user@localhost:8089','https://localhost:8089']:
            with self.assertRaises(ValueError):assert_url(url)
    def test_independent_app_metadata_required(self):
        from c360_bi import assert_app
        cfg=dict(HDG_BI_INSTANCE='hdg-c360',APP_NAME='HDG C360',
                 SQLALCHEMY_DATABASE_URI='postgresql+psycopg2://hdg_superset@metadata:5432/hdg_superset')
        assert_app(cfg)
        for k,v in [('APP_NAME','DataHub'),('HDG_BI_INSTANCE','other'),
                    ('SQLALCHEMY_DATABASE_URI','postgresql://user@superset-db-1/superset')]:
            with self.assertRaises(ValueError):assert_app(dict(cfg,**{k:v}))
    def test_wrong_runtime_blocked_before_app_creation(self):
        from unittest.mock import patch
        from c360_bi import assert_runtime
        with patch.dict(os.environ,{'HDG_BI_INSTANCE':'DataHub','SUPERSET_CONFIG_PATH':'/app/datahub_config.py'}):
            with self.assertRaisesRegex(ValueError,'NOT_HDG_RUNTIME'):assert_runtime()
    def test_correct_container_allowed(self):
        from c360_bi import assert_container
        assert_container(self.info())
    def test_datahub_container_and_compose_blocked(self):
        from c360_bi import assert_container
        info=self.info();info['Name']='/superset-superset-1'
        with self.assertRaises(ValueError):assert_container(info)
        info=self.info();info['Config']['Labels']['com.docker.compose.project']='superset'
        with self.assertRaises(ValueError):assert_container(info)
    def test_wrong_port_mount_or_marker_blocked(self):
        from c360_bi import assert_container
        info=self.info();info['NetworkSettings']['Ports']['8088/tcp'][0]['HostPort']='8088'
        with self.assertRaises(ValueError):assert_container(info)
        info=self.info();info['Mounts']=[dict(Source='E:/Superset_Source/superset/docker')]
        with self.assertRaises(ValueError):assert_container(info)
        info=self.info();info['Config']['Env']=[]
        with self.assertRaises(ValueError):assert_container(info)

if __name__=="__main__":
    unittest.main()
