"""Full Load C360 từ Mirror; chuẩn hóa dùng chung qua normalize_stream(codes=None).
Không ghi Mirror/sink_meta, không gọi Oracle/Kafka, không tạo trigger.
"""
import argparse
import calendar
import itertools
import json
import os
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from schema_contract import source_schema, pg_catalog_type, validate_pg_columns

ROOT = Path(__file__).resolve().parents[1]
BAD_TYPES = ("GT", "GM", "GG", "GK")
BAD_APPROVALS = (1, 3, 4)
ROLES = ("", "_lh", "_ks")
TARGET = "c360.c360_thong_tin_ca_nhan"


class Blocked(ValueError):
    pass


def eligible(row):
    return (row.get("typeid") not in BAD_TYPES
            and row.get("trang_thai_duyet") not in BAD_APPROVALS)


def representative(rows):
    """Cùng hàm cho Full/incremental. Không tự quyết NULL/tie giữa nhiều hộ."""
    rows = [r for r in rows if eligible(r)]
    if not rows:
        return None
    keys = ("modify_date", "created_date", "id")
    if len(rows) > 1 and any(r.get(k) is None for r in rows for k in keys):
        raise Blocked("REPRESENTATIVE_NULL_ORDER_UNKNOWN")
    rows.sort(key=lambda r: tuple(r.get(k) for k in keys), reverse=True)
    if len(rows) > 1 and all(rows[0].get(k) == rows[1].get(k) for k in keys):
        raise Blocked("REPRESENTATIVE_TIE_UNKNOWN")
    return rows[0]


def clean_name(value):
    if value is None:
        return None, None
    name = " ".join(value.split()) or None
    if name is None:
        return None, None
    unsigned = unicodedata.normalize("NFD", name.replace("đ", "d").replace("Đ", "D"))
    return name, "".join(c for c in unsigned if not unicodedata.combining(c)).upper()


def birth(value, bounds=None):
    """Không biến YYYY thành ngày 01/01; không tự đặt khoảng năm hợp lý."""
    if value is None:
        return None, None, "NGAY_SINH_LOI", None
    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            return None, None, "NGAY_SINH_LOI", None
        text = str(int(value))
    else:
        text = str(value)
    if not re.fullmatch(r"[0-9]{4}|[0-9]{6}|[0-9]{8}", text):
        return None, None, "NGAY_SINH_LOI", None
    year = int(text[:4])
    month = int(text[4:6]) if len(text) >= 6 else 0
    day = int(text[6:]) if len(text) == 8 else 0
    if not 1 <= year <= 9999 or (day and not month) or month > 12 or (len(text)==6 and month==0):
        return None, None, "NGAY_SINH_LOI", None
    if day and not 1 <= day <= calendar.monthrange(year, month)[1]:
        return None, None, "NGAY_SINH_LOI", None
    candidate = text[:4] if month == 0 else text[:6] if day == 0 else text
    if bounds is None:
        return None, candidate, "NGAY_SINH_NAM_UNKNOWN", "BIRTH_YEAR_RANGE"
    if not bounds[0] <= year <= bounds[1]:
        return None, candidate, "NGAY_SINH_LOI", None
    return candidate, candidate, None, None


def identity(cccd, cmnd):
    for value in (cccd, cmnd):
        if value is not None and re.fullmatch(r"[0-9]{12}", value):
            return value
    return None


def phone(value):
    if value is None:
        return None, "SDT_LOI", None
    value = "".join(value.split())
    if re.fullmatch(r"0[0-9]{9,10}", value):
        return value, None, None
    if re.fullmatch(r"[0-9]+", value) and (value.startswith("84") or len(value) == 9):
        return None, "SDT_QUY_TAC_UNKNOWN", "PHONE_CONVERSION"
    return None, "SDT_LOI", None


def build_maps(rows):
    """Tách mã cũ một lần trên danh mục nhỏ, không split trong JOIN nhân khẩu."""
    rows = [r for r in rows if r.get("tinh_trang") == "1" and r.get("status") == "1"]
    provinces = {}
    communes = {}
    for r in rows:
        code = r.get("ma_dbhc") or ""
        if r.get("ma_cha") is None and re.fullmatch(r"[0-9]{2}TTT", code):
            item = (code[:2].zfill(3), r.get("ten"))
            if code in provinces and provinces[code] != item:
                raise Blocked("DBHC_PROVINCE_CONFLICT")
            provinces[code] = item
    for r in rows:
        code = r.get("ma_dbhc") or ""
        if r.get("ma_cha") is not None:
            if not re.fullmatch(r"[0-9]{5}", code) or r["ma_cha"] not in provinces:
                raise Blocked("DBHC_COMMUNE_PARENT_OR_CODE_INVALID")
            item = (code, r.get("ten"), *provinces[r["ma_cha"]])
            if code in communes and communes[code] != item:
                raise Blocked("DBHC_COMMUNE_CONFLICT")
            communes[code] = item
    if not provinces or not communes:
        raise Blocked("EMPTY_ACTIVE_MAP")
    outputs = []
    for level, current in (("province", provinces), ("commune", communes)):
        direct, old = {}, {}
        for r in rows:
            code = r.get("ma_dbhc")
            if code not in current:
                continue
            item = current[code]
            key = item[0]
            direct.setdefault(key, set()).add(item)
            for alias in (r.get("ma_dbhc_sn") or "").split(";"):
                alias = alias.strip()
                if not alias:
                    continue
                pattern = r"[0-9]{2}" if level == "province" else r"[0-9]{5}"
                if not re.fullmatch(pattern, alias):
                    raise Blocked("DBHC_ALIAS_INVALID")
                old.setdefault(alias.zfill(3) if level == "province" else alias, set()).add(item)
        result = {}
        for key in direct.keys() | old.keys():
            choices = direct.get(key) or old[key]
            mode = "HIEN_HANH" if key in direct else "SAP_NHAP"
            if len(choices) == 1:
                item = next(iter(choices))
                data = (dict(ma_tinh=item[0], ten_tinh=item[1]) if level == "province"
                        else dict(ma_xa=item[0], ten_xa=item[1], ma_tinh=item[2], ten_tinh=item[3]))
            else:
                mode = "MO_HO"
                data = dict(ma_tinh=None, ten_tinh=None)
                if level == "commune":
                    common = {(x[2], x[3]) for x in choices}
                    if len(common) == 1:
                        data["ma_tinh"], data["ten_tinh"] = next(iter(common))
                    data.update(ma_xa=None, ten_xa=None)
            result[key] = dict(ma_tra_cuu=key, loai_map=mode, so_ung_vien=len(choices), **data)
        outputs.append(result)
    return outputs


def address(province, commune):
    """Tỉnh suy ra từ xã ưu tiên hơn tỉnh khai báo; giữ lỗi input để đối soát."""
    flags = []
    if not province or not province.get("ma_tinh"):
        flags.append("TINH_KHONG_MAP")
    if not commune:
        flags.append("XA_KHONG_MAP")
    elif commune["loai_map"] == "MO_HO":
        flags.append("XA_MO_HO")
    p = province.get("ma_tinh") if province else None
    pt = province.get("ten_tinh") if province else None
    if commune and commune.get("ma_tinh"):
        if p is not None and p != commune["ma_tinh"]:
            flags.append("TINH_LECH_XA")
        p, pt = commune["ma_tinh"], commune["ten_tinh"]
    return dict(ma_tinh=p, ten_tinh=pt, ma_xa=commune.get("ma_xa") if commune else None,
                ten_xa=commune.get("ten_xa") if commune else None), flags


def normalize(row, run_id, sync_date, bounds=None):
    """Nguồn logic chuẩn hóa duy nhất; incremental sau này gọi lại hàm này."""
    name, unsigned = clean_name(row.get("ho_ten"))
    dob, candidate, dob_flag, dob_unknown = birth(row.get("ngay_sinh"), bounds)
    tel, tel_flag, tel_unknown = phone(row.get("phone_number"))
    ident = identity(row.get("so_cccd"), row.get("so_cmtnd"))
    flags = [x for x in (dob_flag, tel_flag) if x]
    unknown = [x for x in (dob_unknown, tel_unknown) if x]
    if ident is None:
        flags.append("KHONG_SO_DINH_DANH")
    if row.get("_dt_ma") is None:
        flags.append("DAN_TOC_KHONG_MAP")
    if row.get("_qt_n",0) > 1:
        unknown.append("NATIONALITY_AMBIGUOUS")
    if row.get("_qt_n") != 1:
        flags.append("QUOC_TICH_MO_HO" if row.get("_qt_n", 0) > 1 else "QUOC_TICH_KHONG_MAP")
    data = dict(ma_so_bhxh=row["ma_cd"], ma_so_bhxh_cu=row.get("ma_cd_old"),
                source_id=row.get("id"), ma_ho_gd=row.get("ma_ho_gd"),
                source_modify_date=row.get("modify_date"), source_created_date=row.get("created_date"),
                ho_ten=name, ho_ten_unsigned=unsigned, ho_ten_goc=row.get("ho_ten"),
                ngay_sinh_chuan=dob, ngay_sinh_ung_vien=candidate, ngay_sinh_goc=row.get("ngay_sinh"),
                so_dinh_danh=ident, so_cccd_goc=row.get("so_cccd"), so_cmtnd_goc=row.get("so_cmtnd"),
                so_dien_thoai=tel, phone_number_goc=row.get("phone_number"),
                gioi_tinh=row.get("gioi_tinh"), is_alive=row.get("is_alive"),
                ma_dan_toc=row.get("_dt_ma"), ten_dan_toc=row.get("_dt_ten"),
                ma_quoc_tich=row.get("_qt_ma") if row.get("_qt_n")==1 else None,
                ten_quoc_tich=row.get("_qt_ten") if row.get("_qt_n")==1 else None,
                dia_chi_goc=row.get("dia_chi"), dc_lien_lac_goc=row.get("dc_lien_lac"),
                noi_ks_goc=row.get("noi_ks"), run_id=run_id, sync_date=sync_date)
    for role in ROLES:
        mapped, problems = address(row.get("_p"+role), row.get("_x"+role))
        if row.get("_p"+role) and row["_p"+role].get("loai_map")=="MO_HO":
            unknown.append("PROVINCE_MAPPING_AMBIGUOUS"+role.upper())
        for key, value in mapped.items():
            data[key+role] = value
        data["ma_tinh"+role+"_goc"] = row.get("ma_tinh"+role)
        data["ma_xa"+role+"_goc"] = row.get("ma_xa"+role)
        flags.extend(code+role.upper() for code in problems)
    data.update(dq_flags=";".join(sorted(set(flags))), unknown_rules=sorted(set(unknown)),
                normalization_status="UNKNOWN" if unknown else "COMPLETE")
    # Decimal được gửi dạng chuỗi rồi PostgreSQL chuyển vào numeric chính xác; raw JSON tách riêng.
    return {k: v.isoformat() if isinstance(v, datetime) else str(v) if isinstance(v, Decimal) else v
            for k, v in data.items()}


def preflight(conn):
    for table in source_schema().values():
        actual = conn.execute("""SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull
          FROM pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped
          ORDER BY a.attnum""", ("public."+table["mirror"],)).fetchall()
        expected = [(c["name"], pg_catalog_type(c), c["required"]) for c in table["columns"]]
        validate_pg_columns(actual, expected, table["mirror"])


def normalize_stream(conn, run_id, sync_date, bounds=None, codes=None):
    # Lọc trước khi chọn đại diện. LEFT JOIN không được nhân dòng: dân tộc PK, quốc tịch aggregate.
    extras, joins = [], []
    for role in ROLES:
        extras += ["to_jsonb(p"+role+") AS \"_p"+role+"\"", "to_jsonb(x"+role+") AS \"_x"+role+"\""]
        joins += ["LEFT JOIN c360.c360_map_dbhc_tinh p"+role+" ON p"+role+".ma_tra_cuu=c360.province_key(m.ma_tinh"+role+")",
                  "LEFT JOIN c360.c360_map_dbhc_xa x"+role+" ON x"+role+".ma_tra_cuu=c360.commune_key(m.ma_xa"+role+")"]
    query = """SELECT m.*,to_jsonb(m)::text AS _raw,d.ma AS _dt_ma,d.ten AS _dt_ten,
      q.n AS _qt_n,q.ma AS _qt_ma,q.ten AS _qt_ten,"""+",".join(extras)+"""
      FROM public.mirror_hdg_nhankhau m
      LEFT JOIN public.mirror_dm_dantoc d ON d.id=m.dm_dan_toc_id
      LEFT JOIN LATERAL (SELECT count(*) n,
        CASE WHEN count(*)=1 THEN min(ma) END ma,CASE WHEN count(*)=1 THEN min(ten) END ten
        FROM public.mirror_dm_quoc_tich q WHERE q.ma=m.quoc_tich OR q.id::text=m.quoc_tich) q ON true
      """+" ".join(joins)+"""
      WHERE coalesce(m.typeid,'ZZ') <> ALL(%s) AND coalesce(m.trang_thai_duyet,0) <> ALL(%s)
      """+(" AND m.ma_cd=ANY(%s)" if codes is not None else "")+"""
      ORDER BY m.ma_cd COLLATE "C",m.modify_date DESC NULLS LAST,m.created_date DESC NULLS LAST,m.id COLLATE "C" DESC NULLS LAST"""
    args = [list(BAD_TYPES), list(BAD_APPROVALS)]+([codes] if codes is not None else [])
    with conn.cursor(name="c360_"+uuid.uuid4().hex, row_factory=dict_row) as cursor:
        cursor.itersize = 500
        cursor.execute(query, args)
        for _, group in itertools.groupby(cursor, key=lambda r:r["ma_cd"]):
            row = representative(list(group))
            yield normalize(row,run_id,sync_date,bounds), row["_raw"]


def refresh_maps(conn, run_id):
    with conn.cursor(row_factory=dict_row) as cursor:
        rows = cursor.execute("SELECT * FROM public.mirror_dm_dbhc").fetchall()
    maps = build_maps(rows)
    for table, items in zip(("tinh", "xa"), maps):
        relation = sql.Identifier("c360", "c360_map_dbhc_"+table)
        conn.execute(sql.SQL("DELETE FROM {}").format(relation))
        for record in items.values():
            record = dict(record, version=run_id)
            conn.execute(sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                relation, sql.SQL(",").join(map(sql.Identifier,record)),
                sql.SQL(",").join(sql.Placeholder() for _ in record)),list(record.values()))
    return {level:len(m) for level,m in zip(("province","commune"),maps)}



def publish_stage(conn,run,codes=None):
    columns = [r[0] for r in conn.execute("""SELECT column_name FROM information_schema.columns
      WHERE table_schema='c360' AND table_name='c360_thong_tin_ca_nhan' AND column_name<>'id'
      ORDER BY ordinal_position""")]
    names = sql.SQL(",").join(map(sql.Identifier,columns))
    projection = sql.SQL(",").join(sql.SQL("s.source_row_goc") if c=="source_row_goc"
                                  else sql.Identifier("r",c) for c in columns)
    updates = sql.SQL(",").join(sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(c),sql.Identifier(c))
                               for c in columns if c!="ma_so_bhxh")
    # UUID run mới chưa có thống kê: tránh anti-join lồng quét Stage hàng chục nghìn lần.
    if codes is None:
        conn.execute("""WITH missing AS MATERIALIZED (
      SELECT ma_so_bhxh FROM c360.c360_thong_tin_ca_nhan
      EXCEPT SELECT ma_so_bhxh FROM c360.c360_stage WHERE run_id=%s)
      DELETE FROM c360.c360_thong_tin_ca_nhan t USING missing k
      WHERE t.ma_so_bhxh=k.ma_so_bhxh""",(run,))
    else:
        conn.execute("""DELETE FROM c360.c360_thong_tin_ca_nhan t WHERE ma_so_bhxh=ANY(%s)
          AND NOT EXISTS(SELECT 1 FROM c360.c360_stage s WHERE s.run_id=%s AND s.ma_so_bhxh=t.ma_so_bhxh)""",(codes,run))

    conn.execute(sql.SQL("""INSERT INTO c360.c360_thong_tin_ca_nhan ({}) SELECT {}
      FROM c360.c360_stage s CROSS JOIN LATERAL
      jsonb_populate_record(NULL::c360.c360_thong_tin_ca_nhan,s.payload) r WHERE s.run_id=%s
      ON CONFLICT(ma_so_bhxh) DO UPDATE SET {}""").format(names,projection,updates),(run,))
    # BƯỚC 5: So nội dung typed + raw giữa staging/published, không chỉ COUNT.
    mismatch = conn.execute("""SELECT count(*) FROM c360.c360_stage s
      CROSS JOIN LATERAL jsonb_populate_record(NULL::c360.c360_thong_tin_ca_nhan,s.payload) r
      LEFT JOIN c360.c360_thong_tin_ca_nhan t ON t.ma_so_bhxh=s.ma_so_bhxh
      WHERE s.run_id=%s AND (t.ma_so_bhxh IS NULL OR
      (to_jsonb(t)-'id'-'source_row_goc') IS DISTINCT FROM (to_jsonb(r)-'id'-'source_row_goc')
      OR t.source_row_goc IS DISTINCT FROM s.source_row_goc)""",(run,)).fetchone()[0]
    actual = conn.execute("SELECT count(*) FROM c360.c360_thong_tin_ca_nhan"+
                          (" WHERE ma_so_bhxh=ANY(%s)" if codes is not None else ""),
                          (codes,) if codes is not None else ()).fetchone()[0]
    count=conn.execute("SELECT count(*) FROM c360.c360_stage WHERE run_id=%s",(run,)).fetchone()[0]
    if mismatch or actual != count:
        raise Blocked("PUBLISH_CONTENT_MISMATCH")
    return actual,mismatch


def full_load(conn, bounds=None):
    # BƯỚC 1: Ghi run trước transaction chính để lỗi cũng có bằng chứng.
    run = str(uuid.uuid4())
    conn.execute("INSERT INTO c360.c360_sync_log(run_id,mode,status) VALUES(%s,'FULL','RUNNING')",(run,))
    metrics = {}
    try:
        with conn.transaction():
            # BƯỚC 2: Một snapshot cho Mirror, danh mục, Map và staging; Control chặn Full/NRT chạy chồng.
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute("SET LOCAL lock_timeout='10s'")
            conn.execute("SET LOCAL statement_timeout='180s'")
            conn.execute("SELECT control_key FROM c360.c360_sync_control WHERE control_key='NORMALIZE' FOR UPDATE")
            conn.execute("UPDATE c360.c360_sync_control SET full_running=true WHERE control_key='NORMALIZE'")
            preflight(conn)
            snapshot = conn.execute("SELECT pg_current_snapshot()::text").fetchone()[0]
            metrics["map_rows"] = refresh_maps(conn,run)
            expected = conn.execute("""SELECT count(*),count(DISTINCT ma_cd) FROM public.mirror_hdg_nhankhau
              WHERE coalesce(typeid,'ZZ') <> ALL(%s) AND coalesce(trang_thai_duyet,0) <> ALL(%s)""",
              (list(BAD_TYPES),list(BAD_APPROVALS))).fetchone()
            metrics.update(eligible_rows=expected[0],expected_citizens=expected[1])
            # BƯỚC 3: Chuẩn hóa theo nhóm mã công dân; ghi batch, không giữ toàn bộ 50k dòng trong RAM.
            sync = datetime.now(timezone.utc)
            batch = []
            with conn.cursor() as writer:
                for payload, raw in normalize_stream(conn,run,sync,bounds):
                    batch.append((run,payload["ma_so_bhxh"],Jsonb(payload),raw))
                    if len(batch) >= 500:
                        writer.executemany("INSERT INTO c360.c360_stage VALUES(%s,%s,%s,%s::jsonb)",batch);batch=[]
                if batch:
                    writer.executemany("INSERT INTO c360.c360_stage VALUES(%s,%s,%s,%s::jsonb)",batch)
            count = conn.execute("SELECT count(*) FROM c360.c360_stage WHERE run_id=%s",(run,)).fetchone()[0]
            if count != expected[1]:
                raise Blocked("STAGE_DISTINCT_COUNT_MISMATCH")
            # Chứng minh dòng đại diện bằng truy vấn cửa sổ độc lập, so toàn bộ raw JSONB.
            wrong = conn.execute("""WITH ranked AS (
              SELECT m.*,row_number() over(partition by ma_cd order by modify_date DESC NULLS LAST,
                created_date DESC NULLS LAST,id COLLATE "C" DESC NULLS LAST) rn
              FROM public.mirror_hdg_nhankhau m
              WHERE coalesce(typeid,'ZZ') <> ALL(%s) AND coalesce(trang_thai_duyet,0) <> ALL(%s))
              SELECT count(*) FROM c360.c360_stage s LEFT JOIN ranked r ON r.ma_cd=s.ma_so_bhxh AND r.rn=1
              WHERE s.run_id=%s AND (r.ma_cd IS NULL OR s.source_row_goc IS DISTINCT FROM (to_jsonb(r)-'rn'))""",
              (list(BAD_TYPES),list(BAD_APPROVALS),run)).fetchone()[0]
            if wrong:
                raise Blocked("REPRESENTATIVE_RECONCILE_MISMATCH")
            # BƯỚC 4: Publish trong cùng transaction. ID kỹ thuật của người đã có được giữ qua UPSERT.
            actual,mismatch=publish_stage(conn,run)
            metrics.update(c360_rows=actual,representative_mismatches=wrong,publish_mismatches=mismatch)
            metrics["dq_flags"] = dict(conn.execute("""SELECT f,count(*) FROM c360.c360_thong_tin_ca_nhan t
              CROSS JOIN LATERAL unnest(string_to_array(t.dq_flags,';')) f WHERE f<>'' GROUP BY f ORDER BY f""").fetchall())
            metrics["unknown_rows"] = conn.execute("SELECT count(*) FROM c360.c360_thong_tin_ca_nhan WHERE normalization_status='UNKNOWN'").fetchone()[0]
            metrics["bounds"] = bounds
            # Full không ACK/xóa Queue; worker xử lý các khóa còn lại sau khi Control được giải phóng.
            conn.execute("UPDATE c360.c360_sync_control SET full_running=false,last_run_id=%s,map_version=%s,"
                         "last_success=clock_timestamp() WHERE control_key='NORMALIZE'",(run,run))
            if conn.execute("SELECT count(*) FROM information_schema.columns WHERE table_schema='c360' AND table_name='c360_sync_control' AND column_name='catalog_digest'").fetchone()[0]:
                conn.execute("UPDATE c360.c360_sync_control SET catalog_digest=%s WHERE control_key='NORMALIZE'",(catalog_digest(conn),))
            status = "SUCCESS_WITH_UNKNOWN" if metrics["unknown_rows"] else "SUCCESS"
            conn.execute("UPDATE c360.c360_sync_log SET status=%s,finished_at=clock_timestamp(),"
                         "source_snapshot=%s,metrics=%s WHERE run_id=%s",(status,snapshot,Jsonb(metrics),run))
        return dict(run_id=run,status=status,**metrics)
    except Exception as error:
        conn.execute("UPDATE c360.c360_sync_log SET status='FAILED',finished_at=clock_timestamp(),error=%s WHERE run_id=%s",
                     (type(error).__name__+": "+str(error),run))
        raise


def checked_queue_trigger(conn, relation):
    """Ngoại lệ duy nhất cho guard Sink/cutover; không tin trigger chỉ dựa vào tên."""
    triggers = conn.execute("""
      SELECT t.tgname,t.tgtype,t.tgenabled::text,t.tgqual IS NULL,t.tgnargs,t.tgattr::text,
       t.tgconstraint,t.tgdeferrable,t.tginitdeferred,
       p.prosrc,p.prosecdef,p.provolatile::text,p.proconfig,p.pronargs,p.proparallel::text,
       n.nspname,p.proname,l.lanname,p.prorettype='trigger'::regtype,
       p.proowner=c.relowner,p.proleakproof
      FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
      JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
      JOIN pg_class c ON c.oid=t.tgrelid
      WHERE t.tgrelid=%s::regclass AND NOT t.tgisinternal""",(relation,)).fetchall()
    if not triggers:
        return 0
    if relation != "public.mirror_hdg_nhankhau" or len(triggers)!=1:
        raise Blocked("UNAPPROVED_MIRROR_TRIGGER")
    r=triggers[0]
    expected=(ROOT/"sql/postgres/06_c360_incremental.sql").read_text(encoding="utf-8").split("$enqueue$")[1].strip()
    if (r[:9] != ("c360_enqueue_nhankhau",29,"O",True,0,"",0,False,False)
        or r[9].strip()!=expected or r[10:] !=
        (False,"v",["search_path=pg_catalog, c360"],0,"u","c360","enqueue_mirror_change","plpgsql",True,True,False)):
        raise Blocked("C360_TRIGGER_CONTRACT")
    actual=conn.execute("""SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
       a.attidentity::text,pg_get_expr(d.adbin,d.adrelid)
       FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
       WHERE a.attrelid='c360.c360_change_queue'::regclass AND a.attnum>0 AND NOT a.attisdropped
       ORDER BY a.attnum""").fetchall()
    if actual != [("id","bigint",True,"a",None),("ma_cd","character varying(30)",True,"",None),
                   ("created_at","timestamp with time zone",True,"","clock_timestamp()")]:
        raise Blocked("C360_QUEUE_SCHEMA")
    checks=conn.execute("""SELECT
      q.relowner=m.relowner AND n.nspowner=m.relowner AND NOT q.relrowsecurity AND NOT q.relforcerowsecurity,
      (SELECT count(*) FROM pg_trigger WHERE tgrelid=q.oid AND NOT tgisinternal),
      (SELECT count(*) FROM pg_policy WHERE polrelid=q.oid),
      (SELECT count(*) FROM pg_rewrite WHERE ev_class=q.oid),
      (SELECT array_agg(pg_get_constraintdef(oid) ORDER BY oid) FROM pg_constraint WHERE conrelid=q.oid),
      has_table_privilege(current_user,q.oid,'INSERT'),
      has_sequence_privilege(current_user,pg_get_serial_sequence('c360.c360_change_queue','id'),'USAGE')
      FROM pg_class q JOIN pg_namespace n ON n.oid=q.relnamespace
      CROSS JOIN pg_class m WHERE q.oid='c360.c360_change_queue'::regclass AND m.oid=%s::regclass""",
      (relation,)).fetchone()
    if checks != (True,0,0,0,["PRIMARY KEY (id)"],True,True):
        raise Blocked("C360_QUEUE_DEPENDENCIES_OR_PRIVILEGES")
    return 1


def catalog_digest(conn):
    import hashlib
    hashes=[]
    for table in ("mirror_dm_dantoc","mirror_dm_quoc_tich","mirror_dm_dbhc"):
        hashes.append(conn.execute(sql.SQL("""
          SELECT encode(sha256(convert_to(coalesce(string_agg(v,'\n' ORDER BY v),''),'UTF8')),'hex')
          FROM (SELECT to_jsonb(t)::text v FROM public.{} t) s""").format(sql.Identifier(table))).fetchone()[0])
    return hashlib.sha256("".join(hashes).encode()).hexdigest()


def nrt_endpoint(conn):
    if (conn.info.host not in ("127.0.0.1","localhost") or conn.info.port!=15432
        or not (conn.info.dbname=="hdg_mirror"
                or re.fullmatch(r"hdg_cutover_clone_[0-9a-f]{8}_test",conn.info.dbname))
        or not conn.autocommit):
        raise Blocked("C360_NRT_ENDPOINT")


def install_incremental(conn):
    # BƯỚC 1: Control trước Mirror, cùng thứ tự khóa với worker/cutover; không phát DML Mirror.
    nrt_endpoint(conn)
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout='10s'")
        conn.execute("SELECT control_key FROM c360.c360_sync_control WHERE control_key='NORMALIZE' FOR UPDATE")
        conn.execute("LOCK TABLE public.mirror_hdg_nhankhau IN SHARE ROW EXCLUSIVE MODE")
        preflight(conn)
        if checked_queue_trigger(conn,"public.mirror_hdg_nhankhau"):
            saved=conn.execute("SELECT catalog_digest FROM c360.c360_sync_control WHERE control_key='NORMALIZE'").fetchone()[0]
            if saved!=catalog_digest(conn):
                raise Blocked("CATALOG_REFRESH_REQUIRED")
            guard_cdc_compatibility(conn)
            return {"status":"ALREADY_ENABLED"}
        # Map phải khớp danh mục hiện tại, không arm một worker dùng Map cũ.
        with conn.cursor(row_factory=dict_row) as cur:
            expected=build_maps(cur.execute("SELECT * FROM public.mirror_dm_dbhc").fetchall())
            for level,want in zip(("tinh","xa"),expected):
                got={r["ma_tra_cuu"]:{k:v for k,v in r.items() if k!="version"}
                     for r in cur.execute(sql.SQL("SELECT * FROM c360.{}").format(
                         sql.Identifier("c360_map_dbhc_"+level))).fetchall()}
                if got!=want:
                    raise Blocked("MAP_REFRESH_REQUIRED")
        conn.execute((ROOT/"sql/postgres/06_c360_incremental.sql").read_text(encoding="utf-8"))
        checked_queue_trigger(conn,"public.mirror_hdg_nhankhau")
        conn.execute("UPDATE c360.c360_sync_control SET catalog_digest=%s WHERE control_key='NORMALIZE'",
                     (catalog_digest(conn),))
        # BƯỚC 2: Bù khoảng trống giữa Full C360 cũ và lúc gắn trigger: chỉ enqueue khóa lệch.
        # Không nạp lại toàn bộ C360; nguồn đang bị khóa ghi trong chính transaction này.
        queued=conn.execute("""
          WITH ranked AS (SELECT m.*,row_number() OVER(PARTITION BY ma_cd ORDER BY modify_date DESC NULLS LAST,
            created_date DESC NULLS LAST,id COLLATE "C" DESC NULLS LAST) rn
            FROM public.mirror_hdg_nhankhau m WHERE coalesce(typeid,'ZZ')<>ALL(%s)
            AND coalesce(trang_thai_duyet,0)<>ALL(%s)),
          winners AS (SELECT ma_cd,to_jsonb(r)-'rn' raw FROM ranked r WHERE rn=1),
          changed AS (SELECT coalesce(w.ma_cd,t.ma_so_bhxh) ma_cd
            FROM winners w FULL JOIN c360.c360_thong_tin_ca_nhan t ON t.ma_so_bhxh=w.ma_cd
            WHERE w.raw IS DISTINCT FROM t.source_row_goc)
          INSERT INTO c360.c360_change_queue(ma_cd) SELECT ma_cd FROM changed""",
          (list(BAD_TYPES),list(BAD_APPROVALS))).rowcount
        # Hòa thứ tự/NULL giữa nhiều hộ cũng phải vào Queue, worker sẽ BLOCKED chứ không đoán.
        queued+=conn.execute("""WITH candidates AS (
          SELECT ma_cd,dense_rank() OVER(PARTITION BY ma_cd ORDER BY modify_date DESC NULLS LAST,
          created_date DESC NULLS LAST,id COLLATE "C" DESC NULLS LAST) rnk,
          count(*) OVER(PARTITION BY ma_cd) n,
          bool_or(modify_date IS NULL OR created_date IS NULL OR id IS NULL) OVER(PARTITION BY ma_cd) uncertain
          FROM public.mirror_hdg_nhankhau WHERE coalesce(typeid,'ZZ')<>ALL(%s)
          AND coalesce(trang_thai_duyet,0)<>ALL(%s))
          INSERT INTO c360.c360_change_queue(ma_cd) SELECT ma_cd FROM candidates
          GROUP BY ma_cd HAVING bool_or(n>1 AND uncertain) OR count(*) FILTER(WHERE rnk=1)>1""",
          (list(BAD_TYPES),list(BAD_APPROVALS))).rowcount
        # Dân tộc/quốc tịch đổi tên nhưng raw nhân khẩu không đổi cũng phải được đánh giá lại.
        queued+=conn.execute("""
          INSERT INTO c360.c360_change_queue(ma_cd)
          SELECT t.ma_so_bhxh FROM c360.c360_thong_tin_ca_nhan t
          LEFT JOIN public.mirror_dm_dantoc d ON d.id::text=t.source_row_goc->>'dm_dan_toc_id'
          LEFT JOIN LATERAL (SELECT count(*) n,
            CASE WHEN count(*)=1 THEN min(ma) END ma,CASE WHEN count(*)=1 THEN min(ten) END ten
            FROM public.mirror_dm_quoc_tich q WHERE q.ma=t.source_row_goc->>'quoc_tich'
            OR q.id::text=t.source_row_goc->>'quoc_tich') q ON true
          WHERE t.ma_dan_toc IS DISTINCT FROM d.ma OR t.ten_dan_toc IS DISTINCT FROM d.ten
            OR t.ma_quoc_tich IS DISTINCT FROM q.ma OR t.ten_quoc_tich IS DISTINCT FROM q.ten""").rowcount
        guard_cdc_compatibility(conn)
    return {"status":"ENABLED","bootstrap_queued":queued}


def incremental(conn, batch_size=500, failpoint=None):
    """Snapshot + publish + ACK đúng ID cùng transaction; không dùng watermark MAX(id)."""
    import time
    nrt_endpoint(conn)
    if failpoint and conn.info.dbname=="hdg_mirror":
        raise Blocked("NRT_FAILPOINT_TEST_ONLY")
    if not 1<=batch_size<=5000:
        raise ValueError("batch_size must be 1..5000")
    run=str(uuid.uuid4());started=time.monotonic()
    try:
        with conn.transaction():
            # NOWAIT tránh chờ Full rồi dùng snapshot cũ. Lỗi serialization được retry cả transaction.
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute("SET LOCAL statement_timeout='60s'")
            control=conn.execute("""SELECT full_running,last_run_id,catalog_digest
              FROM c360.c360_sync_control WHERE control_key='NORMALIZE' FOR UPDATE NOWAIT""").fetchone()
            if not control or control[0]:
                raise Blocked("FULL_C360_RUNNING")
            if control[2]!=catalog_digest(conn):
                raise Blocked("CATALOG_REFRESH_REQUIRED")
            if checked_queue_trigger(conn,"public.mirror_hdg_nhankhau")!=1:
                raise Blocked("NRT_TRIGGER_MISSING")
            claimed=conn.execute("""SELECT id,ma_cd FROM c360.c360_change_queue
              ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED""",(batch_size,)).fetchall()
            if not claimed:
                return {"status":"IDLE","claimed":0}
            preflight(conn)
            ids=[r[0] for r in claimed];codes=sorted({r[1] for r in claimed})
            bounds=None
            if control[1]:
                record=conn.execute("SELECT metrics->'bounds' FROM c360.c360_sync_log WHERE run_id=%s",
                                    (control[1],)).fetchone()
                if not record:
                    raise Blocked("C360_POLICY_RUN_MISSING")
                bounds=record[0]
            conn.execute("INSERT INTO c360.c360_sync_log(run_id,mode,status) VALUES(%s,'NRT','RUNNING')",(run,))
            snapshot=conn.execute("SELECT pg_current_snapshot()::text").fetchone()[0]
            old={r[0] for r in conn.execute("SELECT ma_so_bhxh FROM c360.c360_thong_tin_ca_nhan WHERE ma_so_bhxh=ANY(%s)",
                             (codes,)).fetchall()}
            # Cùng normalize_stream với Full Load, nhưng WHERE ma_cd=ANY(codes).
            batch=[(run,p["ma_so_bhxh"],Jsonb(p),raw)
                   for p,raw in normalize_stream(conn,run,datetime.now(timezone.utc),bounds,codes)]
            if batch:
                with conn.cursor() as cur:
                    cur.executemany("INSERT INTO c360.c360_stage VALUES(%s,%s,%s,%s::jsonb)",batch)
            actual,mismatch=publish_stage(conn,run,codes)
            if failpoint:failpoint("after_publish")
            ack=conn.execute("DELETE FROM c360.c360_change_queue WHERE id=ANY(%s)",(ids,)).rowcount
            if ack!=len(ids):
                raise Blocked("QUEUE_ACK_MISMATCH")
            if failpoint:failpoint("after_ack")
            unknown=conn.execute("""SELECT count(*) FROM c360.c360_thong_tin_ca_nhan
              WHERE ma_so_bhxh=ANY(%s) AND normalization_status='UNKNOWN'""",(codes,)).fetchone()[0]
            remaining={r[0] for r in conn.execute("SELECT ma_so_bhxh FROM c360.c360_thong_tin_ca_nhan WHERE ma_so_bhxh=ANY(%s)",(codes,)).fetchall()}
            metrics=dict(claimed=len(ids),citizens=len(codes),upserted=actual,
                         removed=len(old-remaining),inserted=len(remaining-old),updated=len(old & remaining),
                         publish_mismatches=mismatch,unknown_rows=unknown,bounds=bounds,
                         seconds=round(time.monotonic()-started,3))
            status="SUCCESS_WITH_UNKNOWN" if unknown else "SUCCESS"
            conn.execute("""UPDATE c360.c360_sync_log SET status=%s,finished_at=clock_timestamp(),
              source_snapshot=%s,metrics=%s WHERE run_id=%s""",(status,snapshot,Jsonb(metrics),run))
            conn.execute("""UPDATE c360.c360_sync_control SET last_incremental_run=%s,
              last_success=clock_timestamp() WHERE control_key='NORMALIZE'""",(run,))
        return dict(status=status,run_id=run,**metrics)
    except Exception as error:
        if isinstance(error,psycopg.errors.LockNotAvailable):
            return {"status":"BUSY","claimed":0}
        message=error.diag.message_primary if isinstance(error,psycopg.Error) else str(error)
        conn.execute("""INSERT INTO c360.c360_sync_log(run_id,mode,status,finished_at,error)
          VALUES(%s,'NRT','FAILED',clock_timestamp(),%s)""",(run,type(error).__name__+": "+message))
        raise


def watch(conn, batch_size=500, interval=15):
    import time
    if not 1<=interval<=60:
        raise ValueError("interval must be 1..60 seconds")
    # Một watcher mỗi DB; Full/NRT dùng chung Control, không chiếm khóa CDC Sink.
    if not conn.execute("SELECT pg_try_advisory_lock(36012026,1)").fetchone()[0]:
        raise Blocked("NRT_WATCHER_ALREADY_RUNNING")
    print(json.dumps({"status":"WATCHING","interval":interval,"pid":os.getpid()}),flush=True)
    try:
        while True:
            for attempt in range(3):
                try:
                    result=incremental(conn,batch_size)
                    break
                except psycopg.Error as error:
                    if error.sqlstate not in ("40001","40P01","55P03"):
                        raise
                    if attempt==2:
                        result={"status":"RETRY_PENDING","claimed":0}
                        break
                    time.sleep(0.2*(attempt+1))
            if result["status"] in ("IDLE","BUSY","RETRY_PENDING"):
                time.sleep(interval)
            else:
                print(json.dumps(result),flush=True)
    finally:
        conn.execute("SELECT pg_advisory_unlock(36012026,1)")

def guard_cdc_compatibility(conn):
    # Khi arm trigger phải chạy chính guard Sink/cutover, không tự gán live_checked=True.
    import sys
    sink=str(ROOT/"cdc/sink")
    if sink not in sys.path:sys.path.insert(0,sink)
    import storage
    import cutover
    from protocol import canonical
    if conn.info.dbname=="hdg_mirror":
        records=conn.execute("SELECT plan,last_data,before_ownership FROM hdg_load.cutovers WHERE state='ACTIVE'").fetchall()
        if len(records)!=1:
            raise Blocked("NRT_REQUIRES_ACTIVE_CUTOVER")
        plan,data,owner=records[0]
        with cutover.target(conn,plan,"hdg_mirror","status"):
            storage.assert_target(conn);cutover.safe_schema(conn);cutover.assert_handoff(conn,plan)
            if cutover.table_hashes(conn)!=data or canonical(cutover.ownership(conn))!=owner:
                raise Blocked("NRT_MIRROR_EXTERNAL_CHANGE")
    else:
        with storage.cutover_target(conn,conn.info.dbname,conn.info.dbname,"clone"):
            storage.assert_target(conn);cutover.safe_schema(conn)


def connect():
    # Cấu hình dùng biến môi trường/jobs/.env; không đọc Docker inspect hoặc hardcode mật khẩu.
    from urllib.parse import urlparse
    env_file = ROOT/"jobs/.env"
    if env_file.exists():
        # Cấu hình KEY=value đơn giản; không nội suy biến hoặc thực thi nội dung file.
        for line in env_file.read_text(encoding="utf-8-sig").splitlines():
            if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() in ("PG_JDBC_URL", "PG_USER", "PG_PASSWORD"):
                value = value.strip()
                if len(value)>=2 and value[0]==value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                os.environ.setdefault(key.strip(), value)
    url = urlparse(os.environ.get("PG_JDBC_URL","jdbc:postgresql://localhost:15432/hdg_mirror").removeprefix("jdbc:"))
    if url.hostname not in ("localhost","127.0.0.1") or url.port!=15432 or url.path!="/hdg_mirror" or url.username or url.password or url.query:
        raise Blocked("C360_ENDPOINT")
    if not os.environ.get("PG_USER") or not os.environ.get("PG_PASSWORD"):
        raise Blocked("PG_USER/PG_PASSWORD required")
    return psycopg.connect(host="127.0.0.1",port=url.port,dbname="hdg_mirror",user=os.environ["PG_USER"],
                           password=os.environ["PG_PASSWORD"],autocommit=True,connect_timeout=10)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command",choices=["install","full","enable-nrt","worker"])
    parser.add_argument("--birth-year-min",type=int)
    parser.add_argument("--birth-year-max",type=int)
    parser.add_argument("--batch-size",type=int,default=500)
    parser.add_argument("--watch",action="store_true")
    parser.add_argument("--interval",type=int,default=15)
    args=parser.parse_args()
    bounds=None
    if args.birth_year_min is not None or args.birth_year_max is not None:
        if args.birth_year_min is None or args.birth_year_max is None or not 1<=args.birth_year_min<=args.birth_year_max<=9999:
            parser.error("Both confirmed year bounds required, in 1..9999")
        bounds=(args.birth_year_min,args.birth_year_max)
    if args.command!="full" and bounds is not None:
        parser.error("Year policy is inherited from the last Full run; no NRT override")
    with connect() as conn:
        preflight(conn)
        if args.command=="install":
            # Chỉ cài schema chưa tồn tại; không CREATE OR REPLACE lên đối tượng lạ.
            if conn.execute("SELECT 1 FROM pg_namespace WHERE nspname='c360'").fetchone():
                raise Blocked("C360_SCHEMA_ALREADY_EXISTS")
            with conn.transaction():
                conn.execute((ROOT/"sql/postgres/05_c360.sql").read_text(encoding="utf-8"))
            print(json.dumps(dict(status="INSTALLED",triggers=False)))
        elif args.command=="full":
            print(json.dumps(full_load(conn,bounds),ensure_ascii=False))
        elif args.command=="enable-nrt":
            print(json.dumps(install_incremental(conn),ensure_ascii=False))
        elif args.watch:
            watch(conn,args.batch_size,args.interval)
        else:
            print(json.dumps(incremental(conn,args.batch_size),ensure_ascii=False))


if __name__=="__main__":
    main()
