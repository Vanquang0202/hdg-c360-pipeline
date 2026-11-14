"""Đối soát Mirror ↔ C360 chỉ đọc; dùng lại logic Full/NRT, không publish hay ACK Queue."""
import argparse
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from psycopg import sql
from psycopg.rows import dict_row
from c360_full_load import (BAD_TYPES, BAD_APPROVALS, Blocked, build_maps, connect,
                            normalize_stream, preflight)


def same_value(key, expected, actual):
    # JSONB PostgreSQL và Python có cách viết timestamp/NUMBER khác nhau nhưng phải cùng giá trị.
    if expected is None or actual is None:
        return expected is actual
    if key in ('source_modify_date', 'source_created_date'):
        return datetime.fromisoformat(expected) == datetime.fromisoformat(actual)
    if key == 'ngay_sinh_goc':
        return Decimal(str(expected)) == Decimal(str(actual))
    return expected == actual


def compare_streams(expected, actual):
    """Hai luồng đã sắp theo mã COLLATE C. Chỉ xuất thống kê, không log dữ liệu cá nhân."""
    result = dict(missing_citizens=0, extra_citizens=0, representative_mismatches=0,
                  normalized_mismatches=0, compared_citizens=0, mismatched_fields={})
    left, right = iter(expected), iter(actual)
    e, a = next(left, None), next(right, None)
    while e is not None or a is not None:
        if a is None or (e is not None and e[0]['ma_so_bhxh'] < a['ma_so_bhxh']):
            result['missing_citizens'] += 1
            e = next(left, None)
        elif e is None or a['ma_so_bhxh'] < e[0]['ma_so_bhxh']:
            result['extra_citizens'] += 1
            a = next(right, None)
        else:
            payload, raw = e
            result['compared_citizens'] += 1
            if raw != a['_raw']:
                result['representative_mismatches'] += 1
            fields = [k for k, v in payload.items() if k not in ('run_id', 'sync_date')
                      and (k not in a['_payload'] or not same_value(k, v, a['_payload'][k]))]
            if fields:
                result['normalized_mismatches'] += 1
                for key in fields:
                    result['mismatched_fields'][key] = result['mismatched_fields'].get(key, 0) + 1
            e, a = next(left, None), next(right, None)
    return result


def outcome(metrics):
    errors = ('missing_citizens', 'extra_citizens', 'representative_mismatches',
              'normalized_mismatches', 'map_mismatches')
    # Queue chưa drain: báo đang chờ đồng bộ, không gọi độ lệch tạm thời là lỗi dữ liệu đã chốt.
    if metrics['queue_rows'] or metrics['full_running']:
        return 'BLOCKED'
    if any(metrics[k] for k in errors) or metrics['expected_citizens'] != metrics['c360_rows']:
        return 'FAIL'
    return 'PASS_WITH_UNKNOWN' if metrics['unknown_rows'] else 'PASS'


def reconcile(conn):
    # BƯỚC 1: Một ảnh REPEATABLE READ cho nguồn, Map, C360, Control và Queue.
    # READ ONLY ngăn job vô tình ghi dữ liệu. Không giữ khóa hàng làm chậm worker/CDC.
    with conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        conn.execute("SET LOCAL statement_timeout='180s'")
        preflight(conn)
        snapshot, observed = conn.execute('SELECT pg_current_snapshot()::text, current_timestamp').fetchone()
        control = conn.execute("SELECT full_running,last_run_id FROM c360.c360_sync_control WHERE control_key='NORMALIZE'").fetchone()
        if not control or not control[1]:
            raise Blocked('C360_POLICY_RUN_MISSING')
        policy = conn.execute('SELECT status,metrics FROM c360.c360_sync_log WHERE run_id=%s', (control[1],)).fetchone()
        if not policy or policy[0] not in ('SUCCESS', 'SUCCESS_WITH_UNKNOWN') or 'bounds' not in (policy[1] or {}):
            raise Blocked('C360_POLICY_NOT_VERIFIED')
        bounds = policy[1]['bounds']
        metrics = dict(snapshot=snapshot, observed_at=observed.isoformat(), policy_run=str(control[1]),
                       bounds=bounds, full_running=control[0])
        # BƯỚC 2: Đếm dòng trước/sau lọc. Dòng bị loại nghiệp vụ không phải lỗi DQ.
        total, valid, distinct = conn.execute("""SELECT count(*),count(*) FILTER(WHERE ok),
          count(DISTINCT ma_cd) FILTER(WHERE ok) FROM (
          SELECT ma_cd,coalesce(typeid,'ZZ')<>ALL(%s) AND coalesce(trang_thai_duyet,0)<>ALL(%s) ok
          FROM public.mirror_hdg_nhankhau) s""", (list(BAD_TYPES), list(BAD_APPROVALS))).fetchone()
        metrics.update(source_rows=total, eligible_rows=valid, excluded_business_rows=total-valid,
                       expected_citizens=distinct, duplicate_eligible_rows=valid-distinct)
        metrics['queue_rows'] = conn.execute('SELECT count(*) FROM c360.c360_change_queue').fetchone()[0]
        # BƯỚC 3: So Map với danh mục hiện tại, không refresh Map hay tự chọn ứng viên mơ hồ.
        with conn.cursor(row_factory=dict_row) as cur:
            expected_maps = build_maps(cur.execute('SELECT * FROM public.mirror_dm_dbhc').fetchall())
            metrics['map_mismatches'] = 0
            for level, expected in zip(('tinh', 'xa'), expected_maps):
                actual = {r['ma_tra_cuu']: {k:v for k,v in r.items() if k!='version'} for r in
                          cur.execute(sql.SQL('SELECT * FROM c360.{}').format(sql.Identifier('c360_map_dbhc_'+level)))}
                metrics['map_mismatches'] += sum(expected.get(k)!=actual.get(k) for k in expected.keys() | actual.keys())
        # BƯỚC 4: Dùng normalize_stream của Full/NRT để chọn nguyên dòng đại diện và tính lại DQ.
        # Hai cursor server-side chỉ giữ một nhóm công dân/lô trong bộ nhớ.
        with conn.cursor(name='c360_reconcile_actual', row_factory=dict_row) as cur:
            cur.itersize = 500
            cur.execute("""SELECT ma_so_bhxh,to_jsonb(t) AS _payload,source_row_goc::text AS _raw
              FROM c360.c360_thong_tin_ca_nhan t ORDER BY ma_so_bhxh COLLATE "C" """)
            metrics.update(compare_streams(normalize_stream(conn, str(control[1]), observed, bounds), cur))
        # BƯỚC 5: Một công dân có thể mang nhiều cờ; tổng lượt cờ có thể lớn hơn tổng công dân.
        metrics['c360_rows'], metrics['dq_empty_rows'], metrics['unknown_rows'], metrics['clean_complete_rows'] = conn.execute("""
          SELECT count(*),count(*) FILTER(WHERE dq_flags=''),count(*) FILTER(WHERE normalization_status='UNKNOWN'),
          count(*) FILTER(WHERE dq_flags='' AND normalization_status='COMPLETE') FROM c360.c360_thong_tin_ca_nhan""").fetchone()
        metrics['dq_flags'] = dict(conn.execute("""SELECT flag,count(*) FROM c360.c360_thong_tin_ca_nhan t
          CROSS JOIN LATERAL (SELECT DISTINCT f flag FROM unnest(string_to_array(t.dq_flags,';')) f WHERE f<>'') d
          GROUP BY flag ORDER BY flag""").fetchall())
        metrics['status'] = outcome(metrics)
        return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, help='JSON tổng hợp, nên lưu trong test-results/ được Git ignore')
    args = parser.parse_args()
    try:
        with connect() as conn:
            result = reconcile(conn)
    except Exception as error:
        # Không in câu SQL/DSN hay raw row trong lỗi; chi tiết kỹ thuật để debug bằng loại lỗi.
        result = dict(status='BLOCKED', error_type=type(error).__name__)
        if isinstance(error, Blocked):
            result['reason'] = str(error)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text+'\n', encoding='utf-8')
    raise SystemExit(0 if result['status'] in ('PASS', 'PASS_WITH_UNKNOWN') else 1)


if __name__ == '__main__':
    main()
