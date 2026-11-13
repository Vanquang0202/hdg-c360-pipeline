-- Feed đã chuẩn hóa, không lấy raw event Oracle. Migration chạy một lần, không reset feed.
BEGIN;
LOCK TABLE c360.c360_thong_tin_ca_nhan IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE c360.change_feed_control (
 singleton boolean PRIMARY KEY CHECK(singleton), feed_id uuid NOT NULL DEFAULT gen_random_uuid(),
 last_id bigint NOT NULL DEFAULT 0, retained_after bigint NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(last_id>=retained_after AND retained_after>=0)
);
INSERT INTO c360.change_feed_control(singleton) VALUES(true);
CREATE TABLE c360.change_events (
 event_id bigint PRIMARY KEY, operation text NOT NULL CHECK(operation IN ('UPSERT','DELETE')),
 ma_so_bhxh varchar(30) NOT NULL, payload jsonb NOT NULL,
 occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(), transaction_id text NOT NULL,
 CHECK(payload->>'maSoBhxh'=ma_so_bhxh)
);
CREATE FUNCTION c360.feed_payload(r c360.c360_thong_tin_ca_nhan) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
 SELECT jsonb_build_object('maSoBhxh',r.ma_so_bhxh,'maSoBhxhCu',r.ma_so_bhxh_cu,
 'hoTen',r.ho_ten,'ngaySinh',r.ngay_sinh_chuan,'soDinhDanh',r.so_dinh_danh,
 'soDienThoai',r.so_dien_thoai,'gioiTinh',r.gioi_tinh,'isAlive',r.is_alive,
 'maTinh',r.ma_tinh,'maXa',r.ma_xa,'danToc',r.ten_dan_toc,'quocTich',r.ten_quoc_tich,
 'dqFlags',r.dq_flags,'normalizationStatus',r.normalization_status)
$$;
-- BEFORE STATEMENT lấy mutex trước row lock: ID tăng theo transaction commit, không theo sequence.
CREATE FUNCTION c360.feed_lock() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,c360 AS $$
BEGIN
 IF TG_OP='TRUNCATE' THEN RAISE EXCEPTION 'C360_TRUNCATE_BLOCKED_USE_DELETE'; END IF;
 PERFORM singleton FROM c360.change_feed_control WHERE singleton FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'C360_FEED_CONTROL_MISSING'; END IF;
 RETURN NULL;
END $$;
CREATE FUNCTION c360.feed_emit(op text,code text,data jsonb) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,c360 AS $$
DECLARE n bigint;
BEGIN
 UPDATE c360.change_feed_control SET last_id=last_id+1 WHERE singleton RETURNING last_id INTO STRICT n;
 INSERT INTO c360.change_events(event_id,operation,ma_so_bhxh,payload,transaction_id)
 VALUES(n,op,code,data,pg_current_xact_id()::text);
END $$;
CREATE FUNCTION c360.feed_capture() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,c360 AS $$
BEGIN
 IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND OLD.ma_so_bhxh IS DISTINCT FROM NEW.ma_so_bhxh) THEN
  PERFORM c360.feed_emit('DELETE',OLD.ma_so_bhxh,jsonb_build_object('maSoBhxh',OLD.ma_so_bhxh,'maSoBhxhCu',OLD.ma_so_bhxh_cu));
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 IF TG_OP='UPDATE' AND c360.feed_payload(OLD)=c360.feed_payload(NEW) THEN RETURN NEW; END IF;
 PERFORM c360.feed_emit('UPSERT',NEW.ma_so_bhxh,c360.feed_payload(NEW));
 RETURN NEW;
END $$;
CREATE TRIGGER c360_feed_serial BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE
 ON c360.c360_thong_tin_ca_nhan FOR EACH STATEMENT EXECUTE FUNCTION c360.feed_lock();
CREATE TRIGGER c360_feed_rows AFTER INSERT OR UPDATE OR DELETE
 ON c360.c360_thong_tin_ca_nhan FOR EACH ROW EXECUTE FUNCTION c360.feed_capture();
-- Bootstrap trong cùng transaction, khóa C360 ngăn mất thay đổi khi kích hoạt feed.
INSERT INTO c360.change_events(event_id,operation,ma_so_bhxh,payload,transaction_id)
 SELECT row_number() OVER(ORDER BY ma_so_bhxh COLLATE "C"),'UPSERT',ma_so_bhxh,
 c360.feed_payload(t),pg_current_xact_id()::text FROM c360.c360_thong_tin_ca_nhan t;
UPDATE c360.change_feed_control SET last_id=(SELECT count(*) FROM c360.change_events);
-- Chỉ xóa prefix hết hạn; mặc định giữ 30 ngày. Không có lịch purge tự động.
CREATE FUNCTION c360.prune_change_events(days integer DEFAULT 30) RETURNS bigint
LANGUAGE plpgsql SET search_path=pg_catalog,c360 AS $$
DECLARE upto bigint; high bigint; removed bigint;
BEGIN
 IF days<1 OR days IS NULL THEN RAISE EXCEPTION 'RETENTION_MINIMUM_ONE_DAY'; END IF;
 SELECT last_id INTO STRICT high FROM c360.change_feed_control WHERE singleton FOR UPDATE;
 SELECT coalesce(min(event_id)-1,high) INTO upto FROM c360.change_events
 WHERE occurred_at>=clock_timestamp()-make_interval(days=>days);
 DELETE FROM c360.change_events WHERE event_id<=upto;
 GET DIAGNOSTICS removed=ROW_COUNT;
 UPDATE c360.change_feed_control SET retained_after=greatest(retained_after,upto) WHERE singleton;
 RETURN removed;
END $$;
REVOKE ALL ON FUNCTION c360.prune_change_events(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION c360.feed_emit(text,text,jsonb) FROM PUBLIC;
GRANT SELECT ON c360.change_feed_control,c360.change_events TO hdg_c360_api,hdg_airflow_reader;
-- Quyền quan sát metadata, không cấp DML/row lock hay quyền đọc raw inbox.
DO $$ BEGIN
 IF to_regclass('sink_meta.epochs') IS NOT NULL THEN
  GRANT USAGE ON SCHEMA sink_meta,hdg_load TO hdg_airflow_reader;
  GRANT SELECT ON sink_meta.guard,sink_meta.epochs,sink_meta.checkpoints,sink_meta.quarantine,
   hdg_load.control,hdg_load.cutovers TO hdg_airflow_reader;
 END IF;
END $$;
COMMIT;
