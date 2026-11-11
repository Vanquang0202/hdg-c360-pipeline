-- Mục đích: kiểm hồi quy mapping, publish, migration và recovery trong database test riêng.
-- Đầu vào: database hdg_full_load_test rỗng; các lệnh đi kèm dùng đường dẫn tương đối hiện tại.
-- Đầu ra: assertion hoặc lỗi; file này có DDL/DML fixture, không chạy trên hdg_mirror.
-- BƯỚC 1: Kiểm đúng database trước khi include DDL và publisher.
-- BƯỚC 2: Nạp fixture, kiểm replay/NOOP và các lỗi mất dữ liệu cùng COUNT.
-- BƯỚC 3: Thử migration/candidate trong test; không ghi Mirror live.
\set ON_ERROR_STOP on
-- ONLY a separately provisioned, empty test database. Never hdg_mirror.
DO $test$ BEGIN
 IF current_database()<>'hdg_full_load_test' THEN RAISE EXCEPTION 'Use hdg_full_load_test only'; END IF;
 IF to_regnamespace('hdg_load') IS NOT NULL THEN RAISE EXCEPTION 'Test database must be empty'; END IF;
END $test$;
\ir ../../sql/postgres/01_create_mirror_tables.sql
\ir ../../sql/postgres/02_publish_full_load.sql
CREATE FUNCTION pg_temp.expect_error(q text, expected text) RETURNS void LANGUAGE plpgsql AS $f$
BEGIN
 BEGIN EXECUTE q;
 EXCEPTION WHEN OTHERS THEN
   IF position(expected IN SQLERRM)=0 THEN RAISE; END IF;
   RETURN;
 END;
 RAISE EXCEPTION 'Expected failure did not occur: %',q;
END $f$;
INSERT INTO hdg_load.runs(run_id,source_scn,status) VALUES
 ('00000000-0000-0000-0000-000000000001',2314948,'VALIDATED'),
 ('00000000-0000-0000-0000-000000000002',2314948,'VALIDATED');
INSERT INTO hdg_stage.mirror_hdg_nhankhau
 (load_run_id,ma_cd,ma_ho_gd,id,ho_ten,dia_chi,so_cccd,ngay_sinh,user_id,dm_dan_toc_id,lock_data,created_date)
 VALUES
 ('00000000-0000-0000-0000-000000000001','0120456789',NULL,'HDG00000000000001',
 'Nguyễn Văn An',repeat('A',500),'001095012345','1E99','9999999999','9999999999999999999','99999999999999999999999999999999999999','2026-11-11 23:59:59 AD'),
 ('00000000-0000-0000-0000-000000000001','0120456789','HO002','HDG00000000000002',
 'Nguyễn Văn An ',NULL,NULL,'1E-99',NULL,NULL,NULL,'0001-01-01 00:00:00 BC');
INSERT INTO hdg_stage.mirror_dm_dantoc(load_run_id,id,can_cu,ma,ngay_hl,ten)
 VALUES ('00000000-0000-0000-0000-000000000001','1','Quyet dinh danh muc','01','2026-01-01 01:02:03 AD','Nhóm dân tộc thử nghiệm');
INSERT INTO hdg_stage.mirror_dm_quoc_tich(load_run_id,id,ma,ten)
 VALUES ('00000000-0000-0000-0000-000000000001','233','VN','Việt Nam');
INSERT INTO hdg_stage.mirror_dm_dbhc(load_run_id,ma_dbhc,ten,id,ngay_hieu_luc,ngay_het_hieu_luc,is_cap_xa)
 VALUES ('00000000-0000-0000-0000-000000000001','01TTT','Hà Nội','99999999999999999999',
 '2026-11-11 12:34:56.123456 AD',NULL,'-0.00000000000000000000000000123');
DO $test$
DECLARE t text; source text;
BEGIN
 FOR source,t IN SELECT * FROM (VALUES
 ('VSS_HO_GD_NHANKHAU','mirror_hdg_nhankhau'),('VSS_DM_DANTOC','mirror_dm_dantoc'),
 ('VSS_DM_QUOC_TICH','mirror_dm_quoc_tich'),('VSS_DM_DBHC','mirror_dm_dbhc')) v LOOP
   EXECUTE format('INSERT INTO hdg_stage.%I SELECT
      (jsonb_populate_record(NULL::hdg_stage.%I,to_jsonb(s)||jsonb_build_object(''load_run_id'',$1))).*
      FROM hdg_stage.%I s WHERE load_run_id=$2',t,t,t)
    USING '00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000001';
   EXECUTE format('INSERT INTO hdg_load.table_runs
      (run_id,source_table,source_rows,stage_rows,elapsed_seconds,content_equal,stage_digest)
      SELECT $1,$2,count(*),count(*),0,true,hdg_load.stage_digest($3,$1)
      FROM hdg_stage.%I WHERE load_run_id=$1',t)
    USING '00000000-0000-0000-0000-000000000001',source,t;
   INSERT INTO hdg_load.table_runs
    SELECT '00000000-0000-0000-0000-000000000002',source_table,source_rows,stage_rows,elapsed_seconds,
           content_equal,stage_digest FROM hdg_load.table_runs
    WHERE run_id='00000000-0000-0000-0000-000000000001' AND source_table=source;
 END LOOP;
 IF hdg_load.publish_full_load('00000000-0000-0000-0000-000000000001')<>'COMMITTED' THEN
    RAISE EXCEPTION 'Expected COMMITTED'; END IF;
 IF hdg_load.publish_full_load('00000000-0000-0000-0000-000000000002')<>'NOOP' THEN
    RAISE EXCEPTION 'Expected NOOP'; END IF;
 IF (SELECT count(DISTINCT id) FROM public.mirror_hdg_nhankhau)<>2
   OR (SELECT min(length(id)) FROM public.mirror_hdg_nhankhau)<>17
   OR (SELECT max(length(dia_chi)) FROM public.mirror_hdg_nhankhau)<>500
   OR (SELECT ten FROM public.mirror_dm_quoc_tich)<>'Việt Nam'
   OR (SELECT to_char(ngay_hieu_luc,'US') FROM public.mirror_dm_dbhc)<>'123456'
   OR (SELECT ngay_sinh FROM public.mirror_hdg_nhankhau WHERE ma_ho_gd IS NULL)<>1E99
   OR (SELECT user_id FROM public.mirror_hdg_nhankhau WHERE ma_ho_gd IS NULL)<>9999999999
   OR (SELECT lock_data FROM public.mirror_hdg_nhankhau WHERE ma_ho_gd IS NULL)<>99999999999999999999999999999999999999
   THEN RAISE EXCEPTION 'Loss of source values'; END IF;
 IF hdg_load.checked_numeric('1.2300',6,2)<>1.23
   OR hdg_load.checked_numeric('1200',4,-2)<>1200
   OR hdg_load.checked_text('Việt Nam',100,false)<>'Việt Nam'
   OR hdg_load.checked_timestamp('2026-11-11 12:34:56.123456 AD',6)<>timestamp '2026-11-11 12:34:56.123456'
   THEN RAISE EXCEPTION 'Exact conversion failed'; END IF;
END $test$;
SELECT pg_temp.expect_error($q$SELECT hdg_load.checked_text('abc ',3,false)$q$,'exceeds');
SELECT pg_temp.expect_error($q$SELECT hdg_load.checked_text('áá',3,true)$q$,'exceeds');
SELECT pg_temp.expect_error($q$SELECT hdg_load.checked_text('',10,false)$q$,'empty');
SELECT pg_temp.expect_error($q$SELECT hdg_load.checked_numeric('1.001',5,2)$q$,'without rounding');
SELECT pg_temp.expect_error($q$SELECT hdg_load.checked_numeric('10000000000000000000',19,0)$q$,'without rounding');
SELECT pg_temp.expect_error($q$SELECT hdg_load.checked_numeric('NaN',NULL,NULL)$q$,'finite');
SELECT pg_temp.expect_error($q$SELECT hdg_load.checked_timestamp('2026-11-11 12:34:56.1 AD',0)$q$,'Non-canonical');
SELECT pg_temp.expect_error($q$SELECT hdg_load.checked_timestamp('2026-11-11 12:34:56.1234567 AD',6)$q$,'Non-canonical');
-- Count stays equal but ID is corrupted: NOOP must not hide it.
UPDATE hdg_load.runs SET status='VALIDATED' WHERE run_id='00000000-0000-0000-0000-000000000002';
UPDATE public.mirror_hdg_nhankhau SET id='HD';
SELECT pg_temp.expect_error($q$SELECT hdg_load.publish_full_load('00000000-0000-0000-0000-000000000002')$q$,'Mirror content differs');
-- Change stage content without changing row count: saved digest must reject.
UPDATE hdg_stage.mirror_dm_quoc_tich SET ten='Khac' WHERE load_run_id='00000000-0000-0000-0000-000000000002';
SELECT pg_temp.expect_error($q$SELECT hdg_load.publish_full_load('00000000-0000-0000-0000-000000000002')$q$,'Stage count/content audit');
UPDATE hdg_stage.mirror_dm_quoc_tich SET ten='Việt Nam' WHERE load_run_id='00000000-0000-0000-0000-000000000002';
-- Simulate the legacy schema only inside this disposable test database.
ALTER TABLE public.mirror_hdg_nhankhau ALTER COLUMN id TYPE varchar(2) USING left(id,2);
ALTER TABLE public.mirror_dm_quoc_tich ALTER COLUMN ten TYPE varchar(2) USING left(ten,2);
SELECT pg_temp.expect_error($q$SELECT hdg_load.assert_mirror_schema()$q$,'schema differs');
\ir ../../sql/postgres/03_fix_mirror_contract.sql
SELECT hdg_load.assert_mirror_schema();
-- Migration widens types but must NOT claim to reconstruct lost values.
DO $test$ BEGIN
 IF (SELECT count(DISTINCT id) FROM public.mirror_hdg_nhankhau)<>1 THEN
   RAISE EXCEPTION 'Migration unexpectedly replaced live rows'; END IF;
END $test$;
\ir ../../sql/postgres/04_build_recovery_candidate.sql
SELECT hdg_load.build_recovery_candidate('00000000-0000-0000-0000-000000000002',2314948,'hdg_recovery_test');
DO $test$ BEGIN
 IF (SELECT count(DISTINCT id) FROM hdg_recovery_test.mirror_hdg_nhankhau)<>2
 OR (SELECT ten FROM hdg_recovery_test.mirror_dm_quoc_tich)<>'Việt Nam'
 OR (SELECT count(DISTINCT id) FROM public.mirror_hdg_nhankhau)<>1
 OR (SELECT last_run_id FROM hdg_load.control WHERE id=1)<>'00000000-0000-0000-0000-000000000001'
 THEN RAISE EXCEPTION 'Recovery isolation failed'; END IF;
END $test$;
SELECT 'Integration assertions completed in isolated test database' AS result;
