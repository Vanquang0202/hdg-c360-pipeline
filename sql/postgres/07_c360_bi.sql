-- BI chỉ đọc dữ liệu: tạo view tổng hợp trong schema riêng; không sửa bảng C360/Mirror.
-- BƯỚC 1: Một transaction cài đủ view. Role BI chỉ được SELECT các view này.
BEGIN;
CREATE SCHEMA hdg_bi;

-- Dòng bị loại nghiệp vụ và dòng trùng hộ được đếm riêng, không coi là công dân lỗi.
CREATE VIEW hdg_bi.reconcile_counts AS
WITH source AS (SELECT count(*) source_rows,count(*) FILTER(WHERE ok) eligible_rows,
 count(DISTINCT ma_cd) FILTER(WHERE ok) expected_citizens FROM (
 SELECT ma_cd,coalesce(typeid,'ZZ') NOT IN ('GT','GM','GG','GK')
 AND coalesce(trang_thai_duyet,0) NOT IN (1,3,4) ok FROM public.mirror_hdg_nhankhau) s),
 target AS (SELECT count(*) citizens FROM c360.c360_thong_tin_ca_nhan)
SELECT source.*,source_rows-eligible_rows excluded_business_rows,
 eligible_rows-expected_citizens duplicate_eligible_rows,citizens,
 citizens-expected_citizens count_difference FROM source CROSS JOIN target;

-- BƯỚC 2: Thống kê theo địa bàn thường trú đã chuẩn hóa; NULL là chưa xác định.
-- Chỉ tính tuổi tròn khi ngày chuẩn có đủ YYYYMMDD. Không suy tuổi từ ngày ứng viên/raw.
CREATE VIEW hdg_bi.population AS
SELECT ma_tinh,ten_tinh,ma_xa,ten_xa,gioi_tinh,is_alive,
 CASE WHEN ngay_sinh_chuan ~ '^[0-9]{8}$'
 THEN extract(year FROM age(current_date,to_date(ngay_sinh_chuan,'YYYYMMDD')))::integer END age_years,
 count(*) citizens FROM c360.c360_thong_tin_ca_nhan
GROUP BY 1,2,3,4,5,6,7;

CREATE VIEW hdg_bi.identity_by_province AS
SELECT ma_tinh,ten_tinh,count(*) citizens,
 count(*) FILTER(WHERE so_dinh_danh ~ '^[0-9]{12}$') valid_identity,
 count(*) FILTER(WHERE so_dinh_danh IS NULL OR so_dinh_danh !~ '^[0-9]{12}$') missing_identity,
 round(100.0*count(*) FILTER(WHERE so_dinh_danh ~ '^[0-9]{12}$')/nullif(count(*),0),2) valid_identity_pct
FROM c360.c360_thong_tin_ca_nhan GROUP BY 1,2;

-- BƯỚC 3: Mỗi cờ chỉ đếm một lần/công dân. Mẫu số là toàn bộ C360, không phải dòng Mirror.
CREATE VIEW hdg_bi.dq_flags AS
SELECT flag,count(*) citizens,
 round(100.0*count(*)/nullif((SELECT count(*) FROM c360.c360_thong_tin_ca_nhan),0),2) citizens_pct
FROM c360.c360_thong_tin_ca_nhan t CROSS JOIN LATERAL (
 SELECT DISTINCT f flag FROM unnest(string_to_array(t.dq_flags,';')) f WHERE f<>'') d GROUP BY flag;

CREATE VIEW hdg_bi.quality_by_province AS
SELECT ma_tinh,ten_tinh,count(*) citizens,count(*) FILTER(WHERE dq_flags<>'') flagged_citizens,
 count(*) FILTER(WHERE dq_flags='') dq_empty_citizens,
 count(*) FILTER(WHERE dq_flags='' AND normalization_status='COMPLETE') clean_complete_citizens,
 count(*) FILTER(WHERE normalization_status='UNKNOWN') unknown_citizens,
 round(100.0*count(*) FILTER(WHERE dq_flags<>'')/nullif(count(*),0),2) flagged_pct
FROM c360.c360_thong_tin_ca_nhan GROUP BY 1,2;

-- BƯỚC 4: Giữ cả lô FAILED. NULL metrics của lô lỗi là chưa biết, không thay bằng 0.
-- Chỉ lấy tên lớp/mã lỗi, không đưa error SQL/raw có thể chứa PII lên dashboard.
CREATE VIEW hdg_bi.sync_batches AS
SELECT run_id,mode,status,started_at,finished_at,
 (metrics->>'citizens')::bigint citizen_keys,(metrics->>'claimed')::bigint queue_rows,
 (metrics->>'upserted')::bigint merged_rows,(metrics->>'removed')::bigint deleted_rows,
 extract(epoch FROM finished_at-started_at) elapsed_seconds,
 CASE WHEN error IS NOT NULL THEN split_part(error,':',1) END error_type
FROM c360.c360_sync_log;

CREATE VIEW hdg_bi.sync_minutes AS
SELECT date_trunc('minute',started_at) batch_minute,count(*) batches,
 count(*) FILTER(WHERE status='FAILED') failed_batches,
 sum(citizen_keys) processed_key_occurrences,sum(merged_rows) merged_rows,sum(deleted_rows) deleted_rows,
 avg(elapsed_seconds) avg_seconds
FROM hdg_bi.sync_batches WHERE mode='NRT' GROUP BY 1;
COMMIT;
