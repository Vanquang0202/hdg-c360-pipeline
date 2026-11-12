-- C360 riêng biệt: không ALTER Mirror, không trigger, không thay sink_meta.
CREATE SCHEMA c360;
CREATE TABLE c360.c360_sync_control (
 control_key varchar(50) PRIMARY KEY, full_running boolean NOT NULL DEFAULT false,
 last_run_id uuid, map_version uuid, last_success timestamptz
);
INSERT INTO c360.c360_sync_control(control_key) VALUES ('NORMALIZE');
CREATE TABLE c360.c360_sync_log (
 run_id uuid PRIMARY KEY, mode text NOT NULL, status text NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
 source_snapshot text, metrics jsonb, error text
);
CREATE TABLE c360.c360_change_queue (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ma_cd varchar(30) NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON c360.c360_change_queue(ma_cd);
CREATE TABLE c360.c360_map_dbhc_tinh (
 ma_tra_cuu varchar(3) PRIMARY KEY, ma_tinh varchar(3), ten_tinh text,
 loai_map text NOT NULL CHECK(loai_map IN ('HIEN_HANH','SAP_NHAP','MO_HO')),
 so_ung_vien integer NOT NULL CHECK(so_ung_vien>0), version uuid NOT NULL
);
CREATE TABLE c360.c360_map_dbhc_xa (
 ma_tra_cuu varchar(5) PRIMARY KEY, ma_xa varchar(5), ten_xa text,
 ma_tinh varchar(3), ten_tinh text,
 loai_map text NOT NULL CHECK(loai_map IN ('HIEN_HANH','SAP_NHAP','MO_HO')),
 so_ung_vien integer NOT NULL CHECK(so_ung_vien>0), version uuid NOT NULL,
 CHECK(loai_map <> 'MO_HO' OR ma_xa IS NULL)
);
-- Chỉ chuẩn hóa hình thức mã; không cắt mã dài hoặc tìm gần đúng theo tên.
CREATE FUNCTION c360.province_key(v text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
 SELECT CASE WHEN k='000' THEN NULL ELSE k END FROM (
 SELECT CASE WHEN btrim(v) ~ '^[0-9]{2}TTT$' THEN lpad(left(btrim(v),2),3,'0')
 WHEN btrim(v) ~ '^[0-9]{1,3}$' THEN lpad(btrim(v),3,'0') END k) s
$$;
CREATE FUNCTION c360.commune_key(v text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
 SELECT CASE WHEN btrim(v) ~ '^[0-9]{1,5}$' AND lpad(btrim(v),5,'0') <> '00000'
 THEN lpad(btrim(v),5,'0') END
$$;
CREATE TABLE c360.c360_thong_tin_ca_nhan (
 id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
 ma_so_bhxh varchar(30) PRIMARY KEY, ma_so_bhxh_cu varchar(30),
 source_id varchar(40), ma_ho_gd varchar(30),
 source_modify_date timestamp(0), source_created_date timestamp(0),
 ho_ten varchar(250), ho_ten_unsigned varchar(250), ho_ten_goc varchar(250),
 ngay_sinh_chuan varchar(8), ngay_sinh_ung_vien varchar(8), ngay_sinh_goc numeric,
 so_dinh_danh varchar(12), so_cccd_goc varchar(50), so_cmtnd_goc varchar(50),
 so_dien_thoai varchar(11), phone_number_goc varchar(50),
 gioi_tinh varchar(1), is_alive varchar(1),
 ma_dan_toc varchar(2), ten_dan_toc varchar(100),
 ma_quoc_tich varchar(2), ten_quoc_tich varchar(100),
 ma_tinh varchar(3), ten_tinh text, ma_xa varchar(5), ten_xa text,
 ma_tinh_lh varchar(3), ten_tinh_lh text, ma_xa_lh varchar(5), ten_xa_lh text,
 ma_tinh_ks varchar(3), ten_tinh_ks text, ma_xa_ks varchar(5), ten_xa_ks text,
 ma_tinh_goc varchar(10), ma_xa_goc varchar(10),
 ma_tinh_lh_goc varchar(10), ma_xa_lh_goc varchar(10),
 ma_tinh_ks_goc varchar(10), ma_xa_ks_goc varchar(10),
 dia_chi_goc varchar(500), dc_lien_lac_goc varchar(250), noi_ks_goc varchar(250),
 dq_flags text NOT NULL, unknown_rules jsonb NOT NULL,
 normalization_status text NOT NULL CHECK(normalization_status IN ('COMPLETE','UNKNOWN')),
 source_row_goc jsonb NOT NULL, run_id uuid NOT NULL, sync_date timestamptz NOT NULL,
 CHECK(so_dinh_danh IS NULL OR so_dinh_danh ~ '^[0-9]{12}$'),
 CHECK(so_dien_thoai IS NULL OR so_dien_thoai ~ '^0[0-9]{9,10}$'),
 CHECK(ngay_sinh_chuan IS NULL OR ngay_sinh_chuan ~ '^([0-9]{4}|[0-9]{6}|[0-9]{8})$')
);
-- Không UNIQUE CCCD hoặc mã cũ: nhiều người có thể cùng khớp khi tra cứu.
CREATE INDEX ON c360.c360_thong_tin_ca_nhan(so_dinh_danh);
CREATE INDEX ON c360.c360_thong_tin_ca_nhan(ma_so_bhxh_cu);
CREATE INDEX ON c360.c360_thong_tin_ca_nhan(sync_date,id);
CREATE TABLE c360.c360_stage (
 run_id uuid NOT NULL REFERENCES c360.c360_sync_log(run_id),
 ma_so_bhxh varchar(30) NOT NULL, payload jsonb NOT NULL, source_row_goc jsonb NOT NULL,
 PRIMARY KEY(run_id,ma_so_bhxh)
);
