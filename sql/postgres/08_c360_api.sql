-- Chỉ tạo schema bảo mật/audit riêng và index tìm kiếm; không UPDATE dữ liệu C360.
CREATE SCHEMA c360_api;
CREATE TABLE c360_api.api_keys (
 key_hash text PRIMARY KEY CHECK(key_hash ~ '^[0-9a-f]{64}$'),
 client_id varchar(100) NOT NULL, scopes text[] NOT NULL,
 enabled boolean NOT NULL DEFAULT true,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE c360_api.c360_api_audit (
 request_id uuid PRIMARY KEY, observed_at timestamptz NOT NULL,
 endpoint text NOT NULL, http_status integer NOT NULL CHECK(http_status BETWEEN 100 AND 599),
 elapsed_ms numeric NOT NULL CHECK(elapsed_ms>=0), result_status text NOT NULL,
 client_id varchar(100)
);
CREATE INDEX ON c360_api.c360_api_audit(observed_at);
-- Các lookup mã đã có index từ 05_c360.sql. Index này phục vụ tên chính xác + ngày rút gọn.
CREATE INDEX c360_api_name_birth_idx ON c360.c360_thong_tin_ca_nhan
 (ho_ten_unsigned, ngay_sinh_chuan text_pattern_ops, ma_so_bhxh);
