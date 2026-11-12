-- Mục đích: khảo sát điều kiện LogMiner trước khi triển khai, không tự bật logging.
-- Đầu vào: phiên SQLPlus SYSDBA của Oracle local.
-- Đầu ra: thông tin CDB/PDB, redo/archive, giao dịch và khóa; ALTER SESSION chỉ đổi container của phiên.
-- BƯỚC 1: Kiểm database và nhật ký ở root; 2314948 phía dưới là mốc khảo sát cũ, không phải barrier mới.
-- BƯỚC 2: Chuyển sang PDB để xem logging/constraint của VSS; không thay dữ liệu nguồn.
-- Read-only checks; run manually as SYSDBA. No ALTER SYSTEM or log switching.
SET LINESIZE 220
SET PAGESIZE 100
SELECT name, dbid, cdb, log_mode, current_scn,
       supplemental_log_data_min, supplemental_log_data_pk,
       supplemental_log_data_all FROM v$database;
SELECT name, open_mode FROM v$pdbs;
SELECT tablespace_name FROM dba_tablespaces WHERE tablespace_name='USERS';
SELECT username FROM cdb_users WHERE username='C##HDG_CDC';
SELECT thread#, sequence#, first_change#, next_change#, status FROM v$log ORDER BY thread#, sequence#;
SELECT thread#, sequence#, first_change#, next_change#, name, deleted, status
FROM v$archived_log
WHERE next_change# > 2314948 AND name IS NOT NULL
ORDER BY thread#, sequence#;
SELECT dest_id, status, destination, error FROM v$archive_dest_status WHERE status <> 'INACTIVE';
SELECT inst_id, xid, start_scn FROM gv$transaction;
ALTER SESSION SET CONTAINER=FREEPDB1;
SELECT tablespace_name FROM dba_tablespaces WHERE tablespace_name='USERS';
SELECT owner, table_name, log_group_name, log_group_type, always
FROM dba_log_groups WHERE owner='VSS' ORDER BY table_name, log_group_name;
SELECT owner, table_name, constraint_name, constraint_type
FROM dba_constraints WHERE owner='VSS' AND constraint_type IN ('P','U');
-- Merely finding one archived log containing the SCN does NOT prove continuous coverage.
-- Verify all threads, resetlogs incarnation, online logs, and long-running transactions.
