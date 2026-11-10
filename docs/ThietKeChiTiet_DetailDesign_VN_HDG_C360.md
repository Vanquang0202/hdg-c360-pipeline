# THIẾT KẾ CHI TIẾT (DETAIL DESIGN) — HDG → C360

> **Trạng thái:** DRAFT — chờ review/phê duyệt. Đây là thiết kế dựa trên tài liệu yêu cầu, không phải bằng chứng pipeline đã chạy. **Nguồn:** `Yeucau_DataPipeline_HGD_C360_4Tuan.md` (yêu cầu HDG → C360 → BI & API). **Ngày:** 10/11/2026 · **Mã tài liệu:** HDG-C360-DD-001 · **Phiên bản:** 0.1

## Bia – Bìa tài liệu

| Thuộc tính | Giá trị |
| :---- | :---- |
| Dự án | Data Pipeline HDG → C360 → BI & API |
| Môi trường | Oracle nguồn giả lập → PostgreSQL đích (đề xuất) |
| Trạng thái | Draft; chưa phê duyệt |
| Người review / phê duyệt |  |
| Deadline mục tiêu | 13/11/2026 |

LichSu – Lịch sử sửa đổi

| Version | Ngày | Thay đổi |
| :---- | :---- | :---- |
| 0.1 | 2026/11/10 | Xây dựng tài liệu thiết kế hệ thống Data Pipeline Hộ gia đình – Nhân khẩu (HDG), thực hiện đồng bộ dữ liệu từ CSDL nguồn HDG sang vùng Mirror bằng Batch/CDC, chuẩn hóa dữ liệu công dân C360 và phục vụ đối soát, báo cáo BI, API tra cứu. |

TongQuanModule – Tổng quan module

| Mục | Nội dung |
| :---- | :---- |
| ID module | DD-HDG-C360-E2E (tập hợp luồng; theo mẫu DD dùng để review tổng thể) |
| Hiện thực cho | F-HDG-001..008 / BAT-HDG-001..003 / BAT-C360-001 / API-C360-001..004 |
| Mức chi tiết | A — luồng tích hợp, dữ liệu định danh, batch & API |
| Tóm tắt xử lý | Seed Oracle → Full Load \+ CDC vào Mirror → Queue → normalize C360 → reconcile → dashboard / 4 API. |
| Lớp / package | Đề xuất: generator/; jobs/full\_load,normalize,reconcile; cdc/connectors; api/ Spring Boot; airflow/dags. |
| Chữ ký phương thức | Đề xuất: full\_load(table, checkpoint); normalize(ma\_cd); reconcile(run\_id); GET/POST /api/v1/ca-nhan/... |
| Được gọi bởi | Airflow DAG, CDC Sink/Trigger, hệ thống nội bộ/API client |
| Gọi tới | Oracle VSS, Kafka/Debezium, PostgreSQL Mirror/C360, Superset |
| Giao dịch | PostgreSQL: map refresh DELETE+INSERT cùng transaction; queue fetch/ACK transaction; Full/NRT lock Sync Control. |
| Xử lý đồng thời / khóa | Full/NRT SELECT FOR UPDATE cùng dòng; queue ACK tập ID nhìn thấy; bootstrap SCN/offset/freeze phải chốt QA-04. |

## ThamSoIO – Tham số vào/ra

| Hướng | No | Trường | Type | Bắt buộc | Mô tả | Fallback | Tham chiếu |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| IN | 1 | seed\_count | int | Có | Số công dân giả lập 50k–200k | Mặc định đề xuất 50k | BAT-HDG-001 |
| IN | 2 | ma\_cd | String | Có | Khóa công dân cần chuẩn hóa | Không có thì bỏ qua/log | BAT-C360-001 |
| IN | 3 | batch\_size | int | Có | Giới hạn RAM khi JDBC Full Load | Cấu hình theo máy | BAT-HDG-002 |
| IN | 4 | api.ma | String | Có | Mã tra cứu theo ưu tiên | 400 nếu trống | API-C360-001 |
| IN | 5 | api.codes | List<String> | Có | Danh sách tra cứu tối đa 1000 | 400 nếu \>1000 | API-C360-002 |
| IN | 6 | api.hoTen, ngaySinh | String | Có | Tìm kiếm name \+ partial DOB | 400 nếu invalid | API-C360-003 |
| IN | 7 | api.cursor | String | Không | Cursor (SYNC\_DATE,ID) | Trang đầu | API-C360-004 |
| OUT | 1 | Mirror rows | DB rows | Có | Giữ dữ liệu nguồn, không dedup MA\_CD | — | TBL-005/006 |
| OUT | 2 | C360 citizen | DB row | Có nếu hợp lệ | Duy nhất 1 dòng/MA\_CD; có thể có DQ\_FLAGS | DELETE nếu hết hợp lệ | TBL-009 |
| OUT | 3 | reconcile metrics | Metrics | Có | Source/Mirror counts; distinct Mirror vs C360 | Sai khác \= issue | F-HDG-005 |
| EXCEPTION | 1 | CDC/DB failure | Error | — | Retry/DLQ, không giả nhận đã đồng bộ | Ghi lỗi sync | MSG-E503 |

## XuLyChiTiet – Xử lý chi tiết

| Bước | Nội dung | Điều kiện | Khi đúng | Khi sai | SQL/CHK | Ghi chú |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| 1 | Sinh dữ liệu nguồn | Theo tham số seed\_count | Seed bốn bảng, cài case bẩn | Trả lỗi seed | CHK-01 | Chỉ dữ liệu giả lập |
| 2 | Full Load các bảng qua JDBC | Bootstrap được phê duyệt | Đọc theo lô, upsert Mirror | Chặn cutover | SQL-01 / QA-04 | Không .cache/.repartition tùy tiện |
| 2.1 | Đối soát sau Full Load | Nguồn và Mirror đã quiescent | Count tổng và theo tỉnh khớp | Ghi chênh lệch | SQL-04 | Rerun không duplicate |
| 3 | Khởi động Oracle Debezium Source và Sink | SCN/offset/barrier được xác định | Consume INSERT/UPDATE/DELETE idempotent | Không bật ghi mới khi thiếu bảo đảm | QA-03/04 | Key đúng từng dòng hộ |
| 3.1 | CDC đổi MA\_CD | Old key khác new key | Delete old \+ upsert new; enqueue cả hai MA\_CD | Chỉ enqueue new cho event khác | QA-01 | Nguồn MA\_CD\_OLD |
| 4 | Mirror trigger enqueue | INSERT/UPDATE/DELETE | Đẩy MA\_CD bị ảnh hưởng; old+new nếu đổi key | Không ảnh hưởng row không đổi | SQL-02 | Trigger nhẹ, transactionally |
| 5 | Normalize công dân | Queue chứa khóa | Lấy tất cả dòng Mirror của MA\_CD | Nếu không còn hợp lệ, DELETE C360 | SQL-03 | Cùng logic Full và NRT |
| 5.1 | Lọc nghiệp vụ | TYPEID nhóm G hoặc duyệt 1,3,4 | Loại dòng khỏi tập ứng viên | Giữ ứng viên | SQL-03 | NULL duyệt coi 0 |
| 5.2 | Chọn dòng đại diện | Có ≥1 ứng viên | MODIFY\_DATE DESC, CREATED\_DATE DESC, ID DESC | DELETE C360 nếu 0 | SQL-03 | NULL tie QA-05 |
| 5.3 | Chuẩn hóa địa bàn | Mã map 1-1 | Tỉnh theo xã; giữ mã gốc | NULL xã, gắn MO\_HO/không map | CHK-03 | Mã cũ split trước |
| 5.4 | Chuẩn hóa trường | Giá trị hợp lệ | Đưa tên, DOB, CCCD, phone, quốc tịch/dân tộc về chuẩn | NULL trường chuẩn và gắn DQ\_FLAGS | CHK-04 | Không ghép tùy ý hộ khác |
| 6 | ACK Queue và ghi Sync Log | Transaction normalize thành công | Xóa đúng các ID đã claim | Giữ Queue để retry | SQL-05 | Không xóa WHERE id \<= MAX(id) |
| 7 | Full Load C360 | Khóa Sync Control đã lấy | Đợi NRT, rebuild snapshot, giữ queue mới | Không TRUNCATE khi NRT đang chạy | QA-04 | Cutover phải rõ commit boundary |
| 8 | Đối soát | CDC đã drain tại checkpoint | Source=Mirror, Mirror distinct=C360, ghi DQ metrics | Cảnh báo sai khác | SQL-04 | Đối soát point-in-time |
| 9 | API/query/dashboard | Có dữ liệu C360 | Serve query có X-API-Key, audit async | 404/409/4xx theo hợp đồng | CHK-05 | API change-feed cần fence |

## DinhNghiaSQL – Đặc tả SQL / pseudo-code

| SQL ID | Mục đích | SQL/pseudo-code | Binds | Index | Số dòng | Có data | Không có | Nhiều | Error | Concurrency |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| SQL-01 | Full load Mirror (pseudo-code) | SELECT \* FROM VSS.VSS\_HO\_GD\_NHANKHAU WHERE ...; đọc chunked bằng JDBC; UPSERT đúng khóa bản ghi | checkpoint, batch\_size | Nguồn partition MA\_TINH; đích QA-01 | 0..n | Nạp thành batch | 0 dòng ghi 0 | Trùng khóa \=\> QA-01 | Retry có giới hạn, log checkpoint | Không khóa toàn bảng; freeze/barrier QA-04 |
| SQL-02 | Queue từ Mirror | INSERT INTO C360\_CHANGE\_QUEUE (MA\_CD) VALUES (:old\_ma\_cd), (:new\_ma\_cd) khi đổi MA\_CD; từng event enqueue khóa liên quan | old,new | idx\_queue\_ma\_cd | 1–2 | Tạo queue row | Không có thay đổi=\>0 | Không áp dụng | Rollback cùng transaction Mirror | Trigger nhẹ |
| SQL-03 | Chọn đại diện hợp lệ | SELECT \* FROM (SELECT m.\*, ROW\_NUMBER() OVER (PARTITION BY MA\_CD ORDER BY MODIFY\_DATE DESC NULLS LAST, CREATED\_DATE DESC NULLS LAST, ID DESC NULLS LAST) rn FROM MIRROR\_HDG\_NHANKHAU m WHERE COALESCE(TYPEID,'ZZ') NOT IN ('GT','GM','GG','GK') AND COALESCE(TRANG\_THAI\_DUYET,0) NOT IN (1,3,4)) x WHERE rn=1 AND MA\_CD=:ma\_cd | ma\_cd | idx (MA\_CD,MODIFY\_DATE) | 0..1 | 1=\>normalize+UPSERT | 0=\>DELETE C360 | \>1 \=\> lỗi logic | Rollback transaction; retry | Không ghép cột từ nhiều hộ |
| SQL-04 | Reconcile | SELECT COUNT(DISTINCT MA\_CD) FROM MIRROR\_HDG\_NHANKHAU WHERE COALESCE(TYPEID,'ZZ') NOT IN ('GT','GM','GG','GK') AND COALESCE(TRANG\_THAI\_DUYET,0) NOT IN (1,3,4); đối chiếu SELECT COUNT(\*) FROM C360\_THONG\_TIN\_CA\_NHAN | run\_id, checkpoint | idx (MA\_CD) | 1+1 | So sánh counts | Không có bảng \=\> lỗi | Chênh lệch=\>alert | Timeout \=\> retry/mark fail | Cùng checkpoint nhất quán |
| SQL-05 | ACK tập ID đã thấy | DELETE FROM C360\_CHANGE\_QUEUE WHERE ID \= ANY(:claimed\_ids) \-- PostgreSQL; chỉ sau normalize commit cùng transaction | claimed\_ids | PK queue ID | 0..n | Xóa đúng ID đã claim | 0=\>không sao nếu replay | Không áp dụng | Rollback để retry | Không dùng ID \<= MAX(id) |
| SQL-06 | Làm mới map | BEGIN; DELETE FROM C360\_MAP\_DBHC\_XA; INSERT INTO ... SELECT từ source chuẩn hóa; COMMIT; (tương tự tỉnh) | batch\_id | PK map | 0..n | Map mới khả dụng atomic | Nếu 0 \=\> verify trước commit | Mã mơ hồ không PK kép | Rollback toàn bộ | Không TRUNCATE |

## KiemTraDauVao – Validation

| CHK ID | Mục | Loại | Điều kiện lỗi | MSG | Thứ tự | Stop | Ref |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| CHK-01 | seed\_count | Giới hạn | Không trong phạm vi cấu hình 50k..200k | MSG-E001 | 1 | Có | BAT-HDG-001 |
| CHK-02 | MA\_CD / MA\_HO\_GD | Khóa | MA\_CD NULL; MA\_HO\_GD NULL/duplicate chưa có quy tắc | QA-01 | 2 | Tùy gate | TBL-001/005 |
| CHK-03 | DBHC map | Dữ liệu | Mã cũ map \>=2 xã mới hoặc tỉnh lệch xã | DQ:XA\_MO\_HO / TINH\_LECH\_XA | 3 | Không | TBL-007/008 |
| CHK-04 | Các trường công dân | Dữ liệu | DOB invalid, CCCD không 12 số, phone sai, danh mục không map | DQ\_FLAGS | 4 | Không | C360 |
| CHK-05 | X-API-Key và input API | API | Key thiếu/invalid/scope sai; batch \>1000; tên/ngày không hợp lệ | MSG-E001/E901 | 5 | Có | API-C360-001..004 |

## XuLyLoi\_Log – Xử lý lỗi / Logging

| Tình huống | Exception | HTTP | MSG | Log level | Log content | Rollback | Ghi chú |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| Tham số API lỗi | Validation | 400 | MSG-E001 | INFO | requestId, checkId, masked code | — | Không log PII đầy đủ |
| Không tìm thấy công dân | NotFound | 404 | MSG-I001 | INFO | requestId, result | — | Chỉ tra mã hiện hành/cũ/định danh |
| Mã cũ/định danh trùng | Conflict | 409 | MSG-E409 | WARN | requestId, candidateCount | — | Danh sách candidates phải theo privacy rule |
| CDC connector lỗi/lag | ConnectorError | — | SYNC-FAIL | ERROR | connector, partition, offset, lag | Không ACK event | Retry/alert/DLQ |
| DB/Job timeout | DataAccessException | 503 | MSG-E503 | ERROR | jobId, sqlId, elapsed | Rollback | Không cập nhật checkpoint sớm |
| Bất thường Queue commit | ConcurrencyError | — | SYNC-QUEUE | WARN | claimedIdsCount, txId | Rollback | ACK tập ID nhìn thấy |
| Không map/CCCD/DOB sai | DataQuality | 200 | DQ\_FLAGS | WARN | DQ flags thống kê, không PII | Không rollback | Giữ công dân nếu nghiệp vụ hợp lệ |

## UnitTest – Test cases (chưa chạy)

| UT ID | Loại | Bước | Precondition | Input | Action | Expected | Actual | OK/NG | Tester | Date | Bug ID |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| UT-001 | Bình thường | 1 | Seed cấu hình hợp lệ | 50000 | Chạy initial seed | 4 bảng nguồn có dữ liệu; case bẩn được tạo |  |  |  |  |  |
| UT-002 | Tích hợp | 2 | Source có nhiều hộ/công dân | MA\_CD nhiều H | Full Load | Mirror giữ đủ từng dòng, rerun không nhân đôi |  |  |  |  |  |
| UT-003 | Tích hợp | 3 | CDC đã bootstrap đúng SCN | INSERT nguồn | Chạy sink | Mirror có dòng mới ≤ SLA |  |  |  |  |  |
| UT-004 | Tích hợp | 3 | Mirror có dòng | UPDATE nguồn | Chạy CDC | Mirror cập nhật đúng dòng hộ |  |  |  |  |  |
| UT-005 | Tích hợp | 3 | Mirror có dòng | DELETE nguồn | Chạy CDC | Mirror xóa đúng dòng nguồn |  |  |  |  |  |
| UT-006 | Biên | 3.1 | MA\_CD đổi | old→new | CDC \+ Queue | C360 mã cũ bị xóa, mã mới xuất hiện nếu hợp lệ |  |  |  |  |  |
| UT-007 | Nghiệp vụ | 5.1 | TYPEID=GT hoặc TRANG\_THAI\_DUYET=1 | Invalid row | Normalize | Không chọn dòng; nếu hết hợp lệ DELETE C360 |  |  |  |  |  |
| UT-008 | Nghiệp vụ | 5.2 | 1 MA\_CD nhiều hộ; newest invalid | 3 rows | Normalize | Chọn dòng hợp lệ mới nhất theo thứ tự |  |  |  |  |  |
| UT-009 | Biên | 5.3 | Mã xã cũ map 2 xã mới | MA\_XA cũ | Normalize | MA\_XA chuẩn=NULL, DQ XA\_MO\_HO |  |  |  |  |  |
| UT-010 | Biên | 5.3 | Tỉnh khai báo khác tỉnh suy ra từ xã | Mismatch | Normalize | Theo tỉnh suy từ xã, gắn TINH\_LECH\_XA |  |  |  |  |  |
| UT-011 | Biên | 5.4 | 19950230 / 19950000 / 19950800 | NGAY\_SINH | Normalize | Sai lịch→NULL+DQL; thiếu phần→YYYY/YYYYMM |  |  |  |  |  |
| UT-012 | Biên | 5.4 | CCCD 12345, phone 12345 | Fields | Normalize | NULL trường chuẩn, gắn DQ\_FLAGS |  |  |  |  |  |
| UT-013 | Biên | 5.4 | QUOC\_TICH VN hoặc 233 | QUOC\_TICH | Normalize | Cùng chuẩn Việt Nam; XX gắn DQ |  |  |  |  |  |
| UT-014 | Tích hợp | 6 | ID cấp 100 commit muộn; 101 commit sớm | 2 transactions | Queue drain | Không xóa/bỏ sót ID100 |  |  |  |  |  |
| UT-015 | Tích hợp | 7 | Full load đồng thời NRT | Concurrent | Run both | Không lost update, không xóa queue mới |  |  |  |  |  |
| UT-016 | Đối soát | 8 | CDC drain, source snapshot nhất quán | run\_id | Reconcile | count Source=Mirror; distinct valid Mirror=count C360 |  |  |  |  |  |
| UT-017 | API | 9 | Mã hiện hành tồn tại | GET /{ma} | Call API | 200 đúng người |  |  |  |  |  |
| UT-018 | API | 9 | Mã cũ khớp 2 công dân | GET /{ma} | Call API | 409 candidates |  |  |  |  |  |
| UT-019 | API | 9 | 1001 mã | POST tra-cuu-lo | Call API | 400 hoặc chuẩn lỗi được duyệt |  |  |  |  |  |
| UT-020 | API | 9 | Tên không dấu \+ ngày YYYY | GET tim-kiem | Call API | Khớp người có ngày sinh rút gọn |  |  |  |  |  |
| UT-021 | API | 9 | Cursor, transaction commit muộn | GET thay-doi | Pages | Không bỏ sót/không trùng do fence; QA-09 |  |  |  |  |  |
| UT-022 | API | 9 | Thiếu key/scope | X-API-Key | Call API | 401/403 |  |  |  |  |  |
| UT-023 | End-to-End | 1-9 | Batch/CDC/BI/API đã chạy | Delta mixed | Demo E2E | Thay đổi phản ánh đúng sau 1–2 phút |  |  |  |  |  |

## QA – QA&nbsp;

| No | Ngày | Người hỏi | Mục | Câu hỏi | Người trả lời | Trả lời | Ngày trả lời | Trạng thái | Phản ánh |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |
|  |  |  |  |  |  | Chưa xác nhận | — | Open | Chưa |

Lưu ý thiết kế có rủi ro cao

**Bootstrap Oracle CDC:** Cần quyết định cơ chế SCN/offset \+ snapshot hoặc freeze writer có kiểm chứng. Không được khẳng định không mất/trùng dữ liệu khi chưa chạy thử.

**Queue concurrency:** Sequence ID không phản ánh thứ tự COMMIT. Không xóa queue dựa trên `ID <= MAX(ID)`; chỉ ACK đúng tập ID đã claim, cùng điều kiện transaction thích hợp.

**Kiểm soát Full/NRT:** Khóa cùng dòng `SYNC_CONTROL`, ngăn TRUNCATE/rebuild khi NRT đang chạy và bảo toàn delta phát sinh trong khi rebuild.

**Mapping DBHC:** Tách `MA_DBHC_SN` thành bảng map phẳng trước khi join fact lớn; mã xã cũ map nhiều xã mới phải giữ NULL và cờ `XA_MO_HO`; nếu xã xác định được tỉnh thì ưu tiên tỉnh theo xã.

**Ngày sinh:** `YYYY0000` hoặc `YYYY` → `YYYY`; `YYYYMM00` hoặc `YYYYMM` → `YYYYMM`; ngày sai lịch → NULL \+ flag, giữ gốc; khoảng năm hợp lý cần QA.

**API change feed:** Điều kiện `SYNC_DATE <= NOW()-120s` chưa chứng minh an toàn với giao dịch commit muộn; cần commit fence/watermark và quyết định tombstone.

## Trạng thái kiểm thử

**23 test case đều CHƯA CHẠY (0 PASS / 0 FAIL).** Các Expected là tiêu chí đề xuất; chưa có Actual.

&nbsp;