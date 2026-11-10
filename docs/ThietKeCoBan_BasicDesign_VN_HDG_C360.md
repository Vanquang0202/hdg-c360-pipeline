# THIẾT KẾ CƠ BẢN (BASIC DESIGN) — HDG → C360

> **Trạng thái:** DRAFT — chờ review/phê duyệt. Đây là thiết kế dựa trên tài liệu yêu cầu, không phải bằng chứng pipeline đã chạy. **Nguồn:** `Yeucau_DataPipeline_HGD_C360_4Tuan.md` (yêu cầu HDG → C360 → BI & API). **Ngày:** 10/11/2026 · **Mã tài liệu:** HDG-C360-BD-001 · **Phiên bản:** 0.1

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

TongQuan – Tổng quan

| Mục | Nội dung |
| :---- | :---- |
| Mục đích | Xây dựng pipeline End-to-End từ Oracle HDG: mirror, chuẩn hóa công dân C360, đối soát, BI và REST API. |
| Phạm vi | 4 bảng nguồn VSS; seed \+ delta; Full Load/CDC; Mirror; C360; NRT ≤ 1 phút; DQ; đối soát; dashboard; 4 API; Airflow. |
| Ngoài phạm vi | Kết nối dữ liệu PROD HDG thật; dữ liệu định danh người thật; sửa trực tiếp dữ liệu nghiệp vụ nguồn. |
| Tác nhân | ACT-01 Developer; ACT-02 Job/Scheduler; ACT-03 Hệ thống nội bộ gọi API; ACT-04 Người khai thác BI. |
| Sơ đồ ngữ cảnh | Oracle HDG → (Python Full Load \| Debezium → Kafka → Sink) → PostgreSQL Mirror → Queue/ETL → C360 → BI/API; Airflow điều phối. |
| Công nghệ | Đề xuất DRAFT: Oracle Free nguồn; PostgreSQL 16 đích; Python 3.11; Kafka+Debezium; Java 21/Spring Boot; Superset; Airflow. Chưa phê duyệt. |
| Giả định / ràng buộc | Deadline demo 13/11/2026; Docker \~6.74 GiB; source 138M dòng PROD nhưng local seed 50k–200k; cần chốt QA-01..10. |
| Thuật ngữ | HDG: Hộ gia đình; Mirror: bản sao dữ liệu nguồn; C360: mỗi MA\_CD một dòng hợp lệ; CDC: Change Data Capture; NRT: Near Real-Time; DQ: Data Quality. |

## DSChucNang – Danh sách chức năng

| ID | Tên chức năng | Loại | Tác nhân | Mô tả | Interface/Batch | Ưu tiên | Nguồn | Ghi chú |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| F-HDG-001 | Sinh dữ liệu giả lập HDG | Offline | Developer | Seed 4 bảng và các trường hợp biên/bẩn theo tỷ lệ | BAT-HDG-001 | Cao | §3.1 | Draft; chưa triển khai |
| F-HDG-002 | Đồng bộ Full Load | Batch | Scheduler | Nạp nguồn→Mirror đủ 4 bảng; chạy lại không nhân đôi | BAT-HDG-002 | Cao | §3.1 | Draft; chưa triển khai |
| F-HDG-003 | Đồng bộ CDC | Streaming | Debezium | INSERT/UPDATE/DELETE nguồn→Kafka→Mirror | BAT-HDG-003 | Cao | §3.1 | Draft; chưa triển khai |
| F-HDG-004 | Chuẩn hóa C360 | Batch / NRT | ETL | Lọc, dedup, map danh mục, chuẩn hóa, DQ\_FLAGS | BAT-C360-001 | Cao | §3.2 | Draft; chưa triển khai |
| F-HDG-005 | Đối soát dữ liệu | Batch | Scheduler | Source↔Mirror, Mirror↔C360 và chất lượng dữ liệu | BAT-REC-001 | Cao | §3.3 | Draft; chưa triển khai |
| F-HDG-006 | Dashboard BI | BI | Người phân tích | Nhân khẩu/định danh, DQ, tình trạng sync | SCR-BI-001 | Cao | §3.3 | Draft; chưa triển khai |
| F-HDG-007 | API C360 | Online | Hệ thống nội bộ | 4 API tra cứu, tìm kiếm, tra lô, change-feed | API-C360-001..004 | Cao | §3.3 | Draft; chưa triển khai |
| F-HDG-008 | Điều phối Airflow | Batch | Scheduler | DAG lịch chạy, retry, logging, kiểm thử E2E | BAT-OPS-001 | Cao | §4 | Draft; chưa triển khai |

## DSManHinh – Danh sách màn hình

| ID | Tên | Chức năng | Mô tả | Nguồn màn hình | Sự kiện | Đích | Quyền |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| SCR-BI-001 | Dashboard C360 | F-HDG-006 | 3 nhóm BI: nhân khẩu/định danh; Data Quality; Sync Monitoring | Menu BI | Chọn nhóm chỉ số | Biểu đồ / bảng | Theo phân quyền (đề xuất) |

## DinhNghiaManHinh – Định nghĩa màn hình BI

| Màn hình | No | Tên hạng mục | Control | I/O | Bắt buộc | Kiểu | Định dạng | Mặc định | Validation | Nguồn | Ghi chú |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| SCR-BI-001 | 1 | Bộ lọc Tỉnh/Xã | Filter | I | Không | Mã địa bàn | Mã chuẩn 3/5 ký tự | Toàn bộ | Chỉ mã hợp lệ | C360 | Đề xuất |
| SCR-BI-001 | 2 | Tổng số công dân | KPI | O | — | Số nguyên | ≥ 0 | — | Đối soát COUNT C360 | C360\_THONG\_TIN\_CA\_NHAN |  |
| SCR-BI-001 | 3 | Tỷ lệ có định danh 12 số | Chart | O | — | Tỷ lệ | 0..100% | — | Chia 0 an toàn | C360 |  |
| SCR-BI-001 | 4 | Phân bố DQ\_FLAGS | Chart | O | — | Theo mã cờ | Số dòng | — | Phân tách nhiều cờ/dòng | C360 |  |
| SCR-BI-001 | 5 | Lịch sử đồng bộ | Grid | O | — | Batch id / thời gian | — | — | Hiển thị cả batch thất bại | C360\_SYNC\_LOG |  |

## DSAPI – Danh sách API

| ID | Tên | Method | Endpoint | Bên gọi | Bên nhận | Kiểu | Xác thực | SLA | Chức năng |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| API-C360-001 | Tra cứu cá nhân theo mã | GET | /api/v1/ca-nhan/{ma} | Hệ thống nội bộ | C360 API | Đồng bộ | X-API-Key \+ scope | Chưa chốt SLA | F-HDG-007 |
| API-C360-002 | Tra cứu lô | POST | /api/v1/ca-nhan/tra-cuu-lo | Hệ thống nội bộ | C360 API | Đồng bộ | X-API-Key \+ scope | ≤1000 mã/lần | F-HDG-007 |
| API-C360-003 | Tìm theo họ tên và ngày sinh | GET | /api/v1/ca-nhan/tim-kiem | Hệ thống nội bộ | C360 API | Đồng bộ | X-API-Key \+ scope | Chưa chốt SLA | F-HDG-007 |
| API-C360-004 | Lấy danh sách thay đổi | GET | /api/v1/ca-nhan/thay-doi | Hệ thống nội bộ | C360 API | Đồng bộ | X-API-Key \+ scope | Cursor \+ 120s safety (cần fence) | F-HDG-007 |

## DinhNghiaAPI – Định nghĩa API

| API | Hướng | No | JSON field | Logic | Type | Required | Giới hạn | Quy tắc | Nguồn |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| API-C360-001 | Request path | 1 | ma | Mã tra cứu | string | Có | MA\_SO\_BHXH / MA\_SO\_BHXH\_CU / SO\_DINH\_DANH | Ưu tiên tìm mã hiện hành→cũ→định danh, dừng tầng đầu tiên có kết quả | C360 |
| API-C360-001 | Response 200 | 1 | maSoBhxh | Mã hiện hành | string | Có | Chuỗi, giữ 0 đầu | Theo dòng C360 được chọn | C360.MA\_SO\_BHXH |
| API-C360-001 | Response 200 | 2 | hoTen | Họ tên | string | Không | ≤250 | Tên đã chuẩn hóa; có thể thiếu | C360.HO\_TEN |
| API-C360-001 | Response 409 | 1 | candidates | Ứng viên trùng | array | Có | ≥2 | Chỉ khi mã cũ hoặc định danh khớp ≥2 người | C360 |
| API-C360-002 | Request JSON | 1 | codes | Danh sách mã | array<string> | Có | 1..1000 | Kết quả theo từng input; giới hạn 1000 | — |
| API-C360-002 | Response 200 | 1 | results | Kết quả từng mã | array | Có | Theo đầu vào | Trạng thái FOUND/NOT\_FOUND/CONFLICT/ERROR từng mã (đề xuất) | C360 |
| API-C360-003 | Request query | 1 | hoTen | Họ tên | string | Có | Có/không dấu | Chuẩn hóa uppercase không dấu để tìm | C360.HO\_TEN\_UNSIGNED |
| API-C360-003 | Request query | 2 | ngaySinh | Ngày sinh | string | Có | YYYY / YYYYMM / YYYYMMDD | Cho phép khớp giá trị ngày sinh rút gọn | C360.NGAY\_SINH\_CHUAN |
| API-C360-004 | Request query | 1 | cursor | Con trỏ phân trang | string | Không | Opaque cursor | Cặp SYNC\_DATE, ID; bảo đảm fence commit | C360.SYNC\_DATE, C360.ID |
| API-C360-004 | Response 200 | 1 | items,nextCursor | Danh sách thay đổi | array,string | Có | Paginated | Chỉ bản ghi qua safety window; DELETE cần tombstone (QA-09) | C360 / change-feed (đề xuất) |

## DSBatch – Danh sách Batch/CDC

| ID | Tên | Lịch | Input | Output | Xử lý | Rerun | Lỗi | Thời gian | Function |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| BAT-HDG-001 | Seed \+ Delta Generator | Thủ công | Schema 4 bảng Oracle | Dữ liệu giả lập 50k–200k; các case bẩn/biến động | Seed và script INSERT/UPDATE/DELETE | Có, có seed cố định | Fail-fast theo từng lô, ghi log | Chưa benchmark | F-HDG-001 |
| BAT-HDG-002 | Full Load nguồn→Mirror | Theo yêu cầu / Airflow | 4 bảng nguồn | 4 bảng Mirror | JDBC chunked; upsert; đối soát; bootstrap offset | Có (idempotent) | Resume/retry có checkpoint | Chưa benchmark | F-HDG-002 |
| BAT-HDG-003 | CDC Oracle→Mirror | Liên tục | Oracle redo/LogMiner | Mirror cập nhật | Debezium→Kafka→Sink upsert/delete | Có nếu event idempotent | DLQ, lag/offset; không mất commit | Độ trễ phụ thuộc hạ tầng | F-HDG-003 |
| BAT-C360-001 | Chuẩn hóa C360 Full/NRT | Full và mỗi phút | Mirror+Map+Queue | C360; Sync Log | Một logic normalize; MERGE/DELETE từng MA\_CD | Có, khóa control | Retry khóa lỗi, không ACK quá sớm | NRT ≤1 phút | F-HDG-004 |
| BAT-REC-001 | Đối soát 3 tầng | Sau batch và định kỳ | Source/Mirror/C360 | Báo cáo chênh lệch/DQ | Count tổng/tỉnh; count distinct hợp lệ; lấy mẫu hộ | Có | Ghi trạng thái lệch/độ trễ | Chưa chốt | F-HDG-005 |
| BAT-OPS-001 | Airflow orchestration | Lịch vận hành | Seed/Full/Map/NRT/REC | Run log / alert | Phối hợp dependency, retry, alert; tránh chồng lô | Có kiểm soát | On failure giữ log, retry giới hạn | Chưa chốt | F-HDG-008 |

## DSBang – Danh sách bảng

| ID | Tên vật lý | Tên logic | Mô tả | Quy mô | Tăng trưởng | Retention | Ghi chú |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| TBL-001 | VSS.VSS\_HO\_GD\_NHANKHAU | HDG nguồn | 111 cột; (MA\_CD,MA\_HO\_GD) logic chưa bảo đảm NULL | 138M PROD; seed 50k–200k | Có biến động | Nguồn quản lý | Oracle source |
| TBL-002 | VSS.VSS\_DM\_DANTOC | Danh mục dân tộc | ID PK, mã có lịch sử hiệu lực | Nhỏ | Chậm | Nguồn quản lý | Join bằng DM\_DAN\_TOC\_ID |
| TBL-003 | VSS.VSS\_DM\_QUOC\_TICH | Danh mục quốc tịch | ID PK; QUOC\_TICH lưu MA hoặc ID | Nhỏ | Chậm | Nguồn quản lý | VN / 233 |
| TBL-004 | VSS.VSS\_DM\_DBHC | Danh mục địa bàn | Không PK; mã mới/cũ và danh sách sáp nhập | Nhỏ | Theo đợt | Nguồn quản lý | QA-02 |
| TBL-005 | MIRROR\_HDG\_NHANKHAU | Nhân khẩu Mirror | Bản sao từng dòng nguồn, không dedup theo MA\_CD | Theo seed | CDC | Đề xuất retention | Khóa row QA-01 |
| TBL-006 | MIRROR\_DM\_\* | Danh mục Mirror | 3 bảng mirror đồng bộ | Nhỏ | CDC/full | Đề xuất retention | Không sửa giá trị gốc |
| TBL-007 | C360\_MAP\_DBHC\_TINH | Map mã tỉnh | Hiện hành / sáp nhập / mơ hồ | Nhỏ | Refresh | Thay map trong transaction | Không REGEXP join fact |
| TBL-008 | C360\_MAP\_DBHC\_XA | Map mã xã | Ưu tiên hiện hành, sáp nhập duy nhất; ambiguous NULL | Nhỏ | Refresh | Thay map trong transaction | Cần tỉnh theo xã |
| TBL-009 | C360\_THONG\_TIN\_CA\_NHAN | Công dân chuẩn hóa | Mỗi MA\_CD hợp lệ đúng một dòng | ≤ distinct MA\_CD | NRT | Delete nếu hết dòng hợp lệ | DQ\_FLAGS |
| TBL-010 | C360\_CHANGE\_QUEUE | Hàng đợi thay đổi | MA\_CD thay đổi \+ ID queue | Theo CDC | Liên tục | ACK chính xác tập row đã thấy | Old/new MA\_CD |
| TBL-011 | C360\_SYNC\_CONTROL | Khóa điều khiển | Chặn chạy chồng Full/NRT | 1..n | Theo job | Giữ lịch sử cần thiết | SELECT FOR UPDATE |
| TBL-012 | C360\_SYNC\_LOG | Lịch sử sync | Run id / counts / status / elapsed | Theo job | Theo phút | Retention QA-10 | BI monitoring |
| TBL-013 | C360\_RECON\_RESULT | Đối soát | Số lượng và sai khác theo tầng/tỉnh | Theo job | Theo job | Retention QA-10 | Nguồn bằng chứng |
| TBL-014 | C360\_API\_AUDIT | Audit API | Client, scope, action, outcome, masked key | Theo API | Theo API | Retention QA-10 | Async batch |

## DinhNghiaBang – Định nghĩa bảng

| Bảng | No | Cột | Logic | Type | Length | PK | NOT NULL | Default | Index | Ghi chú |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| MIRROR\_HDG\_NHANKHAU | 1 | MA\_CD | Mã công dân | VARCHAR | 30 | PK logic\* | Có | — | INDEX (MA\_CD) | \*Khóa vật lý pending QA-01 |
| MIRROR\_HDG\_NHANKHAU | 2 | MA\_HO\_GD | Mã hộ | VARCHAR | 30 | PK logic\* | Chưa rõ | NULL | QA-01 | Nguồn cho phép NULL |
| MIRROR\_HDG\_NHANKHAU | 3 | MODIFY\_DATE | Thời điểm sửa | TIMESTAMP | — |  | Không | NULL | INDEX (MA\_CD, MODIFY\_DATE) | Giữ nguyên nguồn |
| MIRROR\_HDG\_NHANKHAU | 4 | TYPEID | Mã biến động | VARCHAR | 2 |  | Không | NULL | — | GT/GM/GG/GK là giảm |
| MIRROR\_HDG\_NHANKHAU | 5 | TRANG\_THAI\_DUYET | Phê duyệt | SMALLINT | — |  | Không | NULL | — | 1,3,4 loại |
| C360\_THONG\_TIN\_CA\_NHAN | 1 | MA\_SO\_BHXH | Mã hiện hành | VARCHAR | 30 | PK | Có | — | UNIQUE | Ánh xạ MA\_CD |
| C360\_THONG\_TIN\_CA\_NHAN | 2 | HO\_TEN | Họ tên chuẩn | VARCHAR | 250 |  | Không | NULL | — | Trim/collapse spaces |
| C360\_THONG\_TIN\_CA\_NHAN | 3 | HO\_TEN\_UNSIGNED | Tên không dấu | VARCHAR | 250 |  | Không | NULL | INDEX tìm kiếm | Uppercase no accents |
| C360\_THONG\_TIN\_CA\_NHAN | 4 | NGAY\_SINH\_CHUAN | Ngày sinh rút gọn | VARCHAR | 8 |  | Không | NULL | INDEX search | YYYY/YYYYMM/YYYYMMDD |
| C360\_THONG\_TIN\_CA\_NHAN | 5 | SO\_DINH\_DANH | CCCD 12 số | VARCHAR | 12 |  | Không | NULL | INDEX lookup | Không bịa từ CMND 9 số |
| C360\_THONG\_TIN\_CA\_NHAN | 6 | DQ\_FLAGS | Các lỗi dữ liệu | TEXT | — |  | Không | NULL | — | Mã lỗi phân tách ; |
| C360\_THONG\_TIN\_CA\_NHAN | 7 | SYNC\_DATE | Thời điểm đồng bộ | TIMESTAMP | — |  | Có | CURRENT\_TIMESTAMP | INDEX (SYNC\_DATE,ID) | Cursor cần fence |
| C360\_CHANGE\_QUEUE | 1 | ID | ID event | BIGINT | — | PK | Có | IDENTITY | INDEX | Không dùng ID \<= MAX làm ACK |
| C360\_CHANGE\_QUEUE | 2 | MA\_CD | Khóa cần re-evaluate | VARCHAR | 30 |  | Có | — | INDEX (MA\_CD) | Cả old/new MA\_CD |
| C360\_SYNC\_CONTROL | 1 | CONTROL\_KEY | Khóa nhóm job | VARCHAR | 50 | PK | Có | — | UNIQUE | FOR UPDATE khi Full/NRT |
| C360\_SYNC\_CONTROL | 2 | FULL\_RUNNING | Đang full load | BOOLEAN | — |  | Có | FALSE | — | Thiết kế đề xuất |

## DSThongBao – Thông báo

| ID | Loại | Nội dung | Tham số | HTTP | Vị trí | Ghi chú |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| MSG-E001 | E | Tham số tra cứu không hợp lệ. | checkId | 400 | API-C360-001..004 | Đề xuất |
| MSG-I001 | I | Không tìm thấy công dân. |  | 404 | API-C360-001 | Theo yêu cầu |
| MSG-E409 | E | Có nhiều công dân cùng khớp mã tra cứu. | Candidates | 409 | API-C360-001 | Không chọn tùy ý |
| MSG-E429 | E | Số mã vượt giới hạn 1000\. | limit=1000 | 400 | API-C360-002 | Đề xuất status 400 |
| MSG-E503 | E | Nguồn dữ liệu tạm thời không sẵn sàng. |  | 503 | C360 API | Không lộ internals |
| MSG-E901 | E | Không có quyền truy cập API. |  | 401/403 | C360 API | X-API-Key \+ scopes |

## PhiChucNang – Phi chức năng

| Nhóm | Nội dung | Yêu cầu | Cách đo | Ghi chú |
| :---- | :---- | :---- | :---- | :---- |
| Hiệu năng | NRT | C360 cập nhật trong 1 phút, demo 1–2 phút sau source change | Timestamp source/mirror/c360 và kiểm tra lag | Yêu cầu gốc |
| Chất lượng | Đối soát | COUNT(C360) \= COUNT(DISTINCT MA\_CD hợp lệ Mirror) | SQL đối soát sau drain CDC | 100% khi đồng bộ xong |
| Chất lượng | Idempotency | Chạy lại Full Load/Sink không nhân đôi | Rerun và count/key checks | Bắt buộc |
| Bảo mật | API | X-API-Key, lưu SHA-256 key, scope API, audit async | Test 401/403, kiểm tra log | Yêu cầu gốc |
| Tài nguyên | Local | Docker \~6.74 GiB, chạy nhóm dịch vụ | docker stats; không OOM | Rủi ro |
| Vận hành | Đồng thời | Không bỏ sót Queue/commit hoặc mất delta khi Full/NRT giao nhau | Test concurrency, cut/ACK ID set | Blocking |
| Vận hành | Quyền riêng tư | Chỉ dữ liệu giả lập, không log số định danh đầy đủ | Review data/log | Đề xuất |

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

Nguyên tắc nghiệp vụ then chốt

1. **Mirror giữ dòng nguồn:** một `MA_CD` có thể thuộc nhiều `MA_HO_GD`; không dedup trước C360.  
2. **C360 chỉ 1 dòng/MA\_CD hợp lệ:** loại `TYPEID` GT/GM/GG/GK, `TRANG_THAI_DUYET` 1/3/4; chọn newest theo MODIFY\_DATE → CREATED\_DATE → ID giảm dần.  
3. **Không suy đoán dữ liệu bẩn:** ngày sinh sai / CCCD thiếu / mã xã mơ hồ vẫn giữ công dân (nếu trạng thái hợp lệ), với giá trị chuẩn NULL \+ `DQ_FLAGS`.  
4. **Cập nhật:** CDC Mirror \+ Queue kích hoạt C360 trong 1 phút; đổi `MA_CD` phải xử lý cả khóa cũ và mới.  
5. **Chứng minh:** số dòng nguồn khớp Mirror khi CDC drain; `COUNT(DISTINCT MA_CD hợp lệ Mirror) = COUNT(*) C360`.

&nbsp;