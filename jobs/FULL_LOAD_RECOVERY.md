# Sửa hợp đồng kiểu dữ liệu và phục hồi Mirror

## Phạm vi và kết luận hiện tại

Lỗi mất chuỗi đã được người vận hành xác nhận: Oracle và staging có 50.000 ID duy nhất, dài 17 ký tự; Mirror có 50.000 dòng nhưng ID đều là HD. Chưa chạy migration, SQL kiểm thử, phục hồi, Full Load hoặc thay đổi offset/topic trong lần sửa này. Debezium Source đang RUNNING theo hiện trạng cung cấp; không suy ra Sink đang hoạt động hoặc dữ liệu đã nối tiếp an toàn từ SCN 2314948.

## 1. Nguyên nhân và phạm vi

Parser cũ dùng `re.search(r"\d+", oracle_type)`, nên VARCHAR2/NVARCHAR2 đều lấy số 2 trong tên kiểu thay vì số trong ngoặc. Lỗi ảnh hưởng các cột chuỗi của cả bốn bảng, không chỉ ID. CHAR(1), NUMBER và thời gian không đi qua nhánh lấy số này.

Schema preflight cũ chỉ so tên kiểu chung, không kiểm độ dài, precision/scale hoặc nullability. Publish dùng CAST AS varchar(n): PostgreSQL cho phép ép kiểu này cắt chuỗi quá dài. NOOP và kiểm tra sau INSERT lại so với staging đã bị CAST tương tự, nên cả hai phía cùng mất thông tin vẫn báo bằng nhau. [Quy tắc chuỗi PostgreSQL 16](https://www.postgresql.org/docs/16/datatype-character.html)

Đối soát Spark độc lập với Oracle vốn dùng dữ liệu vận chuyển không cắt và EXCEPT ALL; không phải phép đối soát này gây cắt chuỗi. Lỗ hổng nằm ở kiểm tra schema và phép so sánh trong publisher. Nay toàn bộ SQL được sinh từ parser chung để tránh mapping Python/DDL/publish lệch nhau.

## 2. Hợp đồng kiểu dữ liệu mới

| Oracle | PostgreSQL | Cách ngăn mất dữ liệu |
|---|---|---|
| VARCHAR2(n BYTE), CHAR(n BYTE) | varchar(n) + CHECK octet_length <= n | Kiểm độ dài cả ký tự và byte trước INSERT; không CAST varchar(n) |
| VARCHAR2(n CHAR), NVARCHAR2(n) | varchar(n) | Kiểm số ký tự, giữ Unicode và khoảng trắng |
| NUMBER | numeric không typmod | Vận chuyển chuỗi TM9, không qua float/Decimal Spark giới hạn 38 số |
| NUMBER(p,s), NUMBER(p) | numeric(p,s), numeric(p,0) | Đọc số exact trước, kiểm round(n,s)=n và giới hạn precision rồi mới gán |
| DATE | timestamp(0) without time zone | Giữ cả giờ/phút/giây, không đổi thành date hoặc tự thêm timezone |
| TIMESTAMP(6) | timestamp(6) without time zone | Vận chuyển đủ 6 số lẻ, kiểm chuỗi → timestamp → chuỗi không đổi |
| NULL | NULL | Không coalesce thành chuỗi rỗng; giữ NOT NULL của nguồn |

Ví dụ: MA_CD varchar(30), ID varchar(40), HO_TEN varchar(250), DIA_CHI varchar(500), SO_CCCD varchar(50), MA_OFFLINE varchar(100), TEN danh mục dân tộc/quốc tịch varchar(100), TEN DBHC varchar(200). QH_CHU_HO, TYPEID và mã MA của hai danh mục vẫn đúng là varchar(2); không thay hàng loạt mọi varchar(2) thành kiểu rộng.

Kiểm tra metadata Oracle đang chạy phải khớp DDL: đủ 111/6/3/12 cột, thứ tự, BYTE/CHAR, precision/scale và nullable. Cấu hình byte-check hiện yêu cầu Oracle AL32UTF8 và PostgreSQL UTF8; charset khác phải thiết kế kiểm tra byte theo encoding tương ứng, không tự đoán. CHAR được lưu varchar để không loại khoảng trắng khi đọc lại. Chuỗi rỗng nguồn Oracle vốn là NULL; chuỗi rỗng tự xuất hiện trong staging bị từ chối, không tự đổi thành NULL.

Hàm checked_text trả text nguyên vẹn hoặc lỗi, kể cả phần vượt độ dài chỉ là dấu cách. checked_numeric trả numeric không typmod sau kiểm tra, không làm tròn trước khi kiểm. checked_timestamp từ chối phần giây vượt độ chính xác và ngày không thể chuyển nguyên vẹn. TIMESTAMP nguồn >6 số lẻ bị parser từ chối thay vì làm tròn âm thầm. [Kiểu số](https://www.postgresql.org/docs/16/datatype-numeric.html), [Kiểu thời gian](https://www.postgresql.org/docs/16/datatype-datetime.html)

Với NUMBER/DATE/TIMESTAMP đang có: việc đọc code không cho thấy chúng bị lỗi lấy độ dài như VARCHAR2. Tuy nhiên không đủ căn cứ kết luận dữ liệu Mirror thực tế chưa từng bị làm tròn hoặc đổi giờ. Phải chạy đối soát toàn dòng tại cùng SCN và các ca integration trên PostgreSQL trước khi xác nhận.

## 3. Validation và NOOP

- Preflight kiểm typmod thực tế bằng pg_catalog.format_type, nullability và thứ tự cột. Publisher kiểm thêm BYTE constraints đã validate và PK ID của hai danh mục.
- Các giá trị staging được kiểm khả năng chuyển không mất thông tin trước khi INSERT hoặc NOOP. Không CAST chuỗi về kiểu bị giới hạn độ dài trong phép so.
- So hai chiều EXCEPT ALL trên toàn bộ cột; giữ số lần lặp của dòng, NULL và khoảng trắng. Chuỗi trong publisher dùng collation C để không che khác biệt do collation.
- Spark vẫn so Oracle snapshot với staging/Mirror nguyên vẹn, chỉ chuẩn hóa cách biểu diễn số exact, ví dụ 1.2300 và 123E-2 bằng nhau về giá trị. Không chuẩn hóa nghiệp vụ tại Mirror.
- Trên metadata có đủ chức năng digest, mỗi bảng/run có stage_digest SHA256 trên tập hash dòng đã sắp xếp, không gồm load_run_id. Stage-only trên contract cũ được phép hoãn digest, với audit_mode=DEFERRED_LEGACY; vẫn phải đối soát toàn dòng với Oracle và chưa được dùng để publish/recovery trước khi re-audit. Nhờ đó đổi nội dung staging dù COUNT không đổi cũng bị chặn. Đây là bảo vệ thay đổi sau đối soát, không thay cho đối chiếu nguồn.
- Các run cũ chưa có digest được giữ nguyên trạng thái và để digest NULL sau migration. Không tự lấy hash hiện tại rồi coi là đã xác minh nguồn. Phải dùng reconcile snapshot bên dưới để ghi audit mới.
- Hàm digest dùng string_agg trên hash dòng, phù hợp 50.000 dòng local; không coi đây là cách tính digest đủ mở rộng cho 138 triệu dòng. Cần thiết kế hash theo lô nếu mở rộng.

Trường hợp Mirror ID toàn HD còn staging ID đủ 17 ký tự: preflight sẽ chặn schema cũ; sau migration mở rộng, publisher vẫn chặn vì nội dung khác. **Không còn được trả NOOP chỉ vì hai bên cùng có 50.000 dòng.**

## 4. File và triển khai schema

Các file SQL được sinh bởi `jobs/build_full_load_sql.py`. Script chỉ đọc DDL và ghi file, không kết nối database:

```powershell
& C:\Python311\python.exe -B jobs/build_full_load_sql.py --check
```

- 01_create_mirror_tables.sql: chỉ dành database mới.
- 02_publish_full_load.sql: bộ hàm kiểm tra, digest, schema contract v2 và publisher mới.
- 03_fix_mirror_contract.sql: migration cho database hiện có, thay publisher trong cùng transaction.
- 04_build_recovery_candidate.sql: cài hàm dựng bốn bảng ứng viên ở schema riêng.

Migration 03 không DELETE/INSERT/TRUNCATE dữ liệu Mirror, không đổi trạng thái run, cdc_enabled hoặc last_run_id. Nó đổi kiểu cột đúng hợp đồng, kiểm dữ liệu trước khi có thể thu hẹp/làm tròn, thêm byte constraints và stage_digest nullable, thay các hàm. Nó dùng lock_timeout 10 giây và khóa Mirror/staging; nếu không lấy được khóa thì dừng và rollback. Hết timeout không phải lý do để kill phiên đang ghi mà chưa kiểm tra.

Chỉ mở rộng ID thành varchar(40) không thể khôi phục phần đã mất. Sau migration giá trị HD vẫn là HD. Migration cũng không giải quyết xong khóa UPDATE/DELETE cho nhân khẩu/DBHC.

Lệnh dưới chỉ là bước triển khai **sau khi integration test và rào chắn writer được xác minh**. Không chạy lại file 01 trên database đang dùng:

```powershell
# Dùng PGPASSWORD trong môi trường, không truyền password trên command line.
psql -h localhost -p 15432 -U $env:PG_USER -d hdg_mirror -v ON_ERROR_STOP=1 -f sql/postgres/03_fix_mirror_contract.sql
psql -h localhost -p 15432 -U $env:PG_USER -d hdg_mirror -v ON_ERROR_STOP=1 -f sql/postgres/04_build_recovery_candidate.sql
```

## 5. Trình tự phục hồi khi Debezium đã phát sự kiện

### Bước 1: Giữ nguyên bằng chứng và ngăn ghi đè

Dừng lịch publish/Full Load, Sink và các job ghi Mirror/C360 nếu đang có. Xác minh writer thực tế, không chỉ nhìn cờ cdc_enabled vì Source có thể RUNNING trong khi cờ vẫn false. Chưa tạo Sink nếu chưa giải quyết khóa.

Giữ Debezium Source chạy để tiếp tục lưu sự kiện nếu có đủ archive/Kafka retention; không pause/reset Source theo mặc định. Dừng generator và hoạt động thử nghiệm ghi nguồn. Lưu bản sao database/schema/data/functions trước migration, giữ cả hai run staging, runs/table_runs, bằng chứng độ dài/ID và phiên bản code. Không ghép dữ liệu để sửa bằng ID đã cắt vì không còn duy nhất.

Ghi nhận connector config, task status, source offsets/SCN, Kafka beginning/end offsets theo từng topic/partition, topic retention, resetlogs incarnation và giao dịch đang mở. Bảo vệ mật khẩu/PII khi lưu bằng chứng. Không xóa topic, history, offset hoặc staging. Nếu retention không đủ hoặc archive sắp hết, xử lý dung lượng trước khi tiếp tục.

### Bước 2: Kiểm thử và migration

Chạy unit/regression offline, sau đó integration trên database riêng. Chỉ khi đạt mới thực hiện migration 03/04 thủ công trong cửa sổ ngừng writer. Có thể nạp snapshot mới bằng --stage-only trước bước migration nếu staging/metadata contract cũ đầy đủ; xem mục 9 của FULL_LOAD.md. Không cần migration Mirror chỉ để giữ snapshot vào staging. Kiểm schema bằng hdg_load.assert_mirror_schema(). Không tự đổi các run cũ thành VALIDATED hoặc sửa cờ CDC để vượt chặn.

### Bước 3: Chọn snapshot phục hồi

**A. Dùng staging còn nguyên của Full Load cũ:** chỉ khi Oracle còn flashback được đúng source_scn của run và cả bốn bảng đối soát đầy đủ khớp. Không chọn run chỉ dựa vào số dòng hoặc vì chạy sau cùng.

```powershell
# Có đọc Oracle/PostgreSQL và ghi digest audit khi chủ động gọi; không ghi Mirror.
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","stage","--source","snapshot","--record-stage-audit")
```

Lệnh này giữ SHARE lock trên staging trong lúc đối soát, rồi ghi digest chỉ sau khi cả bốn bảng khớp Oracle AS OF SCN. Giữ nguyên trạng thái lịch sử COMMITTED/NOOP của run. Nếu flashback báo lỗi hoặc không khớp thì dừng; không tự đổi sang source=current và ký audit cho snapshot cũ.

**B. Nếu snapshot cũ không thể kiểm chứng:** không dùng nó làm trạng thái nguồn hiện tại. Chuẩn bị snapshot mới tại SCN R bằng chế độ chỉ staging:

```powershell
# Đây là lần đọc/nạp staging mới, chỉ chạy khi được chủ động triển khai.
.\jobs\run_spark.ps1 -Job full_load -JobArguments @("--stage-only","--source-scn","<SCN_R>","--batch-size","1000")
```

Chế độ này dùng run_id mới, không publish hoặc ghi Mirror; không xóa staging cũ. Nó vẫn ghi runs/table_runs cho lần nạp mới và giữ status VALIDATED khi hoàn thành; log kết quả STAGED. Source có thể tiếp tục chạy và dữ liệu nguồn đọc nhất quán tại R. UNDO phải đủ giữ snapshot.

### Bước 4: Dựng ứng viên cách ly

Sau khi đủ audit/digest, gọi SQL thủ công với run và SCN đã chọn:

```sql
-- Ví dụ tham số, thay UUID và SCN thực sự đã kiểm chứng; không phải lệnh đã chạy.
SELECT hdg_load.build_recovery_candidate(
 '<RUN_UUID>'::varchar, <SCN_DA_KIEM_CHUNG>::numeric, 'hdg_recovery_verified');
```

Hàm tạo schema mới và đủ bốn bảng, kiểm số dòng/digest, kiểm chuyển kiểu và đối chiếu EXCEPT ALL hai chiều. Nếu schema đã tồn tại thì lỗi, không tự ghi đè. Nó không đổi Mirror đang dùng, control hoặc status run. Dựng ứng viên không có nghĩa đã khôi phục dữ liệu live.

Đối soát ứng viên với nguồn tại đúng SCN bằng lệnh riêng:

```powershell
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","candidate","--candidate-schema","hdg_recovery_verified","--source","snapshot")
```

Reconcile giữ SHARE lock trên đủ bốn bảng đích trong suốt các lần đọc JDBC để không so các thời điểm khác nhau; lock_timeout là 10 giây. Cần cửa sổ dừng writer phù hợp vì lock này chặn ghi. Với `--source current`, job chốt một SCN hiện tại rồi đọc cả bốn bảng AS OF SCN đó; không đọc nguồn trôi theo thời gian ở từng action. So thành công vẫn chỉ xác nhận checkpoint đã chọn, không tự xác lập cutover CDC.

Kiểm 50.000 ID duy nhất và độ dài 17 nếu dùng đúng snapshot đã xác nhận, nhưng kiểm cả MA_CD, hộ NULL, tên Unicode, địa chỉ, CCCD, NUMBER và thời gian của cả bốn bảng. Dữ liệu ở snapshot R mới có thể khác 50.000 dòng, không áp cứng số cũ.

### Bước 5: Rào chắn CDC trước khi thay Mirror

SCN 2314948 là mốc snapshot cũ, không tự trở thành offset của connector no_data. Source RUNNING hoặc đã có sự kiện không chứng minh khoảng từ Full Load đến bootstrap được lưu đủ.

Hai hướng cần thiết kế/kiểm chứng trước khi chuyển live:

1. Nếu dùng snapshot cũ, chứng minh toàn bộ thay đổi sau snapshot còn đủ trong redo/archive hoặc Kafka, có lịch sử schema, giao dịch bắt đầu trước mốc nhưng commit sau mốc và thứ tự áp dụng đúng. Có khoảng trống thì không dùng hướng này.
2. Nếu chọn snapshot mới R, có thể lập cửa sổ dừng writer nguồn, chờ các giao dịch liên quan kết thúc, xác lập R và ranh giới offset Kafka từng partition với bằng chứng nguồn/connector. Giữ nguồn ngừng ghi đến lúc publish xong và đã xác lập cơ chế nhận thay đổi tiếp theo. Hoặc thiết kế replay đồng thời với snapshot mới; phương án này cần xử lý giao dịch giao nhau, không chỉ lọc source.scn > R.

Không lấy offset cao nhất rồi bỏ qua sự kiện trước đó mà chưa chứng minh snapshot bao phủ mọi commit; không giả định có một offset chung cho bốn topic. Với source vẫn đang ghi, phải chứng minh buffered/replayed events không mất, không lặp và không làm snapshot lùi trạng thái.

### Bước 6: Chuyển live có kiểm soát, chưa cung cấp lệnh ghi đè tự động

Chỉ thực hiện sau khi đã có ranh giới CDC, khóa dòng cho UPDATE/DELETE (kể cả MA_HO_GD NULL, DBHC không PK) và cơ chế phát lại đã kiểm chứng. Giữ writer Mirror và job Queue/C360 ngừng chạy, chụp backup cuối cùng, rồi thiết kế transaction thay **nội dung** cả bốn bảng từ ứng viên để giữ OID/dependency/permissions của Mirror. Không đổi tên bảng ứng viên thành Mirror tùy tiện vì view, FK và trigger có thể vẫn trỏ bảng cũ.

Cần đánh giá trigger Queue trước khi bulk replace: không cho job đọc trạng thái nửa chừng; nếu bỏ qua trigger phải có kế hoạch rebuild/ghi queue đầy đủ cả mã cũ và mới. Đối soát trong transaction, rollback nếu lệch; sau commit kiểm lại, rồi mới cho downstream hoạt động. Ghi riêng biên bản phục hồi với snapshot/offset/barrier đã dùng, không sửa lịch sử Full Load để làm mất bằng chứng lỗi.

Bộ sửa này cố ý không có hàm restore trực tiếp hoặc reset bootstrap/offset. Chưa đủ dữ kiện về Sink/barrier nên không thể viết một lệnh ghi đè live an toàn cho trạng thái hiện tại. Chỉ khởi động Sink sau khi chứng minh định danh dòng và khả năng replay; không coi bước sửa schema là CDC cutover hoàn tất.

## 6. Kiểm thử và tiêu chí đạt

```powershell
& C:\Python311\python.exe -B tests/full_load/test_schema_contract.py
& C:\Python311\python.exe -B jobs/build_full_load_sql.py --check
& C:\Python311\python.exe -B tests/full_load/test_spark_transport.py
# Chỉ chạy integration sau khi tự tạo database test riêng, rỗng:
psql -h localhost -p 15432 -U $env:PG_USER -d hdg_full_load_test -v ON_ERROR_STOP=1 -f tests/full_load/integration.sql
```

Integration script tự chặn nếu database không đúng hdg_full_load_test hoặc đã có hdg_load. Nó thực hiện DDL/DML trong database test khi người vận hành gọi; không chạy trên hdg_mirror. Script để lại đối tượng/bằng chứng trong database test, không tự DROP database.

Các ca gồm: giữ ID dài, một công dân nhiều hộ và hộ NULL; Unicode/khoảng trắng; địa chỉ đúng 500 byte; CCCD có 0 đầu; số 19/20/38 chữ số và số mũ lớn/nhỏ; NUMBER(p,s) không bị làm tròn; DATE giữ thời gian và BC; TIMESTAMP giữ microsecond; số dòng bằng nhau nhưng mất ID vẫn fail; staging đổi nội dung cùng COUNT vẫn fail; COMMITTED rồi NOOP thật; migration mở rộng không giả phục hồi giá trị; recovery candidate không chạm live/control. Kiểm thử Spark so cả NULL/chuỗi rỗng và số lần lặp của dòng.

Điều kiện đóng lỗi triển khai: schema thực tế khớp, toàn bộ bốn bảng đối soát nguồn/candidate/live ở cùng checkpoint, không có truncation/rounding, lịch sử lỗi còn nguyên và có bằng chứng CDC nối tiếp không thiếu/trùng. Chưa có bằng chứng thực tế thì không ghi PASS cho các điều kiện này.

## 7. Kết quả kiểm tra trong lần sửa này

- 9 unit test offline: đạt; có kiểm schema thực tế giả lập sai varchar(2) bị chặn trước khi ghi và kiểm tra asset SQL được sinh nhất quán.
- 4 test Spark local với DataFrame trong bộ nhớ: đạt, thời gian khoảng 136 giây; phát hiện ID cắt, thay đổi Unicode/khoảng trắng, NUMBER, DATE/TIMESTAMP, NULL và dòng trùng. Spark vẫn in cảnh báo native Hadoop/winutils và ResourceWarning socket; các assertion qua và tiến trình thoát mã 0.
- Python AST, các khối Markdown và dấu dollar-quote SQL: đã kiểm; kiểm delimiters không thay cho parse/thực thi PostgreSQL.
- Integration test PostgreSQL, metadata Oracle/PostgreSQL thật, migration và phục hồi: **chưa chạy**. Chưa có bằng chứng dữ liệu live đã được khôi phục hoặc CDC cutover an toàn.

## 8. Bổ sung: staging không phụ thuộc publisher v2

Lỗi preflight stage-only đã được tách khỏi hợp đồng publisher. Ngoài version check, đường ghi audit cũ cũng đã được sửa để không bắt buộc stage_digest trên database legacy. Không triển khai migration chỉ để vượt lỗi preflight.

Dependency tối thiểu để stage-only hoạt động: JDBC Oracle/PostgreSQL; metadata Oracle đúng hợp đồng; bốn bảng hdg_stage đủ 132 cột text và load_run_id; hai bảng hdg_load.runs/table_runs theo cấu trúc cũ, default started_at, PK và FK. Các khóa advisory, CAST metadata số và clock_timestamp dùng hàm tích hợp PostgreSQL. Không cần hdg_load.control, checked_text/checked_numeric/checked_timestamp, assert_mirror_schema, publish_full_load hoặc contract_version ở đường staging legacy. Trong Python, source_schema vẫn kiểm kiểu/độ dài/precision nguồn, transport vẫn giữ chuỗi số và thời gian, reconcile vẫn so toàn dòng hai chiều.

Nếu metadata có cả stage_digest varchar(64) nullable và hàm stage_digest(text,text), job dùng chúng. Nếu cả hai đều chưa có thì job ghi audit cũ và hoãn digest. Nếu cài dở hoặc schema sai thì dừng, không tự sửa database. Triển khai digest tối thiểu về sau chỉ cần thêm cột đúng kiểu và hàm digest từ bộ SQL đã kiểm chứng, không nhất thiết đổi Mirror; tuy nhiên phải có kế hoạch DDL riêng, không áp nguyên file publisher v2 trên schema cũ để giả coi toàn bộ contract đã nâng cấp.

Run stage-only legacy vẫn kết thúc VALIDATED/STAGED sau khi dữ liệu nguồn và staging khớp, nhưng không có hash bảo vệ thay đổi staging về sau. Giữ nguyên staging, không gọi publisher legacy, và re-audit đúng Oracle SCN để ghi digest sau khi có thành phần cần thiết. Nếu UNDO của SCN đó đã hết thì không tự ký digest bằng count hoặc dữ liệu current; chọn lại snapshot theo quy trình phục hồi.

Xem lệnh SCN 2438031, đối soát staging và kết quả 21 unit/regression test offline trong [FULL_LOAD.md](FULL_LOAD.md#9-stage-only-trước-khi-migration-mirror). Theo thông tin người vận hành, integration v2 đã PASS trên database test riêng; lần sửa này không chạy lại integration hoặc Full Load thật và không thay dữ liệu database.
