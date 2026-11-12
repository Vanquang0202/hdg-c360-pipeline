# Chạy kiểm thử Full Load bằng PySpark và JDBC

> Đã xác nhận lỗi mapping cũ cắt chuỗi trong Mirror. Database hiện có phải theo [quy trình migration và phục hồi](FULL_LOAD_RECOVERY.md), không chạy lại DDL tạo mới hoặc publish để ghi đè. Debezium đang RUNNING không đồng nghĩa CDC cutover đã an toàn.

## 1. Phiên bản và cách chạy

Launcher mặc định dùng `C:\Python311\python.exe` với PySpark 4.0.1 đã cài, Python 3.11 và Java 21. Có thể chọn Python khác bằng tham số `-Python`, nhưng môi trường đó phải có PySpark 4.0.1. [Tài liệu cài PySpark 4.0.1](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html)

Job chạy local[2], từng bảng nối JDBC với một partition đọc và một partition ghi. Không dùng partitionColumn, lowerBound/upperBound cho MA_CD dạng VARCHAR2. Toàn bộ dữ liệu đi qua Spark DataFrame và Spark JDBC. Các lệnh điều khiển transaction, metadata và publish đi qua JDBC của JVM.

Các file phục vụ lần triển khai này gồm spark_common.py, full_load.py, reconcile.py, run_spark.ps1, tests/full_load/check_jdbc.py, tests/full_load/test_spark_transport.py, requirements-spark.txt, .env.example và hai file SQL trong sql/postgres.

## 2. JDBC Driver và cấu hình

Chuẩn bị hai file trong jobs/jars bằng cách tải từ trang phát hành chính thức.

- ojdbc11.jar thuộc bản Oracle JDBC 23.26.4.0.0, được Oracle liệt kê hỗ trợ JDK 21 và Oracle 23ai/26ai. Kiểm tra phiên bản Oracle Free thực tế trước smoke test. [Oracle JDBC downloads](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)
- postgresql-42.7.14.jar, bản pgJDBC dành cho Java từ 8. [pgJDBC downloads](https://jdbc.postgresql.org/download/)

Hai JAR được nạp bằng `--driver-class-path` cho JVM driver và `spark.executor.extraClassPath` cho executor. Launcher dùng đường dẫn tuyệt đối, phân cách bằng `;` trên Windows và đặt `spark.jars` rỗng để không kế thừa danh sách JAR cần phân phối. Cách này dành cho `local[2]`, nơi driver và executor dùng cùng máy; không áp dụng nguyên trạng cho cluster nhiều máy.

`--jars` yêu cầu Spark phân phối rồi lấy file về qua `Utils.fetchFile`. Trên Windows, đường xử lý này có thể gọi chức năng Hadoop cần `HADOOP_HOME`/winutils. Nạp qua classpath tránh bước phân phối JDBC không cần thiết. Không tạo winutils giả hoặc tải binary từ nguồn không rõ ràng. Các file JAR vẫn được Git ignore.

Từ thư mục project, chuẩn bị môi trường bằng PowerShell.

```powershell
& "C:\Python311\python.exe" -m pip install -r jobs/requirements-spark.txt
# Chỉ cần cài nếu môi trường đang thiếu thư viện; không cần tạo venv mới.

$env:JAVA_HOME = "C:\Tools\jdk-21\jdk-21.0.8+9"
& "$env:JAVA_HOME\bin\java.exe" -version

New-Item -ItemType Directory -Force jobs/jars | Out-Null
# Đặt ojdbc11.jar và postgresql-42.7.14.jar đã tải vào jobs/jars.
Get-FileHash jobs/jars/*.jar -Algorithm SHA256
& "$env:JAVA_HOME\bin\java.exe" -jar jobs/jars/ojdbc11.jar
```

Cấu hình kết nối bằng biến môi trường. Địa chỉ PostgreSQL là cổng được công bố cho host, container đích là hdg-c360-postgres.

```powershell
$env:ORACLE_JDBC_URL = "jdbc:oracle:thin:@//localhost:1521/FREEPDB1"
$env:ORACLE_USER = "VSS"
$env:ORACLE_PASSWORD = [System.Net.NetworkCredential]::new("", (Read-Host "Oracle password" -AsSecureString)).Password
$env:PG_JDBC_URL = "jdbc:postgresql://localhost:15432/hdg_mirror"
$env:PG_USER = "postgres"
$env:PG_PASSWORD = [System.Net.NetworkCredential]::new("", (Read-Host "PostgreSQL password" -AsSecureString)).Password
```

Có thể dùng jobs/.env theo mẫu jobs/.env.example. Biến môi trường được ưu tiên. Tài khoản PostgreSQL cần quyền tạo các schema/bảng khi áp dụng DDL và quyền đọc/ghi các đối tượng của job. Khi tách role vận hành, cấp quyền phù hợp với vai trò cụ thể.

Có thể kiểm tra phép so số chính xác và đối soát dòng trùng bằng bộ test Spark cục bộ. Test tạo DataFrame nhỏ trong bộ nhớ và chỉ chạy khi gọi lệnh dưới đây.

```powershell
& "C:\Python311\python.exe" tests/full_load/test_spark_transport.py
```

### Kiểm tra khởi động và JDBC trước khi kết nối database

```powershell
.\jobs\run_spark.ps1 -Job check_jdbc
```

Lệnh này khởi động Spark, nạp cả hai driver ở JVM driver và trong hai task executor, kiểm tra không có JAR phân phối, rồi dừng Spark. Không đọc `.env`, không mở kết nối Oracle/PostgreSQL và không chạy Full Load. Kết quả mong đợi là log `jdbc_check_complete`, hai tên class JDBC và `distributed_jars: 0`.

Nếu thao tác Spark khác vẫn báo thiếu Hadoop native/winutils, cần kiểm tra stack trace cụ thể. Không coi thay đổi classpath là xử lý mọi phụ thuộc Hadoop trên Windows. Có thể chuyển sang WSL2/Linux với Python/PySpark 4.0.1, Java tương thích và hai JAR từ nguồn chính thức. Khi đó phải dùng đường dẫn Linux và kiểm tra địa chỉ/cổng Oracle, PostgreSQL từ WSL2; launcher PowerShell này không phải launcher Linux. Không tự thay đổi môi trường máy để xử lý lỗi.

## 3. DDL và mapping kiểu

Đầu vào là đầy đủ 111/6/3/12 cột của bốn bảng trong sql/oracle/01_create_source_tables.sql. Tên đích theo Basic Design, dùng tên PostgreSQL không quote nên hiển thị chữ thường.

| Oracle nguồn | PostgreSQL Mirror |
|---|---|
| VSS.VSS_HO_GD_NHANKHAU | public.mirror_hdg_nhankhau |
| VSS.VSS_DM_DANTOC | public.mirror_dm_dantoc |
| VSS.VSS_DM_QUOC_TICH | public.mirror_dm_quoc_tich |
| VSS.VSS_DM_DBHC | public.mirror_dm_dbhc |

Mapping kiểu của Mirror được tạo từ DDL nguồn.

- NUMBER không precision thành numeric, NUMBER(p) thành numeric(p,0). Cả NUMBER(19) và NUMBER(20) giữ kiểu numeric để tránh tràn bigint.
- DATE thành timestamp(0) without time zone, giữ cả ngày và giờ. TIMESTAMP(6) thành timestamp(6) without time zone.
- VARCHAR2/NVARCHAR2 thành varchar theo số trong ngoặc; cột BYTE có CHECK octet_length. Yêu cầu Oracle AL32UTF8 và PostgreSQL UTF8 khi dùng hợp đồng byte này. CHAR(1) thành varchar(1) để giữ giá trị khoảng trắng khi đọc lại.
- Giữ NOT NULL và khóa chính ID của hai danh mục có PK trong nguồn. Bảng nhân khẩu có index MA_CD, giữ MA_HO_GD nullable. Danh mục DBHC giữ toàn bộ dòng và các cột nullable.
- Giá trị TYPEID, duyệt, địa bàn, định danh và các cột còn lại đều được truyền nguyên trạng. Không áp bộ lọc hoặc chuẩn hóa C360 tại Mirror.

Spark decimal có giới hạn precision và có thể điều chỉnh scale khi đọc NUMBER. Để bảo toàn dữ liệu, Oracle JDBC xuất NUMBER bằng TO_CHAR với định dạng TM9 và dấu thập phân cố định. Thời gian xuất chuỗi đầy đủ, có phần microsecond khi cần và hậu tố AD/BC. Staging giữ các giá trị vận chuyển này bằng text. Hàm publish dùng chuyển kiểu có kiểm tra độ dài BYTE/CHAR, precision/scale và thời gian trước khi gán kiểu đích; không CAST varchar(n) để cắt chuỗi. Nó so toàn bộ tập dòng và kiểm digest staging trong cùng transaction. [Spark JDBC type mapping](https://spark.apache.org/docs/4.0.1/sql-data-sources-jdbc.html), [PostgreSQL timestamp input](https://www.postgresql.org/docs/16/datatype-datetime.html)

Oracle dùng NLS_DATE_LANGUAGE=AMERICAN và NLS_CALENDAR=GREGORIAN cho các phiên JDBC đọc. Định dạng vận chuyển chỉ thay cách biểu diễn, giá trị nguồn vẫn được giữ. Chuỗi Unicode, số 0 đầu, NULL, dòng trùng và các trạng thái bị lọc ở C360 đều cần có trong kiểm thử.

Áp dụng hai file DDL một lần bằng tài khoản PostgreSQL phù hợp, theo thứ tự bên dưới. DDL là bước thủ công trước job.

```powershell
$env:PGPASSWORD = $env:PG_PASSWORD
psql -h localhost -p 15432 -U $env:PG_USER -d hdg_mirror -v ON_ERROR_STOP=1 -f sql/postgres/01_create_mirror_tables.sql
psql -h localhost -p 15432 -U $env:PG_USER -d hdg_mirror -v ON_ERROR_STOP=1 -f sql/postgres/02_publish_full_load.sql
Remove-Item Env:PGPASSWORD
```

Nếu host chưa có psql, có thể dùng client trong container đã có, sau khi đặt PGPASSWORD như trên.

```powershell
$env:PGPASSWORD = $env:PG_PASSWORD
Get-Content -Raw sql/postgres/01_create_mirror_tables.sql | docker exec -i -e PGPASSWORD hdg-c360-postgres psql -U $env:PG_USER -d hdg_mirror -v ON_ERROR_STOP=1
Get-Content -Raw sql/postgres/02_publish_full_load.sql | docker exec -i -e PGPASSWORD hdg-c360-postgres psql -U $env:PG_USER -d hdg_mirror -v ON_ERROR_STOP=1
Remove-Item Env:PGPASSWORD
```

Các câu CREATE dùng chế độ tạo mới. Nếu đối tượng đã tồn tại, kiểm tra cấu trúc trước khi quyết định migration. Job không tự chạy các file DDL.

## 4. Snapshot Oracle và quyền đọc

Mỗi lượt Full Load dùng một SCN chung cho cả bốn bảng. Mặc định job lấy SCN bằng DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER và đọc từng bảng bằng AS OF SCN. Count và các lần đọc lại để so nội dung đều dùng SCN đó.

Tài khoản Oracle cần được phép lấy SCN và thực hiện flashback query trên các bảng nguồn. Nếu môi trường chưa cấp các quyền này, trao đổi với quản trị để chuẩn bị quyền đọc cần thiết. Có thể truyền SCN đã lấy hợp lệ bằng --source-scn. Không tự chuyển sang đọc current khi flashback thất bại.

UNDO cần giữ được snapshot trong suốt lần đọc và đối soát. Nếu snapshot hết hạn hoặc DDL nguồn thay đổi làm mất khả năng đọc, lượt chạy phải báo lỗi và giữ staging để kiểm tra.

## 5. Nạp và publish

Lệnh sau thực hiện nạp staging, đối soát rồi publish.

```powershell
.\jobs\run_spark.ps1 -Job full_load -JobArguments @("--batch-size", "1000")
# Nếu đã có SCN được cấp, có thể truyền thêm
# -JobArguments @("--batch-size", "1000", "--source-scn", "SCN_HOP_LE")
```

Mỗi lần có run_id mới. Job kiểm tra schema đích rồi đọc Oracle qua Spark JDBC và append vào hdg_stage theo run_id. Spark dùng batchsize đã cấu hình. Sau khi ghi từng bảng, job so count và toàn bộ nội dung trước khi ghi kết quả VALIDATED.

Publish diễn ra trong một transaction PostgreSQL cho cả bốn bảng. Hàm publish khóa control, Mirror và staging, kiểm schema/typmod/nullability, count và digest staging; kiểm chuyển kiểu không mất mát rồi so toàn bộ nội dung bằng EXCEPT ALL hai chiều.

Quy tắc chạy lại bảo vệ Mirror như sau.

1. Lần đầu chỉ cho phép publish khi cả bốn Mirror rỗng và cờ cdc_enabled=false.
2. Sau lần đầu, nếu staging mới giống hệt Mirror thì trả NOOP và giữ nguyên Mirror.
3. Nếu Mirror khác staging hoặc cdc_enabled=true, publish dừng. Các dòng đã có trong Mirror được giữ để kiểm tra.
4. Ghi lỗi Spark, lỗi cast hoặc kiểm tra không khớp làm lượt chạy thất bại. Staging của run đó được giữ. Lượt sau tạo run_id mới.
5. Spark speculation tắt và số lần thử task là 1. Nếu JDBC ghi trùng do kết quả commit không rõ, đối soát multiset sẽ phát hiện và chặn publish.

Đây là giới hạn có chủ đích của job khởi tạo. Nạp lại một snapshot khác lên Mirror đã dùng cho CDC cần cơ chế phối hợp riêng. Bản triển khai này không có nhánh tự động xóa hoặc replace Mirror đang có dữ liệu.

Trước khi bật CDC trong giai đoạn tiếp theo, cần tích hợp cập nhật cờ hdg_load.control.cdc_enabled vào quy trình điều khiển. Cờ này là một lớp chặn bổ sung. Nhánh publish chỉ có thể thêm dữ liệu lần đầu hoặc xác nhận NOOP, nên cũng chặn ghi đè dữ liệu khác với staging ngay cả khi cờ chưa được tích hợp.

## 6. Log và đối soát độc lập

Log JSON có run_id, source_scn, tên bảng, read_rows, written_rows, content_equal và elapsed_seconds. hdg_load.runs lưu trạng thái lượt chạy. hdg_load.table_runs lưu số dòng và thời gian theo bảng. Kết quả publish là COMMITTED hoặc NOOP.

Sau Full Load, lấy run_id trong log rồi chạy đối soát riêng.

```powershell
$runId = "UUID_TU_LOG_RUN_FINISHED"
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id", $runId, "--target", "stage")
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id", $runId, "--target", "mirror")
```

Mặc định đối soát với Oracle tại SCN của run. Thêm --source current để so trạng thái hiện tại, khi đó cần kiểm soát writer và độ trễ trước khi kết luận lệch. Nếu CDC đã hoạt động, Mirror có thể mới hơn snapshot cũ.

Ngoài COUNT, script so toàn bộ 111 cột nhân khẩu và mọi cột của ba danh mục bằng phép trừ đa tập ở hai chiều. Cách này giữ số lần xuất hiện của dòng trùng và phân biệt NULL với chuỗi có giá trị. Số được đưa về biểu diễn chính xác tương đương để so sánh, ví dụ 1.2300 và 123E-2 có cùng giá trị. Nội dung từng người không được in vào log.

Có thể xem thêm dữ liệu cụ thể bằng SQL trong công cụ DB.

```sql
-- PostgreSQL, kiểm fixture cần chuẩn hóa và trạng thái nguồn đã được giữ
SELECT id, ma_cd, ma_ho_gd, ngay_sinh, typeid, trang_thai_duyet, ghi_chu
FROM public.mirror_hdg_nhankhau
WHERE ghi_chu LIKE 'HDG-SEED:%'
ORDER BY id
LIMIT 20;

SELECT ma_cd, count(*)
FROM public.mirror_hdg_nhankhau
GROUP BY ma_cd HAVING count(*) > 1;

SELECT run_id, source_table, source_rows, stage_rows, elapsed_seconds, content_equal
FROM hdg_load.table_runs
ORDER BY run_id, source_table;
```

Đối chiếu các ID mẫu tương ứng tại Oracle, gồm hộ NULL, công dân nhiều hộ, ngày sinh sai lịch, mã xã cũ, CCCD thiếu và dòng chưa duyệt. Kiểm cả cột BYTE/CHAR có Unicode và ngày giờ có microsecond.

## 7. Thứ tự kiểm thử và phần cần kiểm chứng riêng

1. Kiểm tra Python mặc định, hai JAR bằng `-Job check_jdbc`, khai báo kết nối và kiểm quyền flashback.
2. Áp dụng hai file DDL PostgreSQL thủ công.
3. Chạy Full Load. Với dữ liệu hiện tại, số dòng mong đợi là 50.000, 3, 2, 8 theo bốn bảng tương ứng.
4. Chạy reconcile cho stage và Mirror với run_id vừa tạo. Cần count và content_equal đều khớp.
5. Chạy lại Full Load khi nguồn chưa đổi. Kết quả publish mong đợi là NOOP, số dòng Mirror giữ nguyên.
6. Trong môi trường kiểm thử được phép, thay đổi một trường Mirror rồi chạy lại. Kết quả mong đợi là publish bị chặn và giá trị thay đổi vẫn còn.
7. Kiểm tình huống job dừng sau khi ghi một phần staging, lỗi cast, task trả kết quả commit không rõ và hai lượt Full Load chạy đồng thời.

Bước chuyển tiếp Full Load → CDC phải thiết kế và kiểm chứng riêng với SCN/offset, nhật ký còn giữ, key của dòng hộ và khả năng phát lại. Copy đủ bốn bảng chỉ chứng minh phần nạp snapshot. Job hiện tại chưa thực hiện bàn giao trách nhiệm cho Debezium/Kafka/Sink.

Đã chạy `-Job check_jdbc` thành công trên Windows với PySpark 4.0.1 và Java 21.0.8: hai driver được nạp tại driver và hai task executor, `distributed_jars=0`, `database_connections=0`, mã thoát 0. Spark vẫn in cảnh báo thiếu winutils/native Hadoop khi khởi động; cảnh báo này không làm dừng bài kiểm tra. Không chạy Full Load, DDL hoặc kết nối database trong lần sửa launcher này.

Việc kiểm tra khởi động/JDBC tách riêng khỏi kiểm thử Full Load. Không suy ra quyền kết nối, tính đúng của staging/publish hoặc chuyển tiếp CDC từ kết quả nạp class JDBC. DDL và Full Load chỉ chạy khi người vận hành chủ động gọi lệnh tương ứng.

## 8. Chỉ staging và kiểm lại audit cũ

`reconcile --target candidate --candidate-schema hdg_recovery_*` đối soát ứng viên với Oracle mà không thay Mirror. Reconcile giữ SHARE lock trên đủ bốn bảng đích trong quá trình so; cần ngừng writer trong cửa sổ kiểm tra. `--source current` cũng chốt một SCN chung.

`full_load --stage-only` tạo run/staging mới và đối soát nguồn ở cùng SCN, không publish Mirror. `reconcile --target stage --source snapshot --record-stage-audit` ghi digest sau khi cả bốn bảng khớp Oracle snapshot, không đổi trạng thái lịch sử run. Migration, recovery candidate, lệnh kiểm thử và các rào chắn CDC được mô tả trong [FULL_LOAD_RECOVERY.md](FULL_LOAD_RECOVERY.md). Không reset offset hoặc đổi cdc_enabled để vượt chặn publish.

## 9. Stage-only trước khi migration Mirror

`--stage-only` không yêu cầu `hdg_load.contract_version()=2`, publisher hoặc bảng Mirror đúng schema mới. Nó vẫn yêu cầu đủ bốn bảng hdg_stage với 132 cột text, load_run_id varchar(36), NOT NULL đúng nguồn; runs/table_runs đúng kiểu, default started_at và các khóa PK/FK. Oracle vẫn phải qua metadata preflight và dữ liệu staging phải khớp toàn dòng với Oracle tại cùng SCN.

Có hai cấu hình metadata hợp lệ:

- Contract cũ: không có cả cột table_runs.stage_digest lẫn hàm stage_digest(text,text). Job ghi sáu trường audit cũ, không gọi hàm v2; log audit_mode=DEFERRED_LEGACY.
- Có đủ cột digest nullable varchar(64) và hàm tương ứng: job ghi digest, log audit_mode=SHA256. Chỉ chức năng staging không đòi hỏi version publisher.

Nếu chỉ có một thành phần digest hoặc kiểu metadata sai, preflight dừng trước khi ghi run. Không tự chạy DDL để chữa cấu hình cài dở.

Lệnh để người vận hành chủ động chạy lại với snapshot đã đọc được:

```powershell
.\jobs\run_spark.ps1 -Job full_load -JobArguments @("--stage-only","--source-scn","2438031","--batch-size","1000")
```

Lệnh tạo run_id mới và append staging theo run đó, không xóa staging cũ, không publish hoặc cập nhật hdg_load.control. Cần UNDO vẫn giữ được SCN 2438031 đến khi nạp và đối soát hoàn tất; lần đọc thành công trước đó không bảo đảm snapshot còn tồn tại về sau. Nếu lỗi, giữ staging một phần và trạng thái FAILED để kiểm tra, không sửa run cũ để dùng lại.

Với contract cũ, có thể đối soát lại mà không ghi digest:

```powershell
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID_MOI>","--target","stage","--source","snapshot")
```

VALIDATED chỉ xác nhận đối soát nguồn/staging tại thời điểm chạy, không phải quyền publish hoặc bằng chứng cutover. Giữ ngừng các lịch publish cũ và không gọi trực tiếp publisher legacy vì hàm cũ vẫn có lỗi cắt chuỗi. Sau khi triển khai đúng các thành phần digest, phải chạy lại `reconcile --target stage --source snapshot --record-stage-audit` trước khi dùng run legacy cho recovery/publish. Lệnh record audit cần cột và hàm digest; chế độ publish/Mirror/candidate tiếp tục yêu cầu contract v2. Không reset offset hoặc thay đổi SCN vì lần nạp staging này.

Kiểm thử cho lần sửa preflight:

```powershell
& C:\Python311\python.exe -B tests/full_load/test_stage_only.py
& C:\Python311\python.exe -B tests/full_load/test_schema_contract.py
& C:\Python311\python.exe -B jobs/build_full_load_sql.py --check
```

Đã chạy 12 regression test mới và 9 unit test hiện có, đều đạt. Regression dùng catalog contract cũ/v2 và Spark/JDBC giả lập; không phải kiểm thử Full Load trên database thật. Không chạy SQL hoặc migration, không thay staging/Oracle/Kafka/Debezium trong lần sửa này.
