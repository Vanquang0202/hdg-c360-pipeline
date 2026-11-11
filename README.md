# HDG — Oracle → PostgreSQL

## 1. Giới thiệu

Project xây dựng pipeline đồng bộ dữ liệu hộ gia đình và nhân khẩu từ Oracle VSS sang PostgreSQL. Dữ liệu nguồn gồm bốn bảng: `VSS_HO_GD_NHANKHAU`, `VSS_DM_DANTOC`, `VSS_DM_QUOC_TICH` và `VSS_DM_DBHC`.

Full Load nạp dữ liệu ban đầu bằng Spark/JDBC. CDC dùng Debezium LogMiner đọc thay đổi Oracle, đưa qua Kafka để Sink xử lý. Tầng Mirror giữ nguyên dữ liệu nguồn, không lọc trạng thái hoặc chuẩn hóa nghiệp vụ C360.

## 2. Kiến trúc

```text
Full Load:
Oracle VSS → Spark JDBC → PostgreSQL Stage → Đối soát → Publish → Mirror

CDC:
Oracle redo log → Debezium LogMiner → Kafka → Sink prototype → PostgreSQL test
```

Full Load đọc cả bốn bảng tại cùng một **SCN** — mốc xác định ảnh dữ liệu nhất quán của Oracle. Mỗi lượt nạp có `run_id` riêng, ghi vào Stage rồi so toàn bộ giá trị và số lần xuất hiện của từng dòng với nguồn trước khi publish.

Publisher kiểm schema, audit và trạng thái CDC. Nó chỉ nạp lần đầu khi Mirror rỗng hoặc trả `NOOP` khi nội dung đã giống nhau; không dùng để ghi đè Mirror khác dữ liệu.

Sink hiện đọc các khoảng **Kafka offset** được khai báo trong manifest, chưa chạy như dịch vụ streaming liên tục. Manifest xác định dữ liệu nền, topic và thứ tự giao dịch cần replay. Việc cập nhật bảng test, ghi nhận sự kiện đã xử lý và checkpoint nằm trong cùng transaction PostgreSQL để tránh áp lại sau retry.

Hai bảng có PK được xử lý theo khóa; hai bảng không PK dùng toàn bộ before-image, so sánh cả NULL và chỉ tác động một bản sao. Tombstone không được coi là một lần DELETE thứ hai.

## 3. Cấu trúc thư mục

```text
generator/          Sinh dữ liệu Oracle và kịch bản biến động
jobs/               Full Load Spark, đối soát và kiểm schema
  jars/             Oracle JDBC và PostgreSQL JDBC
sql/
  oracle/           DDL bốn bảng nguồn VSS
  postgres/         DDL Mirror/Stage, publisher và SQL phục hồi
cdc/                Compose Kafka/Connect, cấu hình Source và LogMiner
  sink/             Sink prototype và cấu hình PostgreSQL test
tests/
  full_load/        Kiểm thử schema, Stage, Spark và JDBC
  cdc_sink/         Kiểm thử decoder, replay, transaction và crash/restart
docs/               Basic Design và Detail Design
```

Các điểm bắt đầu và module chính:

| File | Chức năng |
|---|---|
| `generator/seed.py` | Sinh dữ liệu bốn bảng theo batch, kiểm số dòng trước khi commit |
| `generator/changes.py` | Tạo INSERT/UPDATE/DELETE, chuyển hộ, đổi mã và trạng thái |
| `jobs/run_spark.ps1` | Launcher cho Full Load, đối soát và kiểm JDBC |
| `jobs/full_load.py` | Đọc snapshot Oracle, ghi Stage, đối soát và gọi publisher |
| `jobs/reconcile.py` | Đối soát nguồn với Stage, Mirror hoặc Recovery Candidate |
| `jobs/schema_contract.py` | Mapping 132 cột Oracle sang PostgreSQL, dùng chung với Sink |
| `jobs/schema_preflight.py` | Kiểm schema và metadata thực tế trước khi ghi |
| `jobs/spark_common.py` | Kết nối JDBC, vận chuyển kiểu dữ liệu và so sánh toàn dòng |
| `cdc/compose.yaml`, `cdc/connectors/oracle-source.json` | Cấu hình Kafka, Connect và Oracle Source |
| `cdc/sink/run.py` | CLI tạo fixture, nạp baseline và replay vào database test |
| `cdc/sink/engine.py` | Kiểm manifest, áp transaction, chống ghi trùng và cập nhật checkpoint |
| `cdc/sink/protocol.py`, `storage.py`, `kafka_reader.py` | Giải mã sự kiện, ghi PostgreSQL test và đọc Kafka |
| `cdc/sink/contract.py`, `fixtures.py` | Contract đích test và dữ liệu synthetic cho kiểm thử |

DDL nguồn ở [01_create_source_tables.sql](sql/oracle/01_create_source_tables.sql). PostgreSQL dùng [01_create_mirror_tables.sql](sql/postgres/01_create_mirror_tables.sql) và [02_publish_full_load.sql](sql/postgres/02_publish_full_load.sql). Các file SQL được quản lý bằng `jobs/build_full_load_sql.py` và template `jobs/full_load_guards.sql`.

## 4. Cấu hình môi trường local

Các lệnh dưới chạy bằng **PowerShell 7**, tại thư mục gốc project. Cần Python 3.11, Java 21 và Docker Desktop cho các dịch vụ local.

| Thành phần | Cấu hình trong project |
|---|---|
| Oracle nguồn | Oracle Free, container `hdg-c360-oracle`, service `FREEPDB1`, schema `VSS`, cổng 1521 |
| PostgreSQL Mirror | PostgreSQL 16, container `hdg-c360-postgres`, database `hdg_mirror`, cổng 55432 |
| Spark | PySpark 4.0.1, `local[2]`; launcher mặc định dùng `C:\Python311\python.exe` |
| Kafka | `apache/kafka:3.8.1`; Windows dùng `localhost:29092`, container dùng `kafka:9092` |
| Kafka Connect | `quay.io/debezium/connect:3.4.3.Final`, REST tại `http://localhost:8083` |
| PostgreSQL Sink test | Database `hdg_cdc_sink_test`, user `sink_test_owner`, cổng 55433 |

Cài dependency cho generator và Spark, tạo môi trường Python riêng cho Sink:

```powershell
C:\Python311\python.exe -m pip install -r generator/requirements.txt -r jobs/requirements-spark.txt
C:\Python311\python.exe -m venv cdc/sink/.venv
.\cdc\sink\.venv\Scripts\python.exe -m pip install -r cdc/sink/requirements.txt
```

- **Generator:** đọc `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN` từ biến môi trường; không tự đọc .env. DSN local là `localhost:1521/FREEPDB1`.
- **Spark:** cấu hình theo [jobs/.env.example](jobs/.env.example) trong `jobs/.env` hoặc biến môi trường. Cần `ORACLE_JDBC_URL`, `ORACLE_USER`, `ORACLE_PASSWORD`, `PG_JDBC_URL`, `PG_USER`, `PG_PASSWORD`.
- **JDBC:** đặt `ojdbc11.jar` và `postgresql-42.7.14.jar` trong `jobs/jars/`. Launcher nạp qua classpath local; Connect cũng mount Oracle JAR này.
- **CDC Source:** cấu hình `cdc/.env` theo [mẫu](cdc/.env.example), gồm network và `ORACLE_CDC_PASSWORD`. Oracle cần ARCHIVELOG, supplemental logging và tài khoản LogMiner theo `cdc/oracle/`. Helper tạo user đọc password từ biến môi trường PowerShell.
- **Sink:** password lấy từ `CDC_TEST_PASSWORD` hoặc `cdc/sink/.env.test`. `Setup-Test.ps1` tạo file password nếu chưa có; giữ đúng password của volume test hiện tại.

Không lưu mật khẩu trong source hoặc đưa các file .env vào Git. Oracle và PostgreSQL Mirror phải được chuẩn bị riêng; Compose CDC không tạo lại hai database này.

## 5. Hướng dẫn chạy

### 5.1. Tạo dữ liệu Oracle

Với nguồn mới, tạo schema VSS và bốn bảng bằng DDL nguồn trước. Sau khi thiết lập các biến môi trường Oracle:

```powershell
C:\Python311\python.exe generator/seed.py seed --count 50000 --batch-size 1000
C:\Python311\python.exe generator/seed.py counts
```

Seed chỉ chạy khi cả bốn bảng rỗng. Nếu đã có dữ liệu, chỉ dùng lệnh counts, không seed lại.

Kịch bản biến động mặc định chỉ in kế hoạch. Thêm --apply khi chủ động muốn ghi thay đổi vào Oracle để kiểm tra CDC:

```powershell
C:\Python311\python.exe generator/changes.py update-info
# Ví dụ ghi một thay đổi lên nguồn local:
# C:\Python311\python.exe generator/changes.py update-info --apply
```

Các kịch bản khác gồm insert, delete-latest, delete-last, add-household, move-household, change-code, cancel-approval, close-type và approve. delete-last chạy sau delete-latest.

### 5.2. Full Load bằng Spark

Database mới cần cài DDL Mirror/Stage và publisher trước. Database đã có dữ liệu cần theo [hướng dẫn phục hồi](jobs/FULL_LOAD_RECOVERY.md), không chạy lại DDL tạo mới để thay thế dữ liệu.

Kiểm JDBC, nạp vào Stage rồi đối soát snapshot:

```powershell
.\jobs\run_spark.ps1 -Job check_jdbc
.\jobs\run_spark.ps1 -Job full_load -JobArguments @("--stage-only")
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","stage","--source","snapshot")
```

Thay <RUN_ID> bằng giá trị trong log Full Load. --stage-only không ghi Mirror. Chỉ chạy full_load không có tùy chọn này khi đáp ứng các điều kiện publisher và quy trình phục hồi; nó tạo một lượt nạp mới, không publish lại Stage cũ.

Chi tiết cấu hình, SCN và audit: [FULL_LOAD.md](jobs/FULL_LOAD.md).

### 5.3. Debezium và Kafka

Với môi trường mới, chuẩn bị logging và user LogMiner trước; `cdc/Invoke-OracleSetup.ps1` dùng để tạo user khi chưa tồn tại. Sau đó nối network và khởi động dịch vụ từ các image đã chuẩn bị:

```powershell
.\cdc\Connect-Network.ps1
docker compose --env-file cdc/.env -f cdc/compose.yaml up -d --pull never
Invoke-RestMethod http://localhost:8083/connector-plugins
# Chỉ đăng ký khi Source chưa tồn tại:
.\cdc\Register-Source.ps1
Invoke-RestMethod http://localhost:8083/connectors/hdg-oracle-observe/status
```

Network mặc định là hdg-cdc-net. Register-Source.ps1 kiểm cấu hình và từ chối ghi đè connector đã có. Với Source đang RUNNING, chỉ kiểm tra trạng thái, không đăng ký lại.

Source dùng snapshot.mode=no_data, không tự nạp dữ liệu lịch sử. SCN snapshot và Kafka offset phải được nối tiếp bằng quy trình riêng trước khi dùng CDC cập nhật Mirror.

### 5.4. Sink trên PostgreSQL test

Chuẩn bị PostgreSQL test bằng image postgres:16 đã có:

```powershell
.\cdc\sink\Setup-Test.ps1
```

Sink yêu cầu manifest với baseline tương ứng, topic ID, range offset và các transaction frame được khai báo rõ. Với manifest đã kiểm chứng:

```powershell
.\cdc\sink\.venv\Scripts\python.exe cdc/sink/run.py init --manifest "<MANIFEST_TEST.json>"
.\cdc\sink\.venv\Scripts\python.exe cdc/sink/run.py replay --manifest "<MANIFEST_TEST.json>" --kafka-bootstrap localhost:29092
```

init chỉ nạp baseline vào database test chưa được khởi tạo. Có thể thay --kafka-bootstrap bằng --input "<RECORDS_TEST.jsonl>" để replay file sự kiện. Dữ liệu synthetic dùng cho nhánh file; không dùng manifest synthetic để đọc topic Oracle thật.

Không tự chọn offset 0/latest làm bằng chứng baseline đã đồng bộ. Hướng dẫn tạo fixture, kiểm thử và giới hạn Sink: [PROTOTYPE.md](cdc/sink/PROTOTYPE.md).

## 6. Trạng thái hiện tại

**Đã có bằng chứng kiểm thử local:**

- Oracle có dữ liệu giả lập cho bốn bảng; Spark đã đọc snapshot, ghi Stage và đối soát.
- Recovery Candidate khớp cả bốn bảng với Oracle tại SCN 2438031; contract v2 và audit SHA256 Stage đã được kiểm chứng.
- Debezium Source đã chạy và phát INSERT/UPDATE/DELETE cùng tombstone của DBHC.
- Sink đã kiểm thử với dữ liệu synthetic trên PostgreSQL test, gồm transaction, replay và crash/restart. Bộ offline gồm 9 test contract, 12 test Stage và 17 test Sink đã đạt.

**Chưa hoàn thành hoặc cần kiểm chứng tiếp:**

- Mirror live vẫn cần phục hồi dữ liệu chuỗi bị cắt; chưa thay bằng Recovery Candidate.
- Chưa chứng minh ranh giới chuyển tiếp an toàn từ SCN Full Load sang Kafka offset.
- Sink hiện replay hữu hạn, chưa là dịch vụ streaming live đa topic; định danh sự kiện qua restart Debezium thật cần kiểm chứng.
- Chưa bật CDC Sink ghi Mirror live; chưa triển khai C360, BI hoặc API.

Các kết quả trên thuộc môi trường local, chưa xác nhận pipeline vận hành hoàn chỉnh.
