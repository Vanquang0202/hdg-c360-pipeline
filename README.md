# HDG → Mirror → C360

Pipeline local đồng bộ bốn bảng Oracle VSS sang PostgreSQL, rồi chuẩn hóa mỗi công dân thành một dòng C360. Mirror giữ nguyên dữ liệu nguồn; lọc nghiệp vụ và DQ_FLAGS chỉ thực hiện ở C360.

## Luồng xử lý

```text
Oracle → Spark/JDBC → Stage → đối soát → publisher/cutover → Mirror
Oracle redo → Debezium → Kafka → Sink transaction + checkpoint → Mirror
Mirror → Map + lọc/chọn đại diện/chuẩn hóa → Stage → publish → C360
Mirror → trigger enqueue → Change Queue → worker incremental → C360
```

Sink hiện replay theo manifest hữu hạn, chưa là dịch vụ Kafka streaming liên tục. Worker C360 chỉ đánh giá lại MA_CD bị ảnh hưởng, ACK Queue cùng transaction với dữ liệu và log.

## Đọc source từ đâu?

| Thành phần | Entrypoint và module |
|---|---|
| Dữ liệu Oracle local | `generator/seed.py`, `generator/changes.py` |
| Full Load Oracle → Mirror | `jobs/run_spark.ps1` → `full_load.py`; `reconcile.py` đối soát |
| Hợp đồng dữ liệu | `jobs/schema_contract.py`, `schema_preflight.py`, `spark_common.py`: 132 cột, kiểu dữ liệu và kiểm tra trước khi ghi |
| CDC Source | `cdc/compose.yaml`, `Register-Source.ps1`, `connectors/oracle-source.json`; `cdc/oracle/` chứa preflight/quyền LogMiner |
| CDC Sink và phục hồi | `cdc/sink/cutover.py`: prepare/apply/extend/replay/rollback; `run.py` là CLI test |
| Module Sink | `protocol.py` giải mã; `engine.py` áp giao dịch/idempotency; `storage.py` kiểm guard và SQL; `kafka_reader.py` đọc range; `contract.py` và `fixtures.py` cung cấp contract/schema dùng chung |
| Full và incremental C360 | `jobs/c360_full_load.py`: normalize, staging/publish, trigger contract, Queue và worker dùng chung logic |
| SQL | `sql/oracle/`: nguồn; `sql/postgres/`: Mirror/Stage, publisher, recovery, C360 và enqueue |
| Kiểm thử | `tests/full_load/`: schema/Stage/transport; `tests/cdc_sink/`: transaction/replay/crash/cutover/E2E; `tests/test_c360*.py`: chuẩn hóa và incremental |
| Yêu cầu và Data Model | `Yeucau_DataPipeline_HGD_C360_4Tuan.md`, `docs/` |

Giữ riêng các module contract, guard và transaction để dễ kiểm tra ranh giới an toàn; không bỏ qua chúng khi chạy job.

## Cấu hình local

- Oracle: `localhost:1521/FREEPDB1`, schema `VSS`.
- PostgreSQL Mirror/C360: `localhost:15432/hdg_mirror`; PostgreSQL test: `localhost:15433`.
- Kafka host: `localhost:29092`; trong container: `kafka:9092`. Connect REST: `localhost:8083`.
- Python 3.11, Java 21, PySpark 4.0.1. Launcher Spark mặc định `C:\Python311\python.exe`.
- JAR local: `jobs/jars/ojdbc11.jar`, `postgresql-42.7.14.jar`; dùng classpath, không phân phối bằng `--jars` trên local Windows.

Dependency theo môi trường nằm trong `generator/requirements.txt`, `jobs/requirements-spark.txt`, `cdc/sink/requirements.txt`. Sink/C360 dùng Python trong `cdc/sink/.venv`.

Spark/C360 đọc `jobs/.env` hoặc biến môi trường theo [mẫu](jobs/.env.example). Generator đọc `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN` từ môi trường. CDC dùng `cdc/.env`; PostgreSQL test dùng `cdc/sink/.env.test`. Không đưa mật khẩu, JAR hoặc dữ liệu vào Git. Oracle/PostgreSQL hiện có được tái sử dụng, không tạo lại volumes.

## Thứ tự chạy

Các lệnh chạy bằng PowerShell tại gốc project. Chỉ chạy bước thiết lập khi môi trường chưa có; không seed/reset database đang giữ dữ liệu.

### 1. Oracle giả lập

```powershell
C:\Python311\python.exe generator/seed.py seed --count 50000 --batch-size 1000
C:\Python311\python.exe generator/seed.py counts
C:\Python311\python.exe generator/changes.py update-info
```

Seed chỉ chấp nhận cả bốn bảng rỗng. `changes.py` mặc định xem kế hoạch; `--apply` mới ghi fixture nguồn.

### 2. Full Load Oracle → Mirror

```powershell
.\jobs\run_spark.ps1 -Job check_jdbc
.\jobs\run_spark.ps1 -Job full_load -JobArguments @("--stage-only")
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","stage","--source","snapshot")
```

Stage-only chưa thay Mirror. Publisher hỗ trợ bootstrap/NOOP; thay baseline live phải qua cutover có fence, backup và checkpoint handoff. Không suy barrier từ SCN hoặc Kafka end offset đơn lẻ. Hướng dẫn DDL, migration và rollback: [FULL_LOAD.md](jobs/FULL_LOAD.md).

### 3. CDC Oracle → Kafka → Mirror

```powershell
.\cdc\Connect-Network.ps1
docker compose --env-file cdc/.env -f cdc/compose.yaml up -d --pull never
Invoke-RestMethod http://localhost:8083/connectors/hdg-oracle-observe/status
# Chỉ khi Source chưa tồn tại và Oracle đã đủ quyền/logging:
# .\cdc\Register-Source.ps1
```

Đọc thay đổi vào Mirror live bằng plan/extension đã kiểm chứng:

```powershell
$py = ".\cdc\sink\.venv\Scripts\python.exe"
& $py cdc/sink/cutover.py extend --plan "<ACTIVE_PLAN.json>" --extension "<VERIFIED_EXTENSION.json>" --confirm-database hdg_mirror
& $py cdc/sink/cutover.py replay --plan "<VERIFIED_EXTENSION.json>" --confirm-database hdg_mirror
```

Không init lại baseline/epoch hiện có. `run.py init/replay` dành cho database test và baseline tương ứng, không dùng để ghi live.

### 4. Mirror → C360

```powershell
# Chỉ cài một lần khi schema c360 chưa tồn tại:
# & $py jobs/c360_full_load.py install
& $py jobs/c360_full_load.py full
# Chỉ bật sau kiểm thử độc lập và guard đạt:
# & $py jobs/c360_full_load.py enable-nrt
& $py jobs/c360_full_load.py worker --watch --interval 15 --batch-size 500
```

Chỉ chạy một watcher. Full/NRT dùng chung Control; trigger chỉ enqueue, worker giữ đúng tập ID khi ACK và retry. Chi tiết UNKNOWN, test và vận hành: [C360.md](jobs/C360.md).

## Kiểm thử và trạng thái

Kiểm tra không ghi database:

```powershell
C:\Python311\python.exe -B tests/full_load/test_schema_contract.py
C:\Python311\python.exe -B tests/full_load/test_spark_transport.py
& $py -B -m unittest discover -s tests/cdc_sink -p test_unit.py
$env:PYTHONPATH = "tests"
& $py -B -m unittest test_c360.Rules
C:\Python311\python.exe -B jobs/build_full_load_sql.py --check
```

Các integration/E2E cần môi trường riêng và có ghi dữ liệu thử. Không chạy tự động trên database đang giữ bằng chứng PASS; test Sink cũ có reset fixture. Test NRT tạo database mới. Không dùng `discover` toàn bộ rồi coi các test SKIP là PASS.

Đã có bằng chứng local cho Full Load/cutover Mirror, CDC bốn bảng, Full C360 **48.747 công dân** và incremental/rollback/replay trên database độc lập. Các bằng chứng máy local nằm trong `test-results/` được Git ignore.

Chưa hoàn tất: Sink streaming liên tục; chính sách khoảng năm sinh và chuyển đổi điện thoại còn UNKNOWN; worker dừng khi danh mục đổi để tránh dùng Map cũ. Chưa triển khai BI/API. Watcher local chưa tự khởi động cùng Windows.
