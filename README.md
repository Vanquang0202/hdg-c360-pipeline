# HDG: Oracle → Spark / CDC → PostgreSQL

Ba luồng cần đọc theo thứ tự:

1. **Sinh dữ liệu:** `generator/seed.py` → Oracle VSS; `changes.py` tạo biến động để kiểm tra CDC.
2. **Full Load:** `jobs/run_spark.ps1` → `full_load.py` → Oracle snapshot → PostgreSQL Stage → đối soát → publish có bảo vệ.
3. **CDC:** Oracle → Debezium → Kafka → `cdc/sink/run.py` → PostgreSQL **test**.

Sink chỉ ghi `hdg_cdc_sink_test` tại cổng **55433**, chưa ghi Mirror live ở cổng **55432**. C360/BI/API chưa triển khai.

## Cấu trúc

```text
generator/          Sinh dữ liệu Oracle và các thao tác thay đổi
jobs/               Full Load, đối soát, schema và launcher Spark
sql/                DDL, publisher và SQL phục hồi có bảo vệ
cdc/                Compose Kafka/Connect và cấu hình Debezium
  sink/             Đọc Kafka, giải mã và áp giao dịch vào DB test
tests/
  full_load/        Test contract, Stage, Spark, JDBC và integration SQL
  cdc_sink/         Unit, integration và tiến trình thử crash
docs/               Bốn file thiết kế gốc, không chỉnh sửa
archive/            Source đã gộp và tài liệu/bản thử nghiệm cũ
```

## 18 file của luồng chính

| File | Dữ liệu đi qua / nhiệm vụ |
|---|---|
| generator/seed.py | Sinh danh mục và nhân khẩu theo batch; kiểm bốn bảng rỗng trước khi seed |
| generator/changes.py | Lập kế hoạch INSERT/UPDATE/DELETE/chuyển hộ/đổi mã; chỉ ghi khi có --apply |
| jobs/run_spark.ps1 | Điểm chạy Spark local[2], chọn Python và classpath JDBC |
| jobs/full_load.py | Chốt SCN, nạp Stage theo run_id, đối soát rồi publish có điều kiện |
| jobs/reconcile.py | So dữ liệu Oracle với Stage/Mirror/candidate, không chỉ đếm dòng |
| jobs/spark_common.py | Kết nối JDBC, vận chuyển số/ngày chính xác, so dữ liệu và ghi audit |
| jobs/schema_contract.py | Đọc đủ 132 cột từ DDL Oracle; dùng chung cho Full Load và Sink |
| jobs/schema_preflight.py | Kiểm schema, metadata Stage, PK/FK/default và khả năng digest legacy/v2 |
| jobs/full_load_guards.sql | Template chặn mất dữ liệu khi cast; bộ sinh nhúng vào publisher |
| sql/postgres/01_create_mirror_tables.sql | DDL Mirror, Stage và metadata cho database mới |
| sql/postgres/02_publish_full_load.sql | Publisher atomic, schema guard và audit; không chạy tự động |
| cdc/sink/run.py | Điểm chạy init/replay/generate-fixture; retry nguyên frame khi lỗi phù hợp |
| cdc/sink/engine.py | Kiểm baseline/manifest; áp giao dịch cùng inbox, ledger và checkpoint |
| cdc/sink/protocol.py | Giải mã schema/payload Debezium, kiểm kiểu và phân biệt tombstone |
| cdc/sink/storage.py | Guard DB test, DDL test và match before-image NULL-safe đúng một bản sao |
| cdc/sink/kafka_reader.py | Chỉ đọc range offset hữu hạn đã khai báo, không commit/reset Kafka offset |
| cdc/sink/contract.py | Pin hash DDL và cố định database/port/schema test |
| cdc/sink/fixtures.py | Sinh dữ liệu synthetic cho lệnh generate-fixture và các bài test |

Giữ decoder, storage và transaction riêng vì mỗi phần kiểm một nhóm điều kiện khác nhau; gộp tiếp sẽ làm Sink khó đọc. SCN xác định ảnh nhất quán Oracle; Kafka offset xác định vị trí sự kiện trong từng partition, không thể thay thế nhau. Checkpoint chỉ tiến cùng transaction PostgreSQL; ledger ngăn áp lại sự kiện sau retry/replay.

## Các file hỗ trợ vẫn cần giữ

- `jobs/build_full_load_sql.py`: sinh SQL từ contract; `--check` chỉ so file. Giữ cùng `full_load_guards.sql` để không sửa lệch mapping trong từng bản SQL.
- `sql/oracle/01_create_source_tables.sql`: DDL nguồn, cũng là đầu vào contract. Giữ nguyên byte vì Sink tính hash trên file này.
- `sql/postgres/03_fix_mirror_contract.sql`, `04_build_recovery_candidate.sql`: giữ cho quy trình phục hồi dữ liệu bị cắt; không tự áp dụng.
- `cdc/Connect-Network.ps1`, `Invoke-OracleSetup.ps1`, `Register-Source.ps1` và hai SQL trong `cdc/oracle/`: chuẩn bị network, user LogMiner và đăng ký Source cho môi trường mới.
- `cdc/sink/Setup-Test.ps1`: chuẩn bị DB test; tùy chọn -RunTests có integration, không chạy trong lượt rút gọn.
- Compose, JSON connector, requirements và `.env.example` là cấu hình cần thiết. Password đọc từ môi trường/`.env`; không đưa secret vào Git.
- [Yêu cầu gốc](Yeucau_DataPipeline_HGD_C360_4Tuan.md) và bốn file trong `docs/` giữ nguyên. Chi tiết vận hành ở [Full Load](jobs/FULL_LOAD.md), [phục hồi](jobs/FULL_LOAD_RECOVERY.md), [Sink](cdc/sink/PROTOTYPE.md).

## Thứ tự chạy

Các lệnh dưới dành cho người vận hành, không phải yêu cầu nạp lại dữ liệu hiện có.

**1. Oracle:** chỉ seed khi bốn bảng nguồn rỗng; cấu hình ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN theo file mẫu.

```powershell
python generator/seed.py seed --count 50000 --batch-size 1000
python generator/seed.py counts
# Chỉ xem kế hoạch thay đổi, chưa ghi Oracle:
python generator/changes.py update-info
```

**2. Full Load:** chuẩn bị PySpark 4.0.1, Java 21, jobs/.env và hai JDBC JAR theo FULL_LOAD.md. Với Mirror đang phục hồi, dùng Stage, chưa publish:

```powershell
.\jobs\run_spark.ps1 -Job full_load -JobArguments @("--stage-only")
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","stage","--source","snapshot")
```

Không bỏ --stage-only trước khi đáp ứng quy trình phục hồi và điều kiện publish. Không chạy DDL tạo mới trên database hiện có chỉ để thử.

**3. Debezium/Kafka:** môi trường mới chuẩn bị network, logging/user, cdc/.env, Compose rồi Register-Source.ps1 theo hướng dẫn. Source đã RUNNING thì chỉ xem trạng thái, không đăng ký lại:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/hdg-oracle-observe/status
```

**4. Sink test:** dùng venv riêng theo cdc/sink/requirements.txt và manifest với baseline/range đã xác minh:

```powershell
.\cdc\sink\.venv\Scripts\python.exe cdc/sink/run.py init --manifest "<MANIFEST_TEST.json>"
.\cdc\sink\.venv\Scripts\python.exe cdc/sink/run.py replay --manifest "<MANIFEST_TEST.json>" --kafka-bootstrap localhost:29092
```

Địa chỉ Kafka phải khớp listener trong cấu hình đang dùng. Có thể thay --kafka-bootstrap bằng --input "<RECORDS_TEST.jsonl>". Không tự lấy offset 0/latest làm baseline; Source RUNNING với no_data chưa chứng minh cutover an toàn.

## Kiểm tra và trạng thái

```powershell
C:\Python311\python.exe -B tests/full_load/test_schema_contract.py
C:\Python311\python.exe -B tests/full_load/test_stage_only.py
.\cdc\sink\.venv\Scripts\python.exe -B -m unittest discover -s tests/cdc_sink -p test_unit.py
C:\Python311\python.exe -B jobs/build_full_load_sql.py --check
# Kiểm JDBC thủ công vẫn giữ giao diện cũ; file nằm trong tests:
# .\jobs\run_spark.ps1 -Job check_jdbc
```

Lượt gộp source này: **38 test offline đạt** (9 contract, 12 Stage, 17 Sink), SQL sinh ra khớp template. Không chạy Spark, integration database hoặc E2E.

Bằng chứng từ các lượt trước: Oracle đã có dữ liệu; Spark Stage/candidate khớp bốn bảng tại SCN 2438031; Source đã phát sự kiện DBHC; Sink synthetic đã kiểm thử trên DB test. Mirror live từng bị cắt chuỗi và chưa được thay bằng candidate. Chưa xác minh cutover barrier, Sink streaming live đa topic hoặc định danh redo qua restart Debezium thật.

## Những gì đã rút gọn

- Gộp `staging_metadata.py` vào `schema_preflight.py`; gộp `manifest.py` vào `engine.py`. Bản gốc giữ nguyên tại `archive/consolidation/`.
- Chuyển 3 test Full Load và check_jdbc.py sang `tests/full_load/`; chuyển 3 file test Sink sang `tests/cdc_sink/`. Launcher và helper đã cập nhật đường dẫn.
- Toàn bộ code ngoài archive: **38 → 36 file**. Trong đó **18 file luồng chính + 10 file hỗ trợ + 8 file test/check**. Trước lượt gộp, nhóm luồng chính tương ứng có 20 file.
- Archive cũ giữ nguyên: 16 bản Python E2E, ba tài liệu lịch sử và README trước lượt rút gọn đầu. Không di chuyển secret, fixture đã sinh, backup hoặc volume.

Việc chuyển test chỉ làm rõ cấu trúc, không làm giảm tổng số file. Không gộp thêm SQL migration/DDL hoặc bỏ guard chỉ để đạt số lượng thấp hơn.
