# Full Load, đối soát và phục hồi Mirror

Hướng dẫn vận hành cho Oracle → PostgreSQL Mirror và handoff sang CDC. Không chạy lại DDL tạo mới hoặc publisher để ghi đè database đang sử dụng. Mỗi lần cutover phải có bằng chứng mới phù hợp trạng thái hiện tại.

## 1. Kết nối và JDBC

Dùng Python 3.11/PySpark 4.0.1, Java 21 và launcher `jobs/run_spark.ps1`. Launcher mặc định `C:\Python311\python.exe`, Spark `local[2]`, một partition JDBC mỗi bảng; không phân vùng bằng MA_CD kiểu chuỗi.

Cấu hình `jobs/.env` hoặc môi trường theo `jobs/.env.example`: ORACLE_JDBC_URL, ORACLE_USER, ORACLE_PASSWORD, PG_JDBC_URL, PG_USER, PG_PASSWORD. Local Oracle là `localhost:1521/FREEPDB1`; PostgreSQL là `localhost:15432/hdg_mirror`.

Hai JAR hiện dùng là `jobs/jars/ojdbc11.jar` và `postgresql-42.7.14.jar`. Launcher nạp qua `--driver-class-path` + `spark.executor.extraClassPath`, đặt `spark.jars` rỗng. Trên Windows local cách này tránh phân phối JAR qua Hadoop fetchFile. Không dùng winutils giả hoặc binary không rõ nguồn. Với WSL/Linux phải chuẩn bị lại đường dẫn/môi trường tương ứng; không áp nguyên launcher Windows lên cluster.

## 2. Schema và hợp đồng dữ liệu

`jobs/schema_contract.py` đọc DDL Oracle, kiểm đủ **111/6/3/12 cột**, làm nguồn chung cho Spark, Sink và bộ sinh SQL.

| Kiểu nguồn | Đích/kiểm tra |
|---|---|
| VARCHAR2/CHAR BYTE | varchar(n) và CHECK octet_length; giữ khoảng trắng |
| VARCHAR2 CHAR/NVARCHAR2 | varchar(n), kiểm số ký tự và Unicode |
| NUMBER không typmod | numeric; vận chuyển text exact, không qua float hoặc giới hạn Decimal Spark |
| NUMBER(p,s) | numeric(p,s); kiểm precision/scale trước gán, không làm tròn âm thầm |
| DATE | timestamp(0) without time zone, giữ cả giờ/phút/giây |
| TIMESTAMP(6) | timestamp(6) without time zone, giữ microsecond |
| NULL | Giữ NULL và NOT NULL nguồn; không tự thay bằng chuỗi rỗng |

Byte-check hiện dựa trên Oracle AL32UTF8/PostgreSQL UTF8. Encoding khác cần thiết kế riêng. Không CAST dữ liệu về varchar bị giới hạn trước đối soát: explicit cast có thể cắt chuỗi và che mất dữ liệu. Các trường hợp bằng COUNT nhưng khác nội dung phải fail.

SQL 01–04 được sinh từ `jobs/build_full_load_sql.py` và template `jobs/full_load_guards.sql`; không sửa tay các file sinh ra:

```powershell
C:\Python311\python.exe -B jobs/build_full_load_sql.py --check
```

- `01_create_mirror_tables.sql`: chỉ database mới/rỗng; tạo Mirror, Stage và metadata.
- `02_publish_full_load.sql`: checked types, schema contract v2, digest và publisher.
- `03_fix_mirror_contract.sql`: migration database cũ; không tự phục hồi dữ liệu đã bị cắt.
- `04_build_recovery_candidate.sql`: tạo hàm dựng ứng viên độc lập từ Stage đã audit.
- SQL 05–06 dành cho C360, xem [C360.md](C360.md).

## 3. Full Load và đối soát

```powershell
.\jobs\run_spark.ps1 -Job check_jdbc
.\jobs\run_spark.ps1 -Job full_load -JobArguments @("--stage-only")
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","stage","--source","snapshot")
```

Có thể cung cấp `--source-scn <SCN>` cho full_load; SCN phải có căn cứ và còn đọc flashback được. Cả bốn bảng dùng cùng snapshot. Mirror không lọc TYPEID/trạng thái duyệt hoặc khử trùng công dân.

Job ghi Stage theo run_id → so hai chiều toàn dòng bằng EXCEPT ALL → audit → VALIDATED. Digest SHA256 trên các hash dòng đã sắp xếp bảo vệ nội dung Stage sau đối soát, không thay thế so với Oracle. Phù hợp quy mô local; chưa phải thuật toán hash phân tán cho 138 triệu dòng.

- `--stage-only` không yêu cầu publisher contract v2 khi metadata/Stage cũ đủ điều kiện; vẫn kiểm đủ 132 cột, kiểu, NULL và metadata.
- Contract cũ có thể nhận `DEFERRED_LEGACY`; chưa được dùng publish/recovery cho đến khi migration và re-audit hợp lệ.
- Chế độ publish cần v2; chỉ bootstrap khi Mirror rỗng hoặc NOOP khi dữ liệu nguyên vẹn đã bằng nhau. CDC đang bật hoặc baseline khác sẽ bị chặn.

Đối soát Mirror/Candidate:

```powershell
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","mirror","--source","snapshot")
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","candidate","--candidate-schema","hdg_recovery_verified","--source","snapshot")
```

Reconcile giữ SHARE lock các bảng đích; cần tránh tranh chấp writer. `--source current` cần quyền lấy SCN bằng DBMS_FLASHBACK; tài khoản CDC đọc hiện từng bị ORA-00904 với package này. Không cấp thêm quyền chỉ để vượt lỗi; snapshot tường minh cần được chốt và kiểm chứng riêng, không sửa metadata run cũ.

## 4. Migration và Recovery Candidate

Chỉ thực hiện khi đã kiểm thử trên database độc lập, có backup/restore và cửa sổ khóa phù hợp. Không tự sửa cờ CDC, trạng thái run hoặc reset offset để vượt guard.

```powershell
# PGPASSWORD/PG_USER lấy từ môi trường, không đưa mật khẩu vào command line.
psql -h localhost -p 15432 -U $env:PG_USER -d hdg_mirror -v ON_ERROR_STOP=1 -f sql/postgres/03_fix_mirror_contract.sql
psql -h localhost -p 15432 -U $env:PG_USER -d hdg_mirror -v ON_ERROR_STOP=1 -f sql/postgres/04_build_recovery_candidate.sql
```

Migration kiểm dữ liệu trước khi đổi kiểu, giữ run cũ và digest NULL nếu chưa re-audit. Mở rộng varchar không phục hồi phần chuỗi đã mất. Khi Stage hợp lệ có thể nạp snapshot mới bằng stage-only trước migration Mirror.

```powershell
.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<RUN_ID>","--target","stage","--source","snapshot","--record-stage-audit")
```

Sau khi nguồn tại snapshot khớp cả bốn bảng, mới dựng ứng viên bằng hàm:

```sql
SELECT hdg_load.build_recovery_candidate('<RUN_ID>', <SNAPSHOT_SCN>, 'hdg_recovery_<UNIQUE_SUFFIX>');
```

Tên schema phải mới và đúng guard. Đối soát Candidate đủ cột/NULL/dòng lặp; không tự đổi tên bảng ứng viên thành Mirror vì có thể làm sai dependency, OID, trigger hoặc quyền. Không xóa Stage/Candidate/backup cũ.

## 5. Cutover và checkpoint handoff

Entrypoint: `cdc/sink/cutover.py`. Cutover thay nội dung bốn bảng trong cùng transaction với epoch/checkpoint/control; giữ OID/ACL, shadow và bản rollback. Guard không cho lấy SCN/offset của bài test hoặc plan hết hạn làm barrier live.

Trước make-plan:

1. Giữ session Oracle mới khóa EXCLUSIVE NOWAIT đủ bốn bảng; chứng minh không có DML mở trên chúng, chốt SCN/incarnation/SID/SERIAL.
2. Full Load stage-only dưới session fence đó; VALIDATED và audit/digest bốn bảng.
3. Xác minh capture completeness: Source/task RUNNING, miner không còn transaction/queue/error, source offset đã flush, lịch sử BEGIN/END đầy đủ và offset vector ổn định. Xét transaction bắt đầu trước SCN nhưng commit sau SCN; không chỉ so source.scn.
4. Fence JSON gồm kind=live-writer-fence-v1, destination=hdg_mirror, plan_id, sid, serial, scn, source_incarnation. Session client_info phải là HDG_CUTOVER:PLAN_UUID:SCN.
5. Backup đúng trạng thái hiện tại, restore/clone proof đúng SHA256 và các kiểm thử crash/rollback/replay/owner/ACL. JMX helper ReadCaptureMetrics/agent phải đọc được; thiếu thì BLOCKED, không tự restart/bật agent.
6. Guard live hiện yêu cầu SCN còn mới trong 30 phút và fence được giữ đến lúc nghiệm thu. Handoff ban đầu dùng range rỗng tại frontier đã chứng minh, không chọn end offset tùy tiện.

Các lệnh dưới là thao tác có ghi, chỉ dùng sau khi đủ điều kiện:

```powershell
$py = ".\cdc\sink\.venv\Scripts\python.exe"
& $py cdc/sink/cutover.py make-plan --run-id "<FRESH_RUN>" --barrier "<FRESH_FENCE.json>" --rehearsal-proof "<CLONE_PASS.json>" --backup "<VERIFIED_BACKUP.dump>" --plan "<NEW_PLAN.json>" --confirm-database hdg_mirror
& $py cdc/sink/cutover.py prepare --plan "<NEW_PLAN.json>" --confirm-database hdg_mirror
& $py cdc/sink/cutover.py apply --plan "<NEW_PLAN.json>" --confirm-database hdg_mirror
```

prepare giữ dữ liệu cũ ở schema rollback và dựng shadow, chưa thay live. apply kiểm lại fence, hash, owner/ACL/OID, schema và handoff trước COMMIT. Sau apply phải đối soát đủ bốn bảng và kiểm epoch/checkpoint. Không tái dùng fence đã giải phóng; backup/proof lịch sử không tự hợp lệ sau khi live đổi trạng thái.

Các frame CDC tiếp theo phải đủ BEGIN/END, đúng topic ID, offset liên tục, thứ tự transaction và giữ nguyên prefix của manifest:

```powershell
& $py cdc/sink/cutover.py extend --plan "<ACTIVE_PLAN.json>" --extension "<VERIFIED_EXTENSION.json>" --confirm-database hdg_mirror
& $py cdc/sink/cutover.py replay --plan "<VERIFIED_EXTENSION.json>" --confirm-database hdg_mirror
```

DML, inbox/dedup, transaction ledger và checkpoint commit cùng nhau. Replay cùng frame không tạo dữ liệu hoặc Queue lần hai. Tombstone chỉ tiêu thụ offset, không DELETE lần nữa. Bảng không PK so toàn bộ before-image NULL-safe và chỉ tác động một bản sao; không coi ID/row_id là khóa bất biến.

Rollback giữ archive, cần fence mới loại live-rollback-fence-v1 và client_info=HDG_ROLLBACK:PLAN_UUID:SCN_ROLLBACK; chứa destination, sid, serial, scn và đủ năm stream/topic ID tại frontier hiện tại:

```powershell
& $py cdc/sink/cutover.py rollback --plan "<ACTIVE_PLAN.json>" --rollback-proof "<FRESH_ROLLBACK_FENCE.json>" --confirm-database hdg_mirror
```

Chỉ trigger enqueue C360 đúng contract được chấp nhận. Các trigger khác/FK/policy/RLS vẫn bị chặn. Cutover khóa Control C360 trước Mirror; không tự disable trigger để vượt guard.

## 6. Test và công cụ giữ lại

- `tests/full_load/test_schema_contract.py`: contract + nhánh Stage legacy/v2, chỉ mock/offline.
- `tests/full_load/test_spark_transport.py`: Spark local trong bộ nhớ, không JDBC.
- `tests/full_load/integration.sql`: regression PostgreSQL contract v2/migration/recovery, chỉ database test rỗng đúng guard.
- `tests/cdc_sink/test_unit.py`: protocol, precision, ordering, endpoint, retry và manifest.
- `tests/cdc_sink/test_integration.py`, `crash_worker.py`: Sink transaction/crash; có reset fixture ở database test cố định, không dùng trên DB giữ PASS.
- `tests/cdc_sink/full_pipeline_e2e.py`: E2E bốn bảng, fence/JMX/barrier, Spark baseline, CDC và đối soát; có Oracle fixture DML, chỉ chạy khi chủ động kiểm thử local.
- `tests/cdc_sink/test_cutover.py`: clone/backup/crash/rollback/replay; runner cần các fixture/backup được chỉ định và môi trường tương ứng. Backup phải khớp live hiện tại; dữ liệu của lần diễn tập cũ không tự tái sử dụng được.

Test E2E tạo dữ liệu/bằng chứng trong database riêng; không tự coi --help hoặc import thành PASS integration. Không reset database/topic/offset/epoch đang giữ bằng chứng. Sinh/kiểm SQL bằng --check không kết nối database.

CLI test Sink vẫn ở `cdc/sink/run.py`:
```powershell
& $py cdc/sink/run.py init --manifest "<TEST_MANIFEST.json>"
& $py cdc/sink/run.py replay --manifest "<TEST_MANIFEST.json>" --kafka-bootstrap localhost:29092
```

Baseline phải đúng range; synthetic manifest có topic UUID giả chỉ dùng file fixture, không đọc topic Oracle thật. `Setup-Test.ps1` chỉ chuẩn bị PostgreSQL test khi không truyền -RunTests; không gọi tùy chọn reset/test trên DB đã PASS.
