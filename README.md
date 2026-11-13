# HDG → Mirror → C360

Pipeline local đồng bộ bốn bảng Oracle VSS sang PostgreSQL, rồi chuẩn hóa mỗi công dân thành một dòng C360. Mirror giữ nguyên dữ liệu nguồn; lọc nghiệp vụ và DQ_FLAGS chỉ thực hiện ở C360.

## Luồng xử lý

```text
Oracle → Spark/JDBC → Stage → đối soát → publisher/cutover → Mirror
Oracle redo → Debezium → Kafka → Sink transaction + checkpoint → Mirror
Mirror → Map + lọc/chọn đại diện/chuẩn hóa → Stage → publish → C360
Mirror → trigger enqueue → Change Queue → worker incremental → C360
```

Sink có chế độ `watch`: kiểm tra Kafka liên tục theo chu kỳ, gom transaction hoàn chỉnh rồi dùng append/replay hiện có. Worker C360 chỉ đánh giá lại MA_CD bị ảnh hưởng, ACK Queue cùng transaction với dữ liệu và log.

## Đọc source từ đâu?

| Thành phần | Entrypoint và module |
|---|---|
| Dữ liệu Oracle local | `generator/seed.py`, `generator/changes.py` |
| Full Load Oracle → Mirror | `jobs/run_spark.ps1` → `full_load.py`; `reconcile.py` đối soát |
| Hợp đồng dữ liệu | `jobs/schema_contract.py`, `schema_preflight.py`, `spark_common.py`: 132 cột, kiểu dữ liệu và kiểm tra trước khi ghi |
| CDC Source | `cdc/compose.yaml`, `Register-Source.ps1`, `connectors/oracle-source.json`; `cdc/oracle/` chứa preflight/quyền LogMiner |
| CDC Sink và phục hồi | `cdc/sink/cutover.py`: watch/prepare/apply/extend/replay/rollback; `continuous.py` gom transaction và chạy vòng lặp; `run.py` là CLI test |
| Module Sink | `protocol.py` giải mã; `engine.py` áp giao dịch/idempotency; `storage.py` kiểm guard và SQL; `kafka_reader.py` đọc range; `contract.py` và `fixtures.py` cung cấp contract/schema dùng chung |
| Superset HDG | `bi/compose.yaml`, `Dockerfile`, `superset_config.py`: instance riêng tại 8089, metadata/volumes riêng |
| Airflow HDG | `airflow/compose.yaml`, `Dockerfile`, `hdg_pipeline.py`: môi trường riêng tại 8091, DAG manual chỉ đọc |
| REST API C360 | `api/pom.xml`, `api/src/main/java/vn/hdg/c360/`: lookup, batch, search, danh sách thay đổi; SHA-256 API Key/scopes và Audit async |
| Đối soát C360 và BI | `jobs/c360_reconcile.py` chỉ đọc; `jobs/c360_bi.py` cài dashboard Superset; `sql/postgres/07_c360_bi.sql` tổng hợp KPI |
| Full và incremental C360 | `jobs/c360_full_load.py`: normalize, staging/publish, trigger contract, Queue và worker dùng chung logic |
| SQL | `sql/oracle/`: nguồn; `sql/postgres/`: Mirror/Stage, publisher, recovery, C360 và enqueue |
| Kiểm thử | `tests/full_load/`: schema/Stage/transport; `tests/cdc_sink/`: transaction/replay/crash/cutover/E2E; `tests/test_c360*.py`: chuẩn hóa và incremental |
| Yêu cầu và Data Model | `Yeucau_DataPipeline_HGD_C360_4Tuan.md`, `docs/` |

Giữ riêng các module contract, guard và transaction để dễ kiểm tra ranh giới an toàn; không bỏ qua chúng khi chạy job.

## Cấu hình local

- Oracle: `localhost:1521/FREEPDB1`, schema `VSS`.
- PostgreSQL Mirror/C360: `localhost:15432/hdg_mirror`; PostgreSQL test: `localhost:15433`.
- Superset HDG: `localhost:8089`, admin `hdg_admin`; cấu hình riêng trong `bi/.env` được Git ignore. DataHub tại 8088 không dùng cho project này.
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

**Chạy Sink liên tục trên epoch đã ACTIVE** (không tạo baseline/cutover mới):

```powershell
$py = ".\cdc\sink\.venv\Scripts\python.exe"
$plan = "test-results/continuous-20261113/live-plan.json"
& $py cdc/sink/cutover.py watch --plan $plan --confirm-database hdg_mirror --interval 5 --stop-file test-results/continuous-20261113/stop.request
```

Watch đọc credential từ `jobs/.env`; cần Docker CLI truy cập container Oracle để SELECT DBID/resetlogs/PDB. Plan truyền vào xác định đúng cutover/epoch; manifest mới nhất luôn lấy từ PostgreSQL. Không sửa plan/seal hoặc đặt offset 0/latest. Sau khi bật Docker, Oracle, PostgreSQL, Kafka, Connect và C360 Watcher sau lần tắt máy, chạy lại đúng lệnh trên. Không tự khởi động Sink thứ hai.

**Dừng:** Ctrl+C ở cửa sổ foreground, hoặc với worker chạy nền:

```powershell
Set-Content test-results/continuous-20261113/stop.request 'stop requested'
Get-Content test-results/continuous-20261113/worker-final.log -Tail 10
```

Chờ `STOPPED` và khóa writer được nhả. Trước lần start tiếp theo, chỉ xóa marker điều khiển này:

```powershell
Remove-Item test-results/continuous-20261113/stop.request
```

**Kiểm tra:** PostgreSQL checkpoint là mốc khôi phục; Kafka consumer offset chỉ ACK sau DB commit. Group dùng assign/offset tường minh nên có thể hiện `Empty` dù process còn chạy; kiểm tra thêm lock/process/log.

```powershell
docker exec hdg-c360-postgres psql -U hdg_user -d hdg_mirror -c "SELECT e.epoch,e.state,e.next_frame,c.topic,c.next_offset FROM sink_meta.epochs e JOIN sink_meta.checkpoints c USING(epoch) WHERE e.state='ACTIVE' ORDER BY c.topic"
docker exec hdg-c360-postgres psql -U hdg_user -d hdg_mirror -c "SELECT a.pid,a.application_name,l.granted FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND l.classid=631874 AND l.objid=2"
docker exec hdg-cdc-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group hdg-mirror-hdg_mirror-70a334b0-bf77-4f34-863d-6ca3646ed63e
```

Một khóa session PostgreSQL chặn watch/replay/extend/cutover chạy cùng lúc. Thiếu BEGIN/END hoặc DML thì chờ, không apply sớm; quá 300 giây dừng để kiểm tra. Lỗi Kafka tạm thời retry tối đa 8 lần, backoff tối đa 30 giây. Mất PostgreSQL thì process thoát để không ghi khi đã mất writer lock; khởi động lại sẽ đọc checkpoint và resume cả manifest đang dở.

Chỉ hỗ trợ đúng năm topic hiện hành, mỗi topic một partition 0; thay topic ID, số partition, source identity hoặc phạm vi capture sẽ BLOCK.

Giới hạn local: backlog mỗi đợt tối đa 5.000 record; raw batch/history tối đa 64 MiB, manifest tối đa 32 MiB; Kafka client queue 16 MiB. Đây là polling liên tục, không phải thay Sink bằng framework streaming. Manifest vẫn giữ lịch sử nên cuối cùng có giới hạn dung lượng; vượt giới hạn sẽ BLOCK, cần kiểm tra/replay có kiểm chứng, không tự xóa lịch sử hoặc reset epoch. Chưa có cơ chế compact manifest tự động.

**Quay về replay thủ công:** dừng watch và xác nhận khóa đã nhả; xuất plan ACTIVE mới nhất vì file lúc start có thể cũ. Lệnh dưới đọc cấu hình `jobs/.env` trong process, không xuất secret:

```powershell
@'
import sys,json
from pathlib import Path
sys.path[:0]=['jobs','cdc/sink']
from c360_full_load import connect
with connect() as c:
    plan=c.execute("SELECT plan FROM hdg_load.cutovers WHERE state='ACTIVE'").fetchone()[0]
    Path('test-results/continuous-20261113/manual-plan.json').write_text(json.dumps(plan),encoding='utf-8')
'@ | & $py -
@'
import sys
sys.path[:0]=['jobs','cdc/sink']
from c360_full_load import connect
with connect(): pass
import cutover
cutover.main()
'@ | & $py - replay --plan test-results/continuous-20261113/manual-plan.json --confirm-database hdg_mirror
```

Lệnh replay chỉ áp range đã đăng ký. Event mới ngoài plan phải qua `extend` với extension đầy đủ đã kiểm chứng như trước; không sửa checkpoint. Không init lại baseline/epoch, không rollback dữ liệu Mirror chỉ để dừng watch. `run.py init/replay` vẫn chỉ dành cho database test.

**Thử E2E bằng tay:** dùng helper local `test-results/continuous-20261113/fixture.ps1` phía dưới. Một fixture riêng đã được để lại để quan sát; kiểm tra `manual-fixture.json` trước khi dùng lại mã đó. Với vòng mới:

```powershell
$fixture = 'W' + [guid]::NewGuid().ToString('N').Substring(0,16).ToUpper()
& ./test-results/continuous-20261113/fixture.ps1 -Action Insert -Fixture $fixture
# Chờ watch APPLIED và Queue rỗng, rồi kiểm tra Mirror/C360/API/BI.
& ./test-results/continuous-20261113/fixture.ps1 -Action Update -Fixture $fixture
# Chỉ DELETE sau khi đã quan sát xong; sự kiện DELETE/tombstone cũng phải được tự xử lý.
& ./test-results/continuous-20261113/fixture.ps1 -Action Delete -Fixture $fixture
```

```powershell
docker exec hdg-c360-postgres psql -U hdg_user -d hdg_mirror -c "SELECT id,ma_cd,ho_ten FROM public.mirror_hdg_nhankhau WHERE id='$fixture'; SELECT ma_so_bhxh,ho_ten,dq_flags FROM c360.c360_thong_tin_ca_nhan WHERE ma_so_bhxh='$fixture'; SELECT count(*) FROM c360.c360_change_queue;"
$key = ((Get-Content api/.env | Where-Object { $_ -match '^C360_API_KEY=' }) -split '=',2)[1].Trim().Trim('"').Trim("'")
Invoke-RestMethod "http://127.0.0.1:8090/api/v1/ca-nhan/$fixture" -Headers @{'X-API-Key'=$key}
docker exec hdg-c360-postgres psql -U hdg_user -d hdg_mirror -c "SELECT observed_at,endpoint,http_status,result_status FROM c360_api.c360_api_audit ORDER BY observed_at DESC LIMIT 5; SELECT * FROM hdg_bi.reconcile_counts;"
```

Dashboard: <http://localhost:8089/superset/dashboard/hdg-c360-local/>. INSERT tăng một công dân, UPDATE chỉ đổi thông tin, DELETE trả API 404 và đưa số lượng về trước thử. Không in `$key` hoặc lưu key vào bằng chứng.

<details>
<summary>Nội dung helper fixture.ps1 để tự tạo lại trong test-results</summary>

```powershell
param(
    [Parameter(Mandatory=$true)][ValidateSet('Insert','Update','Delete')][string]$Action,
    [Parameter(Mandatory=$true)][ValidatePattern('^W[0-9A-F]{16}$')][string]$Fixture
)
# Chỉ ghi fixture có ID = MA_CD và GHI_CHU riêng; không nhận SQL tùy ý.
$statement = switch ($Action) {
    'Insert' { "INSERT INTO VSS.VSS_HO_GD_NHANKHAU(ID,MA_CD,HO_TEN,GHI_CHU,TYPEID,TRANG_THAI_DUYET,GIOI_TINH,CREATED_DATE,MODIFY_DATE) VALUES('$Fixture','$Fixture','  watch   pipeline  ','HDG-WATCH:$Fixture','CD',2,'1',SYSDATE,SYSDATE);" }
    'Update' { "UPDATE VSS.VSS_HO_GD_NHANKHAU SET HO_TEN='  watch   pipeline updated  ',MODIFY_DATE=SYSDATE WHERE ID='$Fixture' AND MA_CD='$Fixture' AND GHI_CHU='HDG-WATCH:$Fixture';" }
    'Delete' { "DELETE FROM VSS.VSS_HO_GD_NHANKHAU WHERE ID='$Fixture' AND MA_CD='$Fixture' AND GHI_CHU='HDG-WATCH:$Fixture';" }
}
$collision = if ($Action -eq 'Insert') { "SELECT count(*) INTO n FROM VSS.VSS_HO_GD_NHANKHAU WHERE ID='$Fixture' OR MA_CD='$Fixture'; IF n<>0 THEN raise_application_error(-20002,'Fixture already exists'); END IF;" } else { '' }
$query = @"
whenever sqlerror exit failure rollback
set heading off feedback off pages 0
alter session set container=FREEPDB1;
declare n number;
begin
 IF sys_context('USERENV','DB_NAME')<>'FREEPDB1' OR sys_context('USERENV','CON_NAME')<>'FREEPDB1' THEN raise_application_error(-20003,'Wrong local database'); END IF;
 $collision
 $statement
 IF SQL%ROWCOUNT<>1 THEN raise_application_error(-20001,'Expected exactly one fixture row'); END IF;
 commit;
end;
/
exit
"@
$query | docker exec -i hdg-c360-oracle sqlplus -L -s / as sysdba
if ($LASTEXITCODE -ne 0) { throw 'Fixture transaction failed; do not retry blindly.' }
Write-Output "$Action committed for fixture $Fixture"
```

</details>

Kiểm thử offline: `& $py -m unittest discover -s tests/cdc_sink -p test_unit.py`. Bộ clone riêng: `& $py tests/cdc_sink/test_cutover.py --continuous-clone` (tạo database mới, không reset test cũ; dùng PostgreSQL thật, records/transport lỗi được chèn có kiểm soát). Không chạy trong lúc có writer nguồn vì kiểm tra cuối yêu cầu live không thay đổi trong suốt bài clone.

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

Chưa hoàn tất: Sink streaming liên tục; chính sách khoảng năm sinh và chuyển đổi điện thoại còn UNKNOWN; worker dừng khi danh mục đổi để tránh dùng Map cũ. BI có dashboard local ba nhóm KPI; xem mục dưới. REST API local và Airflow kiểm tra chỉ đọc đã có; các job ghi dữ liệu chưa đưa vào lịch. Watcher local chưa tự khởi động cùng Windows.


## Đối soát và Dashboard BI

```powershell
& $py -B jobs/c360_reconcile.py --output test-results/c360-bi/reconcile.json
```

Job dùng lại logic chuẩn hóa Full/NRT, so thiếu/thừa công dân, dòng đại diện, dữ liệu chuẩn hóa và DQ_FLAGS trong snapshot chỉ đọc. Queue chưa rỗng sẽ báo BLOCKED; không tự sửa dữ liệu. Dòng bị loại nghiệp vụ và dòng dư do nhiều hộ được thống kê riêng.

Dashboard Superset: [HDG C360](http://localhost:8089/superset/dashboard/hdg-c360-local/). Gồm nhân khẩu/định danh, chất lượng dữ liệu và lịch sử đồng bộ. Tài khoản DB của BI chỉ đọc view tổng hợp, không đọc thông tin cá nhân. Tuổi chưa xác định khi ngày sinh chuẩn chưa đủ căn cứ; không thay quy tắc UNKNOWN để làm báo cáo.

Cách cài trên môi trường mới, câu SQL kiểm tra và giới hạn báo cáo: [C360.md](jobs/C360.md#đối-soát-và-bi). Lệnh kiểm tra runtime: `jobs/c360_bi.py verify --output test-results/c360-bi/hdg-superset-http.json`. Guard chặn container DataHub hoặc URL khác localhost:8089. Kiểm tra bố cục trực quan vẫn cần trình duyệt.


## REST API C360 local

Java 21 + Spring Boot 3.5.16 + Spring JDBC, build bằng Maven. URL `http://127.0.0.1:8090`. API đọc C360, xác thực SHA-256 API Key/scopes và ghi Audit theo lô; không dùng JPA, không thay logic Full Load/CDC/C360.

| Endpoint | Vai trò | Scope |
|---|---|---|
| `GET /api/v1/ca-nhan/{ma}` | Mã hiện hành → mã cũ → định danh; dừng tầng đầu tiên có kết quả. Không có: 404, trùng: 409. | `c360:lookup` |
| `POST /api/v1/ca-nhan/tra-cuu-lo` | JSON `{"codes":["<ma>"]}`, 1–1000 mã; giữ thứ tự/mã lặp và trạng thái từng mã. | `c360:batch` |
| `GET /api/v1/ca-nhan/tim-kiem?hoTen=...&ngaySinh=...` | Tên chính xác sau chuẩn hóa không dấu viết hoa; ngày YYYY/YYYYMM/YYYYMMDD hợp lịch, khớp phần cùng biết với ngày rút gọn. | `c360:search` |
| `GET /api/v1/ca-nhan/thay-doi?limit=100&cursor=...` | Danh sách bản ghi còn tồn tại, cursor HMAC theo SYNC_DATE/ID và giữ cùng mốc `until` qua các trang. | `c360:changes` |

Mọi API cần `X-API-Key`. Thiếu/sai key: 401; thiếu scope: 403; input sai: 400; DB lỗi: 503. Hash và chữ ký cursor được so bằng constant-time comparison. Mã là chuỗi, giữ 0 đầu, dấu `;` và ký tự URL encoded; query dùng bind parameter. Không trả raw/source JSON. Conflict trả tối đa 100 mã ứng viên cùng `truncated`; search mặc định 100, tối đa 1000. Key và cursor secret cũ được giữ để client không phải đổi cấu hình. Không tìm từ ngày sinh ứng viên UNKNOWN, không tự ghép thông tin giữa các hộ.

### Đọc source và khởi chạy

| File trong `api/` | Trách nhiệm |
|---|---|
| `pom.xml`, `src/main/resources/application.properties` | Dependency, cổng, cấu hình kết nối và tắt tự chạy SQL |
| `Application.java` | Khởi động, guard endpoint/quyền DB và transaction READ ONLY |
| `C360Controller.java` | Bốn endpoint, JSON và HTTP status |
| `C360Service.java` | Validation, thứ tự tra cứu, batch/search/cursor |
| `C360Repository.java` | JdbcTemplate, allowlist tên cột và tham số SQL |
| `ApiSecurity.java` | API Key/scopes, route an toàn và Audit metadata |
| `CursorCodec.java` | Encode/decode HMAC tương thích cursor cũ |
| `AuditLog.java` | Outbox trên đĩa, retry, replay idempotent |
| `src/test/java/vn/hdg/c360/ApiTest.java`, `AuditTest.java` | Test HTTP/validation/auth/scope và độ bền Audit |

Các class chính nằm trong `api/src/main/java/vn/hdg/c360/`.

```powershell
# Tại thư mục gốc project, dùng Java 21 và Maven 3.9.x:
mvn -f api/pom.xml verify
# Chỉ chạy khi 8090 chưa có API khác lắng nghe:
java -Xms64m -Xmx256m -jar api/target/c360-api-1.0.0.jar
```

`api/.env` hoặc biến môi trường cung cấp `C360_API_DB_USER`, `C360_API_DB_PASSWORD`, `C360_API_CURSOR_SECRET`, `C360_API_PG_PORT=15432`, `C360_API_DATABASE=hdg_mirror`. Client dùng `C360_API_KEY` để gửi header; server chỉ kiểm hash trong `c360_api.api_keys`, không tự dùng key trong env thay cho request. File `.env` được Git ignore và không đóng gói vào JAR.

Tái sử dụng schema/role hiện có từ `sql/postgres/08_c360_api.sql`; ứng dụng không tự chạy DDL/migration hoặc tạo key mới. Runtime không có quyền INSERT/UPDATE/DELETE C360. Không dùng owner/superuser để chạy API. Schema thiếu hoặc cấu hình sai phải sửa theo quy trình quản trị riêng, không chạy lại DDL để reset dữ liệu.

Ví dụ gọi (key lấy từ môi trường local, dữ liệu minh họa đã ẩn):

```powershell
$base = 'http://127.0.0.1:8090/api/v1/ca-nhan'
$headers = @{ 'X-API-Key' = $env:C360_API_KEY }
$ma = [uri]::EscapeDataString('<MA_DA_AN>')
Invoke-RestMethod "$base/$ma" -Headers $headers
Invoke-RestMethod "$base/tra-cuu-lo" -Method Post -Headers $headers -ContentType 'application/json' -Body '{"codes":["<MA_DA_AN>"]}'
$name = [uri]::EscapeDataString('<HO_TEN_DA_AN>')
Invoke-RestMethod "$base/tim-kiem?hoTen=$name&ngaySinh=1995" -Headers $headers
Invoke-RestMethod "$base/thay-doi?limit=100" -Headers $headers
```

Response lookup rút gọn, đã ẩn dữ liệu: `{"status":"FOUND","data":{"maSoBhxh":"<redacted>","hoTen":"<redacted>"}}`.

### Audit và giới hạn

Mỗi response được giữ trong buffer cho tới khi Audit ghi file, fsync và atomic rename thành công. Sau đó worker riêng ghi PostgreSQL theo lô tối đa 100, mỗi khoảng 100ms. `request_id` PK + `ON CONFLICT DO NOTHING`; chỉ xóa file sau commit. Process dừng giữa chừng sẽ replay pending khi khởi động lại. Lock ngăn hai JVM dùng chung outbox; fingerprint DB/user ngăn replay sang sai đích. Không xóa pending hoặc đổi DB để bỏ qua guard.

Outbox mặc định: `test-results/c360-api/java-outbox/` (Git ignore). **Đây là state Audit đang dùng, không xóa khi dọn kết quả test/log.** Có thể cấu hình bằng `C360_API_AUDIT_DIR`; phải giữ thư mục khi restart. Audit chỉ chứa UUID, thời điểm, route mẫu, HTTP status, thời gian xử lý, kết quả và client_id; không có PII, key/hash, query/body/IP. DB Audit lỗi: giữ file và báo `AUDIT_UNAVAILABLE`; ghi đĩa lỗi/đầy 2000 pending: trả 503 `AUDIT_BACKPRESSURE`, không âm thầm trả kết quả thành công. Log chỉ ghi loại lỗi. Đây là bảo đảm qua process restart đã kiểm thử, chưa phải bảo đảm mất điện/hỏng ổ đĩa hoặc audit production không mất log.

Đọc Audit bằng owner/DBA local; runtime chỉ được INSERT và SELECT cột `request_id` cần kiểm tra conflict:

```sql
SELECT observed_at,endpoint,http_status,elapsed_ms,result_status,client_id
FROM c360_api.c360_api_audit ORDER BY observed_at DESC LIMIT 20;
SELECT client_id,scopes,enabled FROM c360_api.api_keys;
```

39/39 test Java PASS. Contract HTTP trên DB test độc lập: 49/49 ca khớp, gồm 404/409, authentication/scope, URL encoded, dấu `;`, batch/search và cursor dùng chéo; process restart và Audit replay PASS. HTTP local và Audit đã được kiểm chứng, fingerprint C360 không đổi. Bằng chứng ở `test-results/c360-api/java-migration-b10713d9/` và `java-handoff-20261113/`.

`tests/test_api_contract.py` chỉ là công cụ so sánh migration, không cần để chạy Java. Công cụ cần frozen Flask artifact được cung cấp qua `--reference-dir`, Python có Flask/psycopg và PostgreSQL test 15433. Khi thiếu artifact phải báo BLOCKED; không tự coi là PASS. Test tạo DB fixture mới, không reset DB giữ bằng chứng. Test Java thường chạy độc lập bằng Maven.

**Giới hạn giữ nguyên:** `/thay-doi` vẫn là `BEST_EFFORT_CURRENT_ROWS`, cửa sổ 120s, chưa có commit fence/watermark hoặc tombstone. Không dùng như CDC feed đầy đủ. Tìm kiếm ngày sinh trên live có thể rỗng vì `NGAY_SINH_CHUAN` đang NULL/UNKNOWN; không dùng `NGAY_SINH_UNG_VIEN` để ép có kết quả. API chưa đăng ký tự khởi động cùng Windows. Không mở rộng Airflow hoặc E2E trong phần API.


## Airflow HDG local

Apache Airflow **2.10.5 / Python 3.11** tại <http://127.0.0.1:8091>, giao diện DAG truyền thống, user `hdg_admin`. Đăng nhập bằng mật khẩu HDG hiện có trong file local `airflow/.env.auth.json`; không đưa file này vào Git.

Compose chạy webserver và scheduler trong một container, LocalExecutor với một task đồng thời. Metadata PostgreSQL 16 dùng database `hdg_airflow2`, container `hdg-c360-airflow2-metadata`, volume `hdg-c360-airflow_metadata2`; logs dùng `hdg-c360-airflow_logs2`. Metadata/log Airflow 3 cũ được giữ nguyên ở volume riêng, không downgrade schema hoặc chuyển lịch sử DagRuns. Image chỉ thêm psycopg và init admin/pool, không dùng DataHub/Superset.

DAG `hdg_pipeline_checks`:

```text
operation_guard → runtime_preflight → reconcile_c360 → read_only_complete
```

- `schedule=None`, paused khi tạo, `catchup=False`, một DagRun và một task chạy cùng lúc. Tài liệu chưa chốt lịch đối soát/Full Load nên không tự gán cron.
- `operation_guard`: chỉ nhận `operation=verify`. Full Load/publish/cutover/replay/C360 full/incremental đều fail-fast trước I/O; không có task ghi dữ liệu ẩn phía sau nhánh verify.
- `runtime_preflight`: kiểm instance, role chỉ SELECT và GET trạng thái Debezium Connector/task. RUNNING không phải bằng chứng SCN/offset barrier.
- `reconcile_c360`: chạy nguyên CLI `jobs/c360_reconcile.py`, không chép lại lọc/dedup/normalize/DQ. PASS_WITH_UNKNOWN là khớp kỹ thuật nhưng còn quy tắc UNKNOWN. FAIL dừng ngay; BLOCKED/transient retry tối đa hai lần, cách một phút.
- `read_only_complete`: báo hoàn tất kiểm tra đọc, không phải pipeline E2E hoặc publish đã thành công.
- Timeout mỗi task 10 phút, subprocess đối soát 9 phút, DagRun 35 phút. Pool `hdg_read_only` có một slot. Pool chỉ kiểm soát task Airflow, không thay khóa DB hoặc fence của writer bên ngoài.

### Cấu hình và chạy

`airflow/.env` giữ metadata password, mật khẩu `hdg_airflow_reader`, Fernet và webserver secret. File auth được mount readonly. `init_local.py` chỉ tạo admin/pool nếu chưa có trên metadata 2, không reset mật khẩu khi restart. Secret không được gửi vào build context, argv hoặc log. Không cấp thêm quyền Mirror.

Role `hdg_airflow_reader` chỉ SELECT bốn Mirror và các bảng C360 cần đối soát, mặc định transaction READ ONLY. Không mount `jobs/.env`, API Key, BI credentials hoặc Docker socket vào Airflow. Cấu hình hiện tại dùng TCP proxy `127.0.0.1:15432` bên trong namespace container → `host.docker.internal:15432`, để giữ nguyên guard localhost trong code C360. Proxy không publish cổng host và không thay network/container PostgreSQL hiện có.

```powershell
docker compose --env-file airflow/.env -f airflow/compose.yaml up -d --build
docker exec hdg-c360-airflow airflow pools set hdg_read_only 1 'HDG serial read-only checks'
docker exec hdg-c360-airflow python /opt/hdg/tests/test_airflow.py -v
# Chỉ nhánh verify, không ghi dữ liệu nghiệp vụ:
docker exec hdg-c360-airflow airflow dags test hdg_pipeline_checks
```

Xem DAG list/Grid/Graph tại UI. `dags test` chỉ ghi metadata/log Airflow. DAG mặc định paused, `schedule=None`. Với manual run qua scheduler, unpause tạm → trigger với `operation=verify` → chờ đủ bốn task success → pause lại. Unpause không tạo cron hay mở đường ghi dữ liệu.

Dừng/start lại Airflow 2 (không xóa volume):

```powershell
docker compose --env-file airflow/.env -f airflow/compose.yaml stop mirror-read-proxy airflow metadata2
docker compose --env-file airflow/.env -f airflow/compose.yaml up -d --no-build metadata2 airflow mirror-read-proxy
```

### Rollback về Airflow 3

Bản sao cấu hình, secret và metadata dump ở `E:/Projects/hdg-c360-pipeline/backups/airflow3-before-airflow2-20261113-110301`; thông tin/hash nằm trong `test-results/airflow2/backup.json` được Git ignore. Volume cũ `hdg-c360-airflow_metadata` và `hdg-c360-airflow_logs`, image `hdg-c360-airflow:3.1.8` được giữ lại. Warning orphan của metadata cũ là dự kiến; không chạy `--remove-orphans` hoặc `down -v`.

```powershell
$b = (Get-Content test-results/airflow2/backup.json -Raw | ConvertFrom-Json).backup
docker compose --env-file airflow/.env -f airflow/compose.yaml stop mirror-read-proxy airflow metadata2
foreach ($name in @('compose.yaml','Dockerfile','.dockerignore','hdg_pipeline.py')) {
    Copy-Item -LiteralPath (Join-Path $b $name) -Destination (Join-Path 'airflow' $name)
}
Copy-Item -LiteralPath (Join-Path $b 'test_airflow.py') -Destination tests/test_airflow.py
docker compose --env-file airflow/.env -f airflow/compose.yaml up -d --no-build metadata airflow mirror-read-proxy
```

Rollback dùng nguyên metadata 3 cũ, không restore dump đè lên metadata 2. Không thay Fernet/auth secret. Hai bộ metadata vẫn độc lập và được giữ lại. Muốn quay lại Airflow 2 sau rollback cần phục hồi bộ cấu hình 2 đã review; không chỉ đổi tag image trên schema metadata khác phiên bản.

### Những job chưa bật qua Airflow

| Job hiện có | Phụ thuộc và điều kiện trước khi đưa vào DAG ghi dữ liệu |
|---|---|
| `generator/seed.py`, `changes.py` | Thủ công; không tự sinh DML theo lịch. |
| `jobs/run_spark.ps1` → `full_load.py --stage-only` | Spark/JDBC Windows; Stage theo run mới, đối soát đủ 4 bảng tại cùng SCN trước mọi publish. Chưa có adapter Spark Linux/Windows executor trong Airflow. |
| `jobs/reconcile.py` | Oracle ↔ Stage/Mirror tại cùng snapshot; job có SHARE lock/audit tùy mode, cần cửa sổ vận hành tránh chặn Sink. Chưa chạy trong lượt này. |
| `cdc/sink/cutover.py` prepare/apply/extend/replay | Cần plan, fence/barrier còn hiệu lực, backup/rollback và checkpoint handoff đã kiểm chứng; không tự lấy offset, reset epoch hoặc retry apply mù. Chưa cấp quyền/entrypoint ghi cho Airflow. |
| `jobs/c360_full_load.py full` | Khóa Control và phối hợp NRT/cutover theo guard hiện có. Chỉ bật sau diễn tập quyền ghi/retry/timeout/cancellation trên DB test. |
| `jobs/c360_full_load.py worker` | Tài liệu yêu cầu mỗi phút; watcher hiện có được giữ nguyên. Chỉ chuyển sang cron mỗi phút sau khi dừng watcher có kiểm soát và kiểm chứng single-worker/ACK/rollback; không chạy thêm worker song song. |

Nhánh ghi dữ liệu hiện BLOCKED trong Airflow. Đây là giới hạn triển khai, không phải các entrypoint Full Load/CDC/C360 cũ bị lỗi. Trước khi bật lịch thật cần chốt tần suất, runner Spark, cơ chế loại trừ writer ngoài Airflow và diễn tập rerun/cancellation với guard hiện có. Chỉ unpause DAG kiểm tra không bật được những job ghi này. Không coi `max_active_runs=1` là bảo vệ checkpoint giữa Airflow và lệnh chạy tay.

Bằng chứng Airflow nằm trong `test-results/airflow/`; kết quả import/dependency/guard và task chỉ đọc được báo riêng, không dùng làm PASS cho Full Load/cutover/replay. Chưa thực hiện E2E toàn trình.

Kiểm thử Airflow 2.10.5: 5/5 test DAG PASS; manual run `manual__hdg_airflow2_324629f9` qua scheduler đạt 4/4 task success, reconciliation `PASS_WITH_UNKNOWN`, 0 sai khác. DAG đã paused lại, lịch vẫn None. DAG list/Grid/Graph và đăng nhập HDG trả HTTP 200. Bằng chứng: `test-results/airflow2/run.json`, `reconcile.json`, `services-after.json`, `summary.json`. Các job ghi pipeline chưa được bật qua Airflow. Tài liệu image/Docker chính thức: [Airflow 2.10.5](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html).