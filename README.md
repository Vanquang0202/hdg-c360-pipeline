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

Topic UUID Windows/Linux đã được kiểm chứng và ghim đầy đủ 128 bit; chính sách khoảng năm sinh và chuyển đổi điện thoại còn UNKNOWN; worker dừng khi danh mục đổi để tránh dùng Map cũ. BI có dashboard local ba nhóm KPI; xem mục dưới. REST API local và Airflow kiểm tra chỉ đọc đã có; các job ghi dữ liệu chưa đưa vào lịch. Watcher local chưa tự khởi động cùng Windows.


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
| `GET /api/v1/ca-nhan/thay-doi?limit=100&cursor=...` | Journal C360 đã commit: UPSERT/DELETE, cursor HMAC theo event ID, trang có high-watermark cố định. | `c360:changes` |

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

### Change feed C360 `/thay-doi`

`sql/postgres/09_c360_change_feed.sql` tạo `c360.change_feed_control`, `change_events` và hai trigger trên C360. Không đổi bốn bảng Mirror hoặc quy tắc chuẩn hóa. Migration chạy **một lần**: khóa C360, tạo feed, bootstrap mỗi công dân hiện có bằng một UPSERT rồi commit. Không chạy lại migration để reset feed. Backup trước khi cài nằm trong `test-results/change-feed/before-live.json`.

Trigger BEFORE STATEMENT khóa control trước row-lock, giữ tới commit; counter là UPDATE transactional, không dùng sequence ID để đoán thứ tự commit. Trigger AFTER ROW ghi payload API và DELETE tombstone cùng transaction C360. Rollback/retry không để lại event; UPDATE chỉ đổi `run_id`/`sync_date` hoặc không đổi payload công khai không phát thêm UPSERT. Đổi khóa phát DELETE mã cũ rồi UPSERT mã mới. TRUNCATE C360 bị chặn để không mất tombstone. Full/Incremental C360 dùng nguyên logic publish hiện có.

Contract feed v2 trên cùng endpoint:

- `items` giữ các trường công khai của ba API còn lại, thêm `operation`, `eventId`, `transactionId`, `syncDate`; `id` hiện là event ID, không phải row ID C360.
- DELETE chỉ có `maSoBhxh`, `maSoBhxhCu` cùng metadata event, không mang họ tên/CCCD.
- `until` hiện là số high-watermark, không còn timestamp. `nextCursor` luôn có, kể cả hết trang; `hasMore` cho biết còn trang trong cửa sổ. Khi cursor đã drain, request tiếp theo tự lấy high-watermark mới.
- Cursor HMAC gắn `feedId`, client và phiên bản 2. Cursor cũ có chữ ký hợp lệ trả **409 CURSOR_VERSION_UNSUPPORTED**; giả mạo trả 400. Cursor quá retention hoặc đổi feed trả **410**, không âm thầm nhảy offset.
- Không truyền cursor: đọc từ event 1, gồm bootstrap nếu còn retained. `cursor=now`: chỉ lấy cursor hiện tại, không trả lịch sử. Đây không tự động là snapshot dữ liệu khách hàng.
- Để resync sau 410: lấy cursor `now`, xuất lại toàn bộ C360 bằng quy trình snapshot được phép, rồi replay từ cursor đã giữ; không ghép cursor legacy vào feed mới. Dữ liệu đã xóa trước khi feed được cài không có tombstone hồi tố; client cũ cần thay baseline.
- Trang có thể chia một transaction; dùng `transactionId` nếu client cần gom nhóm. Client phải lưu cursor cùng dữ liệu local và dedup theo `(feedId,eventId)`. Server không ghi nhận ACK của client và không tuyên bố exactly-once phía client.

Retention có kiểm soát: owner mới được gọi `SELECT c360.prune_change_events(30);`, mặc định 30 ngày, tối thiểu một ngày. Chỉ xóa prefix đã hết hạn, cập nhật `retained_after` trong cùng transaction/mutex. Chưa bật purge tự động; cần chốt chính sách lưu giữ và backup trước khi chạy. Không DELETE/TRUNCATE journal bằng tay, không sửa counter/feed ID.

Ví dụ curl.exe chỉ đọc metadata (không in key hoặc PII):

```powershell
$key = ((Get-Content api/.env | Where-Object { $_ -match '^C360_API_KEY=' }) -split '=',2)[1].Trim().Trim('"').Trim("'")
$config = 'header = "X-API-Key: ' + $key.Replace('\','\\').Replace('"','\"') + '"'
$config | curl.exe --config - --silent --show-error --get "http://127.0.0.1:8090/api/v1/ca-nhan/thay-doi" --data-urlencode "cursor=now" --data-urlencode "limit=1"
# Lưu page/cursor trong bộ nhớ, không dump dữ liệu công dân lên terminal:
$page = $config | curl.exe --config - --silent --show-error --get "http://127.0.0.1:8090/api/v1/ca-nhan/thay-doi" --data-urlencode "limit=1000" | ConvertFrom-Json
$cursor = $page.nextCursor
$config | curl.exe --config - --silent --show-error --get "http://127.0.0.1:8090/api/v1/ca-nhan/thay-doi" --data-urlencode "cursor=$cursor" --data-urlencode "limit=1" --output test-results/feed-next-page.json
```

Build ứng viên không ghi đè JAR live: `mvn -f api/pom.xml -DskipTests -Dhdg.buildDirectory=E:/Projects/hdg-c360-pipeline/test-results/change-feed/java-build package`. JAR đang chạy nằm tại thư mục này; JVM dùng cùng secret/outbox Audit của project. JAR cũ giữ trong backup trước feed. Không chạy hai JVM dùng chung port/outbox.

Kiểm thử: `mvn -f api/pom.xml test`; `cdc/sink/.venv/Scripts/python.exe tests/test_change_feed.py --run-clone` tạo DB test mới, không reset clone cũ. DB journal: 8/8 PASS (IUD, tombstone, concurrent late commit, REPEATABLE READ retry, hard crash, rollback, retention). HTTP clone: 6/6 PASS gồm pagination/restart/410/Audit. Ba API không đổi: 37 contract cases khớp Flask reference. Contract v2 thay thế các ca cursor legacy, không còn so cursor dùng chéo Flask/Java. Bằng chứng `test-results/change-feed/` và `test-results/c360-api/java-migration-7aa0da09/`.

Giới hạn nghiệp vụ còn nguyên: ngày sinh/điện thoại UNKNOWN không tự suy đoán; feed phát đúng payload C360 hiện hành. API chưa tự start cùng Windows. Bootstrap live đã đọc đủ 48.747 khóa qua 49 trang. E2E Oracle IUD mới đã PASS qua continuous Sink, Watcher, API và BI: feed thêm UPSERT 48.748, UPSERT 48.749, DELETE tombstone 48.750; cursor limit=1/retry/restart Java không bỏ sót trong bài thử. Fixture đã dọn khỏi bảng hiện tại; lịch sử feed vẫn được giữ. Bằng chứng `test-results/topic-identity/e2e-result.json`.


## Airflow HDG local

Apache Airflow **2.10.5 / Python 3.11** tại <http://127.0.0.1:8091>, giao diện DAG truyền thống, user `hdg_admin`. Đăng nhập bằng mật khẩu HDG hiện có trong file local `airflow/.env.auth.json`; không đưa file này vào Git.

Compose chạy webserver và scheduler trong một container, LocalExecutor với một task đồng thời. Metadata PostgreSQL 16 dùng database `hdg_airflow2`, container `hdg-c360-airflow2-metadata`, volume `hdg-c360-airflow_metadata2`; logs dùng `hdg-c360-airflow_logs2`. Metadata/log Airflow 3 cũ được giữ nguyên ở volume riêng, không downgrade schema hoặc chuyển lịch sử DagRuns. Image chỉ thêm psycopg và init admin/pool, không dùng DataHub/Superset.

DAG `hdg_pipeline_checks`:

```text
operation_guard → runtime_preflight → await_mirror → await_c360
  → reconcile_c360 → verify_feed → serving_checks → read_only_complete
```

- `schedule=None`, paused khi tạo, `catchup=False`, một DagRun và một task chạy cùng lúc. Tài liệu chưa chốt lịch đối soát/Full Load nên không tự gán cron.
- `operation_guard`: chỉ nhận `operation=verify`. Full Load/publish/cutover/replay/C360 full/incremental đều fail-fast trước I/O; không có task ghi dữ liệu ẩn phía sau nhánh verify.
- `runtime_preflight`: kiểm role chỉ đọc, Connector/task và Kafka; chụp vector offset hữu hạn qua XCom. Pin topic ID/partition theo epoch hiện tại; không bootstrap từ latest và không coi đây là Oracle SCN barrier.
- `await_mirror`: sensor `reschedule` chờ checkpoint đạt vector đã chụp; kiểm schema, ownership/ACL, sealed content, epoch/handoff, lock Sink và quarantine. Chỉ quan sát, không replay hay sửa checkpoint.
- `await_c360`: sensor chờ Queue rỗng, Watcher giữ lock và catalog digest đúng. Không khởi động vòng lặp worker trong PythonOperator.
- `verify_feed`: kiểm control/retention và dải event không có gap.
- `serving_checks`: gọi Java feed bằng API Key/scope, rồi đăng nhập đúng Superset HDG 8089 và query 10 chart-data thật. Không dùng health HTTP 200 thay cho kết quả dữ liệu.
- `reconcile_c360`: chạy nguyên CLI `jobs/c360_reconcile.py`, không chép lại lọc/dedup/normalize/DQ. PASS_WITH_UNKNOWN là khớp kỹ thuật nhưng còn quy tắc UNKNOWN. FAIL dừng ngay; BLOCKED/transient retry tối đa hai lần, cách một phút.
- `read_only_complete`: chỉ chạy sau mọi bước bắt buộc; kết luận đạt Kafka goal đã quan sát, không phải Oracle quiescent barrier hoặc nghiệm thu production.
- Sensor poke 10 giây, timeout 180 giây, không soft-fail. Task mặc định retry 2 lần cách một phút; service dừng, lag hết timeout, schema/identity/quarantine sai hoặc reconciliation lỗi sẽ chặn downstream. Timeout mỗi task 10 phút, subprocess đối soát 9 phút, DagRun 35 phút. Pool `hdg_read_only` có một slot. Pool chỉ kiểm soát task Airflow, không thay khóa DB hoặc fence của writer bên ngoài.

### Cấu hình và chạy

`airflow/.env` giữ metadata password, mật khẩu `hdg_airflow_reader`, Fernet và webserver secret. File auth được mount readonly. `init_local.py` chỉ tạo admin/pool nếu chưa có trên metadata 2, không reset mật khẩu khi restart. Secret không được gửi vào build context, argv hoặc log. Không cấp thêm quyền Mirror.

Role `hdg_airflow_reader` chỉ SELECT bốn Mirror và các bảng C360 cần đối soát, mặc định transaction READ ONLY. Không mount `jobs/.env`, `api/.env`, `bi/.env`, `.env.test` hoặc Docker socket. Credential HTTP cần thiết được đặt trong `airflow/.env` với `HDG_C360_API_KEY`, `HDG_SUPERSET_USERNAME`, `HDG_SUPERSET_PASSWORD`; không log giá trị. Kafka đọc qua external network `hdg-cdc-net`. Cấu hình hiện tại dùng TCP proxy `127.0.0.1:15432` bên trong namespace container → `host.docker.internal:15432`, để giữ nguyên guard localhost trong code C360. Proxy không publish cổng host và không thay network/container PostgreSQL hiện có.

```powershell
docker compose --env-file airflow/.env -f airflow/compose.yaml up -d --build
docker exec hdg-c360-airflow airflow pools set hdg_read_only 1 'HDG serial read-only checks'
docker exec hdg-c360-airflow python /opt/hdg/tests/test_airflow.py -v
# Chỉ nhánh verify, không ghi dữ liệu nghiệp vụ:
docker exec hdg-c360-airflow airflow dags test hdg_pipeline_checks
```

Xem DAG list/Grid/Graph tại UI. `dags test` chỉ ghi metadata/log Airflow. DAG mặc định paused, `schedule=None`. Với manual run qua scheduler, unpause tạm → trigger với `operation=verify` → chờ đủ tám task success → pause lại. Unpause không tạo cron hay mở đường ghi dữ liệu.

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

### DAG ETL `hdg_full_load_pipeline`

DAG riêng tại <http://127.0.0.1:8091/dags/hdg_full_load_pipeline/grid> gọi job thật, không đổi tám task của `hdg_pipeline_checks`:

`preflight → spark_stage → reconcile_stage → publish_mirror → c360_full → reconcile_final`

- `preflight`: xác nhận phê duyệt clone, PDB Oracle test, database comment/endpoint/schema; chặn writer khác bằng advisory lock và kiểm CDC/Watcher/Control. Không nhận `hdg_mirror` làm đích.
- `spark_stage`: gọi `full_load.py --stage-only --source-scn ... --batch-size ...`; lấy đúng run_id từ log hoàn thành và số đọc/ghi từ bốn audit của run.
- `reconcile_stage`: gọi `reconcile.py --target stage --source snapshot --record-stage-audit`; phải khớp đủ 132 cột, số lần lặp và SHA256 trước publish.
- `publish_mirror`: gọi nguyên `hdg_load.publish_full_load(run_id)` trong transaction. Chỉ bootstrap clone rỗng; không tự thay Mirror khác baseline hoặc gọi cutover live.
- `c360_full`: gọi `c360_full_load.full_load(connection_clone)`, giữ nguyên lọc/khử trùng/chuẩn hóa/DQ và staging/publish. Không nới CLI C360 live.
- `reconcile_final`: Spark so Oracle snapshot với Mirror clone rồi gọi `c360_reconcile.reconcile(connection_clone)`; sai khác làm task fail.

Lịch kỹ thuật đề xuất: `0 2 * * *`, timezone UTC+7; đổi bằng `HDG_FULL_LOAD_CRON` trong môi trường Airflow. Tài liệu chưa chốt lịch vận hành. DAG mặc định paused, `catchup=False`, một run/task đồng thời. `allow_clone_write=False` mặc định; scheduled run còn cần `allow_scheduled_clone=true` trong connection. Unpause một mình không cấp quyền ghi. Hiện chỉ hỗ trợ bootstrap clone mới, không dùng cron này để refresh Mirror đang có CDC.

Task có timeout 35 phút, retry một lần sau hai phút; preflight không retry. Runner giới hạn mỗi Spark subprocess 30 phút và dừng cây tiến trình khi timeout. Log riêng từng lần chạy ở `test-results/airflow-etl/<run-hash>/` trên runner; Airflow chỉ nhận JSON tổng hợp, không nhận credential hoặc raw row. Số bản ghi publish lấy từ audit Stage đúng run, không dùng tổng dữ liệu live làm số đã xử lý. Kết quả dùng lại có `cached=true`, `processed_this_attempt=0`. Nếu Spark/C360 đã bắt đầu nhưng chưa có kết quả chắc chắn, hoặc publish đã commit nhưng mất phản hồi, retry fail-closed để kiểm tra thủ công; không reset trạng thái run hoặc xóa Stage.

**Runner:** image Airflow hiện không có Java/PySpark. DAG dùng OpenSSH client có sẵn để gọi hữu hạn `airflow/etl_runner.py` trên máy runner có Spark. Trên Windows, runner nạp đủ biến môi trường clone từ `.env.etl` rồi gọi lại launcher đã có. Các lệnh dưới đây minh họa lệnh do runner sinh; không chạy trực tiếp trong shell đang dùng `jobs/.env` live:

```powershell
& E:/Projects/hdg-c360-pipeline/jobs/run_spark.ps1 -Job full_load -Python C:/Python311/python.exe -JobArguments @('--stage-only','--source-scn','<SCN_PDB_TEST>','--batch-size','1000')
& E:/Projects/hdg-c360-pipeline/jobs/run_spark.ps1 -Job reconcile -Python C:/Python311/python.exe -JobArguments @('--run-id','<run_id>','--target','stage','--source','snapshot','--record-stage-audit')
```

Launcher dùng Spark 4.0.1, Java 21, `local[2]`, driver heap 1 GiB, `--driver-class-path` cùng `spark.executor.extraClassPath` cho hai JDBC JAR. Linux runner gọi `spark-submit` với cùng classpath/local[2] và job Python, không chạy Spark trong webserver Airflow. Runner cần PySpark 4.0.1, python-dotenv, oracledb (chỉ SELECT identity), psycopg 3.3.8; mọi copy Oracle→Stage vẫn qua Spark JDBC.

**Thiết lập trước khi chạy thật trên clone:**

1. Chuẩn bị Oracle PDB test độc lập có tên `HDG_ETL_<8 ký tự HEX hoa>`, schema VSS và bốn bảng đúng DDL. Không trỏ vào FREEPDB1 hiện tại. Chốt SCN của PDB test, không lấy SCN từ Mirror live.
2. Tạo PostgreSQL test mới tên `hdg_airflow_<8 hex thường>_test`; cài `01_create_mirror_tables.sql`, `02_publish_full_load.sql`, `05_c360.sql` vào đúng database mới. Đặt database comment `HDG_AIRFLOW_ETL_CLONE_V1` bằng quy trình quản trị trên clone. Không copy epoch/checkpoint hoặc trigger live vào clone bootstrap này.
3. Trên runner tạo `airflow/.env.etl` (Git ignore), có đủ `PG_JDBC_URL`, `PG_USER`, `PG_PASSWORD`, `ORACLE_JDBC_URL`, `ORACLE_USER=VSS`, `ORACLE_PASSWORD`, `ORACLE_TEST_PDB`. URL PostgreSQL chỉ localhost:15432/15433 và đúng tên clone; URL Oracle kết thúc bằng tên PDB test đã khai báo. Không dùng file `jobs/.env` live làm cấu hình ETL này.
4. SSH key đã được đăng ký cho user Windows `admin`; public host key đọc trực tiếp từ Windows qua helper UAC, đối chiếu key qua mạng rồi pin known_hosts. Private key/known_hosts local ở `airflow/.env.etl_ssh/` (Git ignore). Runtime dùng external volume `hdg-c360-airflow_etl_ssh` mount read-only tại `/opt/airflow/etl-ssh/`, owner 50000, directory 0700/key 0600 để không phụ thuộc NTFS permissions. Không tự trust key lấy từ mạng. Volume key đã được import trước khi recreate riêng Airflow/proxy; metadata/logs2 giữ nguyên. Không xóa volume này khi dọn test. Nếu chuyển máy, import key vào volume với đúng quyền rồi xác minh lại host key trước khi start.
5. Trong Admin → Connections tạo **Generic** connection `hdg_etl_runner`: Host `host.docker.internal` (runner Windows local), Login là tài khoản runner, Port 22. Extra gồm `identity_file`, `known_hosts_file`, `python_path`, `runner_path`, `allow_scheduled_clone=false`. Ví dụ path: `/opt/airflow/etl-ssh/id_ed25519`, `/opt/airflow/etl-ssh/known_hosts`, `C:/Python311/python.exe`, `E:/Projects/hdg-c360-pipeline/airflow/etl_runner.py`. Password để trống; không đưa key hoặc password vào Extra/README.
6. Sau khi xác minh runner/clone, trigger thủ công với `allow_clone_write=true`, đúng `target_db`, `source_scn`, `batch_size`. Unpause tạm để scheduler xử lý rồi pause lại. Không gửi shell command hoặc secret trong DAG conf.

**E2E thực tế 14/11 đã PASS:** run `manual__etl_real_bcdaddc7` đủ 6/6 task SUCCESS, đều ở attempt 1, không dùng kết quả cache. Airflow 2.10.5 gọi Windows Spark 4.0.1 qua connection `hdg_etl_runner`, SSH publickey-only và StrictHostKeyChecking. Nguồn là PDB `HDG_ETL_94E22BB2` (cùng Oracle container, tách khỏi FREEPDB1), SCN `7207171`; đích là `hdg_airflow_94e22bb2_test`, không có sink epoch hoặc CDC.

| Task | Thời gian Airflow | Kết quả thật |
|---|---:|---|
| preflight | 2.58s | Clone/PDB/driver/guard READY |
| spark_stage | 104.71s | Oracle→Stage: 1.000 nhân khẩu, 3 dân tộc, 2 quốc tịch, 8 địa bàn; tổng 1.013 dòng |
| reconcile_stage | 78.38s | Đủ bốn bảng/132 cột, content_equal và SHA256 PASS |
| publish_mirror | 7.03s | Publisher COMMITTED, chép 1.013 dòng vào Mirror clone |
| c360_full | 5.22s | 975 dòng hợp lệ → 972 công dân, SUCCESS_WITH_UNKNOWN |
| reconcile_final | 88.10s | Oracle snapshot↔Mirror đủ bốn bảng; Mirror↔C360 PASS_WITH_UNKNOWN, mọi sai khác bằng 0 |

Full Load run `4bf022ee-6747-4f62-b473-70f1a71d5555`; C360 run `d5077c36-0f48-4886-95f7-bd21cb82188c`. Thời gian trên gồm overhead task/SSH; log Spark còn ghi riêng thời gian từng bảng. Nguồn được seed mới bằng generator hiện có, không lấy tổng dòng live làm số đã xử lý. PDB test ban đầu RESTRICTED do common user cần tablespace USERS; chỉ bổ sung tablespace còn thiếu trong PDB test và reopen PDB test, không cấp RESTRICTED SESSION hay sửa FREEPDB1.

Regression 20/20 PASS sau run; kiểm tra cả sáu task log có SUCCESS/exit 0. Bằng chứng `test-results/airflow-etl/20261114/summary.json`, `dag-result.json`, log từng task và `test-results/airflow-etl/b42536a3ab8f288d27f657fa/` (Git ignore). Private `.env.etl` hiện trỏ clone đã PASS; không rerun để ghi đè clone này. Lượt tiếp theo cần clone rỗng mới và SCN/config được kiểm chứng lại. DAG đã paused lại, không bật lịch ghi tự động.

Live vẫn 50.000 Mirror / 48.747 C360, Queue/quarantine 0; hash bốn Mirror, epoch và năm checkpoint giữ nguyên. Watcher vẫn có lock. Sau đó đã khôi phục Debezium/Sink ngày 14/11: task từng FAILED ORA-01435 lúc 16:28:07 UTC+7 trong khi PDB test chưa đồng bộ common user C##HDG_CDC vì thiếu USERS tablespace. Vi phạm đã RESOLVED; đọc lại cùng query và SCN lỗi trên phiên LogMiner riêng PASS trước khi restart duy nhất task 0. Source tiếp tục từ offset cũ, snapshot SKIPPED, config không đổi; Sink đã qua guard và chạy IDLE bằng plan ACTIVE cũ. Không reset offsets/checkpoint/epoch, không replay bỏ qua SCN. Bằng chứng `test-results/cdc-recovery-20261114/summary.json`, `mining-probe.json`, `connect-redacted.log`. Không xác nhận đây là lỗi vendor đã có mã bug: Oracle không nêu user ID trong ORA-01435; kết luận metadata/common-user sync dựa trên timeline và PDB violation. PDB test cùng CDB không cô lập metadata LogMiner; với các lần thử DDL tạo PDB mới nên dùng Oracle CDB/container test riêng để tránh ảnh hưởng capture của live.

Cutover Mirror đang có CDC vẫn là thao tác riêng qua `cdc/sink/cutover.py`, cần fence/barrier mới, backup/rollback và checkpoint handoff. DAG này không reset connector/offset/epoch, không start/stop continuous Sink hoặc Watcher và chưa được phép điều phối cutover live. Seed và NRT vô hạn cũng không đưa vào PythonOperator.

Bằng chứng Airflow 2 hiện tại nằm trong `test-results/topic-identity/airflow/`: manual run `manual__hdg_pipeline_bc899557` SUCCESS 8/8, log từng task có exit code 0, reconciliation PASS_WITH_UNKNOWN, API và 10 chart-data PASS. DAG đã paused lại. Không chạy Full Load/cutover trong bài thử này.

Regression DAG hiện tại: **11/11 PASS**, gồm dependency, guard ghi, service dừng, lag, identity mismatch, timeout và reconciliation sai. Toàn DAG kiểm tra đã chạy thủ công thành công; continuous Sink/Watcher vẫn là service bên ngoài DAG, không chạy vòng lặp vô hạn trong task.

### Kafka Topic UUID trên Windows và Linux

SDK Windows 2.16.1 đã trả UUID với hai nửa 64 bit bị thu về signed 32 bit rồi sign-extend. Đổi Base64 thông thường sang Base64 URL-safe không khôi phục được bit đã mất. `kafka_reader.py` đọc UUID đầy đủ bằng Kafka CLI Java trong container Kafka local trên Windows; Linux dùng metadata SDK. Hai cách được chuẩn hóa về Base64 URL-safe 22 ký tự, giữ đủ 128 bit. CLI phải trỏ cùng cluster ID; không có fallback chấp nhận UUID rút gọn.

Identity cũ trong checkpoint/manifest/inbox được giữ nguyên để không đổi wire hash và lịch sử replay. Bảng bổ sung `sink_meta.topic_identity_bindings` ghim cặp legacy/native theo epoch và cluster. Mỗi cặp phải có proof SHA256: lịch sử KRaft từ offset 0, đúng một lần tạo topic trước cutover, không xóa/tạo lại, cùng UUID hiện tại, toàn bộ prefix Kafka khớp inbox và fingerprint transaction/frame. Topic đổi dù chỉ bit cao vẫn bị chặn.

Diễn tập clone đạt 10/10 kiểm tra, gồm cài lỗi rollback nguyên tử, revoke fail-closed, sửa proof bị chặn và replay lịch sử không đổi dữ liệu/checkpoint/seal. Binding local được cài sau backup tại `backups/topic-identity-20261113-151618`; bằng chứng và plan/proof nằm trong `test-results/topic-identity/` (Git ignore). Không restore dump metadata đè lên checkpoint đang tiến.

Nếu cần rollback binding: dừng Sink bằng stop-file, chờ writer lock được nhả, rồi chạy lệnh bên dưới. Lệnh chỉ revoke binding và khiến writer/preflight fail-closed; không tua lại dữ liệu hoặc offsets. Giữ nguyên code đọc UUID đầy đủ khi rollback. Chỉ re-enable sau khi proof gốc và toàn bộ prefix hiện tại được xác minh lại.

```powershell
cdc/sink/.venv/Scripts/python.exe cdc/sink/cutover.py rollback-identities --plan test-results/topic-identity/plan.json --confirm-database hdg_mirror --identity-proof test-results/topic-identity/proof.json
# Re-enable bằng proof gốc sau khi nguyên nhân đã được kiểm tra; không sửa checkpoint/seal:
cdc/sink/.venv/Scripts/python.exe cdc/sink/cutover.py bind-identities --plan test-results/topic-identity/plan.json --confirm-database hdg_mirror --identity-proof test-results/topic-identity/proof.json
```

Windows hiện cần Docker CLI và container Kafka `hdg-cdc-kafka-1` để lấy UUID native. Migration tự động bị chặn nếu thiếu lịch sử KRaft gốc hoặc prefix Kafka đã hết retention; không suy nguồn gốc topic từ tên/offset trùng nhau.

Khi service dừng: kiểm tra đúng task đỏ/log tại <http://127.0.0.1:8091/dags/hdg_pipeline_checks/grid>, sửa nguyên nhân rồi start lại worker theo phần CDC/C360; không reset Connector/epoch/offset. Khi lag: xem checkpoint so goal trong XCom, để sensor retry hữu hạn; timeout vẫn fail. Khi reconciliation sai/quarantine: dừng nghiệm thu, giữ bằng chứng, không publish/cutover để che sai khác. Full Load/cutover vẫn là thao tác riêng có fence/backup; không tự chạy từ DAG này.

Kết quả E2E local sau sửa UUID: 3 DML Oracle mới (commit SCN 6112124 / 6113011 / 6114196), Kafka nhân khẩu [24,28), transaction [36,42); ba topic danh mục vẫn offset 16. PostgreSQL checkpoint bằng Kafka consumer committed offset; inbox/frame/transaction/event ledger đã đối chiếu fingerprint. Mirror trở lại 50.000, C360 48.747, Queue/quarantine 0; hash cả bốn Mirror bằng baseline trước fixture và đối soát C360 không sai khác. Đây là bằng chứng demo local, chưa chứng minh tải/độ trễ production hay exactly-once phía client. Việc lấy UUID bằng CLI Windows tăng chi phí mỗi batch; chưa benchmark tải lớn.

Khôi phục continuous Sink sau incident 14/11 dùng plan xuất nguyên trạng từ ACTIVE tại `test-results/cdc-recovery-20261114/live-plan.json`. Log/PID mới ở cùng thư mục; stop-file vẫn là `test-results/continuous-20261113/stop.request`, không đổi lệnh stop an toàn hiện có. Chỉ start lại sau khi Connector và tất cả task RUNNING ổn định, đủ redo từ persisted SCN và preflight schema/ACL/epoch/topic identity/checkpoint PASS. Không xem TCP/container RUNNING là task khỏe.
