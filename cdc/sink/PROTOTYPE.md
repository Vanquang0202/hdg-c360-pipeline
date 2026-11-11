# CDC Sink prototype — PostgreSQL test độc lập

## 1. Trạng thái và phạm vi

Đã triển khai prototype và chạy kiểm thử trên database **hdg_cdc_sink_test**, PostgreSQL riêng tại **127.0.0.1:55433**, container **hdg-cdc-sink-test**. Chỉ dùng hai schema `mirror_test` và `sink_meta`.

Không đọc/ghi dữ liệu ở hdg_mirror.public, staging hoặc hdg_recovery_verified; không sửa Oracle, Debezium Source, Kafka topics hoặc offsets. Không có lệnh restore/cutover live trong code. Image postgres:16 đã có được tái sử dụng với --pull never; không tải image mới.

Bằng chứng người vận hành cung cấp cho DBHC: key-null, c/u/d có đủ 12 trường, một tombstone, các commit SCN 2457218/2457221/2457224, ALL COLUMN LOGGING ALWAYS ở bốn bảng. Đây là căn cứ chọn nhánh xử lý key-null/full-image để làm prototype, **không** chứng minh NHANKHAU có đủ 111 trường hoặc cutover barrier đã an toàn.

Bài test/demo dùng **synthetic events độc lập**, không phải bản export bốn record thật. Demo dùng các số SCN giống chuỗi được báo để minh họa; dữ liệu, schema fixture, transaction IDs và topic UUID đều do code tạo. Baseline demo là rỗng được khai báo tường minh trong manifest, không lấy từ Recovery Candidate và không suy từ offset 0.

## 2. File và thành phần

| File | Nội dung |
|---|---|
| contract.py | Đọc lại jobs/schema_contract.py và DDL Oracle, hash hợp đồng; không sửa chúng |
| protocol.py | Giải mã Debezium JSON schema/payload, kiểm đủ cột và chuyển kiểu exact |
| storage.py | Guard endpoint/database, tạo đủ 132 cột ở Mirror test, metadata và SQL NULL-safe |
| manifest.py | Baseline thử nghiệm, các range offset hữu hạn và giao dịch hoàn chỉnh |
| engine.py | Áp một giao dịch, inbox/ledger/checkpoint cùng transaction, quarantine khi lỗi |
| kafka_reader.py | Đọc hữu hạn các range đã khai báo; pin topic ID; không commit/reset Kafka offsets |
| fixtures.py | Sinh schema+payload synthetic cho bốn bảng, có replay/tombstone |
| run.py | CLI generate/init/replay/reset-test, retry lỗi DB có giới hạn |
| compose.test.yaml | PostgreSQL test độc lập, giới hạn RAM 256 MiB, shared_buffers 32 MiB |
| Setup-Test.ps1 | Chuẩn bị riêng container test, giữ credential/volume và tùy chọn chạy test |
| requirements.txt | Dependency cố định trong venv riêng |
| tests/test_unit.py | Unit test parser, manifest, endpoint, retry và Kafka reader giả lập |
| tests/test_integration.py | Integration trên PostgreSQL thật, chỉ reset các bảng fixture test |
| tests/crash_worker.py | Tiến trình con dùng để kiểm tra crash thật trước/sau commit |

Lựa chọn Python/Psycopg ở đây dành cho CDC prototype. Job Full Load Spark/JDBC không thay đổi.

## 3. Mapping và kiểm tra schema

Tái sử dụng `jobs/schema_contract.py`: NHANKHAU 111 cột, DANTOC 6, QUOC_TICH 3, DBHC 12. DDL test được tạo từ `column["postgres"]`, không trích chữ số từ tên VARCHAR2.

| Oracle/Connect | Xử lý prototype |
|---|---|
| VARCHAR2/CHAR BYTE | varchar(n) + CHECK octet_length; kiểm trước cả ký tự và byte UTF-8 |
| VARCHAR2 CHAR/NVARCHAR2 | varchar(n), giữ Unicode và khoảng trắng, không trim |
| NUMBER không precision | VariableScaleDecimal, giải base64 two's-complement và scale bằng Decimal exact |
| NUMBER(p,s) | Logical Decimal hoặc kiểu nguyên theo wire schema; kiểm precision/scale trước khi gán numeric(p,s) |
| DATE | Logical Timestamp/MicroTimestamp theo schema, giữ ngày và giờ đến giây, không đổi thành date |
| TIMESTAMP(6) | MicroTimestamp; NanoTimestamp chỉ nhận khi chia đúng cho 1000, không bỏ phần lẻ |
| NULL | Giữ NULL; thiếu trường khác NULL; trường NOT NULL của Oracle không được NULL |

JSON number thập phân được parse bằng Decimal, không qua float. Fixed Decimal hỗ trợ base64 mặc định của JsonConverter và decimal.format=NUMERIC khi là số JSON exact. Integer wire phải đúng số nguyên, không nhận bool, float hoặc chuỗi số để che schema sai.

Kiểm đủ tên/thứ tự 132 cột, họ kiểu dữ liệu, logical type và đầy đủ before/after theo op. Các giá trị rỗng không phù hợp Oracle, NUL, placeholder unavailable, chuỗi vượt độ dài, NaN/Infinity, scale làm tròn hoặc thời gian sai precision đều bị từ chối. DATE có phần giây lẻ bị chặn. Không có CAST varchar(n) để cắt chuỗi trong đường DML.

PostgreSQL target preflight kiểm format_type, nullability, PK đúng hai danh mục và không có PK giả ở hai bảng còn lại, byte constraints đã validate; từ chối trigger ứng dụng/RLS policy/FK ngoài thiết kế của Mirror test. Những kiểm tra này không phải migration live.

Giới hạn kiểu dữ liệu: datetime Python chỉ hỗ trợ năm 1..9999; BC/ngoài khoảng này bị chặn rõ ràng, chưa hỗ trợ. Chưa hỗ trợ float/double, ZonedTimestamp, converter boolean tùy biến hoặc schema evolution. Connect schema không luôn chứa độ dài VARCHAR2/Oracle metadata; code kiểm hợp đồng đã pin và giá trị, chưa phát hiện được mọi DDL nguồn nếu Source không phát metadata tương ứng. [Debezium Oracle converters](https://github.com/debezium/debezium/blob/3.4/debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/OracleValueConverters.java)

## 4. Cách áp dữ liệu và giữ dòng trùng

Hai bảng PK: key trong Kafka phải có ID đúng ảnh trước/ảnh sau. INSERT mới trùng PK không tự upsert ghi đè; UPDATE/DELETE cần before khớp. Hỗ trợ UPDATE thay ID và chuỗi d/tombstone/c khi đổi key. Replay phải có bằng chứng từ ledger, không suy bằng việc thấy cùng after.

Hai bảng không PK: yêu cầu key-null trong phiên bản prototype này. UPDATE/DELETE match **toàn bộ before-image** bằng IS NOT DISTINCT FROM, chuỗi dùng COLLATE C. Khóa/chọn một tuple bằng ctid trong chính transaction, rồi cập nhật/xóa đúng một dòng. ctid không được lưu làm khóa; source.row_id cũng không tham gia khóa dòng hoặc dedup.

Nếu có hai dòng giống hệt, một DELETE chỉ xóa một bản sao. Hai INSERT mới cùng payload vẫn tạo hai dòng. ID NULL, trùng hay thay đổi không bị gán ý nghĩa UNIQUE. Không tìm được before thì rollback/chặn, không fallback ID hoặc cập nhật tất cả dòng khớp.

Mirror test giữ nguyên TYPEID/trạng thái duyệt, không chuẩn hóa C360. Prototype chưa triển khai trigger Queue/C360 hoặc ghi dữ liệu downstream.

Tombstone chỉ là Kafka value=null; nó được lưu inbox và tiến checkpoint cùng giao dịch, không xóa lần hai. JSON có payload=null nhưng value không null là message lỗi, không tự coi là tombstone.

## 5. Atomicity, ordering, dedup và lỗi

Manifest phải nêu rõ:
- mode synthetic-test hoặc captured-fixture-test; không có mode live.
- epoch, contract hash và source incarnation của fixture.
- schema và các dòng baseline của **đủ bốn bảng**, kể cả bảng rỗng.
- topic prefix, topic ID, partition 0, start/end offsets tường minh của năm stream: bốn bảng và transaction topic.
- danh sách frame theo thứ tự đã xác minh; mỗi frame nêu txid, event_count và toàn bộ tọa độ record.

Một frame chứa đủ DML của một giao dịch và END có event_count/data_collection counts khớp; BEGIN là tùy chọn. Kiểm total_order/data_collection_order và commit SCN không đi lùi với sự kiện mới. Frame chỉ có tombstone được phép nếu khai báo event_count=0 và không giả tạo giao dịch.

Không có thuật toán tự suy thứ tự toàn cục giữa các topic. Đây là **finite transaction replay theo manifest đã biết**, chưa là dịch vụ streaming tự ráp giao dịch live. Kafka reader chỉ là đường nhập range hữu hạn để replay; không tạo topic, không subscribe vào group production, không commit hoặc reset offsets.

Trong **một transaction PostgreSQL**: lấy advisory lock + khóa epoch; kiểm next_frame/next_offset; giải mã; áp Mirror test; ghi ledger sự kiện/giao dịch; lưu raw inbox; ghi frame và checkpoint. Commit xong mới trả kết quả. Offset cập nhật sớm trong transaction cũng rollback nếu decode/DML lỗi. Không có distributed exactly-once claim với Kafka.

Có hai lớp replay:
- Cùng frame/offset: hash raw record phải khớp; trả REPLAY_FRAME, không áp lại.
- Cùng redo ở offset mới: dùng source identity thử nghiệm và fingerprint; chỉ nhận lại khi cả giao dịch/ledger khớp. Trả REPLAY_SOURCE, không áp DML.

Source identity hiện gồm incarnation, bảng, txId, scn, commit_scn, rs_id, ssn, redo_thread, op và key discriminator. Hai sự kiện có identity giống nhau trong cùng giao dịch bị chặn vì mơ hồ; identity cũ nhưng payload đổi cũng bị chặn. **Tổ hợp này mới được thử với fixture, chưa chứng minh duy nhất/ổn định trên Oracle/Debezium thật.** Không dùng nó để cho phép cutover live.

Lỗi transient DB được retry tối đa 2 lần sau lần đầu, reconnect và phát lại nguyên frame. Lỗi dữ liệu/schema/before/order rollback toàn transaction, rồi lưu raw records + reason vào sink_meta.quarantine bằng transaction riêng và chuyển epoch BLOCKED. Không skip/DLQ rồi tiếp tục checkpoint. Không có lệnh tự bỏ qua quarantine để chạy tiếp; với test có thể reset fixture tường minh. [Psycopg transactions](https://www.psycopg.org/psycopg3/docs/basic/transactions.html)

Dữ liệu quarantine chứa raw event, cần giữ kín nếu dùng captured fixture. Log CLI chỉ in frame/outcome/target, không in password hoặc toàn bộ nhân khẩu.

## 6. Môi trường và lệnh PowerShell

Chạy từ project root. Các bước dưới chỉ dùng container/database test cố định, không cần biến môi trường của Oracle/Mirror live.

```powershell
& C:\Python311\python.exe -m venv cdc/sink/.venv
& .\cdc\sink\.venv\Scripts\python.exe -m pip install --index-url https://pypi.org/simple -r cdc/sink/requirements.txt

# Chuẩn bị container test và chạy 2 bộ test:
.\cdc\sink\Setup-Test.ps1 -RunTests
```

Nếu Windows có proxy/chứng chỉ nội bộ và pip báo certificate verify failed, dùng kho chứng chỉ hệ thống; không tắt xác minh TLS:

```powershell
& .\cdc\sink\.venv\Scripts\python.exe -m pip install --use-feature=truststore --index-url https://pypi.org/simple -r cdc/sink/requirements.txt
```

Setup-Test.ps1 tạo mật khẩu ngẫu nhiên một lần vào `.env.test` được Git ignore, chỉ công bố 127.0.0.1:55433. Code đọc CDC_TEST_PASSWORD hoặc file này; giữ mật khẩu khớp volume test. Không in docker compose config đầy đủ/inspect env vì có thể lộ credential.

Script từ chối container trùng tên thuộc project khác. Nó không tải image, không nối network CDC cũ, không xóa volume và chỉ chạy tests khi có -RunTests. Có thể chạy riêng:

```powershell
& .\cdc\sink\.venv\Scripts\python.exe -B -m unittest discover -s cdc/sink/tests -p test_unit.py -v
& .\cdc\sink\.venv\Scripts\python.exe -B -m unittest discover -s cdc/sink/tests -p test_integration.py -v
```

Integration tự tạo schemas test nếu chưa có và reset **chỉ 11 bảng fixture của prototype** trước mỗi test. Không chạy khi đang cần giữ dữ liệu thử nghiệm quan trọng. Sau test negative có thể thấy epoch BLOCKED; đây là kết quả mong đợi của test, không phải bằng chứng suite FAIL.

### Demo CLI sau khi đã chạy integration

```powershell
& .\cdc\sink\.venv\Scripts\python.exe -B cdc/sink/run.py generate-fixture

# Dòng sau xóa fixture/ledger trong DB TEST để demo lại, không đụng schema khác:
& .\cdc\sink\.venv\Scripts\python.exe -B cdc/sink/run.py reset-test --confirm-test-db hdg_cdc_sink_test

& .\cdc\sink\.venv\Scripts\python.exe -B cdc/sink/run.py init --manifest cdc/sink/fixtures/generated/manifest.json
& .\cdc\sink\.venv\Scripts\python.exe -B cdc/sink/run.py replay --manifest cdc/sink/fixtures/generated/manifest.json --input cdc/sink/fixtures/generated/records.jsonl

# Chạy lại: phải chỉ REPLAY_FRAME, không áp dữ liệu lần hai:
& .\cdc\sink\.venv\Scripts\python.exe -B cdc/sink/run.py replay --manifest cdc/sink/fixtures/generated/manifest.json --input cdc/sink/fixtures/generated/records.jsonl
```

Generate không ghi đè fixture đã có; khi chạy lần nữa chọn --out một thư mục mới hoặc dùng lại các file cũ. init không ghi đè epoch/baseline đã có. Fresh DB có thể init trực tiếp; reset-test chỉ dùng khi schemas/guard đã tồn tại và muốn bỏ fixture cũ. Không có tham số đổi host/port/database/schema đích sang hdg_mirror.

Tùy chọn đọc Kafka, **chưa chạy trên broker thật**:

```powershell
# Chỉ dùng manifest captured fixture đã biết baseline/ranges/UUID và đủ transaction metadata.
# Manifest do demo tạo có UUID giả, không dùng để đọc topic thật.
& .\cdc\sink\.venv\Scripts\python.exe -B cdc/sink/run.py replay --manifest <MANIFEST_TEST_DA_XAC_MINH.json> --kafka-bootstrap localhost:29092
```

Reader pin topic IDs, kiểm beginning/end còn đủ, manual assign đúng start đã khai báo, chỉ đọc tới end; không latest/auto reset. Prefix chỉ được hdg_sink_test hoặc hdg_observe. Nếu dùng hdg_observe, kết quả vẫn chỉ ghi database test; baseline phải được cung cấp hợp lệ riêng, không tự nối từ Recovery Candidate. Tất cả ranges phải được manifest bao phủ, có giới hạn tối đa 100.000 record, file/đợt raw khoảng 64 MiB và mỗi JSON message 8 MiB. Dữ liệu vượt giới hạn bị từ chối, không chia nhỏ source transaction để commit nửa chừng.

Để xem dữ liệu test:

```powershell
docker exec hdg-cdc-sink-test psql -U sink_test_owner -d hdg_cdc_sink_test -c "SELECT * FROM sink_meta.frames ORDER BY frame_no;"
docker exec hdg-cdc-sink-test psql -U sink_test_owner -d hdg_cdc_sink_test -c "SELECT topic,next_offset FROM sink_meta.checkpoints ORDER BY topic;"
```

Dừng riêng dịch vụ test để tiết kiệm RAM; volume/fixture được giữ:

```powershell
docker compose --env-file cdc/sink/.env.test -f cdc/sink/compose.test.yaml stop postgres
```

Không dùng down -v hoặc reset-test để xử lý vấn đề ở Mirror live.

## 7. Kết quả kiểm tra thực tế ngày 11/11/2026

Môi trường đã đọc từ endpoint test:
- Python 3.11.9.
- psycopg/psycopg-binary 3.3.8.
- confluent-kafka 2.16.1.
- PostgreSQL 16.14 (Debian 16.14-1.pgdg13+1), database hdg_cdc_sink_test, cổng host 55433.

| Bộ kiểm tra | Kết quả cuối | Phạm vi |
|---|---|---|
| Unit | **PASS — 17/17**, khoảng 0,13 giây | 132 cột, Decimal/byte/time/NULL, schema, manifest, endpoint guard, retry, Kafka reader mock, reset guard |
| PostgreSQL integration | **PASS — 24/24**, khoảng 73,08 giây | Bốn bảng thật trong DB test, PK/key change, no-PK duplicate/NULL/ID change, atomicity, ordering, quarantine |
| Crash trước commit | **PASS**, nằm trong integration | Tiến trình con os._exit(91), PostgreSQL rollback cả Mirror/inbox/ledger/checkpoint; restart áp đúng một lần |
| Crash sau commit | **PASS**, nằm trong integration | Tiến trình con os._exit(92), restart nhận REPLAY_FRAME, không tạo thêm dòng |
| CLI demo + replay lần hai | **PASS** | 3 APPLIED + 1 REPLAY_SOURCE ở lượt đầu, 4 REPLAY_FRAME ở lượt sau |
| Kafka broker thật / Oracle→Debezium E2E | **CHƯA CHẠY** | Không đọc/ghi Kafka thật, không phát DML Oracle hoặc đổi Source |
| Cutover / thay Mirror live | **CHƯA THỰC HIỆN** | Không có barrier, code không có chế độ ghi live |

Tổng cuối: **41 unit/integration tests PASS, 0 FAIL, 0 ERROR**. Các dòng crash/PASS phía trên là chi tiết của 24 integration, không cộng thêm vào tổng.

Sau CLI demo hai lượt, đã đọc lại từ DB test:
- Mirror DBHC: 0 dòng, đúng trạng thái sau DELETE.
- Inbox: 13 raw records, gồm metadata transaction/tombstone/replay.
- Ledger: 3 sự kiện DML nguồn duy nhất.
- next_frame: 4.
- next_offset DBHC: 5, chỉ thuộc topic synthetic hdg_sink_test (c/u/d/tombstone và replay c). Không liên quan end=4 của topic thật hdg_observe.

Trong quá trình chuẩn bị có lỗi TLS pip và import TopicCollection; đã xử lý bằng truststore Windows và import đúng API, rồi chạy lại các test/CLI nêu trên. Không bỏ qua kiểm tra TLS hoặc test lỗi.

## 8. Điều kiện nghiệm thu prototype và giới hạn còn mở

Prototype đạt khi:
1. Đích được khóa vào database/schema test; thử trỏ hdg_mirror bị từ chối trước SQL.
2. Đủ 132 cột và kiểu exact; vượt kiểu/precision/thiếu field phải lỗi, không sửa dữ liệu âm thầm.
3. Một source event mới tác động đúng một dòng/bản sao; replay không tác động thêm; tombstone không xóa hai lần.
4. Mirror test, inbox, dedup và checkpoint commit/rollback nguyên khối, có bằng chứng crash/restart.
5. Before/schema/order sai bị quarantine, không tiến checkpoint qua lỗi.
6. Không mặc định baseline từ offset 0/latest, không tự reset topic/group/Source.

Các điều kiện prototype trên đã qua bộ test nêu trên, trong phạm vi synthetic fixture và PostgreSQL test độc lập.

**Chưa đủ để bật live:** source-event identity thật qua Debezium restart; full before/after của NHANKHAU và các dạng DML khác; assembler streaming nhiều topic/giao dịch lớn; frontier toàn cục; barrier giữa SCN và offsets; restore giữ FK/trigger/Queue; vận hành retention/lag và bảo mật production. Không có Kafka DLQ producer, quarantine nằm ở PostgreSQL và cần quy trình xử lý riêng. Kafka network adapter mới được unit test bằng mock, chưa có bằng chứng kết nối broker thật.

Nguồn test không chứa toàn bộ trạng thái Recovery Candidate tại SCN 2438031. Prototype không thay thế bước thiết kế cutover trong [CDC_SINK_CUTOVER_DESIGN.md](../CDC_SINK_CUTOVER_DESIGN.md). Muốn nâng cấp tiếp cần thu raw schema/key/value/transaction metadata của cả bốn bảng, chứng minh baseline và replay nguồn thực tế trên test, rồi mới xem xét một kế hoạch live riêng.

Sau khi lấy bằng chứng cuối, đã dừng riêng hdg-cdc-sink-test để tiết kiệm RAM; volume test và fixture được giữ. Các container Oracle, PostgreSQL live, Kafka, Connect và Superset đang chạy vẫn được giữ nguyên vòng đời. Chạy Setup-Test.ps1 để mở lại endpoint test.
