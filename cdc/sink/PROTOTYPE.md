# CDC Sink prototype — PostgreSQL test độc lập

## 1. Trạng thái và phạm vi

Đã triển khai prototype và chạy kiểm thử trên database **hdg_cdc_sink_test**, PostgreSQL riêng tại **127.0.0.1:15433**, container **hdg-cdc-sink-test**. Chỉ dùng hai schema `mirror_test` và `sink_meta`.

Luồng prototype run.py không đọc/ghi dữ liệu ở hdg_mirror.public, staging hoặc hdg_recovery_verified; không sửa Oracle, Debezium Source, Kafka topics hoặc offsets. Luồng cutover riêng và kết quả clone được mô tả ở mục 11; chưa thực hiện cutover live. Image postgres:16 đã có được tái sử dụng với --pull never; không tải image mới.

Bằng chứng người vận hành cung cấp cho DBHC: key-null, c/u/d có đủ 12 trường, một tombstone, các commit SCN 2457218/2457221/2457224, ALL COLUMN LOGGING ALWAYS ở bốn bảng. Đây là căn cứ chọn nhánh xử lý key-null/full-image để làm prototype, **không** chứng minh NHANKHAU có đủ 111 trường hoặc cutover barrier đã an toàn.

Bài test/demo dùng **synthetic events độc lập**, không phải bản export bốn record thật. Demo dùng các số SCN giống chuỗi được báo để minh họa; dữ liệu, schema fixture, transaction IDs và topic UUID đều do code tạo. Baseline demo là rỗng được khai báo tường minh trong manifest, không lấy từ Recovery Candidate và không suy từ offset 0.

## 2. File và thành phần

| File | Nội dung |
|---|---|
| contract.py | Đọc lại jobs/schema_contract.py và DDL Oracle, hash hợp đồng; không sửa chúng |
| protocol.py | Giải mã Debezium JSON schema/payload, kiểm đủ cột và chuyển kiểu exact |
| storage.py | Guard endpoint/database, tạo đủ 132 cột ở Mirror test, metadata và SQL NULL-safe |
| engine.py | Kiểm manifest/baseline/range rồi áp một giao dịch, inbox/ledger/checkpoint cùng transaction, quarantine khi lỗi |
| kafka_reader.py | Đọc hữu hạn các range đã khai báo; pin topic ID; không commit/reset Kafka offsets |
| fixtures.py | Sinh schema+payload synthetic cho bốn bảng, có replay/tombstone |
| run.py | CLI generate/init/replay/reset-test, retry lỗi DB có giới hạn |
| compose.test.yaml | PostgreSQL test độc lập, giới hạn RAM 256 MiB, shared_buffers 32 MiB |
| Setup-Test.ps1 | Chuẩn bị riêng container test, giữ credential/volume và tùy chọn chạy test |
| requirements.txt | Dependency cố định trong venv riêng |
| ../../tests/cdc_sink/test_unit.py | Unit test parser, manifest, endpoint, retry và Kafka reader giả lập |
| ../../tests/cdc_sink/test_integration.py | Integration trên PostgreSQL thật, chỉ reset các bảng fixture test |
| ../../tests/cdc_sink/crash_worker.py | Tiến trình con dùng để kiểm tra crash thật trước/sau commit |

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

Setup-Test.ps1 tạo mật khẩu ngẫu nhiên một lần vào `.env.test` được Git ignore, chỉ công bố 127.0.0.1:15433. Code đọc CDC_TEST_PASSWORD hoặc file này; giữ mật khẩu khớp volume test. Không in docker compose config đầy đủ/inspect env vì có thể lộ credential.

Script từ chối container trùng tên thuộc project khác. Nó không tải image, không nối network CDC cũ, không xóa volume và chỉ chạy tests khi có -RunTests. Có thể chạy riêng:

```powershell
& .\cdc\sink\.venv\Scripts\python.exe -B -m unittest discover -s tests/cdc_sink -p test_unit.py -v
& .\cdc\sink\.venv\Scripts\python.exe -B -m unittest discover -s tests/cdc_sink -p test_integration.py -v
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

Tùy chọn đọc Kafka; kết quả E2E trên broker local xem mục 9:

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
| Kafka broker thật / Oracle→Debezium E2E | **Đã kiểm thử local, xem mục 9** | Bằng chứng E2E mới được ghi riêng, không cộng vào bộ synthetic bên trên |
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

**Chưa đủ để bật live:** source-event identity thật qua Debezium restart; độ phủ các giá trị đặc thù của NHANKHAU ngoài fixture đã kiểm thử; assembler streaming nhiều topic/giao dịch lớn; frontier toàn cục; barrier giữa SCN và offsets; restore giữ FK/trigger/Queue; vận hành retention/lag và bảo mật production. Không có Kafka DLQ producer, quarantine nằm ở PostgreSQL và cần quy trình xử lý riêng. Kafka network adapter đã có bằng chứng E2E local ở mục 9; chưa chứng minh khả năng vận hành streaming liên tục.

Nguồn test không chứa toàn bộ trạng thái Recovery Candidate tại SCN 2438031. Prototype không thay thế bước thiết kế cutover trong [CDC_SINK_CUTOVER_DESIGN.md](../../archive/cdc/CDC_SINK_CUTOVER_DESIGN.md). Muốn nâng cấp tiếp cần thu raw schema/key/value/transaction metadata của cả bốn bảng, chứng minh baseline và replay nguồn thực tế trên test, rồi mới xem xét một kế hoạch live riêng.

Sau khi lấy bằng chứng cuối, đã dừng riêng hdg-cdc-sink-test để tiết kiệm RAM; volume test và fixture được giữ. Các container Oracle, PostgreSQL live, Kafka, Connect và Superset đang chạy vẫn được giữ nguyên vòng đời. Chạy Setup-Test.ps1 để mở lại endpoint test.

## 9. E2E QUOC_TICH và NHANKHAU trên local

Đã kiểm thử Oracle → Debezium → Kafka → Sink → PostgreSQL test cho hai bảng còn lại. Dữ liệu thử gồm hai fixture mới có mã riêng; chỉ các fixture này được UPDATE/DELETE. Số dòng Oracle trước và sau là 2 quốc tịch, 50.000 nhân khẩu. Các database/checkpoint E2E DBHC và DANTOC được giữ nguyên, không chạy lại hai bài E2E đó.

Database mới: `hdg_cdc_remaining_1111_3dbfac5d_test`, cùng PostgreSQL test tại 127.0.0.1:55433. Script test cố định tiền tố database riêng và đổi tên đích chỉ trong tiến trình kiểm thử; không thay contract/runtime hoặc cho phép ghi Mirror live.

| Kiểm tra | Kết quả |
|---|---|
| QUOC_TICH c/u/d | PASS; ảnh dòng đủ 3 trường, key PK hợp lệ |
| NHANKHAU c/u/d | PASS; ảnh dòng đủ 111 trường, key null, match before-image NULL-safe |
| Transaction hai bảng | PASS; 6 DML trong 3 giao dịch, mỗi giao dịch có BEGIN/END và event_count=2 |
| Đối soát Mirror test | PASS; so toàn bộ cột sau mỗi frame, cuối cùng không còn fixture |
| Rollback trước commit | PASS; Mirror/inbox/ledger/checkpoint không ghi một phần, áp lại thành công |
| Replay sau reconnect và từ tiến trình mới | PASS; REPLAY_FRAME, không tăng ledger hoặc checkpoint |
| Tombstone/checkpoint | PASS; 2 tombstone, mỗi topic dữ liệu kết thúc ở offset 4, transaction ở 18 |
| Offline | PASS; 9 contract + 12 Stage + 17 Sink; SQL sinh ra khớp template |

Khoảng Kafka đọc là [0,4) của từng topic QUOC_TICH/NHANKHAU và [12,18) của transaction topic. Offset 0 ở đây chỉ là range sự kiện fixture đã quan sát, không phải offset cutover của toàn bộ dữ liệu.

Đã giữ INSERT chưa commit khi chốt SCN kiểm thử **2535617**. Session độc lập đọc AS OF SCN xác nhận hai fixture chưa hiện diện. DML SCN INSERT là **2535616** (QUOC_TICH) và **2535617** (NHANKHAU); cả hai commit tại **2535627**. UPDATE commit **2535642**, DELETE commit **2535649**. Sink nhận giao dịch commit sau snapshot dù DML đã bắt đầu trước/ở SCN đó. Đây là phép thử snapshot có phạm vi fixture, **không** phải chạy lại Full Load 50.000 dòng.

**Full Load → CDC live: BLOCKED.** Snapshot hiện có là SCN **2438031**, run_id `2ce4d328-976f-4a7f-9531-1fae4a22a34b`. Metadata xác nhận 50.013 dòng và audit đủ bốn bảng. Source offset có snapshot_scn 2331622; giá trị đó không phải barrier của run 2438031. Chưa có bằng chứng writer fence, giao dịch đang mở và frontier offset tại snapshot cũ, nên không suy offset từ SCN hoặc dùng range test để nối vào Mirror live. Việc cập nhật Mirror sau Full Load toàn bộ dữ liệu và phục hồi candidate vẫn chưa được kiểm chứng; không thay đổi Mirror, Stage hay Recovery Candidate.

Bằng chứng local được giữ trong `tests/cdc_sink/test-results/remaining-d5ac1927df7541b7a3c520410f649aad/`: result.json, manifest.json và records.jsonl. Thư mục test-results được Git ignore; không đưa raw CDC vào Git.

Lệnh kiểm thử (tạo fixture mới, chỉ dùng khi chủ động muốn phát DML local):

```powershell
.\cdc\sink\.venv\Scripts\python.exe -B tests/cdc_sink/local_remaining_e2e.py --execute-local
```

Lệnh kiểm tra replay của run đã lưu, không tạo lại fixture hoặc reset test:

```powershell
.\cdc\sink\.venv\Scripts\python.exe -B tests/cdc_sink/local_remaining_e2e.py --execute-local --resume tests/cdc_sink/test-results/remaining-d5ac1927df7541b7a3c520410f649aad
```

Trong quá trình chuẩn bị, kiểm identity CDB/PDB và fixture MA quá dài đã làm script dừng; đã sửa trước run thành công, các transaction lỗi không commit. Khi topic mới xuất hiện có lỗi leader metadata tạm thời; đợt kiểm thử được resume từ range đã lưu, không phát lại DML. Các kết quả lỗi chuẩn bị vẫn được giữ trong test-results, không tính là PASS.

Để tiếp tục cutover cần một cửa sổ dừng writer có kiểm chứng, xử lý hết giao dịch mở, chốt snapshot/frontier của tất cả topic rồi phục hồi và replay toàn bộ trên database độc lập. Chỉ sau đối soát đầy đủ mới xem xét kế hoạch thay Mirror live.

## 10. E2E Spark Full Load → baseline Stage → Kafka CDC → Sink

Đã chạy thực tế trên database mới **hdg_full_e2e_fa5d5786_test**, PostgreSQL test 127.0.0.1:55433. Run Spark mới **32061aec-b690-4f46-a1fe-6c21b0bad23e**, snapshot **SCN 2563582**, trạng thái VALIDATED. Không dùng run 2541215 hoặc Kafka end offset đơn lẻ làm barrier.

Barrier cho cửa sổ test được xác lập bằng các bằng chứng đồng thời:
- Cùng session Oracle giữ khóa EXCLUSIVE NOWAIT (mode 6) trên đủ bốn bảng, ngăn writer khác và loại trừ DML chưa commit trên chúng trước lúc chốt snapshot.
- JMX local đã có sẵn: Connected=true, NumberOfActiveTransactions=0, queue rỗng, ErrorCount=0 và AbandonedTransactionCount=0.
- OffsetScn của miner và Source offset đã flush vượt snapshot, vector Kafka ổn định qua hai lần đọc.
- Đã đọc toàn bộ range lịch sử còn giữ, xác nhận đủ BEGIN/END, số DML và collection order của 9 transaction trước snapshot. Topic ID được pin trong manifest.
- Sau fixture commit, lấy lại khóa NOWAIT, chốt SCN cuối và kiểm lại capture completeness. Không restart Connector hoặc reset offsets.

Cầu nối mới nằm trong `engine.initialize()`: manifest có `baseline.kind=spark-stage-v2`, tham chiếu run_id, SCN, số dòng và SHA256 của từng bảng thay vì nhúng 50.000 dòng JSON. Stage phải nằm trong cùng database test. Initializer khóa Stage/run, kiểm đủ schema/audit/digest, copy bằng guard kiểu v2 và so EXCEPT ALL hai chiều. Baseline, epoch và checkpoint commit cùng transaction; lỗi rollback toàn bộ. Timeout 180 giây chỉ áp dụng LOCAL cho bước baseline lớn, tự trở về sau transaction. CLI init vẫn giữ guard database cố định; runner test chọn riêng database mới, manifest không tự đổi endpoint.

| Bước | Bằng chứng cuối |
|---|---|
| Spark Full Load + reconcile Stage | PASS: NHANKHAU 50.000; DANTOC 3; QUOC_TICH 2; DBHC 8 |
| Nạp baseline từ Stage + Spark reconcile | PASS, đủ 132 cột; thử digest sai bị từ chối và rollback |
| Oracle → Debezium → Kafka | PASS: 12 DML và 4 tombstone, đủ ảnh dòng của bốn bảng |
| Transaction CDC | txid 020014004f030000, commit SCN 2565108; 12 DML trong cùng một transaction |
| Kafka range | Bốn data topic partition 0: [4,8); transaction partition 0: [18,20) |
| Sink apply/replay | APPLIED rồi REPLAY_FRAME sau reconnect; ledger 12 event/1 transaction, inbox 18 record, quarantine 0 |
| Reconcile sau CDC | PASS toàn dòng tại SCN 2565110, số dòng bốn bảng giữ nguyên như baseline |
| Kiểm thử offline | PASS: 20 Sink + 9 contract + 12 Stage = 41 test; SQL sinh ra khớp template |

Fixture dùng mã mới, không sửa dòng Oracle có sẵn. Mỗi bảng có INSERT → UPDATE → DELETE trong cùng transaction nên hiệu ứng cuối là net-zero; before-image và thứ tự vẫn phải đúng để Sink áp đủ 12 DML. Phép thử này chưa bao phủ nhiều transaction đồng thời hoặc streaming liên tục. Không thực hiện cutover live.

Trong lần chuẩn bị đầu có lỗi launcher Windows; đã chuyển runner sang spark-submit trực tiếp theo classpath của launcher hiện có. Lần nạp baseline đầu bị timeout, đã rollback. Sau khi điều chỉnh bước baseline lớn, đợt test được resume từ Stage/range đã có bằng chứng, không tạo lại DML hoặc reset database. Các lỗi và log gốc được giữ lại; không tính các lần lỗi là PASS.

Lệnh đã dùng:

```powershell
C:\Python311\python.exe -B tests/cdc_sink/full_pipeline_e2e.py --execute-local

# Tiếp tục cùng đợt sau khi baseline timeout đã được sửa:
C:\Python311\python.exe -B tests/cdc_sink/full_pipeline_e2e.py --execute-local --resume tests/cdc_sink/test-results/full-fa5d5786
```

Mỗi lần chạy mới tạo database test mới. Runner tái sử dụng credential CDC read-only từ môi trường/cdc/.env cho Spark và password PostgreSQL test từ cấu hình hiện có. Cần javac/Java có sẵn; helper JMX chỉ đọc agent local đã bật, được sinh trong test-results và không vào Git. Nếu không lấy được khóa hoặc không chứng minh được capture completeness thì dừng, không suy barrier.

Bằng chứng tại `tests/cdc_sink/test-results/full-fa5d5786/`: result.json, manifest.json, records.jsonl, log Spark và baseline_reconcile.json/final_reconcile.json. Giữ kín raw CDC; cả thư mục được Git ignore. Mirror live, run/Stage cũ, Recovery Candidate và database E2E PASS cũ được giữ nguyên.

## 11. Publisher/Cutover và checkpoint handoff — diễn tập clone

Entry point mới: `cdc/sink/cutover.py`. Luồng `run.py` vẫn mặc định chỉ vào database test cổng 15433; cutover là scope riêng, đích cố định 127.0.0.1:15432, yêu cầu xác nhận đúng tên DB. Không tự chuyển từ test sang live theo manifest.

Quy trình:
1. **prepare:** kiểm contract v2, FK/trigger/RLS, writer/control; khóa bảng, lưu nguyên dữ liệu cũ vào schema hdg_rollback_UUID và nạp shadow hdg_cutover_UUID từ Stage đã audit. Dữ liệu public chưa đổi.
2. **apply:** kiểm dữ liệu/owner/ACL/OID chưa đổi từ prepare; thay dữ liệu bốn bảng bằng DELETE/INSERT trong cùng transaction với epoch, checkpoint ban đầu và control. Không DROP/RENAME/TRUNCATE bảng public nên dependency/OID và quyền còn nguyên. Archive không bị xóa.
3. **replay:** tái sử dụng engine, trước mỗi frame khóa writer và kiểm hash dữ liệu/control/checkpoint. DML, inbox/ledger, checkpoint và hash phục vụ rollback commit cùng transaction.
4. **extend:** chỉ nối thêm frame/range sau frontier đã xử lý hết, giữ nguyên baseline và prefix cũ. Thay manifest hash atomically; không reset Kafka hoặc checkpoint.
5. **rollback:** chỉ khôi phục khi dữ liệu hiện tại khớp mốc do Sink ghi nhận, tránh xóa thay đổi ngoài luồng. Khôi phục từ archive và control cũ atomically; epoch bị BLOCKED, ledger/checkpoint/quarantine và archive được giữ lại.

**Kết quả cuối: PASS**, clone `hdg_cutover_clone_acec4095_test`. Các clone PASS trước đó cũng được giữ nguyên.
- Restore backup giữ ownership và ACL bảng/schema/cột như nguồn.
- Crash thật sau DELETE bảng đầu: rollback cả publish, không tạo checkpoint.
- Crash thật sau commit: retry nhận ALREADY_ACTIVE, không seed lại.
- Frame sai thứ tự: BLOCKED, không tiến checkpoint; rollback khôi phục dữ liệu cũ và giữ OID/ACL.
- Crash trong CDC trước commit và crash trong extend: dữ liệu/metadata rollback cùng nhau.
- Republish bằng epoch mới, append rồi replay hai transaction Kafka thực: PASS; 24 DML ledger, 36 inbox, 2 transaction, quarantine của epoch cuối bằng 0.
- Đối soát toàn bộ 132 cột hai chiều với shadow đã audit và Candidate: NHANKHAU 50.000, DANTOC 3, QUOC_TICH 2, DBHC 8.
- Checkpoint clone: data topics 12, transaction topic 22. Đây là range fixture diễn tập từ các E2E đã lưu, **không phải barrier cho Mirror live**.
- 46 test offline PASS: 25 Sink + 9 contract + 12 Stage. Schema SQL sinh ra vẫn khớp template.

Lệnh diễn tập đã chạy, mỗi lần tạo clone mới, không sửa Oracle/Kafka:

```powershell
C:\Python311\python.exe -B tests/cdc_sink/test_cutover.py --run-clone --backup backups/hdg_mirror_readiness_20261111_215302_011a2a64.dump --baseline tests/cdc_sink/test-results/full-fa5d5786 --next tests/cdc_sink/test-results/full-f2526210
```

Bằng chứng: `tests/cdc_sink/test-results/cutover-acec4095/result.json`, commands.log và các plan JSON. Log giữ exit 91/92 của crash probe; đây là các lỗi chủ đích được assertion xác nhận, không phải lỗi bị bỏ qua. Không đưa plan/raw CDC/credential vào Git.

### Lệnh live — chưa thực hiện

Chưa có fresh live plan nên live vẫn **NOT ARMED**. Không đổi purpose/SCN của plan clone để dùng cho hdg_mirror. Trước khi tạo plan cần:
- Quyết định cửa sổ triển khai; dùng session Oracle mới giữ EXCLUSIVE NOWAIT đủ bốn bảng.
- Chọn SCN mới dưới khóa; lấy SID, SERIAL, DBID:RESETLOGS_CHANGE# và plan UUID mới. Gắn client_info của session bằng DBMS_APPLICATION_INFO.SET_CLIENT_INFO với chuỗi HDG_CUTOVER:PLAN_UUID:SCN.
- Full Load --stage-only vào hdg_mirror tại SCN đó, đối soát và audit đủ bốn bảng. Đây là run live mới, không import run test. Giữ session fence trong suốt make-plan/prepare/apply và đối soát.
- Fresh fence JSON có kind=live-writer-fence-v1, destination=hdg_mirror, plan_id, sid, serial, scn và source_incarnation từ bằng chứng thực.
- Backup được restore kiểm chứng với owner/ACL và diễn tập bằng đúng backup đó. Các biến PG_USER/PG_PASSWORD dùng credential hiện có, không ghi vào lệnh/file nguồn.
- JMX local/ReadCaptureMetrics hiện có phải đọc được; nếu thiếu, guard dừng, không tự bật agent hoặc restart Connector.

Các lệnh dưới là cú pháp triển khai, **chưa chạy trên Mirror live**; thay placeholder bằng dữ liệu mới đã kiểm chứng:

```powershell
$python = ".\cdc\sink\.venv\Scripts\python.exe"

& $python cdc/sink/cutover.py make-plan --run-id "<LIVE_RUN_ID_MOI>" --barrier "<FRESH_LIVE_FENCE.json>" --rehearsal-proof "<CLONE_PASS_RESULT.json>" --backup "<VERIFIED_BACKUP.dump>" --plan "<LIVE_PLAN_MOI.json>" --confirm-database hdg_mirror
& $python cdc/sink/cutover.py prepare --plan "<LIVE_PLAN_MOI.json>" --confirm-database hdg_mirror
& $python cdc/sink/cutover.py apply --plan "<LIVE_PLAN_MOI.json>" --confirm-database hdg_mirror

.\jobs\run_spark.ps1 -Job reconcile -JobArguments @("--run-id","<LIVE_RUN_ID_MOI>","--target","mirror","--source","snapshot")
```

make-plan chỉ SELECT/đọc Kafka/GET và xuất plan mới; không publish. Nó kiểm fence đang giữ, SCN còn mới trong 30 phút, run được tạo dưới session fence, incarnation/config capture, JMX không còn transaction/queue/error, offset đã flush và vector hiện tại qua hai lượt. Initial handoff live bắt buộc frames rỗng/start=end tại frontier đã chứng minh; không chọn end offset tùy tiện.

Sau khi nghiệm thu publish và đối soát, mới giải phóng fence nguồn. Để nhận CDC tiếp theo cần extension có đủ transaction BEGIN/END, đúng thứ tự và prefix bất biến:

```powershell
& $python cdc/sink/cutover.py extend --plan "<LIVE_PLAN_DANG_DUNG.json>" --extension "<LIVE_PLAN_NOI_THEM.json>" --confirm-database hdg_mirror
& $python cdc/sink/cutover.py replay --plan "<LIVE_PLAN_NOI_THEM.json>" --confirm-database hdg_mirror
```

Rollback live yêu cầu fence mới riêng: kind=live-rollback-fence-v1, destination, sid, serial, scn và vector streams đủ năm stream tại frontier hiện tại; client_info là HDG_ROLLBACK:PLAN_UUID:SCN_ROLLBACK. Không tái dùng barrier cũ:

```powershell
& $python cdc/sink/cutover.py rollback --plan "<LIVE_PLAN_MOI_NHAT.json>" --rollback-proof "<FRESH_ROLLBACK_FENCE.json>" --confirm-database hdg_mirror
```

Phạm vi đã chứng minh là cutover và replay hữu hạn/append có kiểm soát trên clone, không phải dịch vụ tự ráp transaction streaming. Range cần còn trong Kafka retention; lỗi hoặc thiếu dữ liệu làm dừng, không skip/reset. Restart Debezium chưa được diễn tập. Chưa chạy make-plan/prepare/apply/rollback live; Mirror chính, Stage/Candidate/backup và database PASS cũ giữ nguyên.
