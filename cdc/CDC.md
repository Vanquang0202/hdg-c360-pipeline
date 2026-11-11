# Chuẩn bị CDC Oracle → Kafka

Đây là bộ cấu hình chuẩn bị, chưa chạy container, SQL hoặc connector. Theo hiện trạng người vận hành cung cấp, Full Load đã hoàn thành và chạy lại NOOP; SCN cần xem xét khi nối tiếp là **2314948**. Các file này không ghi PostgreSQL, không cập nhật `hdg_load.control`, không tạo Sink và không chạy generator.

## 1. Phiên bản và phạm vi hỗ trợ

- Broker: `apache/kafka:3.8.1`, dùng image đã có, một node vừa làm broker vừa làm KRaft controller; không cần ZooKeeper.
- Connect: `quay.io/debezium/connect:3.4.3.Final`, bản Final cố định. Release 3.4.2 bổ sung Oracle 26ai (23.26.x), bản 3.4.3 là bản vá tiếp theo. Đây là căn cứ chọn nhánh hỗ trợ Oracle 26ai, không phải bằng chứng đã kiểm thử container Oracle hiện có. [Release notes](https://debezium.io/releases/3.4/release-notes)
- Bản Connect này được xây dựng và kiểm thử upstream với Kafka 4.1.1. Việc giữ broker 3.8.1 dựa trên cơ chế thương lượng phiên bản giao thức của Kafka; tổ hợp cụ thể vẫn phải kiểm chứng ở các bước bên dưới. Nếu gặp lỗi API không tương thích, dừng để đánh giá broker 4.1.1 riêng, không tự nâng cấp broker/volume. [Giao thức Kafka](https://kafka.apache.org/protocol.html)
- Oracle JDBC dùng lại `jobs/jars/ojdbc11.jar`, chỉ mount đúng file vào thư mục plugin Oracle, không mount đè cả thư mục plugin. Kiểm tra bản JAR thực tế hỗ trợ Java của image Connect và Oracle 26ai; không suy ra phiên bản từ tên file. [Oracle JDBC](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)

Image tag được cố định trong Compose nhưng chưa pull hoặc xác minh digest tại máy này. Sau khi tải từ registry chính thức, ghi lại RepoDigests và phiên bản plugin. Không dùng `latest`.

## 2. Bộ nhớ và lưu trữ

| Dịch vụ | Giới hạn container | Java heap tối đa |
|---|---:|---:|
| Kafka | 1024 MiB | 512 MiB |
| Connect | 1408 MiB | 768 MiB |

Thêm tối đa khoảng 2,38 GiB giới hạn container vào mức dùng hiện tại khoảng 2,9 GiB, còn khoảng 1,46 GiB trong tổng 6,74 GiB theo số liệu đã cung cấp. Đây là ước tính, không phải đo thực tế. Heap không bao gồm toàn bộ bộ nhớ JVM, thread, native và cache. Đã giới hạn thêm direct memory/metaspace; cần theo dõi OOMKilled và Oracle PGA khi LogMiner hoạt động. Không chạy thêm Spark/Airflow/Superset cùng lúc trong lần kiểm tra đầu.

Hàng đợi sự kiện Connect giới hạn 2048 sự kiện/16 MiB, mỗi batch tối đa 512. Giới hạn này **không** giới hạn toàn bộ bộ đệm giao dịch chưa commit của LogMiner. Giao dịch lớn/kéo dài vẫn có thể gây OOM. Không đặt ngưỡng bỏ giao dịch hoặc thời hạn loại giao dịch để giảm RAM; khi áp lực bộ nhớ tăng phải dừng tạo biến động và đánh giá lại kích thước lô/bộ đệm.

Kafka có volume riêng `hdg-cdc_kafka_data`; giữ ổn định cluster ID. Topic dữ liệu mặc định giữ 7 ngày hoặc khoảng 512 MiB mỗi partition, tùy ngưỡng đến trước; mỗi topic hiện chỉ một partition. Giới hạn này không bảo đảm giữ toàn bộ backlog khi lag lâu. Schema history dùng retention vô hạn; các topic cấu hình/offset/status dùng compact. Theo dõi cả dung lượng archive Oracle và volume Kafka. Một node/replication factor 1 phù hợp local, không có dự phòng khi mất volume.

## 3. Mạng và mật khẩu

Network ngoài Compose mặc định `hdg-cdc-net`. Script nối hai container hiện có vào network này, giữ các network/volume đang dùng. Compose không khai báo lại Oracle/PostgreSQL.

- Container: Kafka tại `kafka:9092`, Oracle tại `hdg-c360-oracle:1521`, PostgreSQL tại `hdg-c360-postgres:5432`.
- Windows host: Kafka tại `localhost:29092`, REST Connect tại `http://localhost:8083`; PostgreSQL vẫn dùng cổng host 55432.
- Controller 9093 không công bố ra host. Trong container không dùng `localhost:29092` để kết nối Kafka.

Hai listener cho Kafka công bố đúng địa chỉ theo phía kết nối. Cấu hình PLAINTEXT chỉ dùng trong mạng local; cổng host bind 127.0.0.1. REST Connect không có xác thực trong bộ local này, không công bố ra LAN/Internet.

Chạy từ thư mục project bằng PowerShell, khi chủ động triển khai:

```powershell
# Chọn một cách cung cấp mật khẩu:
# A. Biến môi trường dùng cho cả Compose và helper tạo user:
$env:ORACLE_CDC_PASSWORD = [System.Net.NetworkCredential]::new("", (Read-Host "CDC password" -AsSecureString)).Password
# B. Hoặc tự sao chép .env.example thành cdc/.env, điền mật khẩu.
# Không cần tạo .env nếu đã dùng biến môi trường.
# File .env đã được .gitignore ở root loại trừ.
```

Với cách B, dùng giá trị được single-quote trong .env để giữ nguyên ký tự $/#. Compose tự đọc file qua `--env-file`, nhưng helper SQL chỉ đọc biến môi trường của PowerShell; nếu chạy helper phải nạp cùng mật khẩu vào biến trên. Không ghi mật khẩu vào lệnh, JSON connector hoặc SQL đã render.

JSON dùng `${env:ORACLE_CDC_PASSWORD}`, được EnvVarConfigProvider trong worker giải quyết. Không in `docker compose config` đầy đủ hoặc chia sẻ `docker inspect` vì có thể lộ biến môi trường. Xác thực provider thực tế qua bước validate khi đăng ký connector.

## 4. Chuẩn bị Oracle thủ công

Không chạy lại ARCHIVELOG hoặc bật supplemental logging mù quáng; người vận hành đã xác nhận bật chúng. Đọc `oracle/00_preflight.sql` rồi chạy thủ công qua SQLPlus SYSDBA để xác minh:

- CDB thực tế có tên FREE, PDB FREEPDB1 mở READ WRITE, USERS tồn tại ở root và PDB.
- Database supplemental logging có đủ; kiểm tra thêm logging cấp bảng. MIN/PK logging riêng không bảo đảm có đủ before-image cho bảng không PK và cột nullable.
- Archive/redo còn liên tục qua SCN 2314948 và thời điểm bắt đầu các giao dịch còn mở; kiểm tra các thread, incarnation/resetlogs và file thực sự còn đọc được. Một dòng archive chứa SCN chưa đủ chứng minh không có khoảng trống.
- Nếu thiếu ALL COLUMNS logging cần đánh giá và thực hiện riêng trước biến động. Không file nào ở đây tự bật logging hoặc ép log switch.

Ví dụ mở SQLPlus; dán các câu kiểm tra từ file, không đưa mật khẩu SYS vào command line:

```powershell
docker exec -it hdg-c360-oracle sqlplus / as sysdba
```

`00_preflight.sql` chỉ SELECT và chuyển container của phiên làm việc; không thay đổi dữ liệu hoặc cấu hình database.

Sau khi xác minh, helper dưới đây **thực hiện DDL tạo tài khoản** khi người vận hành chủ động gọi, không phải lệnh kiểm tra:

```powershell
.\cdc\Invoke-OracleSetup.ps1
```

Tài khoản common `C##HDG_CDC` dùng LogMiner ở root và đọc bốn bảng VSS trong PDB. Script cấp quyền package/view/catalog theo hướng dẫn Debezium, giới hạn SELECT/FLASHBACK dữ liệu nghiệp vụ vào bốn bảng; vẫn có quyền catalog và LOCK ANY TABLE rộng, phù hợp tài khoản chuyên dụng local, cần rà soát lại trước production. Dùng USERS sẵn có, quota 100 MiB; không tạo tablespace/datafile hoặc cấp quota vô hạn. [Quyền LogMiner và cấu hình Oracle](https://debezium.io/documentation/reference/3.4/connectors/oracle.html#creating-users-for-the-connector)

Helper lấy password từ biến môi trường, truyền SQL trong bộ nhớ vào SQLPlus; không ghi SQL có mật khẩu ra đĩa. Password cho helper phải là ASCII in được, không khoảng trắng hoặc dấu nháy kép. Không chạy SQL template trực tiếp vì còn placeholder. Script dừng nếu user đã tồn tại, không reset password; Oracle DDL đã thực hiện không rollback tự động nếu grant sau đó lỗi. Khi lỗi phải kiểm tra user/grants trước khi bổ sung thủ công.

Connector có thể tạo/ghi bảng flush nội bộ trong schema tài khoản CDC để LogMiner làm việc. Đây không phải biến động nghiệp vụ VSS, nhưng khởi động connector không có nghĩa Oracle hoàn toàn không phát sinh thao tác nội bộ. Không chạy generator hoặc INSERT/UPDATE/DELETE vào bốn bảng VSS trong giai đoạn này.

## 5. Khởi động Kafka và Connect

Các lệnh sau là hướng dẫn, chưa được thực hiện. Với mật khẩu đã có trong biến môi trường, có thể bỏ `--env-file cdc/.env` trong toàn bộ lệnh nếu không tạo .env.

```powershell
.\cdc\Connect-Network.ps1
# Nếu đổi network, truyền -Network và đặt HDG_DOCKER_NETWORK cùng giá trị.
Get-Item jobs/jars/ojdbc11.jar
Get-FileHash jobs/jars/ojdbc11.jar -Algorithm SHA256

docker compose --env-file cdc/.env -f cdc/compose.yaml config --quiet
# Chỉ tải Connect; Kafka 3.8.1 đã có.
docker compose --env-file cdc/.env -f cdc/compose.yaml pull connect
docker compose --env-file cdc/.env -f cdc/compose.yaml up -d --pull never kafka
docker compose --env-file cdc/.env -f cdc/compose.yaml ps
docker compose --env-file cdc/.env -f cdc/compose.yaml exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092
# Kiểm tra thêm listener dành cho host; cần Kafka CLI cài trên Windows để kiểm chứng đầy đủ metadata từ host.
Test-NetConnection localhost -Port 29092
```

TCP thông không chứng minh advertised listener đúng. Nếu có Kafka CLI trên Windows, chạy `kafka-topics.bat --bootstrap-server localhost:29092 --list`; metadata phải trả `localhost:29092`. Phía container dùng `kafka:9092`.

Tạo schema history một partition với retention vô hạn trước khi đăng ký source:

```powershell
docker compose --env-file cdc/.env -f cdc/compose.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic hdg-observe-schema-history --partitions 1 --replication-factor 1 --config cleanup.policy=delete --config retention.ms=-1 --config retention.bytes=-1
docker compose --env-file cdc/.env -f cdc/compose.yaml exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name hdg-observe-schema-history --describe
docker compose --env-file cdc/.env -f cdc/compose.yaml up -d --pull never connect
docker compose --env-file cdc/.env -f cdc/compose.yaml logs --tail 100 connect
Invoke-RestMethod http://localhost:8083/
Invoke-RestMethod http://localhost:8083/connector-plugins
docker compose --env-file cdc/.env -f cdc/compose.yaml exec -T connect sh -c 'find /kafka/connect/debezium-connector-oracle -name "ojdbc*.jar"'
```

Nếu topic đã có, `--if-not-exists` không sửa config cũ: kiểm tra thực sự chỉ một partition, retention.ms/bytes=-1, cleanup.policy=delete trước khi tiếp tục. Kiểm tra topic nội bộ Connect có cleanup.policy=compact; không áp dụng retention dữ liệu ngắn cho offset/config/history.

Danh sách plugin phải có `io.debezium.connector.oracle.OracleConnector`, phiên bản 3.4.3.Final. Kiểm tra JDBC chỉ có một phiên bản trong plugin và không có xung đột JAR; nếu image đã chứa driver khác, dừng để điều chỉnh image riêng, không chạy với hai phiên bản. REST worker hoạt động chưa chứng minh driver/LogMiner kết nối thành công.

## 6. Đăng ký source để quan sát, chưa nối Mirror

Trước khi đăng ký, đối chiếu lại database.dbname=FREE và database.pdb.name=FREEPDB1 trong JSON với kết quả Oracle; dbname phải là CDB, không phải service PDB. Không đổi prefix/history topic sau khi đã có offset.

```powershell
.\cdc\Register-Source.ps1
Invoke-RestMethod http://localhost:8083/connectors/hdg-oracle-observe/status
Invoke-RestMethod http://localhost:8083/connectors/hdg-oracle-observe/offsets
docker compose --env-file cdc/.env -f cdc/compose.yaml logs --tail 150 connect
docker compose --env-file cdc/.env -f cdc/compose.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker stats --no-stream
```

Script validate rồi POST một connector mới; không PUT ghi đè hoặc reset connector đang có. Validate có thể kết nối Oracle, nhưng không phát sinh biến động nghiệp vụ. Khi thành công cần cả connector và task là RUNNING, log hoàn thành đọc schema và LogMiner không báo ORA. Với nguồn yên lặng, chưa có topic/sự kiện dữ liệu không đồng nghĩa lỗi; heartbeat cũng không bảo đảm tăng offset liên tục khi không có giao dịch được capture. Chưa tạo bất kỳ heartbeat action query ghi nguồn nào.

Connector giữ envelope before/after, op, source SCN và metadata giao dịch; không unwrap, không lọc TYPEID/trạng thái duyệt. Số NUMBER giữ dạng precise theo schema Connect; downstream phải xử lý Decimal/VariableScaleDecimal chính xác, không mặc định đổi thành float. Cấu hình online_catalog yêu cầu không thay đổi DDL bảng khi đang thử nghiệm; schema evolution phải kiểm chứng riêng.

Chưa đặt `message.key.columns`. Dân tộc/quốc tịch có PK ID nên có thể có key theo PK. Nhân khẩu và DBHC không có physical PK trong DDL: key mặc định có thể null. Không lấy MA_CD làm key riêng, không giả định ID là unique, không coi (MA_CD, MA_HO_GD) với NULL là đủ cho UPDATE/DELETE. Topic dữ liệu giữ delete policy để không compact mất các sự kiện chưa xác định được key.

Phải kiểm tra before-image, old/new key, supplemental logging, dòng trùng hoàn toàn và khả năng phát lại trước khi viết Sink. Không có Sink hay credentials PostgreSQL trong Compose/JSON nên bộ này không ghi Mirror. Tombstone được giữ nếu connector có key và phát sinh; không giả định bảng key-null có chuỗi DELETE/tombstone giống bảng có PK.

## 7. SCN Full Load và việc nối tiếp an toàn

`snapshot.mode=no_data` chỉ đọc cấu trúc, sau đó theo dõi thay đổi từ vị trí connector xác định khi bootstrap (hoặc offset đã lưu khi khởi động lại). **Không có cấu hình trong bộ này ép bắt đầu tại SCN 2314948.** Đăng ký source quan sát hôm nay có thể bỏ qua thay đổi xảy ra giữa Full Load và bootstrap. Không dùng `log.mining.start.scn` tự đặt hoặc chỉnh offset Kafka tùy tiện.

Trước khi nối Sink vào Mirror cần một trong các phương án được thiết kế và kiểm chứng riêng:

1. Chứng minh nguồn không đổi từ snapshot Full Load đến mốc connector, có bằng chứng nội dung chứ không chỉ COUNT và xử lý giao dịch mở. Đây mới là điều kiện để xem xét tái sử dụng snapshot hiện có, không phải mặc định đúng.
2. Thiết kế bootstrap/replay từ SCN phù hợp bằng cơ chế offset/snapshot được phiên bản Debezium hỗ trợ, giữ lịch sử schema tại mốc và toàn bộ redo/archive cần thiết. Giao dịch bắt đầu trước snapshot nhưng commit sau snapshot đòi hỏi đọc đủ từ trước mốc; không chỉ lọc event.scn > 2314948.
3. Nếu không đủ log/bằng chứng, lên kế hoạch snapshot mới đồng bộ với CDC vào vùng thử nghiệm/staging, đối soát rồi publish có kiểm soát. Không tự ghi đè Mirror hiện tại để làm lại.

Cần chứng minh thứ tự commit/giao dịch, quy tắc replay idempotent, nhận diện đúng dòng khi UPDATE/DELETE/đổi MA_CD, và phục hồi sau restart. Kafka Connect có thể phát lại sự kiện sau lỗi; RUNNING không đồng nghĩa exactly-once hay cutover hoàn tất. Không đổi `hdg_load.control.cdc_enabled`, SCN hoặc trạng thái Full Load trong giai đoạn quan sát.

## 8. Dừng và lưu bằng chứng

```powershell
# Dừng hai dịch vụ CDC, giữ container và volume:
docker compose --env-file cdc/.env -f cdc/compose.yaml stop connect kafka
```

Không dùng down -v, xóa topic offset/history, hoặc xóa volume để xử lý lỗi. Khi lag, theo dõi offset/SCN, thời gian commit nguồn, queue và bộ đệm giao dịch; tốc độ tăng archive/Kafka phải đủ giữ dữ liệu đến khi bắt kịp. Nếu connector OOM, giữ logs và topic offset/history, giảm tải rồi xem xét tài nguyên trước khi restart; không reset offset để làm số lag đẹp hơn.

Bằng chứng cần lấy khi triển khai: digest image và JDBC SHA256, trạng thái worker/task, endpoint offsets, cấu hình retention/history, số liệu bộ nhớ và log không lỗi LogMiner. Chưa chạy ở bước chuẩn bị nên chưa có bằng chứng vận hành, khả năng UPDATE/DELETE hoặc cutover.

## 9. Kiểm tra khi chuẩn bị file

Đã kiểm tra `docker compose -f cdc/compose.yaml config --quiet` bằng mật khẩu giả chỉ dành cho kiểm tra cú pháp: mã thoát 0, không khởi động container. PowerShell AST của ba script đọc được không lỗi; JSON connector hợp lệ; các khối Markdown đóng/mở cân bằng; `cdc/.env` được Git ignore. Chưa pull image, kiểm thử JDBC trong Connect, thực hiện SQL, đăng ký source hoặc kiểm chứng LogMiner/cutover.
