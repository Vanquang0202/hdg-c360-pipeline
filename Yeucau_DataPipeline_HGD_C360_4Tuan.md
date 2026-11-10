# XÂY DỰNG PIPELINE DỮ LIỆU HỘ GIA ĐÌNH - NHÂN KHẨU (HDG -> C360 -> BI & API)

## 1. Mục tiêu

Trong 4 tuần tới, các bạn sẽ xây dựng một data pipeline hoàn chỉnh từ đầu đến cuối (End-to-End) dựa trên bài toán thực tế của dự án: đồng bộ dữ liệu từ hệ thống Hộ gia đình - Nhân khẩu (HDG), chuẩn hóa thành bảng thông tin công dân (Customer 360), xây dựng báo cáo đối soát / Dashboard BI và API tra cứu.

Luồng tổng thể gồm 4 bước:
**DB Nguồn (HDG) -> Đồng bộ (Batch & CDC) -> Chuẩn hóa dữ liệu (C360) -> Đối soát, Báo cáo BI & API**

Yêu cầu chung:
- Chỉ cung cấp cấu trúc các bảng nguồn, quy tắc dữ liệu thực tế và yêu cầu đầu ra. Các bạn tự khảo sát, tự thiết kế cấu trúc các bảng đích (bảng Mirror, bảng chuẩn hóa C360, các bảng phụ trợ/điều khiển/báo cáo) và đề xuất giải pháp thực hiện.
- Trước khi code phải viết tài liệu thiết kế (Basic Design & Detail Design theo mẫu đã gửi trước đó) và duyệt thiết kế trước.
- **Về hạ tầng chạy thử nghiệm:** Toàn bộ pipeline gồm nhiều thành phần (Oracle DB, Kafka, Debezium Connect, Spark/Python, Airflow, công cụ BI). Nếu máy tính cá nhân không đủ tài nguyên (RAM/CPU) để chạy toàn bộ, các bạn có thể đăng ký gói **Azure for Students** bằng tài khoản email sinh viên (được miễn phí **100$ trong 1 năm**) để tạo VM cài đặt và chạy các dịch vụ.

---

## 2. Cấu trúc các bảng nguồn (Schema `VSS`)

Hệ thống nguồn (Oracle) gồm 1 bảng chính lưu thông tin hộ gia đình - nhân khẩu và 3 bảng danh mục:

### 2.1. Bảng chính: `VSS.VSS_HO_GD_NHANKHAU` (111 cột, ~138 triệu dòng trên PROD)

**Đặc điểm vật lý & nghiệp vụ ở nguồn:**
- **Partition:** bảng nguồn được chia `PARTITION BY RANGE (MA_TINH)`.
- **Khóa & Ràng buộc:** Cột `MA_CD` (mã công dân / mã số BHXH) là `NOT NULL`. Tuy nhiên trên thực tế, 1 công dân (`MA_CD`) có thể xuất hiện nhiều dòng ở các hộ gia đình khác nhau (`MA_HO_GD`), nên khóa xác định duy nhất một dòng dữ liệu trên bảng này là cặp **`(MA_CD, MA_HO_GD)`**.
- **Ý nghĩa một số cột trạng thái nghiệp vụ quan trọng:**
  - `TYPEID`: Loại biến động của bản ghi hộ gia đình. Các bản ghi đang hoạt động bình thường có `TYPEID` là `NULL` hoặc mã tăng/thay đổi thông tin. Riêng nhóm mã bắt đầu bằng chữ `G` là **biến động Giảm (đóng/không còn hiệu lực)**:
    - `'GT'`: Giảm tử (đã mất).
    - `'GM'`: Giảm mất tích / đóng mã.
    - `'GG'`: Giảm gộp (bản ghi có mã số BHXH trùng đã bị gộp sang mã khác, mã này không còn dùng).
    - `'GK'`: Giảm khác (di cư ra nước ngoài, hủy mã...).
  - `TRANG_THAI_DUYET`: Trạng thái phê duyệt hồ sơ kê khai/điều chỉnh nhân khẩu trên phần mềm HDG:
    - `0` hoặc `NULL`: Bản ghi gốc đã ổn định, đang có hiệu lực.
    - `2`: Hồ sơ điều chỉnh đã được phê duyệt (có hiệu lực).
    - `1`: Hồ sơ mới nhập, đang chờ phê duyệt (chưa có hiệu lực chính thức).
    - `3`: Hồ sơ bị từ chối phê duyệt.
    - `4`: Hồ sơ đã bị hủy / đề nghị xóa.

```sql
CREATE TABLE VSS.VSS_HO_GD_NHANKHAU (
    MA_CD                    VARCHAR2(30 BYTE) NOT NULL,
    MA_CN                    VARCHAR2(30 BYTE),
    MA_HO_GD                 VARCHAR2(30 BYTE),
    HO_TEN                   VARCHAR2(250 BYTE),
    HO_TEN_KHAC              VARCHAR2(250 BYTE),
    GIOI_TINH                CHAR(1 BYTE),
    NGAY_SINH                NUMBER,
    NOI_SINH                 VARCHAR2(250 BYTE),
    NGAY_KS                  NUMBER,
    SO_GIAY_KS               VARCHAR2(50 BYTE),
    NOI_KS                   VARCHAR2(250 BYTE),
    QH_CHU_HO                VARCHAR2(2 BYTE),
    SO_CMTND                 VARCHAR2(50 BYTE),
    NGAY_CAP_CMTND           NUMBER,
    NOI_CAP_CMTND            VARCHAR2(250 BYTE),
    SO_HC                    VARCHAR2(50 BYTE),
    NGAY_CAP_HC              NUMBER,
    NOI_CAP_HC               VARCHAR2(250 BYTE),
    DIA_CHI                  VARCHAR2(500 BYTE),
    DC_LIEN_LAC              VARCHAR2(250 BYTE),
    EMAIL_ACC                VARCHAR2(50 BYTE),
    PHONE_NUMBER             VARCHAR2(50 BYTE),
    MA_TINH                  VARCHAR2(10 BYTE),
    MA_HUYEN                 VARCHAR2(10 BYTE),
    MA_XA                    VARCHAR2(10 BYTE),
    NGHE_NGHIEP              VARCHAR2(250 BYTE),
    QUOC_TICH                VARCHAR2(10 BYTE),
    IS_ALIVE                 CHAR(1 BYTE),
    DEAD_DATE                NUMBER,
    IS_BH                    CHAR(1 BYTE),
    LOAI_BH                  VARCHAR2(30 BYTE),
    MA_BH                    VARCHAR2(30 BYTE),
    CREATED_DATE             DATE,
    MODIFY_DATE              DATE,
    MA_NV                    VARCHAR2(100 CHAR),
    GHI_CHU                  VARCHAR2(500 BYTE),
    MA_THE                   VARCHAR2(2000 BYTE),
    MA_SO                    VARCHAR2(2000 BYTE),
    ISSYNCH                  CHAR(1 BYTE),
    NGAY_BATDAU              NUMBER,
    NGAY_HETHAN              NUMBER,
    MA_THON                  VARCHAR2(10 BYTE),
    ISCHU_HO                 CHAR(1 BYTE),
    TEN_DOITUONG_BH          VARCHAR2(2000 BYTE),
    TEN_TINH                 VARCHAR2(250 BYTE),
    TEN_HUYEN                VARCHAR2(250 BYTE),
    TEN_XA                   VARCHAR2(250 BYTE),
    TEN_THON                 VARCHAR2(250 BYTE),
    TEN_QH_CHUHO             VARCHAR2(250 BYTE),
    MA_DOITUONG_BH           VARCHAR2(10 BYTE),
    STATUS                   CHAR(1 BYTE),
    CARD_ID                  VARCHAR2(2000 BYTE),
    TYPEID                   VARCHAR2(2 BYTE),
    NGAY_PHATSINH            NUMBER,
    NGUOI_KE_KHAI            VARCHAR2(200 BYTE),
    TRAN_STATUS              VARCHAR2(5 BYTE),
    TRANG_THAI               CHAR(1 BYTE),
    MA_TINH_KS               VARCHAR2(5 BYTE),
    MA_HUYEN_KS              VARCHAR2(5 BYTE),
    MA_XA_KS                 VARCHAR2(5 BYTE),
    TEN_TINH_KS              VARCHAR2(200 BYTE),
    TEN_HUYEN_KS             VARCHAR2(200 BYTE),
    TEN_XA_KS                VARCHAR2(200 BYTE),
    ID                       VARCHAR2(40 BYTE),
    MA_OFFLINE               NVARCHAR2(100),
    NGAY_SINH_BK             NUMBER,
    MA_HUYEN_IMPORT          VARCHAR2(20 BYTE),
    ID_CA                    VARCHAR2(20 BYTE),
    NGOAI_TINH               VARCHAR2(10 BYTE),
    MA_NV_TAO                VARCHAR2(100 CHAR),
    MA_TINH_THE              VARCHAR2(10 BYTE),
    USER_NGOAI_TINH          NVARCHAR2(30),
    MA_THE_MOI               VARCHAR2(50 CHAR),
    CARD_ID_MOI              VARCHAR2(50 BYTE),
    SYNC_TST                 NUMBER(1),
    MA_CD_OLD                VARCHAR2(30 BYTE),
    HO_TEN_UNSIGNED          VARCHAR2(250 BYTE),
    USER_ID                  NUMBER(10),
    USER_ID_TINH             NUMBER(10),
    USER_ID_TW               NUMBER(10),
    TRANG_THAI_DUYET         NUMBER(2),
    TYPE_PM                  NUMBER(1),
    USER_UPDATE              NUMBER(10),
    LY_DO_PHE_DUYET          VARCHAR2(1000 CHAR),
    APPROVED_DATE            DATE,
    TEN_CHA_ME               VARCHAR2(250 BYTE),
    KEY_SL                   VARCHAR2(250 CHAR),
    DM_DAN_TOC_ID            NUMBER(19),
    NGAY_TAO_HC              DATE,
    USER_HC_ID               NUMBER(10),
    USER_HC                  VARCHAR2(100 BYTE),
    USER_DUYET_HC_ID         NUMBER(10),
    USER_DUYET_HC            VARCHAR2(100 BYTE),
    NGAY_DUYET_HC            DATE,
    LY_DO_HC                 VARCHAR2(1000 BYTE),
    USER_DMS                 VARCHAR2(100 CHAR),
    USER_DMS_ID              NUMBER(19),
    NGAY_DE_NGHI_DMS         DATE,
    USER_DUYET_DMS_ID        NUMBER(19),
    USER_DUYET_DMS           VARCHAR2(100 CHAR),
    NGAY_DUYET_DMS           DATE,
    LY_DO_DMS                VARCHAR2(1000 CHAR),
    LOCK_DATA                NUMBER,
    MA_TINH_LH               VARCHAR2(10 BYTE),
    MA_HUYEN_LH              VARCHAR2(10 BYTE),
    MA_XA_LH                 VARCHAR2(10 BYTE),
    XAC_THUC_BCA             NUMBER(2),
    DANG_TG                  VARCHAR2(20 BYTE),
    SO_CCCD                  VARCHAR2(50 BYTE),
    TRANG_THAI_QUET          NUMBER(2),
    TRANG_THAI_XAC_THUC_BCA  NUMBER(2)
)
PARTITION BY RANGE (MA_TINH) (
    PARTITION P_DEFAULT VALUES LESS THAN (MAXVALUE)
);
```

### 2.2. Bảng danh mục Dân tộc: `VSS.VSS_DM_DANTOC`
- **Đặc điểm ở nguồn:** Có khóa chính (Primary Key) trên cột `ID`. Lưu ý bảng này có lịch sử hiệu lực (`NGAY_HL`, `NGAY_HET_HL`), tức 1 mã dân tộc `MA` có thể có nhiều dòng theo thời gian; cột dùng để join với `VSS_HO_GD_NHANKHAU.DM_DAN_TOC_ID` là cột `ID`.

```sql
CREATE TABLE VSS.VSS_DM_DANTOC (
    ID              NUMBER(19)          NOT NULL,
    CAN_CU          VARCHAR2(255 CHAR)  NOT NULL,
    MA              VARCHAR2(2 CHAR)    NOT NULL,
    NGAY_HL         DATE                NOT NULL,
    NGAY_HET_HL     DATE,
    TEN             VARCHAR2(100 CHAR)  NOT NULL,
    CONSTRAINT PK_VSS_DM_DANTOC PRIMARY KEY (ID)
);
```

### 2.3. Bảng danh mục Quốc tịch: `VSS.VSS_DM_QUOC_TICH`
- **Đặc điểm ở nguồn:** Có khóa chính (Primary Key) trên cột `ID` (ví dụ `ID = 233`, `MA = 'VN'`, `TEN = 'Việt Nam'`). Dữ liệu nhân khẩu trên thực tế có dòng lưu mã 2 ký tự (`'VN'`), có dòng lại lưu `ID` (`'233'`).

```sql
CREATE TABLE VSS.VSS_DM_QUOC_TICH (
    ID              NUMBER(19)          NOT NULL,
    MA              VARCHAR2(2 CHAR)    NOT NULL,
    TEN             VARCHAR2(100 CHAR)  NOT NULL,
    CONSTRAINT PK_VSS_DM_QUOC_TICH PRIMARY KEY (ID)
);
```

### 2.4. Bảng danh mục Địa bàn hành chính: `VSS.VSS_DM_DBHC`
- **Đặc điểm ở nguồn:** Không có Primary Key và tất cả các cột đều nullable.
- **Cách tổ chức dữ liệu thực tế trong `VSS_DM_DBHC` (sau sáp nhập 2025, bỏ cấp Huyện, chỉ còn Tỉnh và Xã):**
  - **Dòng cấp Tỉnh:** có `MA_CHA IS NULL`, `TINH_TRANG = '1'`, `STATUS = '1'`.
    - Cột `MA_DBHC` của Tỉnh lưu dạng 2 chữ số + đuôi `'TTT'`: ví dụ Hà Nội là `'01TTT'`, Hưng Yên là `'33TTT'`, Thanh Hóa là `'38TTT'`, TP.HCM là `'79TTT'`, Bộ Quốc phòng là `'97TTT'`, Bộ Công an là `'98TTT'`. (Khi chuẩn hóa sang bảng C360, mã tỉnh chuẩn sẽ đổi thành 3 chữ số: `'01TTT'` -> `'001'`, `'33TTT'` -> `'033'`, `'79TTT'` -> `'079'`).
    - Cột `MA_DBHC_SN` lưu các mã tỉnh cũ (2 chữ số, ngăn cách bởi dấu `;`) đã sáp nhập vào tỉnh đó. Ví dụ: Tỉnh Hưng Yên (`MA_DBHC = '33TTT'`) có `MA_DBHC_SN = '33;34'` (do tỉnh Thái Bình cũ mã `'34'` đã sáp nhập vào Hưng Yên).
  - **Dòng cấp Xã/Phường:** có `MA_CHA` trỏ vào `MA_DBHC` của Tỉnh (ví dụ `MA_CHA = '01TTT'` hoặc `'33TTT'`), `TINH_TRANG = '1'`, `STATUS = '1'`.
    - Cột `MA_DBHC` của Xã lưu mã 5 chữ số (ví dụ `'00004'`, `'12457'`).
    - Cột `MA_DBHC_SN` lưu danh sách các mã xã cũ (5 chữ số, ngăn cách bởi dấu `;`) đã sáp nhập vào xã mới đó. Ví dụ: Phường mới có `MA_DBHC = '00004'` và `MA_DBHC_SN = '00013;00004;00019'` (tức 3 phường cũ `'00013'`, `'00004'`, `'00019'` gộp thành phường `'00004'`). Có trường hợp 1 mã xã cũ bị tách ra nằm ở `MA_DBHC_SN` của 2 xã mới khác nhau.

```sql
CREATE TABLE VSS.VSS_DM_DBHC (
    MA_DBHC             VARCHAR2(5 BYTE),
    MA_CHA              VARCHAR2(5 BYTE),
    TEN                 VARCHAR2(200 BYTE),
    TINH_TRANG          VARCHAR2(1 BYTE),
    STATUS              CHAR(1 BYTE),
    CREATED_DATE        DATE,
    ID                  NUMBER(20),
    MA_DBHC_SN          VARCHAR2(200 BYTE),
    IS_LT               NUMBER(1),
    IS_CAP_XA           NUMBER,
    NGAY_HIEU_LUC       TIMESTAMP(6),
    NGAY_HET_HIEU_LUC   TIMESTAMP(6)
);
```

---

## 3. Yêu cầu chi tiết từng phần

### Phần 1: Tạo script giả lập dữ liệu nguồn & Đồng bộ về vùng Mirror

Vì thực hiện trên môi trường thực tập (không kết nối trực tiếp vào DB giao dịch thật), các bạn cần viết script sinh dữ liệu giả lập trên bảng nguồn trước, sau đó mới thực hiện đồng bộ.

1. **Viết bộ script sinh dữ liệu nguồn và giả lập biến động (Data Generator):**
   - **Script 1 – Sinh dữ liệu ban đầu (Initial Seed):**
     - Sinh dữ liệu cho 3 bảng danh mục và bảng `VSS.VSS_HO_GD_NHANKHAU` (số lượng người tùy chỉnh theo tham số, ví dụ 50.000 - 200.000 người).
     - Phải **chủ đích cài cắm các trường hợp dữ liệu thực tế và dữ liệu lỗi** theo tỷ lệ nhất định để tầng chuẩn hóa có dữ liệu xử lý và bắt lỗi:
       - **Trùng công dân ở nhiều hộ:** Một `MA_CD` (ví dụ `'0120456789'`) xuất hiện ở 2-3 dòng với `MA_HO_GD` khác nhau và `MODIFY_DATE` khác nhau.
       - **Dữ liệu bị lọc nghiệp vụ:** Cài khoảng 2-3% dòng thuộc diện biến động giảm `TYPEID IN ('GT','GM','GG','GK')` hoặc hồ sơ chưa duyệt/từ chối/hủy `TRANG_THAI_DUYET IN (1,3,4)` để kiểm tra bộ lọc.
       - **Các dạng dữ liệu của `MA_TINH` (và `MA_TINH_LH`, `MA_TINH_KS`):**
         - Dạng `'NNTTT'` (chiếm đa số, ví dụ `'01TTT'` Hà Nội, `'33TTT'` Hưng Yên, hoặc mã tỉnh cũ đã sáp nhập như `'34TTT'` Thái Bình cũ).
         - Dạng 3 chữ số `'0NN'` (ví dụ `'001'`, `'034'`, `'079'`).
         - Dạng 1-2 chữ số `'NN'` hoặc `'N'` (ví dụ `'01'`, `'1'`, `'34'`, `'79'`).
         - Mã rỗng/không xác định: `'00TTT'`, `'000'`, `'00'`, hoặc `NULL`.
         - Mã tỉnh sai không tồn tại trong danh mục: ví dụ `'99TTT'`.
       - **Các dạng dữ liệu của `MA_XA` (và `MA_XA_LH`, `MA_XA_KS`):**
         - Mã xã 5 chữ số đang hiện hành (ví dụ `'00004'`).
         - Mã xã cũ 5 chữ số trước sáp nhập (ví dụ `'00013'` - nằm trong `MA_DBHC_SN` của xã `'00004'`).
         - Mã xã bị mất số `0` ở đầu do lỗi ép kiểu số (ví dụ lưu `'4'` thay vì `'00004'`, hoặc `'13'` thay vì `'00013'`).
         - Mã xã cũ bị tách ra nhiều xã mới (1 mã xã cũ xuất hiện trong `MA_DBHC_SN` của 2 xã mới khác nhau -> trường hợp mơ hồ).
         - **Tỉnh lệch Xã:** Ví dụ `MA_XA = '00004'` (thuộc Hà Nội `'01TTT'`), nhưng cột `MA_TINH` của dòng đó lại ghi `'79TTT'` (TP.HCM).
         - Giá trị không xác định hoặc sai: `'00000'`, mã sai `'99999'`, hoặc `NULL`.
       - **Ngày sinh (`NGAY_SINH` kiểu `NUMBER`):**
         - Đúng chuẩn 8 số `YYYYMMDD` (ví dụ `19950815`).
         - Chỉ có năm sinh dạng `YYYY0000` (ví dụ `19950000`) hoặc 4 số `1995`.
         - Chỉ có năm và tháng sinh dạng `YYYYMM00` (ví dụ `19950800`) hoặc 6 số `199508`.
         - Ngày sinh sai lịch (ví dụ `19950230` - ngày 30/02, hoặc năm sinh `17500101`) hoặc `NULL`.
       - **Các trường khác:**
         - `SO_CCCD` / `SO_CMTND`: người có đủ 12 số hợp lệ (`'001095012345'`), người chỉ có CMND 9 số cũ (`'123456789'`) hoặc `NULL`.
         - `PHONE_NUMBER`: `'0912345678'`, `'84912345678'`, `'840912345678'`, thiếu số 0 đầu `'912345678'`, có khoảng trắng `'0912 345 678'`, hoặc số sai `'12345'`.
         - `QUOC_TICH`: mã `'VN'`, hoặc lưu dạng ID `'233'`, hoặc mã sai không tồn tại `'XX'`.
         - `DM_DAN_TOC_ID`: ID hợp lệ (ví dụ `1`), hoặc ID không tồn tại (ví dụ `9999`).
         - `HO_TEN` / `HO_TEN_UNSIGNED`: có dòng bị thừa nhiều khoảng trắng ở đầu/giữa/cuối (ví dụ `'  Nguyễn   Văn  An '`).
   - **Script 2 – Đẩy dữ liệu thay đổi vào bảng nguồn (Change / Delta Generator):**
     - Dùng để chạy sau khi pipeline đã lên, nhằm kiểm tra luồng **CDC -> Near Real-time C360 -> Đối soát, BI & API** có hoạt động đúng không.
     - Script cần tạo ra đủ các kịch bản biến động trực tiếp trên bảng nguồn:
       - **Thêm lô nhân khẩu mới (`INSERT`):** Sinh thêm một lô công dân mới (ví dụ 50.000 - 100.000 dòng) và trường hợp công dân cũ phát sinh thêm dòng ở hộ gia đình mới (`MA_HO_GD` mới) với `MODIFY_DATE` mới hơn.
       - **Cập nhật thông tin (`UPDATE`):**
         - Sửa thông tin của một số công dân đã có (bổ sung CCCD 12 số cho người trước đó thiếu, sửa lại mã xã/ngày sinh từ sai thành đúng để xem cờ lỗi `DQ_FLAGS` có tự động được gỡ không).
         - Chuyển `TRANG_THAI_DUYET` sang `4` hoặc `TYPEID` sang `'GG'` để xem công dân đó có tự động bị loại khỏi bảng C360 không (và ngược lại: duyệt hồ sơ từ `1` sang `2` để xem công dân có tự động xuất hiện trên C360 không).
         - **Đổi mã công dân (`UPDATE` cột `MA_CD`):** Đổi `MA_CD` của một bản ghi sang mã mới (đồng thời lưu mã cũ vào `MA_CD_OLD`) để kiểm tra xem bảng C360 có tạo mã mới và xóa sạch mã cũ không.
       - **Xóa bản ghi (`DELETE`):** Xóa bớt 1 dòng hộ mới nhất của người có nhiều hộ (xem bảng C360 có tự lùi về lấy dòng hộ cũ còn lại không), hoặc xóa toàn bộ các dòng của một công dân ở nguồn (xem công dân đó có bị xóa khỏi C360 không).

2. **Job nạp toàn bộ (Full Load):**
   - Thiết kế các bảng Mirror bên DB đích (bàn bạc xem ở bảng Mirror có nên giữ Partition như bảng nguồn không hay để bảng phẳng, đánh Unique Index trên `(MA_CD, MA_HO_GD)` ở thời điểm nào để không làm chậm lúc nạp Full Load, giải thích lý do trong thiết kế).
   - Viết job đồng bộ 3 bảng danh mục và bảng chính `VSS.VSS_HO_GD_NHANKHAU` sang DB đích.
   - Tìm cách chia partition khi đọc JDBC trên cột chuỗi `MA_CD` để đọc song song mà không làm treo DB nguồn; xử lý các cột `NUMBER` không khai báo precision/scale và không dùng `.cache()`/`.repartition()` gây tràn RAM (OOM) hoặc tràn ổ đĩa shuffle.
   - Đảm bảo chạy lại job không bị nhân đôi dữ liệu và có bước kiểm tra số dòng giữa nguồn và đích sau khi nạp xong.

3. **Luồng CDC (Kafka + Debezium):**
   - Cấu hình Debezium Source đọc thay đổi (`INSERT`/`UPDATE`/`DELETE`) từ bảng nguồn đẩy vào Kafka, và Sink Connector ghi cập nhật (`upsert` + `delete`) vào bảng Mirror bên đích.
   - Xác định đúng khóa để cấu hình Sink và Unique Index trên bảng Mirror sao cho khi 1 công dân (`MA_CD`) ở nhiều hộ (`MA_HO_GD`) có biến động thì không bị ghi đè mất hộ hoặc lỗi constraint.
   - Giải quyết bài toán vận hành: làm sao để bật CDC nối tiếp sau khi chạy Full Load mà không bị mất hoặc trùng dữ liệu? Nếu chạy Script đẩy hàng trăm nghìn dòng thay đổi cùng lúc gây lag ở Kafka/Sink thì xử lý thế nào?

---

### Phần 2: Chuẩn hóa dữ liệu công dân (C360)

Từ dữ liệu thô trên bảng Mirror, tự thiết kế bảng đích chuẩn hóa thông tin cá nhân (mỗi công dân chỉ có 1 dòng duy nhất) và xây dựng luồng ETL xử lý.

Yêu cầu xử lý dữ liệu:
1. **Lọc và khử trùng:**
   - **Bộ lọc nghiệp vụ:** Loại bỏ các bản ghi thuộc diện đã giảm/đóng mã (`NVL(TYPEID, 'ZZ') IN ('GT','GM','GG','GK')`) và các bản ghi đang chờ duyệt, bị từ chối hoặc đã hủy (`NVL(TRANG_THAI_DUYET, 0) IN (1,3,4)`). Chỉ giữ lại các bản ghi nhân khẩu đang có hiệu lực.
   - **Khử trùng theo `MA_CD`:** Trong các dòng hợp lệ của cùng một `MA_CD`, chỉ chọn 1 dòng mới nhất theo `MODIFY_DATE` (nếu `MODIFY_DATE` bằng nhau thì xét tiếp `CREATED_DATE` và `ID` giảm dần).
2. **Tiền xử lý bảng Map & Chuẩn hóa địa chỉ (Tỉnh/Xã 2 cấp sau sáp nhập 2025):**
   - **Thiết kế View và Bảng Map trung gian:** Cột `MA_DBHC_SN` trong danh mục địa bàn lưu chuỗi nhiều mã cũ phân cách bằng dấu `;` (ví dụ `'00013;00004;00019'`). Không được dùng `REGEXP` tách chuỗi trực tiếp lúc JOIN với bảng nhân khẩu lớn vì sẽ rất chậm. Cần xây dựng View lọc danh mục đang hiệu lực và tách sẵn chuỗi `MA_DBHC_SN` ra 2 bảng tra cứu phẳng cho Tỉnh và Xã (có Primary Key), phân biệt rõ nguồn map:
     - Ưu tiên 1 (`HIEN_HANH`): Mã đang còn hiệu lực thì map vào chính nó.
     - Ưu tiên 2 (`SAP_NHAP`): Mã cũ trong `MA_DBHC_SN` chỉ thuộc về duy nhất 1 đơn vị mới thì map sang đơn vị mới đó.
     - Ưu tiên 3 (`MO_HO`): Mã xã cũ nằm trong `MA_DBHC_SN` của $\ge 2$ xã mới (xã cũ bị tách) thì không tự đoán `MA_XA` (để `NULL`), gắn trạng thái `'MO_HO'`, nhưng nếu các xã mới đó cùng thuộc 1 tỉnh thì vẫn giữ được `MA_TINH`.
   - **Quy tắc làm mới bảng Map:** Khi làm mới dữ liệu 2 bảng Map này, phải thực hiện `DELETE + INSERT` trong cùng 1 transaction (không dùng `TRUNCATE`) để luồng cập nhật Near Real-time đang chạy song song không bao giờ đọc phải bảng Map rỗng giữa chừng.
   - **Chuẩn hóa cho cả 3 nhóm địa chỉ (Thường trú, Liên hệ `_LH`, Nơi khai sinh `_KS`):**
     - Nếu Xã tra cứu được Tỉnh thì ưu tiên lấy Tỉnh theo Xã; nếu Tỉnh suy ra từ Xã khác với Tỉnh khai báo ở cột `MA_TINH` thì vẫn lấy theo Xã và gắn cờ lệch (`TINH_LECH_XA`).
     - Giữ lại giá trị gốc của nguồn ở các cột `*_GOC` để đối soát.
3. **Làm sạch các trường thông tin khác:**
   - Cắt bỏ khoảng trắng thừa ở Họ tên và tạo cột Họ tên không dấu viết hoa.
   - Chuẩn hóa ngày sinh về dạng chuỗi (`YYYYMMDD` nếu đủ ngày tháng năm hợp lệ, `YYYYMM` nếu chỉ có năm-tháng như `19950800`, `YYYY` nếu chỉ có năm như `19950000`; nếu sai lịch hoặc năm sinh ngoài khoảng hợp lý thì để `NULL` và giữ giá trị gốc ở `NGAY_SINH_GOC`).
   - Chuẩn hóa Số định danh 12 chữ số (lấy từ `SO_CCCD`, nếu không có 12 số thì xét tiếp `SO_CMTND`).
   - Chuẩn hóa Số điện thoại về dạng chuẩn bắt đầu bằng số `0` (10 hoặc 11 chữ số).
   - Join với bảng danh mục để lấy mã và tên Dân tộc, Quốc tịch (hỗ trợ cả trường hợp nguồn lưu mã `'VN'` lẫn trường hợp nguồn lưu ID `'233'`).
4. **Đánh cờ chất lượng dữ liệu (`DQ_FLAGS`):**
   - Những dòng có dữ liệu lỗi (không map được tỉnh/xã, tỉnh lệch xã, xã mơ hồ, ngày sinh lỗi, SĐT lỗi, quốc tịch/dân tộc không map được, không có số định danh 12 số...) vẫn giữ lại trong bảng chuẩn hóa và nối các mã lỗi vào cột `DQ_FLAGS` (ngăn cách bằng dấu `;`).
5. **Cơ chế cập nhật kép (Full Load & Near Real-time qua Queue):**
   - Cần hỗ trợ cả 2 cách chạy dùng chung **duy nhất 1 nguồn logic chuẩn hóa** (tránh sửa 1 nơi quên 1 nơi làm lệch kết quả giữa Full Load và Near Real-time):
     - **Chạy Full Load:** Nạp lại toàn bộ bảng chuẩn hóa.
     - **Chạy cập nhật Near Real-time (trong vòng 1 phút):**
       - Thiết kế Trigger trên bảng Mirror để ghi nhận khóa `MA_CD` thay đổi vào bảng hàng đợi (`Queue`). Lưu ý: Trigger phải thật nhẹ (chỉ `INSERT` khóa vào Queue) để không làm chậm CDC Sink; và khi dòng trên Mirror bị `UPDATE` đổi `MA_CD` (`:OLD.MA_CD <> :NEW.MA_CD`), phải đẩy **cả mã mới lẫn mã cũ** vào Queue.
       - Thiết kế Procedure / Job chạy định kỳ mỗi phút đọc từng lô từ Queue: `MERGE` lại các `MA_CD` còn dòng hợp lệ ở Mirror, đồng thời **`DELETE` khỏi bảng C360** những `MA_CD` trong lô không còn bất kỳ dòng hợp lệ nào ở Mirror (do đã bị xóa ở nguồn, bị đổi `MA_CD`, hoặc bị chuyển sang `TYPEID`/`TRANG_THAI_DUYET` bị loại).
   - **Xử lý đua tranh (Concurrency & Sync Control):**
     - Khi nhiều transaction CDC cùng ghi vào Queue, các `ID` tự tăng có thể commit không theo thứ tự. Phải thiết kế cách cắt lô (`v_cut`) và xóa Queue sao cho không xóa nhầm hoặc bỏ sót các dòng chưa kịp commit.
     - Thiết kế bảng điều khiển (`Sync Control`) có cơ chế khóa dòng (`SELECT ... FOR UPDATE`): khi chạy Full Load phải đặt cờ tạm dừng, chờ lô Near Real-time đang chạy dở kết thúc hẳn mới được `TRUNCATE` bảng C360, và chỉ xóa các dòng Queue đã nằm trong snapshot của lần Full Load đó (giữ lại các thay đổi phát sinh trong lúc Full Load đang chạy để phút sau xử lý tiếp).

---

### Phần 3: Đối soát dữ liệu, Báo cáo BI và Dịch vụ API

Từ dữ liệu trên các bảng Nguồn, Mirror và bảng chuẩn hóa C360, xây dựng các đầu ra khai thác dữ liệu:

1. **Viết Job / Script Đối soát dữ liệu giữa các tầng (Data Reconciliation):**
   - **Đối soát Nguồn <-> Mirror:** So sánh tổng số bản ghi và số bản ghi theo từng mã tỉnh giữa `VSS.VSS_HO_GD_NHANKHAU` và bảng Mirror để phát hiện lệch hoặc trễ CDC.
   - **Đối soát Mirror <-> C360:**
     - Kiểm tra số lượng công dân hợp lệ duy nhất (`COUNT(DISTINCT MA_CD)` thỏa điều kiện lọc `TYPEID` và `TRANG_THAI_DUYET`) trên bảng Mirror có khớp 100% với tổng số dòng (`COUNT(*)`) trên bảng chuẩn hóa C360 hay không.
     - Kiểm tra mẫu các công dân có nhiều dòng hộ gia đình (`MA_HO_GD`) trên bảng Mirror xem thông tin trên bảng C360 (như địa chỉ, `SO_CCCD`, ngày sinh...) đã lấy đúng theo dòng có `MODIFY_DATE` mới nhất chưa.
     - Kiểm tra không còn tồn tại bản ghi nào trên C360 thuộc diện `MA_CD` đã bị xóa hết ở nguồn hoặc đã chuyển sang `TYPEID`/`TRANG_THAI_DUYET` không hợp lệ.
   - **Đối soát kết quả Chuẩn hóa & Chất lượng dữ liệu:** Tổng hợp số lượng bản ghi sạch hoàn toàn và số lượng bản ghi bị dính từng loại cờ lỗi trong `DQ_FLAGS`.

2. **Xây dựng Dashboard BI:**
   - Tạo các bảng tổng hợp (Data Mart) hoặc View cần thiết và kết nối công cụ BI (Superset / Metabase / PowerBI...) để dựng 3 nhóm biểu đồ:
     - **Nhóm 1 – Thống kê Nhân khẩu & Định danh theo Địa bàn:**
       - Số lượng công dân theo Tỉnh/Thành phố và Xã/Phường mới (sau sáp nhập), phân bổ theo độ tuổi, giới tính, tình trạng còn sống (`IS_ALIVE`).
       - Tỷ lệ công dân đã có Số định danh / CCCD 12 số hợp lệ so với số chưa có (chỉ mới có CMND 9 số hoặc trống) theo từng Tỉnh/Thành phố.
     - **Nhóm 2 – Báo cáo Chất lượng Dữ liệu (Data Quality):**
       - Thống kê số lượng và tỷ lệ % bản ghi mắc từng loại lỗi trong `DQ_FLAGS` (`TINH_KHONG_MAP`, `XA_KHONG_MAP`, `XA_MO_HO`, `TINH_LECH_XA`, `NGAY_SINH_LOI`, `SDT_LOI`, `KHONG_SO_DINH_DANH`...).
       - Biểu đồ so sánh tỷ lệ dữ liệu lỗi giữa các Tỉnh/Thành phố để biết địa phương nào đang có nhiều dữ liệu bẩn cần làm sạch.
     - **Nhóm 3 – Giám sát Vận hành Đồng bộ (Sync Monitoring):**
       - Theo dõi lịch sử các lô chạy Near Real-time (số khóa `MA_CD` biến động mỗi phút, số dòng `MERGE`, số dòng `DELETE`, thời gian xử lý mỗi lô và các lô bị lỗi nếu có).

3. **Xây dựng Dịch vụ REST API Tra cứu C360:**
   - Thiết kế bộ Index tối ưu trên bảng C360 và xây dựng ứng dụng API cung cấp dữ liệu cho các hệ thống nội bộ:
     - **API 1 – Tra cứu cá nhân theo mã (`GET /api/v1/ca-nhan/{ma}`):** Hỗ trợ chế độ tự động dò theo thứ tự ưu tiên: `MA_SO_BHXH` (mã hiện hành) $\rightarrow$ `MA_SO_BHXH_CU` (mã cũ) $\rightarrow$ `SO_DINH_DANH` (12 số), dừng ở tầng đầu tiên tìm thấy. Trả về `404` nếu không tìm thấy, và trả về `409 Conflict` kèm danh sách ứng viên nếu 1 mã cũ hoặc 1 số định danh khớp với $\ge 2$ người khác nhau.
     - **API 2 – Tra cứu theo lô (`POST /api/v1/ca-nhan/tra-cuu-lo`):** Nhận danh sách mã (tối đa 1.000 mã/lần) và trả về kết quả từng mã.
     - **API 3 – Tìm kiếm theo Họ tên + Ngày sinh (`GET /api/v1/ca-nhan/tim-kiem`):** Hỗ trợ nhập họ tên có dấu hoặc không dấu (tự động chuẩn hóa về không dấu viết hoa để tìm) kết hợp ngày sinh (`YYYY`, `YYYYMM` hoặc `YYYYMMDD` - khớp được cả người chỉ có năm/tháng sinh rút gọn trên DB).
     - **API 4 – Lấy danh sách bản ghi thay đổi (`GET /api/v1/ca-nhan/thay-doi`):** Phân trang theo con trỏ (`cursor` dựa trên `SYNC_DATE` và `ID`), chỉ trả các bản ghi có `SYNC_DATE <= SYSDATE - độ trễ an toàn (ví dụ 120 giây)` để tránh bỏ sót các bản ghi thuộc lô Near Real-time đang commit dở.
     - **Bảo mật & Audit Log:** Xác thực bằng `X-API-Key` (DB chỉ lưu mã băm SHA-256 của key, phân quyền theo từng scope API) và ghi nhật ký tra cứu bất đồng bộ theo lô (Async Batch Audit Log) để không làm chậm tốc độ phản hồi của API.

---

## 4. Kế hoạch thực hiện trong 4 tuần

### Tuần 1: Khảo sát, Thiết kế, Sinh dữ liệu giả lập và làm phần Đồng bộ (Nguồn -> Mirror)
- Khảo sát cấu trúc 4 bảng nguồn và quy tắc dữ liệu.
- Viết tài liệu Basic Design và Detail Design cho toàn bộ luồng, liệt kê các vấn đề chưa rõ vào mục QA để chốt.
- Viết script sinh dữ liệu giả lập ban đầu (có cài cắm dữ liệu bẩn theo các ví dụ ở Phần 1) và script đẩy biến động dữ liệu nguồn.
- Thiết kế bảng Mirror, code job Full Load và cấu hình Kafka + Debezium (Source/Sink) để đồng bộ CDC.

### Tuần 2: Làm phần Chuẩn hóa dữ liệu & Đồng bộ Near Real-time (Mirror -> C360)
- Thiết kế View danh mục, 2 bảng Map địa bàn hành chính (`C360_MAP_DBHC_TINH`/`_XA`), bảng đích `C360_THONG_TIN_CA_NHAN` và các bảng điều khiển/hàng đợi/log (`Queue`, `Sync Control`, `Sync Log`).
- Viết logic chuẩn hóa dữ liệu dùng chung (khử trùng, lọc nghiệp vụ, map Tỉnh/Xã, làm sạch ngày sinh/SĐT/CCCD, gán cờ `DQ_FLAGS`).
- Xây dựng job chạy Full Load và cơ chế Trigger + Procedure cập nhật Near Real-time mỗi phút.
- Chạy script đẩy dữ liệu thay đổi ở nguồn để test thực tế luồng CDC -> Near Real-time C360 (bao gồm cả thêm mới, sửa, đổi `MA_CD`, xóa và chuyển trạng thái không hợp lệ).

### Tuần 3: Làm Job Đối soát, Dashboard BI và Dịch vụ REST API
- Viết script/job đối soát tự động số liệu giữa 3 tầng (Nguồn <-> Mirror <-> C360) và thống kê kết quả chuẩn hóa.
- Thiết kế các bảng tổng hợp/view phục vụ báo cáo và dựng 3 nhóm Dashboard BI theo yêu cầu ở Phần 3.
- Đánh Index trên bảng C360 và xây dựng dịch vụ REST API tra cứu C360 (4 API + xác thực `X-API-Key` + Audit Log).

### Tuần 4: Lập lịch Airflow, Kiểm thử đối soát toàn trình và Nghiệm thu
- Đưa các job đã viết lên Airflow DAGs (cấu hình lịch chạy, tham số, retry).
- Chạy kịch bản kiểm thử toàn trình: Sinh dữ liệu ban đầu -> Full Load -> Bật CDC & Near Real-time -> Chạy script đẩy biến động (`INSERT`/`UPDATE`/`DELETE`) ở nguồn -> Kiểm tra dữ liệu cập nhật sang Mirror, C360, BI và gọi thử qua REST API.
- Chạy job đối soát số liệu giữa các tầng trước và sau khi đẩy biến động để chứng minh dữ liệu không bị lệch.
- Hoàn thiện tài liệu, báo cáo kết quả test và nghiệm thu cuối kỳ.

---

## 5. Yêu cầu khi nghiệm thu

1. **Tài liệu:** Có đủ Basic Design và Detail Design khớp với code thực tế, không còn câu hỏi bỏ ngỏ trong sheet QA.
2. **Code & Cấu hình:**
   - Tổ chức thư mục gọn gàng, tách riêng cấu hình DB/môi trường ra file `.env` hoặc config, không hard-code thông tin nhạy cảm trong code.
   - Không đẩy dữ liệu nặng hoặc file rác lên Git.
3. **Chất lượng dữ liệu, Kiểm thử biến động & Hiệu năng:**
   - Có bộ script sinh dữ liệu ban đầu và script giả lập thay đổi (`INSERT`/`UPDATE`/`DELETE`) bao phủ đủ các tình huống dữ liệu bẩn và biên.
   - Demo trực tiếp việc chạy script thay đổi dữ liệu ở nguồn và chứng minh sau 1-2 phút bảng Mirror, bảng C360, báo cáo BI và API cập nhật đúng (cả trường hợp thêm người mới, sửa thông tin làm thay đổi cờ lỗi, đổi `MA_CD`, và xóa/chuyển trạng thái không hợp lệ làm bản ghi biến mất khỏi C360).
   - Có câu lệnh và số liệu đối soát chứng minh tổng số công dân hợp lệ ở bảng chuẩn hóa luôn khớp 100% với `COUNT(DISTINCT MA_CD)` thỏa điều kiện lọc ở bảng Mirror.
   - Các truy vấn/job xử lý và các API tra cứu được đánh index phù hợp, không quét full bảng trên các thao tác tra cứu đơn lẻ và không bị OOM khi chạy lô lớn.
