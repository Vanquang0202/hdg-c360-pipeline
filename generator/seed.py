"""Generate identifiable HDG fixtures for the supplied VSS schema.

PowerShell from the project root:
  $env:ORACLE_USER = "VSS"
  $env:ORACLE_PASSWORD = [System.Net.NetworkCredential]::new("", (Read-Host "Oracle password" -AsSecureString)).Password
  $env:ORACLE_DSN = "localhost:1521/YOUR_SERVICE"
  python generator/seed.py seed --count 50000 --batch-size 1000
  python generator/seed.py counts
  python generator/seed.py fixtures

--count is the total number of household rows, including fixed fixtures.
Repeated citizen codes in different households reduce the distinct-person count.
Small datasets retain all fixed cases, so the exclusion ratio can exceed 2.5%.
For 50000 rows the excluded-row target is exactly 2.5%.
Seed uses one transaction and requires all four tables to be empty.
Re-running after success stops with counts; no reset/delete command is provided.
Configuration comes from environment variables, without a local secret file.
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from itertools import islice

PEOPLE = "VSS.VSS_HO_GD_NHANKHAU"
ETHNICITY = "VSS.VSS_DM_DANTOC"
NATIONALITY = "VSS.VSS_DM_QUOC_TICH"
GEO = "VSS.VSS_DM_DBHC"
TABLES = (ETHNICITY, NATIONALITY, GEO, PEOPLE)
BASE_DATE = datetime(2025, 7, 1)

# All geography and identity values are test fixtures. The shared old code
# 00019 is deliberately ambiguous; 00013 maps to exactly one new commune.
CATALOGS = {
    ETHNICITY: [
        dict(ID=1, CAN_CU="HDG fixture", MA="01", NGAY_HL=BASE_DATE,
             NGAY_HET_HL=None, TEN="Kinh"),
        dict(ID=2, CAN_CU="HDG fixture", MA="02", NGAY_HL=BASE_DATE,
             NGAY_HET_HL=None, TEN="Tày"),
        dict(ID=3, CAN_CU="HDG fixture", MA="01", NGAY_HL=datetime(2020, 1, 1),
             NGAY_HET_HL=datetime(2025, 6, 30), TEN="Kinh"),
    ],
    NATIONALITY: [
        dict(ID=233, MA="VN", TEN="Việt Nam"),
        dict(ID=234, MA="LA", TEN="Lào"),
    ],
    GEO: [
        dict(ID=index, MA_DBHC=code, MA_CHA=parent, TEN=name,
             TINH_TRANG=active, STATUS=active, CREATED_DATE=BASE_DATE,
             MA_DBHC_SN=aliases, NGAY_HIEU_LUC=BASE_DATE)
        for index, (code, parent, name, aliases, active) in enumerate([
            ("01TTT", None, "Hà Nội", "01", "1"),
            ("33TTT", None, "Hưng Yên", "33;34", "1"),
            ("79TTT", None, "TP.HCM", "79", "1"),
            ("00004", "01TTT", "Xã giả lập 04", "00013;00004;00019", "1"),
            ("00008", "01TTT", "Xã giả lập 08", "00019", "1"),
            ("12457", "33TTT", "Xã giả lập 12457", None, "1"),
            ("26734", "79TTT", "Xã giả lập 26734", None, "1"),
            ("00013", "01TTT", "Xã cũ không hiệu lực", None, "0"),
        ], start=1)
    ],
}


def citizen_code(index):
    return f"90{index:08d}"


def person(index, label="normal"):
    code = citizen_code(index)
    return dict(
        ID=f"HDG-SEED-{index:08d}",
        MA_CD=code,
        MA_CD_OLD=None,
        MA_HO_GD=f"HDG-H-{index:08d}",
        HO_TEN="Nguyễn Văn An" if index % 2 else "Trần Thị Bình",
        HO_TEN_UNSIGNED="NGUYEN VAN AN" if index % 2 else "TRAN THI BINH",
        GIOI_TINH=str(index % 2),
        IS_ALIVE="1",
        NGAY_SINH=19950815,
        SO_CCCD=f"0000{index:08d}",
        SO_CMTND=None,
        PHONE_NUMBER=f"09{index:08d}",
        DIA_CHI="Địa chỉ giả lập",
        DC_LIEN_LAC="Liên hệ giả lập",
        NOI_KS="Nơi khai sinh giả lập",
        MA_TINH="01TTT", MA_XA="00004",
        MA_TINH_LH="01TTT", MA_XA_LH="00004",
        MA_TINH_KS="01TTT", MA_XA_KS="00004",
        QUOC_TICH="VN", DM_DAN_TOC_ID=1,
        TYPEID=None, TRANG_THAI_DUYET=2,
        CREATED_DATE=BASE_DATE, MODIFY_DATE=BASE_DATE,
        GHI_CHU=f"HDG-SEED:{label}",
    )


def address(province, commune):
    return {
        key: value
        for suffix in ("", "_LH", "_KS")
        for key, value in ((f"MA_TINH{suffix}", province), (f"MA_XA{suffix}", commune))
    }


def fixtures():
    # Put independent CDC targets first. Deleting the latest household leaves
    # its older row available for the separate delete-last scenario.
    cases = [
        ("cdc_repair", dict(SO_CCCD=None, NGAY_SINH=19950230,
                            **address("99TTT", "99999"))),
        ("cdc_add_household", {}),
        ("cdc_move_household", {}),
        ("cdc_change_code", {}),
        ("cdc_cancel_approval", {}),
        ("cdc_close_type", {}),
        ("cdc_approve_pending", dict(TRANG_THAI_DUYET=1)),
        ("cdc_delete_older", {}),
        ("cdc_delete_latest", dict(MA_CD=citizen_code(8),
                                   MODIFY_DATE=BASE_DATE + timedelta(days=1))),
        ("duplicate_payload", {}),
        ("duplicate_payload_new_household", dict(MA_CD=citizen_code(10))),
        ("null_household", dict(MA_HO_GD=None)),
        ("missing_fields", dict(HO_TEN=None, NGAY_SINH=None, PHONE_NUMBER=None,
                                SO_CCCD=None, QUOC_TICH=None, DM_DAN_TOC_ID=None)),
        ("name_spaces", dict(HO_TEN="  Nguyễn   Văn  An ",
                             HO_TEN_UNSIGNED="  NGUYEN   VAN  AN ")),
        ("cmnd_9", dict(SO_CCCD=None, SO_CMTND="123456789")),
        ("cmnd_12_fallback", dict(SO_CCCD="12345", SO_CMTND="000123456789")),
        ("nationality_id", dict(QUOC_TICH="233")),
        ("unknown_catalogs", dict(QUOC_TICH="XX", DM_DAN_TOC_ID=9999)),
        ("province_old_ttt", address("34TTT", "12457")),
        ("province_old_3_digits", address("034", "12457")),
        ("province_old_2_digits", address("34", "12457")),
        ("province_current_3_digits", address("001", "00004")),
        ("province_current_2_digits", address("01", "00004")),
        ("province_current_1_digit", address("1", "00004")),
        ("commune_old", address("01TTT", "00013")),
        ("commune_old_unpadded", address("01TTT", "13")),
        ("commune_current_unpadded", address("01TTT", "4")),
        ("commune_ambiguous", address("01TTT", "00019")),
        ("province_commune_mismatch", address("79TTT", "00004")),
        ("zero_ttt", address("00TTT", "00000")),
        ("zero_3_digits", address("000", "00000")),
        ("zero_2_digits", address("00", "00000")),
        ("address_missing", address(None, None)),
        ("address_unknown", address("99TTT", "99999")),
        ("birth_year_zeros", dict(NGAY_SINH=19950000)),
        ("birth_year", dict(NGAY_SINH=1995)),
        ("birth_month_zeros", dict(NGAY_SINH=19950800)),
        ("birth_month", dict(NGAY_SINH=199508)),
        ("birth_bad_calendar", dict(NGAY_SINH=19950230)),
        ("birth_very_old", dict(NGAY_SINH=17500101)),
        ("phone_country_code", dict(PHONE_NUMBER="84912345678")),
        ("phone_country_code_with_zero", dict(PHONE_NUMBER="840912345678")),
        ("phone_without_zero", dict(PHONE_NUMBER="912345678")),
        ("phone_spaces", dict(PHONE_NUMBER="0912 345 678")),
        ("phone_invalid", dict(PHONE_NUMBER="12345")),
        ("approval_null", dict(TRANG_THAI_DUYET=None)),
        ("approval_zero", dict(TRANG_THAI_DUYET=0)),
        ("approval_rejected", dict(TRANG_THAI_DUYET=3)),
        ("approval_cancelled", dict(TRANG_THAI_DUYET=4)),
        *[(f"type_{code}", dict(TYPEID=code)) for code in ("GT", "GM", "GG", "GK")],
        ("third_household", dict(MA_CD=citizen_code(10),
                                 MODIFY_DATE=BASE_DATE + timedelta(days=2))),
    ]
    rows = {}
    for index, (label, overrides) in enumerate(cases, start=1):
        rows[label] = person(index, label) | overrides
    # Duplicate personal data across two household rows, with distinct row IDs.
    original = rows["duplicate_payload"]
    duplicate = rows["duplicate_payload_new_household"]
    for column in ("HO_TEN", "SO_CCCD", "PHONE_NUMBER", "GIOI_TINH"):
        duplicate[column] = original[column]
    return rows


def excluded(row):
    return row["TYPEID"] in ("GT", "GM", "GG", "GK") or row["TRANG_THAI_DUYET"] in (1, 3, 4)


def population(count):
    fixed = fixtures()
    yield from fixed.values()
    extra_excluded = max(0, round(count * 0.025) - sum(map(excluded, fixed.values())))
    for index in range(len(fixed) + 1, count + 1):
        row = person(index)
        if index <= len(fixed) + extra_excluded:
            row.update(TYPEID="GT", GHI_CHU="HDG-SEED:excluded_extra")
        yield row


def positive_int(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def connect():
    names = ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))
    import oracledb

    return oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=os.environ["ORACLE_DSN"],
    )


def insert_batches(cursor, table, rows, batch_size):
    iterator = iter(rows)
    columns = None
    inserted = 0
    while batch := list(islice(iterator, batch_size)):
        if columns is None:
            columns = tuple(batch[0])
            placeholders = ", ".join(f":{i}" for i in range(1, len(columns) + 1))
            statement = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(statement, [tuple(row[col] for col in columns) for row in batch])
        inserted += cursor.rowcount
    return inserted


def counts(cursor):
    result = {}
    for table in TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        result[table] = cursor.fetchone()[0]
    return result


def seed(cursor, count, batch_size):
    # Lock before checking emptiness, including the tables without a PK.
    # The caller commits only after the counts have been verified.
    for table in TABLES:
        cursor.execute(f"LOCK TABLE {table} IN EXCLUSIVE MODE NOWAIT")
    current = counts(cursor)
    if any(current.values()):
        raise RuntimeError(f"Seed requires four empty source tables. Current counts: {current}")
    expected = {table: len(rows) for table, rows in CATALOGS.items()}
    expected[PEOPLE] = count
    for table, rows in CATALOGS.items():
        insert_batches(cursor, table, rows, batch_size)
    insert_batches(cursor, PEOPLE, population(count), batch_size)
    result = counts(cursor)
    if result != expected:
        raise RuntimeError(f"Count mismatch: expected {expected}, got {result}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    load = commands.add_parser("seed", help="Seed all four tables if they are empty")
    load.add_argument("--count", type=positive_int, default=50_000, help="Number of rows, including fixtures")
    load.add_argument("--batch-size", type=positive_int, default=1_000)
    commands.add_parser("counts", help="Read total row counts from all four tables")
    commands.add_parser("fixtures", help="List fixture labels, IDs and citizen/household codes offline")
    args = parser.parse_args()
    if args.command == "fixtures":
        for label, row in fixtures().items():
            print(label, row["ID"], row["MA_CD"], row["MA_HO_GD"])
        return
    if args.command == "seed" and not len(fixtures()) <= args.count <= 99_999_999:
        parser.error(f"--count must be between {len(fixtures())} and 99999999")
    with connect() as connection, connection.cursor() as cursor:
        if args.command == "seed":
            result = seed(cursor, args.count, args.batch_size)
            connection.commit()
        else:
            result = counts(cursor)
    for table, count in result.items():
        print(f"{table}: {count}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ImportError) as error:
        sys.exit(str(error))
