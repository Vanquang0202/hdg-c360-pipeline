# Mục đích: Đọc DDL Oracle thành hợp đồng kiểu dữ liệu dùng chung cho Full Load và Sink.
# Đầu vào: DDL nguồn nguyên bản, không tự thêm cột hoặc giả định khóa.
# Đầu ra: Đủ 132 cột cùng độ dài, BYTE/CHAR, precision/scale, NULL và PK đã khai báo.
"""Strict Oracle DDL contract; shared by runtime checks and SQL generation."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TABLE_NAMES = {
    "VSS_HO_GD_NHANKHAU": "mirror_hdg_nhankhau",
    "VSS_DM_DANTOC": "mirror_dm_dantoc",
    "VSS_DM_QUOC_TICH": "mirror_dm_quoc_tich",
    "VSS_DM_DBHC": "mirror_dm_dbhc",
}
EXPECTED_COLUMNS = (111, 6, 3, 12)


def parse_type(value):
    # BƯỚC 1: Lấy độ dài trong ngoặc VARCHAR2(n), không lấy chữ số 2 trong tên kiểu; chặn kiểu chưa hỗ trợ.
    value = value.upper().strip()
    char = re.fullmatch(r"(VARCHAR2|NVARCHAR2|CHAR)\(\s*(\d+)\s*(BYTE|CHAR)?\s*\)", value)
    if char:
        kind, size, unit = char.groups()
        if unit is None and kind != "NVARCHAR2":
            raise ValueError("Explicit BYTE/CHAR semantics required: " + value)
        return dict(postgres=f"varchar({int(size)})", family="string",
                    length=int(size), unit=unit or "CHAR", kind=kind,
                    precision=None, scale=None, temporal_precision=None)
    number = re.fullmatch(r"NUMBER(?:\(\s*(\d+)\s*(?:,\s*(-?\d+)\s*)?\))?", value)
    if number:
        p, s = number.groups()
        p, s = (int(p), int(s or 0)) if p else (None, None)
        if p is not None and not 1 <= p <= 38:
            raise ValueError("Unsupported Oracle precision")
        return dict(postgres="numeric" if p is None else f"numeric({p},{s})",
                    family="number", precision=p, scale=s)
    timestamp = re.fullmatch(r"TIMESTAMP\((\d+)\)", value)
    if value == "DATE" or timestamp:
        precision = 0 if value == "DATE" else int(timestamp[1])
        if precision > 6:
            raise ValueError("Oracle TIMESTAMP > 6 needs a lossless alternative to PostgreSQL timestamp")
        return dict(postgres=f"timestamp({precision}) without time zone",
                    family="datetime", temporal_precision=precision)
    raise ValueError("Unsupported Oracle type: " + value)


def source_schema(ddl=None):
    # BƯỚC 2: Đọc đúng bốn bảng/132 cột và ràng buộc có trong DDL; không suy diễn ID là PK.
    if ddl is None:
        ddl = (ROOT / "sql/oracle/01_create_source_tables.sql").read_text(encoding="utf-8")
    result = {}
    for source, mirror in TABLE_NAMES.items():
        match = re.search(r"CREATE TABLE VSS\." + source + r"\s*\(\s*\n(.*?)\n\)", ddl, re.S)
        if not match:
            raise ValueError("Missing source table: " + source)
        columns, primary_key = [], []
        for line in match[1].splitlines():
            line = line.strip().rstrip(",")
            if not line:
                continue
            if line.startswith("CONSTRAINT "):
                key = re.fullmatch(r"CONSTRAINT \w+ PRIMARY KEY \((\w+)\)", line)
                if not key:
                    raise ValueError("Unsupported constraint: " + line)
                primary_key.append(key[1].lower())
                continue
            column = re.fullmatch(
                r"(\w+)\s+(VARCHAR2\([^)]*\)|NVARCHAR2\([^)]*\)|CHAR\([^)]*\)"
                r"|NUMBER(?:\([^)]*\))?|DATE|TIMESTAMP\(\d+\))(?:\s+(NOT NULL))?", line)
            if not column:
                raise ValueError("Unparsed source definition: " + line)
            name, oracle, required = column.groups()
            columns.append(dict(name=name.lower(), oracle=oracle, required=bool(required),
                                **parse_type(oracle)))
        result[source] = dict(mirror=mirror, columns=columns, primary_key=primary_key)
    if tuple(len(t["columns"]) for t in result.values()) != EXPECTED_COLUMNS:
        raise ValueError("Source DDL column counts differ from contract")
    return result


def pg_catalog_type(column):
    return column["postgres"].replace("varchar(", "character varying(")


def byte_constraint(table, column):
    return f"ck_{table['mirror']}_{column['name']}_bytes"


def checked_expression(column):
    # BƯỚC 3: Sinh lời gọi guard theo kiểu cột để publisher kiểm khả năng lưu trước khi gán kiểu đích.
    name = column["name"]
    if column["family"] == "string":
        return f"hdg_load.checked_text({name}, {column['length']}, {str(column['unit'] == 'BYTE').lower()})"
    if column["family"] == "number":
        p, s = column["precision"], column["scale"]
        return f"hdg_load.checked_numeric({name}, {p if p is not None else 'NULL'}, {s if s is not None else 'NULL'})"
    return f"hdg_load.checked_timestamp({name}, {column['temporal_precision']})"


def validate_pg_columns(actual, expected, label):
    """actual/expected are ordered (name, format_type, required) rows."""
    if actual != expected:
        mismatches = [(a, e) for a, e in zip(actual, expected) if a != e]
        raise ValueError(f"Schema mismatch {label}: {mismatches[:4]}; columns {len(actual)}/{len(expected)}")
