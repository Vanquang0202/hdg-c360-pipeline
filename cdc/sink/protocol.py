# Mục đích: Giải mã Debezium JSON có schema/payload mà không làm mất giá trị.
# Đầu vào: Raw key/value, logical type, danh tính nguồn và hợp đồng Oracle.
# Đầu ra: Ảnh trước/sau exact, thông tin giao dịch và fingerprint; không kết nối DB/Kafka.
"""Strict schema-aware Debezium decoder. No database or Kafka I/O."""
import base64
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, Inexact, InvalidOperation, localcontext
from contract import TABLES

MAX_MESSAGE_BYTES = 8 * 1024 * 1024
EPOCH = datetime(1970, 1, 1)


class Rejected(ValueError):
    def __init__(self, code, detail=""):
        self.code = code
        super().__init__(code + (": " + detail if detail else ""))


def reject(code, detail=""):
    raise Rejected(code, detail)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            reject("DUPLICATE_JSON_KEY", key)
        result[key] = value
    return result


def loads(raw):
    # BƯỚC 1: Đọc JSON không qua float; từ chối key lặp, NaN và message vượt giới hạn.
    if not isinstance(raw, (bytes, str)) or len(raw) > MAX_MESSAGE_BYTES:
        reject("MESSAGE_SIZE_OR_TYPE")
    try:
        return json.loads(raw, parse_float=Decimal, object_pairs_hook=_object,
                          parse_constant=lambda value: reject("NONFINITE_JSON"))
    except (UnicodeError, json.JSONDecodeError, TypeError) as exc:
        raise Rejected("MALFORMED_JSON") from exc


def canonical(value):
    # Chuẩn hóa biểu diễn để băm exact, không sửa giá trị nghiệp vụ hoặc gộp các source event khác nhau.
    if isinstance(value, Decimal):
        if not value.is_finite():
            reject("NONFINITE_NUMBER")
        sign, digits, exponent = value.as_tuple()
        digits = list(digits)
        if not any(digits):
            return {"decimal": "0"}
        while digits and digits[-1] == 0:
            digits.pop()
            exponent += 1
        return {"decimal": ("-" if sign else "") + "".join(map(str, digits)) + "E" + str(exponent)}
    if isinstance(value, datetime):
        return {"timestamp": value.isoformat(timespec="microseconds")}
    if isinstance(value, bytes):
        return {"bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): canonical(v) for k, v in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [canonical(v) for v in value]
    return value


def digest(value):
    return hashlib.sha256(json.dumps(canonical(value), ensure_ascii=True,
                                     sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def integer(value, label, minimum=None):
    if isinstance(value, bool) or not (isinstance(value, int) or
                                      isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value)):
        reject("INTEGER_REQUIRED", label)
    result = int(value)
    if minimum is not None and result < minimum:
        reject("INTEGER_RANGE", label)
    return result


def unscaled(raw):
    if not isinstance(raw, str):
        reject("DECIMAL_BYTES_REQUIRED")
    try:
        binary = base64.b64decode(raw, validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise Rejected("INVALID_BASE64") from exc
    if not binary or len(binary) > 512:
        reject("DECIMAL_BYTES_RANGE")
    return int.from_bytes(binary, byteorder="big", signed=True)


def exact_decimal(number, scale):
    # Dựng Decimal từ hệ số và scale, tránh bị context mặc định làm tròn số nhiều chữ số.
    scale = integer(scale, "decimal scale")
    if abs(scale) > 1000:
        reject("DECIMAL_SCALE_RANGE")
    digits = tuple(int(c) for c in str(abs(number)))
    return Decimal((int(number < 0), digits, -scale))


def check_descriptor(field, column):
    kind = field.get("type")
    logical = field.get("name")
    family = column["family"]
    if family == "string":
        if kind != "string" or logical is not None:
            reject("SCHEMA_TYPE", column["name"])
    elif family == "number":
        if logical == "org.apache.kafka.connect.data.Decimal":
            if kind != "bytes" or "scale" not in field.get("parameters", {}):
                reject("DECIMAL_SCHEMA", column["name"])
            integer(field["parameters"]["scale"], "decimal scale")
        elif logical == "io.debezium.data.VariableScaleDecimal":
            fields = field.get("fields", [])
            if kind != "struct" or [(f.get("field"), f.get("type")) for f in fields] != [
                    ("scale", "int32"), ("value", "bytes")]:
                reject("VARIABLE_DECIMAL_SCHEMA", column["name"])
        elif kind not in ("int8", "int16", "int32", "int64") or logical is not None:
            reject("SCHEMA_NUMBER_TYPE", column["name"])
    else:
        allowed = ("io.debezium.time.Timestamp", "org.apache.kafka.connect.data.Timestamp",
                   "io.debezium.time.MicroTimestamp", "io.debezium.time.NanoTimestamp")
        if kind != "int64" or logical not in allowed:
            reject("SCHEMA_TIMESTAMP", column["name"])
        if column["temporal_precision"] > 3 and logical not in (
                "io.debezium.time.MicroTimestamp", "io.debezium.time.NanoTimestamp"):
            reject("TIMESTAMP_UPSTREAM_PRECISION", column["name"])


def descriptors(schema, table):
    # BƯỚC 2: Kiểm schema đủ cột và logical type trước khi đọc dữ liệu.
    if not isinstance(schema, dict) or schema.get("type") != "struct":
        reject("ROW_SCHEMA_REQUIRED")
    fields = schema.get("fields", [])
    if not isinstance(fields, list) or not all(isinstance(f, dict) for f in fields):
        reject("ROW_SCHEMA_FIELDS")
    names = [f.get("field") for f in fields]
    expected = [c["name"].upper() for c in table["columns"]]
    if names != expected:
        reject("SCHEMA_COLUMNS", table["mirror"])
    for field, column in zip(fields, table["columns"]):
        check_descriptor(field, column)
    return fields


def value(raw, field, column):
    # BƯỚC 3: Giữ NULL/chuỗi/số/ngày theo contract; không cắt chuỗi hoặc bỏ phần giây lẻ.
    if raw is None:
        if column["required"]:
            reject("REQUIRED_NULL", column["name"])
        return None
    family = column["family"]
    if family == "string":
        if not isinstance(raw, str):
            reject("STRING_REQUIRED", column["name"])
        if raw == "__debezium_unavailable_value":
            reject("UNAVAILABLE_VALUE", column["name"])
        try:
            size = len(raw.encode("utf-8"))
        except UnicodeError as exc:
            raise Rejected("INVALID_UNICODE", column["name"]) from exc
        if not raw or "\x00" in raw or len(raw) > column["length"] or (
                column["unit"] == "BYTE" and size > column["length"]):
            reject("STRING_LENGTH_OR_CONTENT", column["name"])
        return raw
    if family == "number":
        logical = field.get("name")
        if logical == "io.debezium.data.VariableScaleDecimal":
            if not isinstance(raw, dict) or set(raw) != {"scale", "value"}:
                reject("VARIABLE_DECIMAL_VALUE")
            n = exact_decimal(unscaled(raw["value"]), raw["scale"])
        elif logical == "org.apache.kafka.connect.data.Decimal":
            scale = integer(field["parameters"]["scale"], "decimal scale")
            if isinstance(raw, str):
                n = exact_decimal(unscaled(raw), scale)
            elif isinstance(raw, (Decimal, int)) and not isinstance(raw, bool):
                n = Decimal(raw)  # JsonConverter decimal.format=NUMERIC, parsed without float.
            else:
                reject("DECIMAL_VALUE")
        else:
            if not isinstance(raw, int) or isinstance(raw, bool):
                reject("INTEGER_WIRE_TYPE", column["name"])
            i = integer(raw, column["name"])
            bits = int(field["type"][3:])
            if not -(2 ** (bits - 1)) <= i < 2 ** (bits - 1):
                reject("INTEGER_WIRE_RANGE")
            n = Decimal(i)
        if not n.is_finite() or len(n.as_tuple().digits) > 1000 or abs(n.as_tuple().exponent) > 1000:
            reject("NUMERIC_RESOURCE_RANGE")
        p, s = column["precision"], column["scale"]
        if p is not None:
            try:
                with localcontext() as context:
                    context.prec = 1100
                    context.traps[Inexact] = True
                    n.quantize(Decimal((0, (1,), -s)))
                    if n.copy_abs() >= Decimal(10) ** (p - s):
                        reject("NUMBER_PRECISION", column["name"])
            except (Inexact, InvalidOperation) as exc:
                raise Rejected("NUMBER_WOULD_ROUND", column["name"]) from exc
        return n
    if not isinstance(raw, int) or isinstance(raw, bool):
        reject("TIMESTAMP_WIRE_TYPE", column["name"])
    ticks = integer(raw, column["name"])
    logical = field["name"]
    if logical == "io.debezium.time.NanoTimestamp":
        if ticks % 1000:
            reject("TIMESTAMP_WOULD_ROUND")
        micros = ticks // 1000
    else:
        micros = ticks if logical == "io.debezium.time.MicroTimestamp" else ticks * 1000
    quantum = 10 ** (6 - column["temporal_precision"])
    if micros % quantum:
        reject("TIMESTAMP_WOULD_ROUND", column["name"])
    try:
        return EPOCH + timedelta(microseconds=micros)
    except OverflowError as exc:
        raise Rejected("TIMESTAMP_PYTHON_RANGE", "BC or outside years 1..9999 unsupported") from exc


def row(raw, schema, table):
    # BƯỚC 4: Ảnh dòng phải đủ trường; thiếu field không được tự coi là NULL.
    fields = descriptors(schema, table)
    if not isinstance(raw, dict) or set(raw) != {c["name"].upper() for c in table["columns"]}:
        reject("MISSING_OR_EXTRA_ROW_FIELD", table["mirror"])
    return {c["name"]: value(raw[c["name"].upper()], f, c)
            for f, c in zip(fields, table["columns"])}


def envelope(raw):
    data = loads(raw)
    if not isinstance(data, dict) or set(data) != {"schema", "payload"}:
        reject("SCHEMA_PAYLOAD_REQUIRED")
    if not isinstance(data["schema"], dict) or data["schema"].get("type") != "struct":
        reject("ENVELOPE_SCHEMA")
    if not isinstance(data["payload"], dict):
        reject("ENVELOPE_PAYLOAD")
    return data["schema"], data["payload"]


@dataclass(frozen=True)
class Record:
    topic: str
    topic_id: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple = ()

    def wire(self):
        def b64(value):
            return None if value is None else base64.b64encode(value).decode("ascii")
        return dict(topic=self.topic, topic_id=self.topic_id, partition=self.partition,
                    offset=self.offset, key=b64(self.key), value=b64(self.value),
                    headers=[[key, b64(value)] for key, value in self.headers])

    @classmethod
    def from_wire(cls, data):
        try:
            def raw(value):
                return None if value is None else base64.b64decode(value, validate=True)
            return cls(data["topic"], data["topic_id"], integer(data["partition"], "partition", 0),
                       integer(data["offset"], "offset", 0), raw(data["key"]), raw(data["value"]),
                       tuple((k, raw(v)) for k, v in data.get("headers", [])))
        except (KeyError, TypeError, ValueError) as exc:
            raise Rejected("RECORD_FORMAT") from exc

    @property
    def coordinate(self):
        return self.topic, self.partition, self.offset


def _decode(record, prefix, incarnation):
    # BƯỚC 5: Phân biệt tombstone/control/c-u-d, kiểm ảnh trước/sau rồi tạo danh tính sự kiện.
    # Tombstone chỉ đánh dấu kết thúc record cũ, không phải một lệnh DELETE thứ hai.
    if record.value is None:
        return dict(kind="tombstone")
    schema, payload = envelope(record.value)
    if record.topic == prefix + ".transaction":
        if payload.get("status") not in ("BEGIN", "END") or not isinstance(payload.get("id"), str):
            reject("TRANSACTION_CONTROL")
        control_fields={item.get("field"):item.get("type") for item in schema.get("fields", [])}
        if any(control_fields.get(name)!=kind for name,kind in [
                ("status","string"),("id","string"),("event_count","int64"),("data_collections","array")]):
            reject("TRANSACTION_CONTROL_SCHEMA")
        return dict(kind="control", payload=payload)
    table_name = record.topic.removeprefix(prefix + ".VSS.")
    if record.topic != prefix + ".VSS." + table_name or table_name not in TABLES:
        reject("TOPIC_TABLE")
    table = TABLES[table_name]
    fields = schema.get("fields", [])
    field_map = {f.get("field"): f for f in fields if isinstance(f, dict)}
    if len(field_map) != len(fields) or not {"before", "after", "source", "op", "transaction"} <= set(field_map):
        reject("ENVELOPE_FIELDS")
    if any(field_map[name].get("type") != kind for name,kind in [
            ("source","struct"),("transaction","struct"),("op","string")]):
        reject("ENVELOPE_FIELD_TYPE")
    before_schema, after_schema = field_map["before"], field_map["after"]
    descriptors(before_schema, table)
    descriptors(after_schema, table)
    op = payload.get("op")
    if op not in ("c", "u", "d"):
        reject("UNSUPPORTED_OPERATION")
    if "before" not in payload or "after" not in payload:
        reject("MISSING_IMAGE")
    before = None if payload["before"] is None else row(payload["before"], before_schema, table)
    after = None if payload["after"] is None else row(payload["after"], after_schema, table)
    if ((op == "c" and (before is not None or after is None)) or
        (op == "u" and (before is None or after is None)) or
        (op == "d" and (before is None or after is not None))):
        reject("OPERATION_IMAGES")
    # Kiểm nguồn, commit SCN và thứ tự trong giao dịch; không nhầm SCN của DML với SCN commit.
    source = payload.get("source")
    transaction = payload.get("transaction")
    if not isinstance(source, dict) or not isinstance(transaction, dict):
        reject("SOURCE_TRANSACTION_REQUIRED")
    if source.get("schema") != "VSS" or source.get("table") != table_name:
        reject("SOURCE_TABLE")
    if source.get("snapshot") not in (False, "false"):
        reject("SNAPSHOT_NOT_SUPPORTED")
    for key in ("txId", "rs_id"):
        if not isinstance(source.get(key), str) or not source[key]:
            reject("SOURCE_IDENTITY_REQUIRED", key)
    scn = integer(source.get("scn"), "scn", 1)
    commit = integer(source.get("commit_scn"), "commit_scn", 1)
    ssn = integer(source.get("ssn"), "ssn", 0)
    thread = integer(source.get("redo_thread"), "redo_thread", 1)
    if scn > commit or transaction.get("id") != source["txId"]:
        reject("TRANSACTION_ID_OR_SCN")
    order = integer(transaction.get("total_order"), "total_order", 1)
    collection_order = integer(transaction.get("data_collection_order"), "data_collection_order", 1)
    key_data = None
    if table["primary_key"]:
        if record.key is None:
            reject("PRIMARY_KEY_REQUIRED")
        key_schema, key_payload = envelope(record.key)
        key_table = dict(table, columns=[c for c in table["columns"] if c["name"] in table["primary_key"]])
        key_data = row(key_payload, key_schema, key_table)
        image = after if op == "c" else before
        if any(key_data[k] != image[k] for k in table["primary_key"]):
            reject("KEY_IMAGE_MISMATCH")
    elif record.key is not None:
        reject("UNEXPECTED_KEY_NO_PK", "prototype policy requires key-null for no-PK tables")
    # Danh tính redo này phục vụ prototype; chưa chứng minh ổn định qua restart Debezium thực tế.
    identity = digest([incarnation, table_name, source["txId"], scn, commit,
                       source["rs_id"], ssn, thread, op, key_data])
    fingerprint = digest(dict(table=table_name, op=op, before=before, after=after,
                              key=key_data, commit_scn=commit, txid=source["txId"],
                              order=order, collection_order=collection_order))
    return dict(kind="dml", table=table_name, op=op, before=before, after=after,
                identity=identity, fingerprint=fingerprint, commit_scn=commit,
                txid=source["txId"], order=order, collection_order=collection_order)


def decode(record, prefix, incarnation):
    # Đưa lỗi cấu trúc về cùng mã Rejected để applier rollback và lưu quarantine.
    try:
        return _decode(record,prefix,incarnation)
    except Rejected:
        raise
    except (KeyError,TypeError,ValueError,OverflowError,AttributeError) as exc:
        raise Rejected("MALFORMED_ENVELOPE") from exc
