# Mục đích: Chặn job nếu schema thực tế hoặc chức năng audit không phù hợp.
# Đầu vào: Kết nối JDBC và hợp đồng Oracle; chế độ có/không kiểm Mirror.
# Đầu ra: Thông tin khả năng ghi digest hoặc lỗi; không tự chạy DDL.
"""Read-only JDBC metadata checks against the explicit Oracle source contract."""
from schema_contract import pg_catalog_type, validate_pg_columns


# BƯỚC 1: Định nghĩa metadata Stage và kiểm catalog trước khi cho phép ghi dữ liệu.

RUN_COLUMNS = [
    ("run_id", "character varying(36)", "true"),
    ("source_scn", "numeric(38,0)", "true"),
    ("status", "character varying(16)", "true"),
    ("started_at", "timestamp with time zone", "true"),
    ("finished_at", "timestamp with time zone", "false"),
]
TABLE_RUN_COLUMNS = [
    ("run_id", "character varying(36)", "true"),
    ("source_table", "character varying(64)", "true"),
    ("source_rows", "bigint", "true"),
    ("stage_rows", "bigint", "true"),
    ("elapsed_seconds", "numeric", "true"),
    ("content_equal", "boolean", "true"),
]
DIGEST_COLUMN = ("stage_digest", "character varying(64)", "false")


def pg_columns(connection, namespace, name, query_rows):
    # Đọc cả typmod và NOT NULL; chỉ kiểm tên kiểu chung sẽ bỏ sót lỗi varchar(2).
    return query_rows(connection,
        "SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),"
        "CASE WHEN a.attnotnull THEN 'true' ELSE 'false' END "
        "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=? AND c.relname=? AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
        (namespace, name))


def staging_capabilities(connection, schema, query_rows):
    # BƯỚC 1: Kiểm runs/table_runs, default và PK/FK; sau đó xác nhận cột/hàm digest cùng có hoặc cùng chưa có.
    validate_pg_columns(pg_columns(connection,"hdg_load","runs",query_rows),
                        RUN_COLUMNS,"hdg_load.runs")
    actual=pg_columns(connection,"hdg_load","table_runs",query_rows)
    has_column=any(c[0]=="stage_digest" for c in actual)
    validate_pg_columns(actual,TABLE_RUN_COLUMNS+([DIGEST_COLUMN] if has_column else []),
                        "hdg_load.table_runs")
    defaults=query_rows(connection,
        "SELECT pg_get_expr(d.adbin,d.adrelid) FROM pg_attrdef d "
        "JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum "
        "WHERE d.adrelid='hdg_load.runs'::regclass AND a.attname='started_at'")
    if len(defaults)!=1 or not defaults[0][0]:
        raise ValueError("hdg_load.runs.started_at requires a default")
    required_keys=[
        ("hdg_load","runs","p","PRIMARY KEY (run_id)"),
        ("hdg_load","table_runs","p","PRIMARY KEY (run_id, source_table)"),
        ("hdg_load","table_runs","f","FOREIGN KEY (run_id) REFERENCES hdg_load.runs(run_id)"),
    ]
    required_keys += [
        ("hdg_stage",t["mirror"],"f","FOREIGN KEY (load_run_id) REFERENCES hdg_load.runs(run_id)")
        for t in schema.values()
    ]
    for namespace,table,kind,definition in required_keys:
        constraints=query_rows(connection,
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid=to_regclass(?) AND contype::text=? AND convalidated",
            (f"{namespace}.{table}",kind))
        if not any(row[0]==definition for row in constraints):
            raise ValueError(f"Missing metadata key: {namespace}.{table}: {definition}")
    # BƯỚC 2: Cột digest và hàm phải đồng bộ; nếu cùng chưa có thì caller ghi audit legacy.
    function=query_rows(connection,"SELECT to_regprocedure('hdg_load.stage_digest(text,text)')::text")
    has_function=bool(function and function[0][0] is not None)
    if has_column != has_function:
        raise ValueError("Incomplete staging digest capability: stage_digest column and "
                         "hdg_load.stage_digest(text,text) must both exist or both be absent; "
                         "no automatic DDL is performed")
    return {"stage_digest": has_column and has_function}


def rows(connection, sql, parameters=()):
    statement=connection.prepareStatement(sql)
    try:
        statement.setQueryTimeout(300)
        for i,value in enumerate(parameters,1):
            statement.setString(i,str(value))
        result=statement.executeQuery()
        try:
            count=result.getMetaData().getColumnCount()
            found=[]
            while result.next():
                found.append(tuple(result.getString(i) for i in range(1,count+1)))
            return found
        finally:
            result.close()
    finally:
        statement.close()


def validate_oracle_columns(actual, table, source):
    expected=table["columns"]
    if len(actual)!=len(expected):
        raise ValueError(f"Oracle column count drift: {source}")
    for record,c in zip(actual,expected):
        name,typ,byte_length,char_length,char_used,precision,scale,nullable=record
        common=(name.lower()==c["name"] and (nullable=="N")==c["required"])
        if c["family"]=="string":
            valid=(typ==c["kind"]
                   and (c["kind"]=="NVARCHAR2" or char_used==("B" if c["unit"]=="BYTE" else "C"))
                   and int(byte_length if c["unit"]=="BYTE" else char_length)==c["length"])
        elif c["family"]=="number":
            valid=(typ=="NUMBER" and (None if precision is None else int(precision))==c["precision"]
                   and (None if scale is None else int(scale))==c["scale"])
        else:
            valid=typ==c["oracle"]
        if not common or not valid:
            raise ValueError(f"Oracle schema drift: {source}.{c['name']}; metadata={record}")


def assert_source_schema(connection, schema):
    # BƯỚC 1: Đối chiếu charset và đủ metadata Oracle, để BYTE/CHAR và số/ngày không bị hiểu sai.
    charset=rows(connection,"SELECT value FROM nls_database_parameters WHERE parameter='NLS_CHARACTERSET'")
    if charset != [("AL32UTF8",)]:
        raise ValueError("BYTE checks currently require Oracle AL32UTF8; implement encoding-aware mapping first")
    for source,table in schema.items():
        actual=rows(connection,
            "SELECT column_name,data_type,data_length,char_length,char_used,data_precision,data_scale,nullable "
            "FROM all_tab_columns WHERE owner='VSS' AND table_name=? ORDER BY column_id",(source,))
        validate_oracle_columns(actual,table,source)


def assert_pg_schema(connection, schema, include_mirror=True, mirror_namespace="public", require_digest=False):
    # BƯỚC 2: Kiểm Stage/metadata trước; publisher v2 chỉ bắt buộc ở nhánh thực sự cần Mirror.
    import re
    if mirror_namespace != "public" and not re.fullmatch(r"hdg_recovery_[a-z0-9_]{1,37}", mirror_namespace):
        raise ValueError("Invalid recovery schema name")
    if rows(connection,"SELECT current_setting('server_encoding')") != [("UTF8",)]:
        raise ValueError("PostgreSQL UTF8 required")
    for table in schema.values():
        for namespace in ((mirror_namespace,"hdg_stage") if include_mirror else ("hdg_stage",)):
            expected=[(c["name"],pg_catalog_type(c) if namespace!="hdg_stage" else "text",
                       "true" if c["required"] else "false") for c in table["columns"]]
            if namespace=="hdg_stage":
                expected.append(("load_run_id","character varying(36)","true"))
            actual=pg_columns(connection,namespace,table["mirror"],rows)
            validate_pg_columns(actual,expected,f"{namespace}.{table['mirror']}")
    # Stage legacy vẫn chạy nếu metadata đầy đủ; không bỏ qua trường hợp digest cài dở.
    capabilities=staging_capabilities(connection,schema,rows)
    # Chỉ nhánh Mirror/candidate mới bắt buộc publisher v2 và guard schema đầy đủ.
    if include_mirror:
        version=rows(connection,"SELECT to_regprocedure('hdg_load.contract_version()')::text")
        if not version or version[0][0] is None or rows(connection,"SELECT hdg_load.contract_version()::text") != [("2",)]:
            raise ValueError("Deploy the corrected publish functions/migration (contract v2) first")
        rows(connection,"SELECT hdg_load.assert_mirror_schema()")
    if (include_mirror or require_digest) and not capabilities["stage_digest"]:
        raise ValueError("Digest audit requires both hdg_load.table_runs.stage_digest varchar(64) "
                         "and hdg_load.stage_digest(text,text); legacy staging is allowed only "
                         "without publish or --record-stage-audit")
    return capabilities
