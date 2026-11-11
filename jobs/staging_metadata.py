"""Metadata contract shared by legacy and v2 staging-only runs (no DDL)."""
from schema_contract import validate_pg_columns

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
    return query_rows(connection,
        "SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),"
        "CASE WHEN a.attnotnull THEN 'true' ELSE 'false' END "
        "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=? AND c.relname=? AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
        (namespace, name))


def staging_capabilities(connection, schema, query_rows):
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
    function=query_rows(connection,"SELECT to_regprocedure('hdg_load.stage_digest(text,text)')::text")
    has_function=bool(function and function[0][0] is not None)
    if has_column != has_function:
        raise ValueError("Incomplete staging digest capability: stage_digest column and "
                         "hdg_load.stage_digest(text,text) must both exist or both be absent; "
                         "no automatic DDL is performed")
    return {"stage_digest": has_column and has_function}
