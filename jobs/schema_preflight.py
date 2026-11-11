"""Read-only JDBC metadata checks against the explicit Oracle source contract."""
from schema_contract import pg_catalog_type, validate_pg_columns
from staging_metadata import pg_columns, staging_capabilities


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
    charset=rows(connection,"SELECT value FROM nls_database_parameters WHERE parameter='NLS_CHARACTERSET'")
    if charset != [("AL32UTF8",)]:
        raise ValueError("BYTE checks currently require Oracle AL32UTF8; implement encoding-aware mapping first")
    for source,table in schema.items():
        actual=rows(connection,
            "SELECT column_name,data_type,data_length,char_length,char_used,data_precision,data_scale,nullable "
            "FROM all_tab_columns WHERE owner='VSS' AND table_name=? ORDER BY column_id",(source,))
        validate_oracle_columns(actual,table,source)


def assert_pg_schema(connection, schema, include_mirror=True, mirror_namespace="public", require_digest=False):
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
    capabilities=staging_capabilities(connection,schema,rows)
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
