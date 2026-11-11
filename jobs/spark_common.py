"""Shared Spark JDBC metadata, transport and reconciliation helpers."""

import json
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path

from schema_contract import ROOT, TABLE_NAMES, EXPECTED_COLUMNS, source_schema
from schema_preflight import assert_pg_schema

SPARK_VERSION = "4.0.1"


def log(event, **fields):
    print(json.dumps(dict(event=event, **fields), ensure_ascii=False), flush=True)


def settings():
    from dotenv import load_dotenv

    load_dotenv(ROOT / "jobs/.env", override=False)
    result = {
        "oracle_url": os.environ.get(
            "ORACLE_JDBC_URL", "jdbc:oracle:thin:@//localhost:1521/FREEPDB1"),
        "oracle_user": os.environ.get("ORACLE_USER", "VSS"),
        "oracle_password": os.environ.get("ORACLE_PASSWORD"),
        "pg_url": os.environ.get(
            "PG_JDBC_URL", "jdbc:postgresql://localhost:55432/hdg_mirror"),
        "pg_user": os.environ.get("PG_USER"),
        "pg_password": os.environ.get("PG_PASSWORD"),
    }
    if not all(result.values()):
        raise ValueError("Set ORACLE_PASSWORD, PG_USER and PG_PASSWORD in the environment or jobs/.env.")
    if not result["oracle_url"].startswith("jdbc:oracle:thin:@//"):
        raise ValueError("ORACLE_JDBC_URL must use jdbc:oracle:thin:@//host:port/service.")
    if not result["pg_url"].startswith("jdbc:postgresql://"):
        raise ValueError("PG_JDBC_URL must be a PostgreSQL JDBC URL.")
    if any(re.search(r"(?i)(?:password|user)=", result[k]) for k in ("oracle_url", "pg_url")):
        raise ValueError("Supply credentials through separate environment variables.")
    return result


def create_spark(name):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "1")
        .config("spark.speculation", "false")
        .config("spark.task.maxFailures", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.redaction.regex", "(?i)secret|password|token|access[.]?key")
        .getOrCreate()
    )
    if spark.version != SPARK_VERSION:
        spark.stop()
        raise ValueError(f"Expected Spark {SPARK_VERSION}. Use jobs/run_spark.ps1.")
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def jdbc_connection(spark, config, database):
    jvm = spark.sparkContext._jvm
    driver = "oracle.jdbc.OracleDriver" if database == "oracle" else "org.postgresql.Driver"
    # Use the context classloader to load drivers from the configured classpath.
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    connection_properties = jvm.java.util.Properties()
    connection_properties.setProperty("user", config[f"{database}_user"])
    connection_properties.setProperty("password", config[f"{database}_password"])
    driver_instance = loader.loadClass(driver).newInstance()
    connection = driver_instance.connect(config[f"{database}_url"], connection_properties)
    try:
        connection.setAutoCommit(False)
        yield connection
    finally:
        connection.close()


def execute(connection, sql, parameters=()):
    statement = connection.prepareStatement(sql)
    try:
        statement.setQueryTimeout(300)
        for index, value in enumerate(parameters, 1):
            statement.setString(index, str(value))
        return statement.executeUpdate()
    finally:
        statement.close()


def write_table_audit(connection, run_id, source, table, source_rows, stage_rows, elapsed, with_digest):
    """Record the source/stage comparison without requiring a legacy DB migration."""
    columns = "run_id,source_table,source_rows,stage_rows,elapsed_seconds,content_equal"
    values = "?,?,?::bigint,?::bigint,?::numeric,true"
    parameters = (run_id, source, source_rows, stage_rows, elapsed)
    if with_digest:
        columns += ",stage_digest"
        values += ",hdg_load.stage_digest(?,?)"
        parameters += (table["mirror"], run_id)
    return execute(connection, f"INSERT INTO hdg_load.table_runs({columns}) VALUES ({values})", parameters)


def scalar(connection, sql, parameters=()):
    statement = connection.prepareStatement(sql)
    try:
        statement.setQueryTimeout(300)
        for index, value in enumerate(parameters, 1):
            statement.setString(index, str(value))
        result = statement.executeQuery()
        try:
            if not result.next():
                raise ValueError("Query returned no row.")
            return result.getString(1)
        finally:
            result.close()
    finally:
        statement.close()


def quote_run(value):
    import uuid

    return str(uuid.UUID(value))


def read_jdbc(spark, config, database, query):
    reader = (
        spark.read.format("jdbc")
        .option("url", config[f"{database}_url"])
        .option("user", config[f"{database}_user"])
        .option("password", config[f"{database}_password"])
        .option("driver", "oracle.jdbc.OracleDriver" if database == "oracle" else "org.postgresql.Driver")
        .option("query", query)
        .option("fetchsize", "1000")
        .option("numPartitions", "1")
        .option("queryTimeout", "300")
    )
    if database == "oracle":
        reader = reader.option("sessionInitStatement",
                               "ALTER SESSION SET NLS_DATE_LANGUAGE='AMERICAN' NLS_CALENDAR='GREGORIAN'")
    return reader.load()


def date_format(column, oracle):
    # Both Oracle and PostgreSQL 16 support FF1..FF6. Never drop sub-seconds.
    precision = column.get("temporal_precision", 0)
    fraction = f".FF{precision}" if precision else ""
    return "YYYY-MM-DD HH24:MI:SS" + fraction + " BC"


def transport_projection(table, database):
    parts = []
    for column in table["columns"]:
        name = column["name"]
        field = name.upper() if database == "oracle" else name
        if column["oracle"].startswith("NUMBER"):
            expression = (f"TO_CHAR({field}, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')"
                          if database == "oracle" else f"{field}::text")
        elif column["oracle"] == "DATE" or column["oracle"].startswith("TIMESTAMP"):
            expression = f"TO_CHAR({field}, '{date_format(column, database == 'oracle')}')"
        else:
            expression = field
        parts.append(f'{expression} AS "{name}"')
    return ", ".join(parts)


def read_source(spark, config, source, table, scn):
    version = "" if scn is None else f" AS OF SCN {int(scn)}"
    return read_jdbc(
        spark, config, "oracle",
        f"SELECT {transport_projection(table, 'oracle')} FROM VSS.{source}{version}",
    )


def read_target(spark, config, table, run_id, target, mirror_namespace="public"):
    if mirror_namespace != "public" and not re.fullmatch(r"hdg_recovery_[a-z0-9_]{1,37}", mirror_namespace):
        raise ValueError("Invalid recovery schema name")
    if target == "stage":
        fields = ", ".join(c["name"] for c in table["columns"])
        query = (f"SELECT {fields} FROM hdg_stage.{table['mirror']} "
                 f"WHERE load_run_id='{quote_run(run_id)}'")
    else:
        query = f"SELECT {transport_projection(table, 'pg')} FROM {mirror_namespace}.{table['mirror']}"
    return read_jdbc(spark, config, "pg", query)


def canonical_number(value):
    """Canonical exact decimal representation using Spark expressions only."""
    from pyspark.sql import functions as F

    value = F.trim(value)
    mantissa = F.regexp_extract(value, r"^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(?:[eE][+-]?[0-9]+)?$", 1)
    exponent = F.regexp_extract(value, r"[eE]([+-]?[0-9]+)$", 1)
    exponent = F.when(exponent == "", F.lit(0)).otherwise(exponent.cast("int"))
    fraction = F.regexp_extract(mantissa, r"\.([0-9]*)$", 1)
    digits = F.regexp_replace(F.regexp_replace(mantissa, r"^[+-]", ""), r"\.", "")
    digits = F.regexp_replace(digits, r"^0+", "")
    significant = F.regexp_replace(digits, r"0+$", "")
    power = exponent - F.length(fraction) + F.length(digits) - F.length(significant)
    sign = F.when(value.startswith("-"), F.lit("-")).otherwise(F.lit(""))
    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(mantissa == "", F.concat(F.lit("INVALID:"), value))
        .when(digits == "", F.lit("0"))
        .otherwise(F.concat(sign, significant, F.lit("E"), power.cast("string")))
    )


def canonical_frame(frame, table):
    from pyspark.sql import functions as F

    return frame.select(*[
        (canonical_number(F.col(c["name"])) if c["oracle"].startswith("NUMBER")
         else F.col(c["name"])).alias(c["name"])
        for c in table["columns"]
    ])


def reconcile_frames(source, target, table):
    # exceptAll compares complete rows as a multiset, retaining duplicate
    # multiplicity and NULL distinctions. No row contents are printed.
    source_count, target_count = source.count(), target.count()
    left, right = canonical_frame(source, table), canonical_frame(target, table)
    missing = left.exceptAll(right).limit(1).count()
    extra = right.exceptAll(left).limit(1).count()
    return dict(source_rows=source_count, target_rows=target_count,
                content_equal=not (missing or extra),
                equal=(source_count == target_count and not (missing or extra)))
