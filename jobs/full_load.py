"""Full Load Oracle -> Spark JDBC -> PostgreSQL staging -> validation -> publish."""

import argparse
import time
import uuid

from spark_common import (
    assert_pg_schema, create_spark, execute, jdbc_connection, log, quote_run,
    read_source, read_target, reconcile_frames, scalar, settings, source_schema, write_table_audit,
)


from schema_preflight import assert_source_schema


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--source-scn", type=int, help="Use an operator-supplied Oracle snapshot SCN")
    parser.add_argument("--stage-only", action="store_true",
                        help="Read/reconcile snapshot into a new staging run; never publish Mirror")
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 10000:
        parser.error("--batch-size must be between 1 and 10000")
    if args.source_scn is not None and args.source_scn < 1:
        parser.error("--source-scn must be positive")
    config, schema = settings(), source_schema()
    spark = create_spark("HDG full load")
    run_id, phase = str(uuid.uuid4()), "preflight"
    started = time.monotonic()
    try:
        with jdbc_connection(spark, config, "pg") as control:
            lock = scalar(control,
                          "SELECT CASE WHEN pg_try_advisory_lock(784027,1) "
                          "THEN 'locked' ELSE 'busy' END")
            if lock != "locked":
                raise RuntimeError("Another Full Load owns the session lock.")
            capabilities = assert_pg_schema(control, schema, include_mirror=not args.stage_only)
            audit_mode = "SHA256" if capabilities["stage_digest"] else "DEFERRED_LEGACY"
            if not args.stage_only and scalar(control, "SELECT CASE WHEN cdc_enabled THEN 'active' ELSE 'idle' END "
                      "FROM hdg_load.control WHERE id=1") != "idle":
                raise RuntimeError("CDC is enabled. Snapshot/CDC handoff must be implemented separately.")
            with jdbc_connection(spark, config, "oracle") as oracle:
                assert_source_schema(oracle, schema)
            if args.source_scn is None:
                with jdbc_connection(spark, config, "oracle") as oracle:
                    scn = int(scalar(oracle, "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM DUAL"))
            else:
                scn = args.source_scn
            execute(control, "INSERT INTO hdg_load.runs(run_id,source_scn,status) "
                    "VALUES (?,?::numeric,'LOADING')", (run_id, scn))
            control.commit()
            log("run_started", run_id=run_id, source_scn=scn, stage_only=args.stage_only, audit_mode=audit_mode)
            try:
                from pyspark.sql import functions as F

                for source, table in schema.items():
                    phase = source
                    table_started = time.monotonic()
                    source_frame = read_source(spark, config, source, table, scn)
                    source_rows = source_frame.count()
                    (
                        source_frame.withColumn("load_run_id", F.lit(run_id))
                        .write.format("jdbc")
                        .option("url", config["pg_url"])
                        .option("user", config["pg_user"])
                        .option("password", config["pg_password"])
                        .option("driver", "org.postgresql.Driver")
                        .option("dbtable", f"hdg_stage.{table['mirror']}")
                        .option("numPartitions", "1")
                        .option("batchsize", str(args.batch_size))
                        .option("queryTimeout", "300")
                        .mode("append").save()
                    )
                    stage_frame = read_target(spark, config, table, run_id, "stage")
                    result = reconcile_frames(source_frame, stage_frame, table)
                    if source_rows != result["source_rows"] or not result["equal"]:
                        raise RuntimeError(f"Stage reconciliation failed for {source}.")
                    elapsed = time.monotonic() - table_started
                    write_table_audit(
                        control, run_id, source, table, source_rows, result["target_rows"], elapsed,
                        with_digest=capabilities["stage_digest"],
                    )
                    control.commit()
                    log("table_validated", run_id=run_id, table=source,
                        read_rows=source_rows, written_rows=result["target_rows"],
                        elapsed_seconds=round(elapsed, 3), content_equal=True, audit_mode=audit_mode)
                phase = "finalize_stage" if args.stage_only else "publish"
                execute(control, "UPDATE hdg_load.runs SET status='VALIDATED' WHERE run_id=?", (run_id,))
                control.commit()
                if args.stage_only:
                    log("run_staged", run_id=run_id, source_scn=scn, outcome="STAGED", audit_mode=audit_mode,
                        elapsed_seconds=round(time.monotonic() - started, 3))
                    return
                outcome = scalar(control, "SELECT hdg_load.publish_full_load(?)", (run_id,))
                control.commit()
                log("run_finished", run_id=run_id, outcome=outcome,
                    elapsed_seconds=round(time.monotonic() - started, 3))
            except Exception:
                control.rollback()
                # Preserve COMMITTED/NOOP if a commit reply was lost. Inspect
                # runs after reconnecting before deciding to run again.
                execute(control, "UPDATE hdg_load.runs SET status='FAILED',finished_at=clock_timestamp() "
                        "WHERE run_id=? AND status NOT IN ('COMMITTED','NOOP')", (run_id,))
                control.commit()
                raise
    except Exception as error:
        log("run_error", run_id=run_id, phase=phase, error_type=type(error).__name__)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
