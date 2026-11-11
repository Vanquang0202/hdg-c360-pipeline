"""Compare raw Oracle snapshot values with staging/Mirror; optionally re-audit retained stage."""
import argparse
import re
import time
from spark_common import (
    create_spark, execute, jdbc_connection, log, quote_run, read_source, read_target,
    reconcile_frames, scalar, settings, source_schema, assert_pg_schema,
)
from schema_preflight import assert_source_schema


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id",required=True)
    parser.add_argument("--target",choices=("stage","mirror","candidate"),default="mirror")
    parser.add_argument("--candidate-schema", help="Existing hdg_recovery_* schema; only with --target candidate")
    parser.add_argument("--source",choices=("snapshot","current"),default="snapshot")
    parser.add_argument("--record-stage-audit",action="store_true",
                        help="Record stage SHA256 only after all four tables match their Oracle snapshot")
    args=parser.parse_args()
    if args.record_stage_audit and (args.target!="stage" or args.source!="snapshot"):
        parser.error("--record-stage-audit requires --target stage --source snapshot")
    if args.target=="candidate":
        if not args.candidate_schema or not re.fullmatch(r"hdg_recovery_[a-z0-9_]{1,37}",args.candidate_schema):
            parser.error("--target candidate requires a valid --candidate-schema")
    elif args.candidate_schema:
        parser.error("--candidate-schema requires --target candidate")
    namespace=args.candidate_schema if args.target=="candidate" else "public"
    run_id=quote_run(args.run_id)
    config,schema=settings(),source_schema()
    spark=create_spark("HDG reconcile")
    try:
        with jdbc_connection(spark,config,"oracle") as oracle:
            assert_source_schema(oracle,schema)
            current_scn=(int(scalar(oracle,"SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM DUAL"))
                         if args.source=="current" else None)
        with jdbc_connection(spark,config,"pg") as connection:
            execute(connection,"SET LOCAL lock_timeout='10s'")
            assert_pg_schema(connection,schema,include_mirror=args.target!="stage",
                             mirror_namespace=namespace,require_digest=args.record_stage_audit)
            scn=scalar(connection,"SELECT source_scn::text FROM hdg_load.runs WHERE run_id=?",(run_id,))
            if scn is None:
                raise ValueError("Unknown run_id")
            if args.record_stage_audit:
                if scalar(connection,"SELECT CASE WHEN pg_try_advisory_xact_lock(784027,1) THEN 'locked' ELSE 'busy' END")!="locked":
                    raise RuntimeError("A Full Load/recovery is active")
            # Keep all target tables stable while Spark performs multiple JDBC actions.
            target_namespace="hdg_stage" if args.target=="stage" else namespace
            for table in schema.values():
                execute(connection,f"LOCK TABLE {target_namespace}.{table['mirror']} IN SHARE MODE")
            comparison_scn=int(scn) if args.source=="snapshot" else current_scn
            results=[]
            for source,table in schema.items():
                started=time.monotonic()
                left=read_source(spark,config,source,table,comparison_scn)
                right=read_target(spark,config,table,run_id,args.target,mirror_namespace=namespace)
                result=reconcile_frames(left,right,table)
                results.append((source,table,result))
                log("reconciliation",run_id=run_id,table=source,target=args.target,
                    source_mode=args.source,source_scn=comparison_scn,
                    elapsed_seconds=round(time.monotonic()-started,3),**result)
            if not all(result["equal"] for _,_,result in results):
                raise RuntimeError("Counts or full-row multisets differ; no audit updated")
            if args.record_stage_audit:
                for source,table,result in results:
                    updated=execute(connection,
                        "UPDATE hdg_load.table_runs SET stage_digest=hdg_load.stage_digest(?,?) "
                        "WHERE run_id=? AND source_table=? AND content_equal "
                        "AND source_rows=?::bigint AND stage_rows=?::bigint",
                        (table["mirror"],run_id,run_id,source,result["source_rows"],result["target_rows"]))
                    if updated!=1:
                        raise RuntimeError("Missing or inconsistent historical table audit")
                connection.commit()
                log("stage_audit_recorded",run_id=run_id,source_scn=scn)
    finally:
        spark.stop()


if __name__=="__main__":
    main()
