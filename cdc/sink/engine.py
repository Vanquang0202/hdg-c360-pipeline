"""Atomic test-only transaction applier. No Source configuration or Kafka offset writes."""
from collections import Counter
from psycopg import sql
from psycopg.types.json import Jsonb
import psycopg
from contract import TABLES
from manifest import validate, manifest_hash
from protocol import decode, digest, canonical, integer, row, Rejected
from storage import assert_target, insert_row, relation, matching_where


class SimulatedCrash(RuntimeError):
    pass


def initialize(connection, document):
    validate(document)
    assert_target(connection)
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(631874,1)")
        if connection.execute("SELECT count(*) FROM sink_meta.epochs").fetchone()[0]:
            raise Rejected("TEST_BASELINE_ALREADY_INITIALIZED", "never overwrite an epoch/baseline")
        for table in TABLES.values():
            if connection.execute(sql.SQL("SELECT count(*) FROM {}").format(relation(table))).fetchone()[0]:
                raise Rejected("TEST_MIRROR_NOT_EMPTY")
        connection.execute(
            "INSERT INTO sink_meta.epochs VALUES(%s,%s,%s,'ACTIVE',0,%s)",
            (document["epoch"],manifest_hash(document),Jsonb(canonical(document)),document["baseline"]["scn"]))
        for stream in document["streams"]:
            connection.execute(
                "INSERT INTO sink_meta.checkpoints VALUES(%s,%s,%s,%s,%s)",
                (document["epoch"],stream["topic"],stream["topic_id"],stream["partition"],stream["start"]))
        for name, spec in document["baseline"]["tables"].items():
            table = TABLES[name]
            for raw in spec["rows"]:
                insert_row(connection, table, row(raw,spec["schema"],table))


def transaction(events, spec, incarnation):
    dml = [event for event in events if event["kind"] == "dml"]
    controls = [event["payload"] for event in events if event["kind"] == "control"]
    if not dml:
        if controls or spec.get("txid") is not None or spec["event_count"] != 0:
            raise Rejected("CONTROL_WITHOUT_COMPLETE_TRANSACTION")
        return None
    txids = {e["txid"] for e in dml}
    commits = {e["commit_scn"] for e in dml}
    if len(txids)!=1 or len(commits)!=1 or spec.get("txid") not in txids:
        raise Rejected("MIXED_OR_WRONG_TRANSACTION")
    if len(dml)!=integer(spec["event_count"],"event_count",1):
        raise Rejected("TRANSACTION_EVENT_COUNT")
    if [e["order"] for e in dml] != list(range(1,len(dml)+1)):
        raise Rejected("TRANSACTION_ORDER")
    orders = Counter()
    counts = Counter()
    for event in dml:
        orders[event["table"]]+=1
        counts[event["table"]]+=1
        if event["collection_order"] != orders[event["table"]]:
            raise Rejected("COLLECTION_ORDER")
    if len({e["identity"] for e in dml}) != len(dml):
        raise Rejected("AMBIGUOUS_SOURCE_IDENTITY_WITHIN_TRANSACTION")
    ends = [c for c in controls if c["status"]=="END"]
    begins = [c for c in controls if c["status"]=="BEGIN"]
    if len(ends)!=1 or len(begins)>1 or any(c["id"]!=spec["txid"] for c in controls):
        raise Rejected("MISSING_OR_INVALID_TRANSACTION_END")
    end=ends[0]
    if integer(end.get("event_count"),"END event_count",1)!=len(dml):
        raise Rejected("END_EVENT_COUNT")
    collections={}
    if not isinstance(end.get("data_collections"),list):
        raise Rejected("END_COLLECTIONS")
    for collection in end["data_collections"]:
        qualified=collection.get("data_collection","")
        name=qualified.rsplit(".",1)[-1]
        if name not in TABLES or not qualified.endswith(".VSS."+name) or name in collections:
            raise Rejected("END_COLLECTION_NAME")
        collections[name]=integer(collection.get("event_count"),"collection event_count",1)
    if collections!=dict(counts):
        raise Rejected("END_COLLECTION_COUNTS")
    commit=next(iter(commits))
    return dict(commit=commit,key=digest([incarnation,spec["txid"],commit]),
                fingerprint=digest([(e["identity"],e["fingerprint"]) for e in dml]),dml=dml)


def apply_dml(connection, event):
    table=TABLES[event["table"]]
    if event["op"]=="c":
        if table["primary_key"]:
            where,params=matching_where(table,event["after"],table["primary_key"])
            if connection.execute(sql.SQL("SELECT 1 FROM {} WHERE {} FOR UPDATE").format(
                    relation(table),where),params).fetchone():
                raise Rejected("PK_INSERT_CONFLICT")
        insert_row(connection,table,event["after"])
        return
    where,params=matching_where(table,event["before"])
    # ctid is a locked, transaction-local tuple locator only, never durable identity.
    target=connection.execute(sql.SQL(
        "SELECT ctid::text FROM {} WHERE {} ORDER BY ctid LIMIT 1 FOR UPDATE").format(
            relation(table),where),params).fetchone()
    if target is None:
        raise Rejected("BEFORE_IMAGE_NOT_FOUND",event["table"])
    if event["op"]=="d":
        affected=connection.execute(sql.SQL("DELETE FROM {} WHERE ctid=%s::tid").format(
            relation(table)),(target[0],)).rowcount
    else:
        names=[c["name"] for c in table["columns"]]
        affected=connection.execute(sql.SQL("UPDATE {} SET {} WHERE ctid=%s::tid").format(
            relation(table),sql.SQL(",").join(sql.SQL("{}=%s").format(sql.Identifier(n)) for n in names)),
            [event["after"][n] for n in names]+[target[0]]).rowcount
    if affected!=1:
        raise Rejected("ROW_EFFECT_NOT_ONE")


def _quarantine(connection,epoch,frame_no,records,error):
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(631874,1)")
        connection.execute(
            "INSERT INTO sink_meta.quarantine(epoch,frame_no,fingerprint,reason,raw_records) "
            "VALUES(%s,%s,%s,%s,%s)",
            (epoch,frame_no,digest([r.wire() for r in records]),error.code,
             Jsonb([r.wire() for r in records])))
        connection.execute("UPDATE sink_meta.epochs SET state='BLOCKED' WHERE epoch=%s",(epoch,))


def apply_frame(connection, document, frame_no, records, failpoint=None):
    """Each complete finite source transaction + its metadata commits atomically."""
    assert_target(connection)
    validate(document)
    spec=document["frames"][frame_no]
    epoch=document["epoch"]
    wire_hash=digest([r.wire() for r in records])
    try:
        with connection.transaction():
            connection.execute("SELECT pg_advisory_xact_lock(631874,1)")
            state=connection.execute(
                "SELECT manifest_hash,state,next_frame,last_commit_scn FROM sink_meta.epochs "
                "WHERE epoch=%s FOR UPDATE",(epoch,)).fetchone()
            if state is None or state[0]!=manifest_hash(document):
                raise Rejected("BASELINE_MANIFEST_MISMATCH")
            if state[1]!="ACTIVE":
                raise Rejected("EPOCH_BLOCKED")
            if [list(r.coordinate) for r in records]!=spec["records"]:
                raise Rejected("FRAME_RECORDS")
            if frame_no<state[2]:
                old=connection.execute("SELECT fingerprint FROM sink_meta.frames WHERE epoch=%s AND frame_no=%s",
                                       (epoch,frame_no)).fetchone()
                if old is None or old[0]!=wire_hash:
                    raise Rejected("REPLAY_FRAME_CONTENT_CONFLICT")
                return "REPLAY_FRAME"
            if frame_no!=state[2]:
                raise Rejected("FRAME_ORDER")
            stream_specs={(s["topic"],s["partition"]):s for s in document["streams"]}
            for record in records:
                stream=stream_specs.get((record.topic,record.partition))
                if stream is None or record.topic_id!=stream["topic_id"]:
                    raise Rejected("TOPIC_ID_CHANGED")
                checkpoint=connection.execute(
                    "SELECT topic_id,next_offset FROM sink_meta.checkpoints WHERE epoch=%s AND topic=%s "
                    "AND partition=%s FOR UPDATE",(epoch,record.topic,record.partition)).fetchone()
                if checkpoint!=(record.topic_id,record.offset):
                    raise Rejected("OFFSET_ORDER_OR_GAP")
                connection.execute(
                    "UPDATE sink_meta.checkpoints SET next_offset=next_offset+1 WHERE epoch=%s AND topic=%s "
                    "AND partition=%s",(epoch,record.topic,record.partition))
            events=[decode(r,document["topic_prefix"],document["source_incarnation"]) for r in records]
            tx=transaction(events,spec,document["source_incarnation"])
            outcome="TOMBSTONE_ONLY"
            if tx is not None:
                if tx["commit"]<=document["baseline"]["scn"]:
                    raise Rejected("EVENT_NOT_AFTER_TEST_BASELINE", "no automatic baseline skip/cutover")
                old=connection.execute(
                    "SELECT fingerprint FROM sink_meta.transaction_ledger WHERE epoch=%s AND source_tx=%s",
                    (epoch,tx["key"])).fetchone()
                seen=[]
                for event in tx["dml"]:
                    prior=connection.execute(
                        "SELECT fingerprint FROM sink_meta.event_ledger WHERE epoch=%s AND event_id=%s",
                        (epoch,event["identity"])).fetchone()
                    if prior and prior[0]!=event["fingerprint"]:
                        raise Rejected("SOURCE_EVENT_CONTENT_CONFLICT")
                    seen.append(prior is not None)
                if old is not None:
                    if old[0]!=tx["fingerprint"] or not all(seen):
                        raise Rejected("SOURCE_TRANSACTION_REPLAY_CONFLICT")
                    outcome="REPLAY_SOURCE"
                else:
                    if any(seen):
                        raise Rejected("PARTIAL_SOURCE_REPLAY")
                    if tx["commit"]<state[3]:
                        raise Rejected("OUT_OF_ORDER_COMMIT")
                    for event in tx["dml"]:
                        apply_dml(connection,event)
                        connection.execute(
                            "INSERT INTO sink_meta.event_ledger VALUES(%s,%s,%s)",
                            (epoch,event["identity"],event["fingerprint"]))
                    connection.execute("INSERT INTO sink_meta.transaction_ledger VALUES(%s,%s,%s)",
                                       (epoch,tx["key"],tx["fingerprint"]))
                    connection.execute("UPDATE sink_meta.epochs SET last_commit_scn=%s WHERE epoch=%s",
                                       (tx["commit"],epoch))
                    outcome="APPLIED"
            if failpoint:
                failpoint("after_dml")
            for record,event in zip(records,events):
                record_outcome=("TOMBSTONE" if event["kind"]=="tombstone" else
                                "CONTROL" if event["kind"]=="control" else outcome)
                connection.execute(
                    "INSERT INTO sink_meta.inbox VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (epoch,record.topic_id,record.partition,record.offset,digest(record.wire()),
                     Jsonb(record.wire()),record_outcome))
            connection.execute("INSERT INTO sink_meta.frames VALUES(%s,%s,%s,%s)",
                               (epoch,frame_no,wire_hash,outcome))
            connection.execute("UPDATE sink_meta.epochs SET next_frame=next_frame+1 WHERE epoch=%s",(epoch,))
            if failpoint:
                failpoint("before_commit")
        if failpoint:
            failpoint("after_commit")
        return outcome
    except Rejected as error:
        # Failure evidence commits separately; target DML/ledger/checkpoints were rolled back.
        if error.code not in ("EPOCH_BLOCKED","BASELINE_MANIFEST_MISMATCH"):
            _quarantine(connection,epoch,frame_no,records,error)
        raise
    except psycopg.Error as error:
        if error.sqlstate and error.sqlstate[:2] in ("22","23"):
            rejected=Rejected("POSTGRES_"+error.sqlstate)
            _quarantine(connection,epoch,frame_no,records,rejected)
            raise rejected from error
        # Connection failures, serialization/deadlock etc. stay retryable, without skipped offsets.
        raise
