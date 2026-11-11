"""Finite test baselines and complete, explicitly ordered transaction manifests."""
import uuid
from contract import TABLES, CONTRACT_HASH
from protocol import loads, digest, integer, row, Rejected


def validate(document):
    if not isinstance(document, dict) or document.get("format") != "hdg-sink-prototype-v1":
        raise Rejected("MANIFEST_FORMAT")
    if document.get("mode") not in ("synthetic-test", "captured-fixture-test"):
        raise Rejected("TEST_MANIFEST_REQUIRED", "no live/cutover mode exists")
    if document.get("identity_policy") != "experimental-redo-v1":
        raise Rejected("IDENTITY_POLICY")
    uuid.UUID(document["epoch"])
    if document.get("contract_hash") != CONTRACT_HASH:
        raise Rejected("MANIFEST_CONTRACT_HASH")
    if not isinstance(document.get("source_incarnation"), str) or not document["source_incarnation"]:
        raise Rejected("SOURCE_INCARCATION_REQUIRED")
    prefix = document.get("topic_prefix")
    if prefix not in ("hdg_sink_test", "hdg_observe"):
        raise Rejected("TOPIC_PREFIX")
    baseline = document.get("baseline")
    if not isinstance(baseline, dict) or baseline.get("assertion") != "verified-test-baseline":
        raise Rejected("EXPLICIT_BASELINE_REQUIRED")
    integer(baseline.get("scn"), "test baseline SCN", 0)
    tables = baseline.get("tables", {})
    if set(tables) != set(TABLES):
        raise Rejected("BASELINE_ALL_FOUR_TABLES")
    for source, spec in tables.items():
        if not isinstance(spec.get("rows"), list):
            raise Rejected("BASELINE_ROWS")
        # Validate schema even when the declared baseline is empty.
        from protocol import descriptors
        descriptors(spec.get("schema"), TABLES[source])
        for raw in spec["rows"]:
            row(raw, spec["schema"], TABLES[source])
    streams = {}
    allowed = {prefix + ".VSS." + source for source in TABLES} | {prefix + ".transaction"}
    for spec in document.get("streams", []):
        if spec.get("topic") not in allowed or integer(spec.get("partition"), "partition", 0) != 0:
            raise Rejected("PROTOTYPE_ONE_PARTITION")
        if not isinstance(spec.get("topic_id"), str) or not spec["topic_id"]:
            raise Rejected("TOPIC_ID_REQUIRED")
        key = spec["topic"], spec["partition"]
        if key in streams:
            raise Rejected("DUPLICATE_STREAM")
        start = integer(spec.get("start"), "start offset", 0)
        end = integer(spec.get("end"), "end offset", 0)
        if end < start or end - start > 100000:
            raise Rejected("BOUNDED_RANGE_REQUIRED")
        streams[key] = spec
    if {topic for topic, _ in streams} != allowed:
        raise Rejected("ALL_STREAM_RANGES_REQUIRED")
    if sum(s["end"]-s["start"] for s in streams.values())>100000:
        raise Rejected("PROTOTYPE_RECORD_LIMIT")
    coordinates = []
    for frame in document.get("frames", []):
        integer(frame.get("event_count"), "frame event_count", 0)
        if not frame.get("records"):
            raise Rejected("EMPTY_FRAME")
        for coordinate in frame["records"]:
            if not isinstance(coordinate, list) or len(coordinate) != 3:
                raise Rejected("FRAME_COORDINATE")
            topic, partition, offset = coordinate
            key = topic, partition
            if key not in streams or not streams[key]["start"] <= offset < streams[key]["end"]:
                raise Rejected("FRAME_RANGE")
            coordinates.append(tuple(coordinate))
    expected = sum(s["end"]-s["start"] for s in streams.values())
    if len(coordinates) != expected or len(set(coordinates)) != expected:
        raise Rejected("FINITE_RANGE_NOT_FULLY_COVERED")
    return document


def manifest_hash(document):
    return digest(document)


def load(path):
    return validate(loads(path.read_bytes()))


def check_records(document, records):
    spec = {(s["topic"],s["partition"]):s for s in document["streams"]}
    found = {}
    total_bytes = 0
    for record in records:
        coordinate = record.coordinate
        stream = spec.get(coordinate[:2])
        if stream is None or record.topic_id != stream["topic_id"]:
            raise Rejected("TOPIC_ID_OR_STREAM")
        if not stream["start"] <= record.offset < stream["end"] or coordinate in found:
            raise Rejected("RECORD_OUTSIDE_FINITE_RANGE")
        found[coordinate] = record
        total_bytes += len(record.value or b"") + len(record.key or b"")
        if total_bytes > 64*1024*1024:
            raise Rejected("PROTOTYPE_BATCH_MEMORY_LIMIT")
    expected = {tuple(c) for frame in document["frames"] for c in frame["records"]}
    if set(found) != expected:
        raise Rejected("INCOMPLETE_FIXTURE")
    return found
