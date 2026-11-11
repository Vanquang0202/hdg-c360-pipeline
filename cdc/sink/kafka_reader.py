"""Bounded, read-only Kafka reader. Never commits/resets group or Source offsets."""
import time
import uuid
from confluent_kafka import Consumer, TopicPartition, TopicCollection
from confluent_kafka.admin import AdminClient
from protocol import Record, Rejected


def read_ranges(document, bootstrap, timeout=30):
    streams=[s for s in document["streams"] if s["start"]<s["end"]]
    if not streams:
        return []
    # Read-only metadata; pin topic UUIDs so a recreated topic cannot silently reuse offsets.
    topics=list({s["topic"] for s in streams})
    admin = AdminClient({"bootstrap.servers": bootstrap, "broker.address.family": "v4"})
    futures = admin.describe_topics(TopicCollection(topics))
    for stream in streams:
        if str(futures[stream["topic"]].result(timeout=10).topic_id)!=stream["topic_id"]:
            raise Rejected("KAFKA_TOPIC_ID_MISMATCH")
    consumer=Consumer({"bootstrap.servers":bootstrap,"broker.address.family":"v4","group.id":"hdg-sink-prototype-readonly-"+str(uuid.uuid4()),
                       "enable.auto.commit":False,"enable.auto.offset.store":False,"auto.offset.reset":"error",
                       "isolation.level":"read_committed","allow.auto.create.topics":False})
    specs={(s["topic"],s["partition"]):s for s in streams}
    records={}
    bytes_read=0
    deadline=time.monotonic()+timeout
    try:
        assigned=[TopicPartition(s["topic"],s["partition"],s["start"]) for s in streams]
        for tp in assigned:
            low,high=consumer.get_watermark_offsets(tp,timeout=10)
            s=specs[(tp.topic,tp.partition)]
            if low>s["start"] or high<s["end"]:
                raise Rejected("KAFKA_RANGE_UNAVAILABLE")
        consumer.assign(assigned)
        expected=sum(s["end"]-s["start"] for s in streams)
        while len(records)<expected:
            if time.monotonic()>deadline:
                raise Rejected("KAFKA_RANGE_TIMEOUT_OR_GAP")
            message=consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise Rejected("KAFKA_READ_ERROR",str(message.error().code()))
            s=specs[(message.topic(),message.partition())]
            if message.offset()>=s["end"]:
                consumer.pause([TopicPartition(message.topic(),message.partition())])
                continue
            if message.offset()<s["start"]:
                raise Rejected("KAFKA_BEFORE_EXPLICIT_RANGE")
            r=Record(message.topic(),s["topic_id"],message.partition(),message.offset(),message.key(),
                     message.value(),tuple(message.headers() or ()))
            if r.coordinate not in records:
                bytes_read+=len(r.value or b" ")+len(r.key or b" ")+sum(len(v or b"") for _,v in r.headers)
                if bytes_read>64*1024*1024:
                    raise Rejected("PROTOTYPE_BATCH_MEMORY_LIMIT")
            records[r.coordinate]=r
        return list(records.values())
    finally:
        # assign(), not subscribe(); no offset commit and no production group membership.
        consumer.close()
