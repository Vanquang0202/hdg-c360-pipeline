# Mục đích: Đọc đúng các range Kafka đã khai báo để replay vào DB test.
# Đầu vào: Bootstrap server và manifest đã xác minh baseline/topic ID/start/end.
# Đầu ra: Raw record hữu hạn; chỉ đọc Kafka, không commit/reset offset hay tạo topic.
"""Bounded, read-only Kafka reader. Never commits/resets group or Source offsets."""
import time
import uuid
import base64
import json
import re
import subprocess
import sys
from contextvars import ContextVar
from contextlib import contextmanager
from confluent_kafka import Consumer, TopicPartition, TopicCollection
from confluent_kafka.admin import AdminClient
from protocol import Record, Rejected


_IDENTITY = ContextVar('hdg_native_topic_identity',default=None)


def canonical_uuid(value):
    # Chỉ đổi encoding, giữ đủ 128 bit; tuyệt đối không cắt/sign-extend để so sánh runtime.
    try:
        if re.fullmatch(r'[0-9a-fA-F-]{36}',value):raw=uuid.UUID(value).bytes
        else:raw=base64.b64decode(value.replace('-','+').replace('_','/')+'='*((-len(value))%4),validate=True)
        if len(raw)!=16:raise ValueError()
        return base64.urlsafe_b64encode(raw).decode().rstrip('=')
    except Exception as e:raise Rejected('TOPIC_UUID_ENCODING') from e


def native_metadata(admin, topics):
    descriptions={t:f.result(timeout=15) for t,f in admin.describe_topics(TopicCollection(topics)).items()}
    cluster=admin.list_topics(timeout=15).cluster_id
    if sys.platform=='win32':
        # SDK Windows describe_topics đã chuyển int64 thành long32 trước khi tạo Uuid.
        # Đọc metadata bằng CLI Java trong Kafka local; không dùng giá trị đã mất bit.
        if any(not re.fullmatch(r'[A-Za-z0-9._-]+',t) for t in topics):raise Rejected('TOPIC_NAME')
        cluster_probe=subprocess.run(['docker','exec','-e','KAFKA_HEAP_OPTS=-Xms16m -Xmx96m','hdg-cdc-kafka-1','/opt/kafka/bin/kafka-cluster.sh','cluster-id','--bootstrap-server','kafka:9092'],capture_output=True,text=True,timeout=30)
        match=re.search(r'Cluster ID:\s+(\S+)',cluster_probe.stdout)
        if cluster_probe.returncode or not match or match[1]!=cluster:raise Rejected('KAFKA_CLI_CLUSTER_MISMATCH')
        pattern='('+'|'.join(re.escape(t) for t in topics)+')'
        result=subprocess.run(['docker','exec','-e','KAFKA_HEAP_OPTS=-Xms16m -Xmx96m','hdg-cdc-kafka-1','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:9092','--describe','--topic',pattern],capture_output=True,text=True,timeout=30)
        if result.returncode:raise Rejected('NATIVE_TOPIC_METADATA_UNAVAILABLE')
        ids={name:canonical_uuid(value) for name,value in re.findall(r'Topic:\s+(\S+)\s+TopicId:\s+(\S+)',result.stdout)}
        if set(ids)!=set(topics):raise Rejected('NATIVE_TOPIC_SCOPE')
    else:ids={t:canonical_uuid(str(d.topic_id)) for t,d in descriptions.items()}
    return cluster,ids,descriptions


def load_bindings(connection, epoch):
    if connection.execute("SELECT to_regclass('sink_meta.topic_identity_bindings')").fetchone()[0] is None:return None
    rows=connection.execute("SELECT topic,legacy_id,native_id,cluster_id,proof_hash,proof,active FROM sink_meta.topic_identity_bindings WHERE epoch=%s ORDER BY topic",(epoch,)).fetchall()
    if not rows:return None
    from protocol import digest
    if len(rows)!=5 or not all(r[6] for r in rows):raise Rejected('TOPIC_BINDINGS_INACTIVE_OR_INCOMPLETE')
    result={'cluster_id':rows[0][3],'topics':{}}
    for topic,legacy,native,cluster,hashed,proof,active in rows:
        if digest(proof)!=hashed or str(proof['epoch'])!=str(epoch) or proof['cluster_id']!=cluster or cluster!=result['cluster_id']:
            raise Rejected('TOPIC_BINDING_PROOF_CHANGED')
        spec=proof['topics'][topic]
        if spec['legacy_id']!=legacy or spec['native_id']!=native:raise Rejected('TOPIC_BINDING_CHANGED')
        result['topics'][topic]=dict(legacy_id=legacy,native_id=native)
    return result


@contextmanager
def identity_scope(connection,epoch,maintenance=False):
    token=_IDENTITY.set(None if maintenance else load_bindings(connection,epoch))
    try:yield
    finally:_IDENTITY.reset(token)


def assert_topic(topic,expected,native,cluster,bindings=None):
    native=canonical_uuid(native)
    binding=_IDENTITY.get() if bindings is None else bindings
    if binding is not None and cluster!=binding['cluster_id']:raise Rejected('KAFKA_CLUSTER_CHANGED')
    if canonical_uuid(expected)==native:return
    item=(binding or {}).get('topics',{}).get(topic)
    if not item or item['legacy_id']!=expected or item['native_id']!=native:
        raise Rejected('KAFKA_TOPIC_ID_MISMATCH')


def read_ranges(document, bootstrap, timeout=30):
    # BƯỚC 1: Kiểm topic ID/watermark, assign đúng start rồi đọc tới end; Kafka offset được giữ nguyên.
    streams=[s for s in document["streams"] if s["start"]<s["end"]]
    if not streams:
        return []
    # Read-only metadata; pin topic UUIDs so a recreated topic cannot silently reuse offsets.
    topics=list({s["topic"] for s in streams})
    admin = AdminClient({"bootstrap.servers": bootstrap, "broker.address.family": "v4"})
    cluster,ids,descriptions=native_metadata(admin,topics)
    for stream in streams:
        assert_topic(stream['topic'],stream['topic_id'],ids[stream['topic']],cluster)
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
        # BƯỚC 2: Đọc đúng [start,end) do manifest chọn, không subscribe/commit group hoặc chọn latest.
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
        # Chặn topic đổi ID/partition trong lúc đọc range, trước khi trả batch cho DB.
        cluster,ids,descriptions=native_metadata(admin,topics)
        for stream in streams:
            assert_topic(stream['topic'],stream['topic_id'],ids[stream['topic']],cluster)
            if stream['partition']!=0 or {p.id for p in descriptions[stream['topic']].partitions}!={0}:
                raise Rejected('KAFKA_PARTITION_LAYOUT_CHANGED')
        return list(records.values())
    finally:
        # assign(), not subscribe(); no offset commit and no production group membership.
        # BƯỚC 3: Đóng reader; Kafka offsets vẫn không bị ghi lại bởi prototype.
        consumer.close()
