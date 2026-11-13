"""Đọc CDC theo đợt nhỏ liên tục; dùng nguyên guard/append/replay của cutover."""
import copy
import json
import time
import subprocess
from urllib.request import urlopen
import psycopg
from confluent_kafka import Consumer, TopicPartition, TopicCollection, KafkaException
from confluent_kafka.admin import AdminClient
import cutover
import engine
import storage
from protocol import decode, Rejected
from kafka_reader import read_ranges

MAX_RECORDS = 5000
MAX_MANIFEST_BYTES = 32 * 1024 * 1024


def assemble(document, records):
    # BƯỚC 1: Đọc metadata theo offset topic transaction; chưa đủ END/data thì chờ.
    events = [(r, decode(r, document['topic_prefix'], document['source_incarnation'])) for r in records]
    controls = sorted([(r,e) for r,e in events if e['kind']=='control'], key=lambda x:x[0].offset)
    groups = []
    active = []
    for pair in controls:
        status = pair[1]['payload']['status']
        if status == 'BEGIN':
            if active: raise Rejected('WATCH_INTERLEAVED_TRANSACTION_METADATA')
            active = [pair]
        elif status == 'END':
            if not active: raise Rejected('WATCH_BEGIN_REQUIRED')
            if pair[1]['payload']['id'] != active[0][1]['payload']['id']:
                raise Rejected('WATCH_TRANSACTION_METADATA_ORDER')
            groups.append(active+[pair]); active=[]
        else: raise Rejected('WATCH_TRANSACTION_STATUS')
    if active: return None
    used = set()
    frames = []
    per_topic = {}
    for r,e in events:
        if e['kind']!='control': per_topic.setdefault((r.topic,r.partition),[]).append((r,e))
    for values in per_topic.values(): values.sort(key=lambda x:x[0].offset)
    for control in groups:
        txid=control[-1][1]['payload']['id']
        dml=[]
        # Source replay cùng txid được phân biệt bằng prefix offset của từng collection.
        for collection in control[-1][1]['payload']['data_collections']:
            name=collection['data_collection'].rsplit('.',1)[-1]
            candidates=sorted([(r,e) for r,e in events if e['kind']=='dml' and e['txid']==txid
                               and e['table']==name and r.coordinate not in used],key=lambda x:x[0].offset)
            count=collection['event_count']
            if len(candidates)<count:return None
            dml+=candidates[:count]
        dml.sort(key=lambda x:x[1]['order'])
        count=control[-1][1]['payload']['event_count']
        if len(dml)!=count:raise Rejected('WATCH_TRANSACTION_EVENT_COUNT')
        batch=[control[0][0]]
        # Tombstone không phải DELETE lần hai; giữ thứ tự offset trong từng data partition.
        for r,e in dml:
            rows=per_topic[(r.topic,r.partition)]
            pos=next(i for i,(item,_) in enumerate(rows) if item.coordinate==r.coordinate)
            for item,event in rows[:pos]:
                if item.coordinate in used:continue
                if event['kind']!='tombstone':raise Rejected('WATCH_DATA_TRANSACTION_ORDER')
                batch.append(item);used.add(item.coordinate)
            batch.append(r);used.add(r.coordinate)
            for item,event in rows[pos+1:]:
                if event['kind']!='tombstone':break
                if item.coordinate not in used:batch.append(item);used.add(item.coordinate)
        batch.append(control[-1][0])
        spec=dict(txid=txid,event_count=len(dml),records=[list(r.coordinate) for r in batch])
        engine.transaction([decode(r,document['topic_prefix'],document['source_incarnation']) for r in batch],spec,document['source_incarnation'])
        frames.append(spec);used.update(r.coordinate for r,_ in control)
    remaining=[(r,e) for r,e in events if r.coordinate not in used]
    if any(e['kind']!='tombstone' for r,e in remaining):return None
    # Tombstone đến sau lần apply DELETE được ACK riêng, vẫn không sinh DML.
    if remaining:
        frames.append(dict(txid=None,event_count=0,records=[list(r.coordinate) for r,e in sorted(remaining,key=lambda x:x[0].coordinate)]))
    # Kiểm prefix từng partition trước khi đăng ký manifest, không làm epoch mắc frame lỗi.
    offsets={(s['topic'],s['partition']):s['start'] for s in document['streams']}
    for spec in frames:
        for topic,part,offset in spec['records']:
            if offsets[(topic,part)]!=offset:raise Rejected('WATCH_PARTITION_ORDER')
            offsets[(topic,part)]+=1
    return frames


def inspect(connection, plan):
    # BƯỚC 2: Guard được kiểm lại mỗi vòng, gồm bảng/ACL/epoch/checkpoint và hash live.
    storage.assert_target(connection)
    cutover.safe_schema(connection)
    cutover.assert_handoff(connection,plan)
    row=connection.execute("SELECT before_ownership,last_data FROM hdg_load.cutovers WHERE id=%s AND state='ACTIVE'",(plan['id'],)).fetchone()
    cutover.require(row and cutover.ownership_matches(connection,row[0]),'OWNERSHIP_CHANGED')
    cutover.require(cutover.table_hashes(connection)==row[1],'MIRROR_EXTERNAL_CHANGE')


def cycle(connection, plan, admin, consumer, bootstrap, failpoint=None):
    inspect(connection,plan)
    # BƯỚC 3: Nếu process chết sau extend/DB commit, resume manifest lưu trong DB trước.
    state=cutover.assert_handoff(connection,plan)
    for index in range(state[1],len(plan['manifest']['frames'])):
        cutover.replay(connection,plan,index,failpoint)
    cp=cutover.checkpoints(connection,plan['manifest']['epoch'])
    # ACK offset Kafka chỉ sau PostgreSQL đã commit; lỗi ACK được retry bằng DB checkpoint.
    futures=admin.describe_topics(TopicCollection([s['topic'] for s in plan['manifest']['streams']]))
    ranges=[]
    for t,tid,p,n in cp:
        description=futures[t].result(timeout=10)
        cutover.require(str(description.topic_id)==tid,'KAFKA_TOPIC_ID_MISMATCH')
        # Manifest local chỉ có partition 0: thêm partition phải dừng, không bỏ mất event mới.
        cutover.require(p==0 and {part.id for part in description.partitions}=={0},'WATCH_PARTITION_LAYOUT_CHANGED')
        lo,hi=consumer.get_watermark_offsets(TopicPartition(t,p),timeout=10)
        cutover.require(lo<=n<=hi,'KAFKA_RANGE_UNAVAILABLE')
        ranges.append(dict(topic=t,topic_id=tid,partition=p,start=n,end=hi))
    consumer.commit(offsets=[TopicPartition(t,p,n) for t,tid,p,n in cp],asynchronous=False)
    backlog=sum(s['end']-s['start'] for s in ranges)
    if not backlog:return plan,'IDLE'
    # Backpressure hữu hạn: vượt giới hạn thì dừng rõ ràng, không bỏ/nhảy offset.
    cutover.require(backlog<=MAX_RECORDS,'WATCH_BACKLOG_LIMIT')
    records=read_ranges({'streams':ranges},bootstrap)
    frames=assemble(dict(plan['manifest'],streams=ranges),records)
    if frames is None:return plan,'WAITING_TRANSACTION'
    new=copy.deepcopy(plan)
    for s in new['manifest']['streams']:
        s['end']=next(r['end'] for r in ranges if r['topic']==s['topic'] and r['partition']==s['partition'])
    new['manifest']['frames']+=frames
    cutover.require(len(json.dumps(new))<=MAX_MANIFEST_BYTES,'WATCH_MANIFEST_LIMIT')
    # BƯỚC 4: Tận dụng validator toàn range và publish checkpoint hiện có.
    cutover.extend(connection,plan,new)
    for index in range(len(plan['manifest']['frames']),len(new['manifest']['frames'])):
        cutover.replay(connection,new,index,failpoint)
    consumer.commit(offsets=[TopicPartition(t,p,n) for t,tid,p,n in cutover.checkpoints(connection,new['manifest']['epoch'])],asynchronous=False)
    return new,'APPLIED'


def watch(connection, supplied, confirmation, bootstrap='127.0.0.1:29092', interval=5, stop_file=None):
    # BƯỚC 5: Giữ khóa session cho toàn vòng đời; mọi replay/extend thủ công dùng cùng khóa.
    cutover.require(stop_file is None or not stop_file.exists(),'WATCH_STOP_FILE_EXISTS')
    connection.execute("SET application_name='hdg_cdc_watch'")
    epoch=supplied['manifest']['epoch']
    conf={'bootstrap.servers':bootstrap,'broker.address.family':'v4'}
    admin=AdminClient(conf)
    consumer=Consumer(dict(conf,**{'group.id':'hdg-mirror-'+connection.info.dbname+'-'+epoch,'enable.auto.commit':False,
        'enable.auto.offset.store':False,'auto.offset.reset':'error','isolation.level':'read_committed',
        'allow.auto.create.topics':False,'queued.max.messages.kbytes':16384,'fetch.message.max.bytes':8388608}))
    retries=0; waiting=None
    try:
        with cutover.target(connection,supplied,confirmation,'watch'):
            while True:
                if stop_file is not None and stop_file.exists():
                    print(json.dumps(dict(state='STOPPED',reason='stop-file',epoch=epoch)),flush=True)
                    break
                try:
                    plan=connection.execute("SELECT plan FROM hdg_load.cutovers WHERE id=%s AND state='ACTIVE'",(supplied['id'],)).fetchone()
                    cutover.require(plan and plan[0]['manifest']['epoch']==epoch,'WATCH_EPOCH_CHANGED')
                    plan=plan[0]
                    cutover.require(plan['target']==supplied['target'] and plan['run_id']==supplied['run_id'],'WATCH_TARGET_CHANGED')
                    # Chỉ đọc identity/config connector; không reset hoặc restart Source.
                    with urlopen('http://127.0.0.1:8083/connectors/hdg-oracle-observe/config',timeout=10) as response:cfg=json.load(response)
                    cutover.require(cfg.get('database.hostname')=='hdg-c360-oracle' and cfg.get('database.pdb.name')=='FREEPDB1' and cfg.get('topic.prefix')==supplied['manifest']['topic_prefix']
                                    and cfg.get('provide.transaction.metadata')=='true','WATCH_SOURCE_CONFIG')
                    includes={item.strip().replace(chr(92),'') for item in cfg.get('table.include.list','').split(',')}
                    cutover.require(includes=={'VSS.'+name for name in cutover.TABLES} and cfg.get('database.connection.adapter')=='logminer','WATCH_SOURCE_TABLE_SCOPE')
                    with urlopen('http://127.0.0.1:8083/connectors/hdg-oracle-observe/status',timeout=10) as response:status=json.load(response)
                    cutover.require(status['connector']['state']=='RUNNING' and all(t['state']=='RUNNING' for t in status['tasks']),'WATCH_CONNECTOR_NOT_RUNNING')
                    # Xác minh DBID/resetlogs thật, không suy identity từ tên connector hay Kafka offset.
                    query="whenever sqlerror exit failure rollback\nset heading off feedback off pages 0\nalter session set container=FREEPDB1;\nselect 'IDENTITY='||dbid||':'||resetlogs_change#||':'||sys_context('USERENV','CON_NAME') from v$database;\nexit\n"
                    source=subprocess.run(['docker','exec','-i','hdg-c360-oracle','sqlplus','-L','-s','/','as','sysdba'],input=query,text=True,capture_output=True,timeout=30)
                    cutover.require(source.returncode==0 and ('IDENTITY='+supplied['manifest']['source_incarnation']+':FREEPDB1') in [line.strip() for line in source.stdout.splitlines()],'SOURCE_INCARNATION')
                    try:
                        plan,state=cycle(connection,plan,admin,consumer,bootstrap)
                    except Rejected as error:
                        if error.code in ('KAFKA_READ_ERROR','KAFKA_RANGE_TIMEOUT_OR_GAP'):
                            raise TimeoutError('Transient Kafka read; PostgreSQL checkpoint retained') from error
                        raise
                    print(json.dumps(dict(state=state,epoch=epoch,frames=len(plan['manifest']['frames']))),flush=True)
                    if state=='WAITING_TRANSACTION':
                        waiting=waiting or time.monotonic()
                        cutover.require(time.monotonic()-waiting<300,'WATCH_INCOMPLETE_TRANSACTION_TIMEOUT')
                    else:waiting=None
                    retries=0;time.sleep(interval)
                except (KafkaException,TimeoutError,psycopg.OperationalError,psycopg.InterfaceError) as error:
                    # Mất DB đồng nghĩa mất writer lock: thoát, không tự reconnect rồi ghi thiếu khóa.
                    if isinstance(error,(psycopg.OperationalError,psycopg.InterfaceError)):raise
                    retries+=1
                    cutover.require(retries<=8,'WATCH_RETRY_EXHAUSTED')
                    print(json.dumps(dict(state='RETRY',attempt=retries,error=type(error).__name__)),flush=True)
                    time.sleep(min(30,2**retries))
    except KeyboardInterrupt:
        print(json.dumps(dict(state='STOPPED',epoch=epoch)),flush=True)
    finally:consumer.close()
