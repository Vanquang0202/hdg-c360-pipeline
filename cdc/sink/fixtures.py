# Mục đích: Sinh Debezium event giả lập để chạy demo/test Sink độc lập.
# Đầu vào: Schema nguồn và các ảnh dòng do test/CLI cung cấp.
# Đầu ra: Manifest, c/u/d, metadata giao dịch và tombstone; không phát DML Oracle/Kafka.
"""Synthetic Debezium schema+payload events with explicit empty/seeded test baselines."""
import base64
import copy
import json
import uuid
from collections import Counter
from datetime import datetime
from decimal import Decimal, localcontext
from contract import TABLES, CONTRACT_HASH
from protocol import EPOCH, Record


def field(column):
    result=dict(field=column["name"].upper(),optional=not column["required"])
    if column["family"]=="string":
        return dict(result,type="string")
    if column["family"]=="datetime":
        return dict(result,type="int64",name=("io.debezium.time.Timestamp"
                    if column["temporal_precision"]==0 else "io.debezium.time.MicroTimestamp"))
    p,s=column["precision"],column["scale"]
    if p is None:
        return dict(result,type="struct",name="io.debezium.data.VariableScaleDecimal",
                    fields=[dict(field="scale",type="int32"),dict(field="value",type="bytes")])
    if s==0 and p<=18:
        bits=8 if p<=2 else 16 if p<=4 else 32 if p<=9 else 64
        return dict(result,type="int"+str(bits))
    return dict(result,type="bytes",name="org.apache.kafka.connect.data.Decimal",
                parameters={"scale":str(s),"connect.decimal.precision":str(p)})


def row_schema(source):
    return dict(type="struct",optional=True,name="fixture.VSS."+source+".Value",
                fields=[field(c) for c in TABLES[source]["columns"]])


def binary_integer(number):
    size=max(1,(number.bit_length()+8)//8)
    return base64.b64encode(number.to_bytes(size,"big",signed=True)).decode("ascii")


def encoded_value(v,f):
    # Đổi dữ liệu giả lập sang logical type Debezium exact, để test đúng đường giải mã của Sink.
    if v is None:
        return None
    if f.get("name")=="io.debezium.data.VariableScaleDecimal":
        d=Decimal(v); sign,digits,exponent=d.as_tuple()
        number=int("".join(map(str,digits))) * (-1 if sign else 1)
        return dict(scale=-exponent,value=binary_integer(number))
    if f.get("name")=="org.apache.kafka.connect.data.Decimal":
        with localcontext() as context:
            context.prec=1100
            shifted=Decimal(v).scaleb(int(f["parameters"]["scale"]))
            if shifted!=shifted.to_integral_value():
                raise ValueError("Fixture would round")
            return binary_integer(int(shifted))
    if f.get("name") in ("io.debezium.time.Timestamp","io.debezium.time.MicroTimestamp"):
        if isinstance(v,str):
            v=datetime.fromisoformat(v)
        delta=v-EPOCH
        micros=(delta.days*86400+delta.seconds)*1000000+delta.microseconds
        return micros//1000 if f["name"]=="io.debezium.time.Timestamp" else micros
    return v


def encoded_row(source, values):
    if values is None:
        return None
    return {f["field"]:encoded_value(values[f["field"].lower()],f) for f in row_schema(source)["fields"]}


def image(source, **overrides):
    result={}
    for c in TABLES[source]["columns"]:
        result[c["name"]]=None
        if c["required"]:
            result[c["name"]]=("X" if c["family"]=="string" else
                               datetime(2026,1,1) if c["family"]=="datetime" else Decimal(1))
    result.update(overrides)
    return result


def envelope_bytes(schema,payload):
    return json.dumps(dict(schema=schema,payload=payload),ensure_ascii=False,
                      separators=(",",":")).encode("utf-8")


class Builder:
    def __init__(self, baseline=None, starts=None):
        self.prefix="hdg_sink_test"
        self.epoch=str(uuid.uuid4())
        self.baseline=baseline or {}
        names=[self.prefix+".VSS."+s for s in TABLES]+[self.prefix+".transaction"]
        self.starts={topic:(starts or {}).get(topic,0) for topic in names}
        self.next=dict(self.starts)
        self.ids={topic:str(uuid.uuid5(uuid.UUID(self.epoch),topic)) for topic in names}
        self.records=[]
        self.frames=[]

    def emit(self,topic,key,value):
        record=Record(topic,self.ids[topic],0,self.next[topic],key,value)
        self.next[topic]+=1
        self.records.append(record)
        return record

    def control(self,status,txid,counts=None):
        payload=dict(status=status,id=txid,event_count=None if counts is None else sum(counts.values()),
                     data_collections=None if counts is None else [
                         dict(data_collection="FREEPDB1.VSS."+name,event_count=count)
                         for name,count in counts.items()])
        schema=dict(type="struct",fields=[
            dict(field="status",type="string"),dict(field="id",type="string"),
            dict(field="event_count",type="int64",optional=True),
            dict(field="data_collections",type="array",optional=True,items=dict(type="struct",fields=[
                dict(field="data_collection",type="string"),dict(field="event_count",type="int64")]))])
        return self.emit(self.prefix+".transaction",None,envelope_bytes(schema,payload))

    def transaction(self,operations,commit=None,txid=None,tombstone=False):
        # Sinh BEGIN, DML và END khớp số lượng; đây là dữ liệu giả, không ghi Oracle/Kafka.
        number=len(self.frames)+1
        txid=txid or "fixture.1."+str(number)
        commit=commit or 100+number*10
        first=len(self.records)
        self.control("BEGIN",txid)
        counts=Counter()
        for order,(source,op,before,after) in enumerate(operations,1):
            counts[source]+=1
            table=TABLES[source]
            row_spec=row_schema(source)
            schema=dict(type="struct",fields=[
                dict(row_spec,field="before"),dict(row_spec,field="after"),
                dict(field="source",type="struct"),dict(field="op",type="string"),
                dict(field="transaction",type="struct"),dict(field="ts_ms",type="int64",optional=True)])
            payload=dict(before=encoded_row(source,before),after=encoded_row(source,after),op=op,
                         source=dict(schema="VSS",table=source,name=self.prefix,db="FREEPDB1",
                                     snapshot=False,txId=txid,scn=str(commit-1),commit_scn=str(commit),
                                     rs_id="fixture-redo-"+str(commit),ssn=order,redo_thread=1,
                                     row_id="physical-rowid-is-not-a-key",ts_ms=1),
                         transaction=dict(id=txid,total_order=order,data_collection_order=counts[source]),
                         ts_ms=1)
            key=None
            if table["primary_key"]:
                data=after if op=="c" else before
                key_fields=[f for f in row_spec["fields"] if f["field"].lower() in table["primary_key"]]
                key_payload={f["field"]:encoded_value(data[f["field"].lower()],f) for f in key_fields}
                key=envelope_bytes(dict(type="struct",fields=key_fields),key_payload)
            self.emit(self.prefix+".VSS."+source,key,envelope_bytes(schema,payload))
            if op=="d" and tombstone:
                self.emit(self.prefix+".VSS."+source,key,None)
        self.control("END",txid,counts)
        frame=dict(txid=txid,event_count=len(operations),
                   records=[list(r.coordinate) for r in self.records[first:]])
        self.frames.append(frame)
        return len(self.frames)-1

    def clone(self,frame_no):
        # Phát lại cùng sự kiện nguồn tại offset mới để kiểm ledger không áp dữ liệu lần hai.
        old=self.frames[frame_no]
        lookup={r.coordinate:r for r in self.records}
        first=len(self.records)
        for coordinate in old["records"]:
            r=lookup[tuple(coordinate)]
            self.emit(r.topic,r.key,r.value)
        self.frames.append(dict(txid=old["txid"],event_count=old["event_count"],
                                records=[list(r.coordinate) for r in self.records[first:]]))
        return len(self.frames)-1

    def document(self):
        return dict(format="hdg-sink-prototype-v1",mode="synthetic-test",identity_policy="experimental-redo-v1",
                    epoch=self.epoch,contract_hash=CONTRACT_HASH,topic_prefix=self.prefix,
                    source_incarnation="synthetic-only:FREEPDB1:incarnation-1",
                    baseline=dict(assertion="verified-test-baseline",scn=0,
                                  tables={source:dict(schema=row_schema(source),
                                          rows=[encoded_row(source,r) for r in self.baseline.get(source,[])])
                                          for source in TABLES}),
                    streams=[dict(topic=topic,topic_id=self.ids[topic],partition=0,
                                  start=self.starts[topic],end=self.next[topic]) for topic in self.next],
                    frames=copy.deepcopy(self.frames))


def demo():
    # Tạo c/u/d/tombstone và một lượt replay INSERT cũ; baseline độc lập, không phải SCN cutover.
    b=Builder()
    dbhc=image("VSS_DM_DBHC",ma_dbhc="00004",ma_cha="01TTT",ten="Phường thử nghiệm",
               id=Decimal("99999999999999999999"),ngay_hieu_luc=datetime(2026,11,11,12,34,56,123456))
    changed=dict(dbhc,ten="Phường sau cập nhật")
    b.transaction([("VSS_DM_DBHC","c",None,dbhc)],commit=2457218)
    b.transaction([("VSS_DM_DBHC","u",dbhc,changed)],commit=2457221)
    b.transaction([("VSS_DM_DBHC","d",changed,None)],commit=2457224,tombstone=True)
    b.clone(0)  # Source replay at NEW Kafka offsets must not resurrect the deleted row.
    return b
