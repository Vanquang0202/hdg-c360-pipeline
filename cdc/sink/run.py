# Mục đích: Điểm bắt đầu CDC Sink prototype: tạo fixture, init baseline hoặc replay.
# Đầu vào: Manifest cùng JSONL/range Kafka hữu hạn; đích DB test được cố định.
# Đầu ra: Kết quả từng frame; không suy baseline từ offset 0/latest và không thực hiện cutover.
"""CLI for a fixed PostgreSQL test target and explicit finite test manifests."""
import argparse
import json
import time
from pathlib import Path
import psycopg
from contract import DATABASE, PORT
from protocol import loads, Record, Rejected
from engine import load, check_records
from storage import connect_test, bootstrap, reset_test_fixture
from engine import initialize, apply_frame


def replay(document,records,connection,retries=2):
    # BƯỚC 3: Áp lần lượt frame đã biết thứ tự; retry nguyên frame sau reconnect, không tự tiến offset khi lỗi.
    found=check_records(document,records)
    outcomes=[]
    original=connection
    try:
        for i,frame in enumerate(document["frames"]):
            batch=[found[tuple(c)] for c in frame["records"]]
            for attempt in range(retries+1):
                try:
                    result=apply_frame(connection,document,i,batch)
                    break
                except psycopg.Error as exc:
                    retryable=exc.sqlstate in ("40001","40P01","55P03","57014") or isinstance(
                        exc,(psycopg.OperationalError,psycopg.InterfaceError))
                    if not retryable or attempt==retries:
                        raise
                    connection.close()
                    time.sleep(0.1*(2**attempt))
                    connection=connect_test()
            outcomes.append(result)
            print(json.dumps(dict(frame=i,outcome=result,target=DATABASE,port=PORT)),flush=True)
        return outcomes
    finally:
        if connection is not original:
            connection.close()


def main():
    # BƯỚC 1: Chọn lệnh và manifest; kiểm baseline/range trước khi mở PostgreSQL test.
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command",choices=("generate-fixture","init","replay","reset-test"))
    parser.add_argument("--confirm-test-db")
    parser.add_argument("--manifest",type=Path)
    parser.add_argument("--input",type=Path)
    parser.add_argument("--kafka-bootstrap")
    parser.add_argument("--out",type=Path,default=Path(__file__).resolve().parent/"fixtures/generated")
    args=parser.parse_args()
    if args.command=="generate-fixture":
        from fixtures import demo
        b=demo()
        args.out.mkdir(parents=True,exist_ok=True)
        for name in ("manifest.json","records.jsonl"):
            if (args.out/name).exists():
                parser.error("Fixture files already exist; select a new --out")
        (args.out/"manifest.json").write_text(json.dumps(b.document(),ensure_ascii=False,indent=2),encoding="utf-8")
        (args.out/"records.jsonl").write_text(
            "\n".join(json.dumps(r.wire(),ensure_ascii=False) for r in b.records)+"\n",encoding="utf-8")
        print("Generated independent synthetic fixture; it is not an Oracle cutover baseline")
        return
    if args.command=="reset-test":
        if args.confirm_test_db!=DATABASE:
            parser.error("reset-test requires --confirm-test-db hdg_cdc_sink_test; destroys test fixtures only")
        with connect_test() as connection:
            reset_test_fixture(connection)
        print("Reset prototype test fixtures only; test schema/guard retained")
        return
    # BƯỚC 2: Bắt buộc baseline/offset tường minh trước init/replay; không đoán từ dữ liệu đang có.
    if args.manifest is None:
        parser.error("--manifest is mandatory; no offset/baseline default exists")
    document=load(args.manifest)
    if args.command=="replay":
        if bool(args.input)==bool(args.kafka_bootstrap):
            parser.error("Choose exactly one of --input or --kafka-bootstrap")
        if args.input:
            if args.input.stat().st_size>64*1024*1024:
                raise Rejected("PROTOTYPE_FIXTURE_FILE_LIMIT")
            records=[Record.from_wire(loads(line)) for line in args.input.read_bytes().splitlines() if line.strip()]
        else:
            from kafka_reader import read_ranges
            records=read_ranges(document,args.kafka_bootstrap)
        check_records(document,records)
    # BƯỚC 4: Chỉ vào DB test cố định; init nạp baseline, replay áp các frame đã kiểm tra.

    with connect_test() as connection:
        if args.command=="init":
            bootstrap(connection)
            initialize(connection,document)
            print("Initialized declared baseline in test database only")
        else:
            replay(document,records,connection)


if __name__=="__main__":
    try:
        main()
    except Rejected as error:
        raise SystemExit("Prototype blocked: "+error.code)
