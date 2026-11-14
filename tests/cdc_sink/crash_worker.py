# Mục đích: Tiến trình con để kiểm tra crash trước/sau commit của Sink test.
# Đầu vào: Manifest, JSONL và tên failpoint do integration test truyền.
# Đầu ra: Thoát tiến trình tại điểm đã chọn để kiểm tra rollback/replay; không import để chạy ứng dụng.
"""Integration subprocess: abruptly terminate only this test applier process."""
import os
import sys
from pathlib import Path
# Nạp module runtime ở vị trí gốc; test không chứa bản sao code Sink.
sys.path.insert(0,str(Path(__file__).resolve().parents[2] / "cdc/sink"))
from engine import load, check_records
from protocol import loads, Record
from storage import connect_test
from engine import apply_frame

document=load(Path(sys.argv[1]))
records=[Record.from_wire(loads(line)) for line in Path(sys.argv[2]).read_bytes().splitlines()]
found=check_records(document,records)
frame=document["frames"][0]
batch=[found[tuple(c)] for c in frame["records"]]
def crash(phase):
    if phase==sys.argv[3]:
        os._exit(91 if phase=="before_commit" else 92)
with connect_test() as connection:
    apply_frame(connection,document,0,batch,failpoint=crash)
raise SystemExit("Crash point was not hit")
