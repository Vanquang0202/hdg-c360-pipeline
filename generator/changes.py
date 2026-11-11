# Mục đích: Lập kế hoạch hoặc áp dụng một kịch bản biến động để thử CDC.
# Đầu vào: Tên kịch bản, fixture đã seed và tùy chọn --apply.
# Đầu ra: Một transaction INSERT/UPDATE/DELETE có kiểm tra đúng fixture; mặc định chỉ in kế hoạch.
"""Plan or apply repeatable HDG CDC scenarios against seeded fixture rows.

Examples from the project root in PowerShell:
  python generator/changes.py update-info
  python generator/changes.py update-info --apply
  python generator/changes.py insert --count 50000 --batch-size 1000 --apply
Each non-insert scenario targets an independent named fixture.
delete-last runs after delete-latest. move-household updates the household key;
add-household inserts a newer row while retaining the original household.
"""

import argparse
import sys
from datetime import datetime, timedelta

from seed import (
    BASE_DATE, PEOPLE, connect, fixtures,
    insert_batches, person, positive_int,
)

TARGETS = {
    "update-info": "cdc_repair",
    "add-household": "cdc_add_household",
    "move-household": "cdc_move_household",
    "change-code": "cdc_change_code",
    "cancel-approval": "cdc_cancel_approval",
    "close-type": "cdc_close_type",
    "approve": "cdc_approve_pending",
    "delete-latest": "cdc_delete_latest",
    "delete-last": "cdc_delete_older",
}
DESCRIPTIONS = {
    "insert": "Insert new citizens in the reserved 98xxxxxxxx namespace once.",
    "update-info": "Repair name, CCCD, birth date and all three addresses.",
    "add-household": "Insert a newer household row for an existing citizen.",
    "move-household": "Update MA_HO_GD on one fixture row.",
    "change-code": "Change MA_CD and preserve the old code in MA_CD_OLD.",
    "cancel-approval": "Set TRANG_THAI_DUYET=4.",
    "close-type": "Set TYPEID=GG.",
    "approve": "Approve the pending row by setting TRANG_THAI_DUYET=2.",
    "delete-latest": "Delete the newer row of citizen 9000000008, leaving the older row.",
    "delete-last": "Delete the last row of citizen 9000000008 after delete-latest.",
}


def current_count(cursor, code):
    cursor.execute(f"SELECT COUNT(*) FROM {PEOPLE} WHERE MA_CD=:code", code=code)
    return cursor.fetchone()[0]


def require_fixture(cursor, row):
    # ID chỉ được dùng cho fixture này sau khi kiểm cả khóa hộ và tính duy nhất hiện tại, không phải khóa nguồn mặc định.
    cursor.execute(
        f"""SELECT ID, GHI_CHU FROM {PEOPLE}
            WHERE MA_CD=:code
              AND (MA_HO_GD=:house OR (MA_HO_GD IS NULL AND :house IS NULL))""",
        code=row["MA_CD"], house=row["MA_HO_GD"],
    )
    matches = cursor.fetchmany(2)
    if matches != [(row["ID"], row["GHI_CHU"])]:
        raise RuntimeError("Fixture absent, already changed, or logical key is duplicated.")
    # Verify the row ID is also unique before using it in DML.
    cursor.execute(f"SELECT COUNT(*) FROM {PEOPLE} WHERE ID=:id", id=row["ID"])
    if cursor.fetchone()[0] != 1:
        raise RuntimeError("Fixture ID is not unique.")


def inserted_people(count, changed_at):
    for index in range(1, count + 1):
        row = person(index, "cdc_insert")
        row.update(
            ID=f"HDG-CDC-INSERT-{index:08d}",
            MA_CD=f"98{index:08d}",
            MA_HO_GD=f"HDG-CDC-H-{index:08d}",
            SO_CCCD=f"0098{index:08d}",
            CREATED_DATE=changed_at, MODIFY_DATE=changed_at,
            GHI_CHU="HDG-CDC:insert",
        )
        yield row


def mutate(cursor, scenario, count, batch_size):
    # One transaction and a fail-fast writer lock make each small demo
    # scenario atomic. Readers/CDC can continue reading committed data.
    # BƯỚC 2: Lấy khóa writer, kiểm fixture còn nguyên rồi áp đúng một kịch bản trong transaction.
    cursor.execute(f"LOCK TABLE {PEOPLE} IN EXCLUSIVE MODE NOWAIT")
    changed_at = max(datetime.now().replace(microsecond=0),
                     BASE_DATE + timedelta(days=2))
    if scenario == "insert":
        require_fixture(cursor, fixtures()["cdc_add_household"])
        cursor.execute(
            f"""SELECT COUNT(*) FROM {PEOPLE}
                WHERE SUBSTR(ID,1,15)='HDG-CDC-INSERT-'
                   OR (LENGTH(MA_CD)=10 AND SUBSTR(MA_CD,1,2)='98')"""
        )
        if cursor.fetchone()[0]:
            raise RuntimeError("CDC insert namespace already contains rows; refusing to append.")
        return insert_batches(cursor, PEOPLE, inserted_people(count, changed_at), batch_size)

    row = fixtures()[TARGETS[scenario]]
    require_fixture(cursor, row)
    if scenario == "add-household":
        cursor.execute(
            f"SELECT COUNT(*) FROM {PEOPLE} WHERE ID=:id OR MA_HO_GD=:house",
            id="HDG-CDC-ADD-HOUSEHOLD", house="HDG-CDC-NEW-HOUSEHOLD",
        )
        if cursor.fetchone()[0]:
            raise RuntimeError("The additional household scenario was already applied.")
        new_row = row | dict(
            ID="HDG-CDC-ADD-HOUSEHOLD", MA_HO_GD="HDG-CDC-NEW-HOUSEHOLD",
            MODIFY_DATE=changed_at, GHI_CHU="HDG-CDC:add-household",
        )
        return insert_batches(cursor, PEOPLE, [new_row], batch_size)

    if scenario in ("delete-latest", "delete-last"):
        expected = 2 if scenario == "delete-latest" else 1
        if current_count(cursor, row["MA_CD"]) != expected:
            raise RuntimeError(f"Expected {expected} household rows for this delete scenario.")
        cursor.execute(
            f"DELETE FROM {PEOPLE} WHERE ID=:id AND MA_CD=:code AND GHI_CHU=:note",
            id=row["ID"], code=row["MA_CD"], note=row["GHI_CHU"],
        )
    else:
        updates = {
            "update-info": dict(
                HO_TEN="Nguyễn Văn An Minh", SO_CCCD="001095012345",
                NGAY_SINH=19950815, PHONE_NUMBER="0912345678",
                MA_TINH="01TTT", MA_XA="00004",
                MA_TINH_LH="01TTT", MA_XA_LH="00004",
                MA_TINH_KS="01TTT", MA_XA_KS="00004",
            ),
            "move-household": dict(MA_HO_GD="HDG-CDC-MOVED-HOUSEHOLD"),
            "change-code": dict(MA_CD="9700000004", MA_CD_OLD=row["MA_CD"]),
            "cancel-approval": dict(TRANG_THAI_DUYET=4),
            "close-type": dict(TYPEID="GG"),
            "approve": dict(TRANG_THAI_DUYET=2),
        }[scenario]
        if scenario == "move-household":
            cursor.execute(
                f"SELECT COUNT(*) FROM {PEOPLE} WHERE MA_CD=:code AND MA_HO_GD=:house",
                code=row["MA_CD"], house=updates["MA_HO_GD"],
            )
            if cursor.fetchone()[0]:
                raise RuntimeError("Destination household already exists for this citizen.")
        if scenario == "change-code" and current_count(cursor, updates["MA_CD"]):
            raise RuntimeError("Destination citizen code already exists.")
        updates.update(MODIFY_DATE=changed_at, GHI_CHU=f"HDG-CDC:{scenario}")
        assignments = ", ".join(f"{column}=:{column}" for column in updates)
        cursor.execute(
            f"""UPDATE {PEOPLE} SET {assignments}
                WHERE ID=:fixture_id AND MA_CD=:old_code AND GHI_CHU=:old_note""",
            updates | dict(fixture_id=row["ID"], old_code=row["MA_CD"],
                           old_note=row["GHI_CHU"]),
        )
    if cursor.rowcount != 1:
        raise RuntimeError(f"Expected one affected row, got {cursor.rowcount}.")
    return cursor.rowcount


def main():
    # BƯỚC 1: Đọc kịch bản; chỉ kết nối và ghi Oracle khi người chạy có --apply.
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario", choices=list(DESCRIPTIONS))
    parser.add_argument("--count", type=positive_int, default=1,
                        help="New rows for the insert scenario")
    parser.add_argument("--batch-size", type=positive_int, default=1_000)
    parser.add_argument("--apply", action="store_true",
                        help="Connect and commit this scenario; default prints a plan offline")
    args = parser.parse_args()
    if args.count > 99_999_999:
        parser.error("--count must be at most 99999999")
    if args.scenario != "insert" and args.count != 1:
        parser.error("--count applies only to insert")
    print(args.scenario, "-", DESCRIPTIONS[args.scenario])
    if args.scenario != "insert":
        row = fixtures()[TARGETS[args.scenario]]
        print("Target", row["ID"], row["MA_CD"], row["MA_HO_GD"])
    # Thiếu --apply thì dừng ở bản kế hoạch, chưa có kết nối hoặc thay đổi Oracle.
    if not args.apply:
        print("Plan only. Add --apply to commit this scenario.")
        return
    # BƯỚC 3: Kiểm guard, áp một kịch bản và commit cùng nhau; lỗi sẽ không ghi một phần.
    with connect() as connection, connection.cursor() as cursor:
        affected = mutate(cursor, args.scenario, args.count, args.batch_size)
        connection.commit()
    print(f"Committed {affected} affected rows.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ImportError) as error:
        sys.exit(str(error))
