# test_compare_grok.py
import sys
import math
from pathlib import Path
from typing import Iterator, Dict, Any
import pytest

# תיקון נתיב
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pymavlink import mavutil
from business_logic.messages_extractor import MessagesExtractor
from utils.enums import RunMode

# === קונפיג ===
BIN_PATH = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"
PRINT_EVERY = 100_000


# === צבעים ===
class bcolors:
    OK = '\033[92m';
    WARN = '\033[93m';
    FAIL = '\033[91m';
    BOLD = '\033[1m';
    ENDC = '\033[0m'


def live(msg: str, color=bcolors.OK):
    print(f"{color}{msg}{bcolors.ENDC}")


# === תיקון NaN ===
def fix_nan(msg: Dict[str, Any]):
    if msg.get("mavpackettype") == "PARM" and "Default" in msg:
        if math.isnan(msg["Default"]):
            msg["mavpackettype"] = "Nan"  # בדיוק כמו שלך!


# === pymavlink stream ===
def mavlink_stream(skip_fmt: bool = False) -> Iterator[Dict[str, Any]]:
    conn = mavutil.mavlink_connection(BIN_PATH)
    while True:
        msg = conn.recv_match(blocking=False)
        if msg is None: break
        d = msg.to_dict()
        if skip_fmt and d["mavpackettype"] == "FMT": continue
        fix_nan(d)
        yield d


# === extractor stream – מוסיף mavpackettype בדיוק כמו שלך! ===
def extractor_stream(mode: RunMode, skip_fmt: bool = False):
    extractor = MessagesExtractor()
    gen = extractor.from_bin(BIN_PATH, to_round=True, run_mode=mode, num_workers=4)

    # בנה מיפוי: type (int) → FMT name
    fmt_messages = extractor._reader.fmt_messages
    type_to_name = {fmt["Type"]: fmt["name"] for fmt in fmt_messages}

    for msg in gen:
        msg_type = msg.get("type")

        # דילוג על FMT
        if skip_fmt and msg_type in type_to_name and type_to_name[msg_type] == "FMT":
            continue

        # הוסף mavpackettype – בדיוק כמו בקוד שלך!
        if msg_type in type_to_name:
            msg["mavpackettype"] = type_to_name[msg_type]
        else:
            msg["mavpackettype"] = f"UNKNOWN_{msg_type}"

        fix_nan(msg)
        yield msg


# === הטסט – עם דילוג על FMT ===
@pytest.mark.parametrize("mode", [RunMode.NORMAL, RunMode.THREADS, RunMode.MULTIPROCESS])
@pytest.mark.parametrize("skip_fmt", [False, True])
def test_reader_vs_mavlink(mode: RunMode, skip_fmt: bool):
    label = "דילוג על FMT" if skip_fmt else "בדיקה מלאה"
    live(f"\n{'=' * 90}", bcolors.BOLD)
    live(f" TEST: {mode.value.upper()} | {label} | 9M+ הודעות", bcolors.WARN)
    live(f"{'=' * 90}", bcolors.BOLD)

    total = err = 0
    start = __import__('time').time()

    try:
        mav_gen = mavlink_stream(skip_fmt)
        ext_gen = extractor_stream(mode, skip_fmt)

        for i, (mav_msg, ext_msg) in enumerate(zip(mav_gen, ext_gen), 1):
            total += 1

            mav_type = mav_msg.get("mavpackettype")
            ext_type = ext_msg.get("mavpackettype")

            if mav_type != ext_type:
                err += 1
                live(f"\n{bcolors.FAIL}שגיאה #{err} בהודעה {i:,}", bcolors.FAIL)
                live(f"   pymavlink: {mav_type}")
                live(f"   extractor: {ext_type}")
                live(f"   raw: {ext_msg}")
                pytest.fail(f"אי-התאמה #{i:,}")

            if i % PRINT_EVERY == 0:
                speed = i / (__import__('time').time() - start)
                live(f" {i:,} | {speed:,.0f}/שנייה | שגיאות: {err}")

    except KeyboardInterrupt:
        live("\n נעצר", bcolors.WARN)
        return

    elapsed = __import__('time').time() - start
    live(f"\n{bcolors.OK}סיום! {total:,} הודעות ב-{elapsed:.1f}ש", bcolors.OK)
    live(f" מהירות: {total / elapsed:,.0f}/שנייה | שגיאות: {err}")
    live(f" {'עבר!' if err == 0 else 'נכשל'}", bcolors.OK if err == 0 else bcolors.FAIL)
    assert err == 0