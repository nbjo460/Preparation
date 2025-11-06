# test_mavlink_extractor.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import math
import pytest
from pymavlink import mavutil
from business_logic.messages_extractor import MessagesExtractor
from utils.enums import RunMode
from utils.logger import AppLogger

logger = AppLogger("Tests")
BIN_PATH = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"


def fix_nan_message(message: dict):
    to_fix = message.get("Default")
    if to_fix and isinstance(to_fix, float) and math.isnan(to_fix):
        message["Default"] = "Nan"


# פונקציית עזר: קריאת כל ההודעות (ל-NORMAL ו-THREADS בלבד)
def read_all_mavlink_messages(filepath):
    messages = []
    conn = mavutil.mavlink_connection(filepath)
    logger.info("Reading full MAVLink file (pymavlink)...")
    while True:
        msg = conn.recv_match(blocking=False)
        if msg is None:
            break
        messages.append(msg.to_dict())
    logger.debug(f"pymavlink loaded {len(messages)} messages.")
    return messages


# פונקציית עזר: גנרטור של הודעות pymavlink (ל-MULTIPROCESS)
def mavlink_message_generator(filepath):
    conn = mavutil.mavlink_connection(filepath)
    logger.info("Starting MAVLink generator stream...")
    while True:
        msg = conn.recv_match(blocking=False)
        if msg is None:
            break
        yield msg.to_dict()
    logger.debug("MAVLink generator finished.")


# Fixture: הודעות מלאות – רק ל-NORMAL ו-THREADS
@pytest.fixture(scope="session")
def full_mavlink_messages():
    return read_all_mavlink_messages(BIN_PATH)


# Fixture: אובייקט extractor
@pytest.fixture
def extractor():
    return MessagesExtractor()


# בדיקה 1: NORMAL ו-THREADS – טעינה מלאה מראש
@pytest.mark.parametrize("run_mode", [RunMode.NORMAL, RunMode.THREADS])
def test_extractor_in_memory_modes(full_mavlink_messages, extractor, run_mode, caplog):
    caplog.set_level("DEBUG", logger="Tests")

    logger.info(f"Testing {run_mode} mode (in-memory comparison)")

    extractor_msgs = list(
        extractor.from_bin(
            BIN_PATH,
            to_round=True,
            run_mode=run_mode,
            num_workers=4
        )
    )

    assert len(extractor_msgs) == len(full_mavlink_messages), \
        f"Length mismatch in {run_mode}"

    successes = failures = 0
    first_fail_idx = None

    for i, (ext_msg, ref_msg) in enumerate(zip(extractor_msgs, full_mavlink_messages)):
        if ext_msg.get("mavpackettype") == "PARM":
            fix_nan_message(ext_msg)
            fix_nan_message(ref_msg)

        if ext_msg != ref_msg:
            failures += 1
            if first_fail_idx is None:
                first_fail_idx = i
                logger.error(f"First mismatch at index {i} in {run_mode}")
                logger.error(f"Extractor: {ext_msg}")
                logger.error(f"Reference: {ref_msg}")
            # אפשר להמשיך או להיכשל מיד
            continue  # או: pytest.fail(...)
        else:
            successes += 1

        if (i + 1) % 100_000 == 0:
            logger.info(f"{run_mode}: {i + 1} messages | OK: {successes}, Fail: {failures}")

    logger.info(f"{run_mode} completed: {successes} OK, {failures} FAILED")
    if failures:
        pytest.fail(f"{failures} messages differed in {run_mode} (first at {first_fail_idx})")


# בדיקה 2: MULTIPROCESS – השוואה תוך כדי זרימה (חוסך זיכרון)
def test_extractor_multiprocess_streaming(extractor, caplog):
    caplog.set_level("DEBUG", logger="Tests")
    run_mode = RunMode.MULTIPROCESS

    logger.info("Testing MULTIPROCESS mode (streaming comparison – low memory)")

    extractor_gen = extractor.from_bin(
        BIN_PATH,
        to_round=True,
        run_mode=run_mode,
        num_workers=4
    )

    ref_gen = mavlink_message_generator(BIN_PATH)

    successes = failures = 0
    first_fail_idx = None

    for i, (ext_msg, ref_msg) in enumerate(zip(extractor_gen, ref_gen)):
        if ext_msg.get("mavpackettype") == "PARM":
            fix_nan_message(ext_msg)
            fix_nan_message(ref_msg)

        if ext_msg != ref_msg:
            failures += 1
            if first_fail_idx is None:
                first_fail_idx = i
                logger.error(f"First streaming mismatch at index {i}")
                logger.error(f"Extractor: {ext_msg}")
                logger.error(f"Reference: {ref_msg}")
        else:
            successes += 1

        if (i + 1) % 1_000_000 == 0:
            logger.info(f"Streaming: {i + 1} messages | OK: {successes}, Fail: {failures}")

    logger.info(f"MULTIPROCESS streaming completed: {successes} OK, {failures} FAILED")
    if failures:
        pytest.fail(f"{failures} messages differed in streaming (first at {first_fail_idx})")