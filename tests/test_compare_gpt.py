import math
import pytest
from business_logic.messages_extractor import MessagesExtractor
from pymavlink import mavutil
from pymavlink.CSVReader import CSVMessage
from utils.enums import RunMode
from utils.logger import AppLogger

logger = AppLogger("Tests")

path = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"

mavlink_data_messages = []
mavlink_fmt_messages = []

messages_extractor = MessagesExtractor()


def fix_nan_message(message: dict):
    if "Default" in message and isinstance(message["Default"], float) and math.isnan(message["Default"]):
        message["mavpackettype"] = "Nan"


def get_mav_messages():
    mav: CSVMessage = mavutil.mavlink_connection(path)
    logger.info("Reading the MAVLink file...")

    while True:
        gps_message: mav = mav.recv_match(blocking=False)
        if gps_message is None:
            break

        gps_message_dict = gps_message.to_dict()
        if gps_message_dict["mavpackettype"] == "FMT":
            mavlink_fmt_messages.append(gps_message_dict)
        else:
            mavlink_data_messages.append(gps_message_dict)

    logger.info(f"Loaded {len(mavlink_fmt_messages)} FMT messages and {len(mavlink_data_messages)} DATA messages.")


def stream_extractor_messages(run_mode: RunMode):
    for msg in messages_extractor.from_bin(path, to_round=True, run_mode=run_mode, num_workers=4):
        yield msg


@pytest.mark.parametrize("run_mode", [RunMode.NORMAL, RunMode.THREADS, RunMode.MULTIPROCESS])
# @pytest.mark.parametrize("check_fmt", [False, True])  # FLAG לבחירת FMT
# def test_one_to_one_messages(run_mode, check_fmt):
def test_one_to_one_messages(run_mode):
    if not mavlink_data_messages and not mavlink_fmt_messages:
        get_mav_messages()

    extractor_stream = stream_extractor_messages(run_mode=run_mode)

    success_count = 0
    # בודק DATA messages
    for num, (e_msg, m_msg) in enumerate(zip(extractor_stream, mavlink_data_messages), start=1):
        if num % 100_000 == 0:
            logger.info(f"Checked {num} DATA messages so far...")

        if e_msg.get("mavpackettype") == "PARM":
            fix_nan_message(e_msg)
            fix_nan_message(m_msg)

        for key in e_msg:
            if key in m_msg:
                if isinstance(e_msg[key], float) and math.isnan(e_msg[key]) and math.isnan(m_msg[key]):
                    continue
                assert e_msg[key] == m_msg[key], (
                    f"Mismatch in DATA message {num}, field '{key}': "
                    f"extractor={e_msg[key]}, mav={m_msg[key]}"
                )
        success_count += 1

    logger.info(f"RunMode {run_mode}: Successfully compared {success_count} DATA messages.")

    # בדיקה אופציונלית של FMT
    if False:
        for num, (e_msg, m_msg) in enumerate(zip(messages_extractor._reader.fmt_messages, mavlink_fmt_messages), start=1):
            for key in e_msg:
                if key in m_msg:
                    if isinstance(e_msg[key], float) and math.isnan(e_msg[key]) and math.isnan(m_msg[key]):
                        continue
                    assert e_msg[key] == m_msg[key], (
                        f"Mismatch in FMT message {num}, field '{key}': "
                        f"extractor={e_msg[key]}, mav={m_msg[key]}"
                    )
        logger.info(f"RunMode {run_mode}: Successfully compared {len(mavlink_fmt_messages)} FMT messages.")
