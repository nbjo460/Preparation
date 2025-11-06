"""Test module for comparing message extraction implementations."""

import math
from typing import Any, Generator, Optional

from pymavlink import mavutil
from pymavlink.CSVReader import CSVMessage

from business_logic.messages_extractor import MessagesExtractor
from utils.enums import RunMode
from utils.logger import AppLogger

logger = AppLogger("Tests")

PATH = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"

mavlink_messages: list[dict[str, Any]] = []
messages_extractor = MessagesExtractor()


def fix_nan_message(message: dict[str, Any]) -> None:
    """Fix NaN values in message dictionary."""
    to_fix = message.get("Default")
    if to_fix and isinstance(to_fix, float) and math.isnan(to_fix):
        message["Default"] = "Nan"

def get_mav_messages():
    """Read all messages from mavlink file."""
    if mavlink_messages != []:
        return

    mav: CSVMessage = mavutil.mavlink_connection(PATH)
    logger.info("Reading the mavlink file...")

    remain_info_in_file = True
    while remain_info_in_file:
        gps_message: mav = mav.recv_match(blocking = False)
        if gps_message is None:
            break
        mavlink_messages.append(gps_message.to_dict())

    logger.debug(f"Found {len(mavlink_messages)} messages.")

def get_mav_messages_generator():
    """Generator yielding messages from mavlink file."""
    if mavlink_messages != []:
        return

    mav: CSVMessage = mavutil.mavlink_connection(PATH)
    logger.info("Reading the mavlink file...")

    remain_info_in_file = True
    while remain_info_in_file:
        gps_message: mav = mav.recv_match(blocking = False)
        if gps_message is None:
            break
        yield gps_message.to_dict()

    logger.debug(f"Found {len(mavlink_messages)} messages.")


# @pytest.mark.parametrize("run_mode", [RunMode.NORMAL, RunMode.THREADS, RunMode.MULTIPROCESS])
def reader_vs_mavlink(run_mode: RunMode) -> None:
    """Compare reader implementation against mavlink."""

    log = {True:0, False:0}

    get_mav_messages()

    extractor_messages = []

    for msg in messages_extractor.from_bin(PATH,to_round=True,
                                           run_mode=run_mode,
                                           num_workers=4):

        extractor_messages.append(msg)
    for num, message in enumerate(extractor_messages):

            if message["mavpackettype"] == "PARM":
                fix_nan_message(message)
                fix_nan_message(mavlink_messages[num])
            success = message == mavlink_messages[num]
            if not success:
                print(message, mavlink_messages[num])
                break
            if num % 100_000 == 0:
                print(log,run_mode)
            log[success] += 1
    print(log)

    print(log, run_mode)




def compare_process() -> None:
    """Compare processing implementations."""
    log = {True:0, False:0}
    counter = 0
    for ext, mav in zip(messages_extractor.from_bin(PATH, to_round=True,
                                           run_mode=RunMode.MULTIPROCESS,
                                           num_workers=4), get_mav_messages_generator()):
        counter += 1

        if ext["mavpackettype"] == "PARM":
            fix_nan_message(ext)
            fix_nan_message(mav)

        result = ext == mav
        if not result:
            print(ext, mav)
            break
        log[result] += 1

        if counter % 1_000_000 == 0:
            print(log)
    print(log)



if __name__ == "__main__":
    reader_vs_mavlink(RunMode.NORMAL)
    reader_vs_mavlink(RunMode.THREADS)
    # compare_process()