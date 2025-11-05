import sys
from pathlib import Path

# Add project root to path if not already there
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import math
from typing import Dict, Any, Iterator
import pytest
from pymavlink import mavutil
from pymavlink.CSVReader import CSVMessage

from business_logic.messages_extractor import MessagesExtractor
from utils.enums import RunMode
from utils.logger import AppLogger

logger = AppLogger("Tests")

TEST_FILE_PATH = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"
PROGRESS_INTERVAL = 100_000  # דווח כל 100K הודעות


def fix_nan_value(message: Dict[str, Any]) -> None:
    """Fix NaN values in PARM messages."""
    if "Default" in message and math.isnan(message.get("Default", 0)):
        message["mavpackettype"] = "Nan"


def mavlink_message_generator(skip_fmt: bool = False) -> Iterator[Dict[str, Any]]:
    """
    Generator for MAVLink messages - doesn't load all to memory!

    Args:
        skip_fmt: If True, skip FMT messages

    Yields:
        Message dictionaries one at a time
    """
    mav: CSVMessage = mavutil.mavlink_connection(TEST_FILE_PATH)

    while True:
        msg = mav.recv_match(blocking=False)
        if msg is None:
            break

        msg_dict = msg.to_dict()

        if skip_fmt and msg_dict["mavpackettype"] == "FMT":
            continue

        yield msg_dict


def extractor_message_generator(
        run_mode: RunMode,
        skip_fmt: bool = False
) -> tuple[Iterator[Dict[str, Any]], list[Dict[str, Any]]]:
    """
    Generator for extractor messages.

    Args:
        run_mode: Execution mode
        skip_fmt: If True, skip FMT messages

    Returns:
        (message_generator, fmt_messages_list)
    """
    extractor = MessagesExtractor()

    # Get the generator
    msg_gen = extractor.from_bin(
        TEST_FILE_PATH,
        to_round=True,
        run_mode=run_mode,
        num_workers=4
    )

    # Return generator and FMT messages
    return msg_gen, extractor._reader.fmt_messages if not skip_fmt else []


def compare_messages_streaming(
        mavlink_gen: Iterator[Dict[str, Any]],
        extractor_gen: Iterator[Dict[str, Any]],
        fmt_messages: list[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Compare messages one-by-one without loading all to memory.

    Returns:
        Statistics dictionary
    """
    # Build FMT lookup for type names
    fmt_lookup = {fmt["Type"]: fmt["name"] for fmt in fmt_messages}

    stats = {
        "total_compared": 0,
        "fmt_messages": 0,
        "data_messages": 0,
        "parm_messages": 0,
        "mismatches": [],
        "last_progress": 0
    }

    logger.info("🚀 Starting streaming comparison...")

    for mav_msg, ext_msg in zip(mavlink_gen, extractor_gen):
        stats["total_compared"] += 1

        # Enrich extractor message with type name
        msg_type = ext_msg.get("type")
        if msg_type in fmt_lookup:
            ext_msg["mavpackettype"] = fmt_lookup[msg_type]

        # Count message types
        packet_type = mav_msg.get("mavpackettype", "")
        if packet_type == "FMT":
            stats["fmt_messages"] += 1
        else:
            stats["data_messages"] += 1

        # Special handling for PARM messages
        if ext_msg.get("mavpackettype") == "PARM":
            stats["parm_messages"] += 1
            fix_nan_value(ext_msg)
            fix_nan_value(mav_msg)

        # Basic validation - check packet type matches
        ext_type = ext_msg.get("mavpackettype", "")
        if ext_type != packet_type:
            if len(stats["mismatches"]) < 100:  # Store first 100 mismatches
                stats["mismatches"].append({
                    "index": stats["total_compared"],
                    "expected": packet_type,
                    "got": ext_type
                })

        # Progress reporting
        if stats["total_compared"] % PROGRESS_INTERVAL == 0:
            logger.info(
                f"📊 Progress: {stats['total_compared']:,} messages | "
                f"FMT: {stats['fmt_messages']:,} | "
                f"Data: {stats['data_messages']:,} | "
                f"PARM: {stats['parm_messages']:,} | "
                f"Mismatches: {len(stats['mismatches'])}"
            )
            stats["last_progress"] = stats["total_compared"]

    # Final report if didn't just report
    if stats["total_compared"] != stats["last_progress"]:
        logger.info(
            f"📊 Final: {stats['total_compared']:,} messages | "
            f"FMT: {stats['fmt_messages']:,} | "
            f"Data: {stats['data_messages']:,} | "
            f"PARM: {stats['parm_messages']:,} | "
            f"Mismatches: {len(stats['mismatches'])}"
        )

    return stats


def verify_fmt_messages(
        mavlink_fmt: list[Dict[str, Any]],
        extractor_fmt: list[Dict[str, Any]]
) -> None:
    """Verify FMT messages match."""
    assert len(extractor_fmt) == len(mavlink_fmt), (
        f"❌ FMT count mismatch: extractor={len(extractor_fmt):,}, "
        f"mavlink={len(mavlink_fmt):,}"
    )
    logger.info(f"✅ FMT messages match: {len(extractor_fmt):,}")


@pytest.mark.parametrize("run_mode", [
    RunMode.NORMAL,
    RunMode.THREADS,
    RunMode.MULTIPROCESS
])
@pytest.mark.parametrize("skip_fmt", [False, True])
def test_reader_vs_mavlink_streaming(run_mode: RunMode, skip_fmt: bool):
    """
    Test MessagesExtractor vs pymavlink with streaming comparison.

    Perfect for 9 MILLION messages! 🚀

    Args:
        run_mode: Execution mode for extractor
        skip_fmt: Skip FMT messages in comparison
    """
    logger.info("=" * 80)
    logger.info(f"🧪 Testing: run_mode={run_mode}, skip_fmt={skip_fmt}")
    logger.info("=" * 80)

    # Get generators (not loading all data!)
    mavlink_gen = mavlink_message_generator(skip_fmt=skip_fmt)
    extractor_gen, extractor_fmt = extractor_message_generator(
        run_mode=run_mode,
        skip_fmt=skip_fmt
    )

    # Get MAVLink FMT messages for comparison (small list, OK to load)
    if not skip_fmt:
        logger.info("Loading FMT messages for reference...")
        mavlink_fmt = [
            msg for msg in mavlink_message_generator(skip_fmt=False)
            if msg["mavpackettype"] == "FMT"
        ]
        verify_fmt_messages(mavlink_fmt, extractor_fmt)
    else:
        logger.info("⏭️  Skipping FMT validation")
        mavlink_fmt = []

    # Compare messages streaming
    stats = compare_messages_streaming(
        mavlink_gen,
        extractor_gen,
        mavlink_fmt if not skip_fmt else extractor_fmt
    )

    # Verify results
    logger.info("=" * 80)
    logger.info("📋 FINAL RESULTS:")
    logger.info(f"   Total messages compared: {stats['total_compared']:,}")
    logger.info(f"   FMT messages: {stats['fmt_messages']:,}")
    logger.info(f"   Data messages: {stats['data_messages']:,}")
    logger.info(f"   PARM messages: {stats['parm_messages']:,}")

    if stats['mismatches']:
        logger.error(f"   ❌ Found {len(stats['mismatches'])} mismatches!")
        logger.error("   First 10 mismatches:")
        for i, mismatch in enumerate(stats['mismatches'][:10], 1):
            logger.error(
                f"      {i}. Index {mismatch['index']:,}: "
                f"expected '{mismatch['expected']}', got '{mismatch['got']}'"
            )
        assert False, f"Found {len(stats['mismatches'])} type mismatches"
    else:
        logger.info("   ✅ No mismatches found!")

    logger.info("=" * 80)
    logger.info(f"✅ Test PASSED for {run_mode}" +
                (", FMT skipped" if skip_fmt else ""))
    logger.info("=" * 80)


@pytest.mark.parametrize("run_mode", [RunMode.NORMAL])
def test_quick_validation(run_mode: RunMode):
    """
    Quick test - only first 1000 messages.
    Good for fast iteration during development.
    """
    logger.info("🏃 Quick validation - first 1000 messages only")

    mavlink_gen = mavlink_message_generator(skip_fmt=False)
    extractor_gen, extractor_fmt = extractor_message_generator(
        run_mode=run_mode,
        skip_fmt=False
    )

    # Compare only first 1000
    count = 0
    for mav_msg, ext_msg in zip(mavlink_gen, extractor_gen):
        count += 1
        if count >= 1000:
            break

        # Basic check
        assert mav_msg["mavpackettype"] == ext_msg.get("mavpackettype", ""), \
            f"Mismatch at {count}"

    logger.info(f"✅ Quick test passed: {count} messages verified")


if __name__ == "__main__":
    # Run with verbose output and stop on first failure
    pytest.main([__file__, "-v", "-s", "-x"])