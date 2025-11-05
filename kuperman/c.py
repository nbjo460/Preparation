
import mmap
import struct
import time
from multiprocessing import Pool
from typing import Dict, List, Tuple, Any

from a import BinLogParser
from b import find_valid_sync_positions, split_ranges

# ---------------------------------------------------------------------
# Global variables shared across workers (each gets its own copy)
# ---------------------------------------------------------------------
SHARED_FMT_DEFINITIONS: Dict[int, Dict[str, Any]] = {}
SHARED_FILE_PATH: str = ""


# ---------------------------------------------------------------------
# Worker Initialization
# ---------------------------------------------------------------------
def _init_worker(fmt_definitions: Dict[int, Dict[str, Any]], file_path: str) -> None:
    """
    Initialize each worker process:
    - Store shared file path and format definitions locally.
    - Rebuild struct objects (since struct.Struct is not picklable).
    """
    global SHARED_FMT_DEFINITIONS, SHARED_FILE_PATH

    SHARED_FILE_PATH = file_path
    SHARED_FMT_DEFINITIONS = {msg_id: dict(fmt) for msg_id, fmt in fmt_definitions.items()}

    for fmt_definition in SHARED_FMT_DEFINITIONS.values():
        # ensure struct object exists for decoding
        if "struct_obj" not in fmt_definition:
            fmt_definition["struct_obj"] = struct.Struct(fmt_definition["struct_fmt"])


# ---------------------------------------------------------------------
# Worker Decode Function
# ---------------------------------------------------------------------
def _decode_file_segment(segment_start: int, segment_end: int, round_floats: bool) -> Dict[str, Any]:
    """
    Decode a specific byte range (segment) from the log file.
    Each worker processes a separate non-overlapping range.
    """
    with open(SHARED_FILE_PATH, "rb") as file:
        mapped_log = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        parser = BinLogParser(mapped_log, format_definitions=SHARED_FMT_DEFINITIONS, round_floats=round_floats)

        decoded_count: int = 0
        sample_messages: List[Dict[str, Any]] = []

        # decode messages in this segment only
        for message in parser.parse_messages_in_range(segment_start, segment_end, as_tuples=False):
            if message["message_type"] == "FMT":
                continue  # skip FMT definitions

            decoded_count += 1

            # keep only a few sample messages from each worker
            if len(sample_messages) < 3:
                sample_messages.append(message)

        mapped_log.close()

    return {"count": decoded_count, "sample": sample_messages}


# ---------------------------------------------------------------------
# Main Parallel Decoder
# ---------------------------------------------------------------------
class ParallelBinDecoder:
    """Splits a BIN log file across multiple workers for parallel decoding."""

    def __init__(self, file_path: str, num_workers: int = 4, round_floats: bool = True) -> None:
        self.file_path = file_path
        self.num_workers = num_workers
        self.round_floats = round_floats

    def run(self) -> int:
        """
        Orchestrate the decoding process:
        - Preload FMT definitions (single-threaded).
        - Split file into byte ranges.
        - Decode each range in parallel using multiple workers.
        """
        start_time: float = time.perf_counter()

        # Step 1: Preload FMT definitions from main process
        with open(self.file_path, "rb") as file:
            mapped_log = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
            parser = BinLogParser(mapped_log)

            parser.preload_fmt_messages()
            file_size: int = mapped_log.size()

            # remove struct objects before sending to workers (not picklable)
            fmt_definitions: Dict[int, Dict[str, Any]] = {
                msg_id: {key: value for key, value in fmt.items() if key != "struct_obj"}
                for msg_id, fmt in parser.fmt_definitions.items()
            }

            # find all valid sync markers to determine safe split points
            sync_positions: List[int] = find_valid_sync_positions(mapped_log, parser.fmt_definitions)
            print(f"Found {len(sync_positions):,} valid sync markers.")

            # divide file into balanced segments
            decode_segments: List[Tuple[int, int]] = split_ranges(sync_positions, self.num_workers, file_size)
            mapped_log.close()

        # Step 2: Launch worker pool to decode file segments in parallel
        with Pool(
            self.num_workers,
            initializer=_init_worker,
            initargs=(fmt_definitions, self.file_path)
        ) as pool:
            results: List[Dict[str, Any]] = pool.starmap(
                _decode_file_segment,
                [(segment_start, segment_end, self.round_floats) for segment_start, segment_end in decode_segments]
            )

        # Step 3: Aggregate results
        total_decoded: int = sum(worker_result["count"] for worker_result in results)
        total_time: float = time.perf_counter() - start_time

        print(f"\n✅ Decoded {total_decoded:,} messages in {total_time:.2f}s using {self.num_workers} workers.\n")

        # Display small sample from each worker
        for worker_index, worker_result in enumerate(results):
            print(f"[Worker {worker_index}] Sample messages:")
            for sample_msg in worker_result["sample"]:
                print(f"  {sample_msg['message_type']}: {sample_msg}")

        return total_decoded


# ---------------------------------------------------------------------
# Script Entry Point
# ---------------------------------------------------------------------
if __name__ == "__main__":
    path = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"
    log_file_path: str = "log_file_test_01.bin"
    decoder = ParallelBinDecoder(file_path=path, num_workers=1, round_floats=True)
    decoder.run()

