import time
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "business_logic"))

from multi_process_reader import MultiProcessReader
from multi_thread_reader import ThreadReader
from utils.enums import RunMode

from reader import Reader
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import AppLogger

class MessagesExtractor:

    def __init__(self) -> None:
        self._logger = AppLogger(self.__class__.__name__)
        self._reader = Reader()
        self._multi_processor_reader = MultiProcessReader()
        self._thread_reader = ThreadReader()




    def from_bin(self, path: str, to_round : bool= False, run_mode : RunMode = RunMode.NORMAL, num_workers : int = 8, wanted_type : str = ""):
        """
        :param path: Path of a bin file.
        :return: List of all messages who founds.
        """

        match run_mode:
            case RunMode.NORMAL:
                with open(path, "rb") as file:
                    data = memoryview(file.read())
                self._logger.info(f"Opened a file length: {len(data)}")
                yield from self._reader.read_messages(data, to_round=to_round, wanted_type=wanted_type)
            case RunMode.MULTIPROCESS:
                for message in self._multi_processor_reader.process_in_parallel(path, num_workers, to_round, wanted_type=wanted_type):
                    yield message
            case RunMode.THREADS:
                for message in self._thread_reader.process_in_parallel(path,num_workers, to_round, wanted_type=wanted_type):
                    yield message


if __name__ == "__main__":
    runners_mode = {RunMode.NORMAL, RunMode.MULTIPROCESS, RunMode.THREADS}
    # runners_mode = {RunMode.NORMAL}

    for mode in runners_mode:
        start = time.time()

        path = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"
        coordinate_ex = MessagesExtractor()

        counter = 0
        for  num , msg in enumerate(coordinate_ex.from_bin(path, True, run_mode=RunMode.MULTIPROCESS, num_workers=8, wanted_type="")):
            c = msg
            counter+=1

        print(f"Got {counter} messages.")
        end = time.time()
        print(f"Elapsed time: {end - start:.6f} seconds, to run mode: {mode}")

