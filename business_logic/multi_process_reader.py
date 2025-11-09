import time
from logging import Logger

from old_reader import Reader
from utils.enums import MessageType
from utils.chunk_splitter import ChunkSplitter
from multiprocessing import Pool




class MultiProcessReader:
    GLOBAL_FMT = None

    def __init__(self):
        self.reader = Reader()
        self.chunk_splitter = ChunkSplitter()

    @staticmethod
    def read_chunk_messages(num_chunk: int, data: memoryview, to_round: bool, fmt_messages: dict, wanted_type : str):
        reader = Reader()
        for type_msg, msg_config in fmt_messages.items():
            reader._compile_processing(type_msg, msg_config["Format"], msg_config["cols"])
        print(f"Process num: {num_chunk} start to work.")
        messages = []

        for msg in reader.read_messages(data, to_round, MessageType.ALL_MESSAGES, fmt_messages, wanted_type):
            messages.append(msg)
        # messages=[]
        return num_chunk, messages

    def process_in_parallel(self, file_path: str, num_workers: int, to_round : bool, wanted_type :str):
        a = time.time()
        with open(file_path, "rb") as file:
            import mmap
            data = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
            for _ in self.reader.read_messages(data=memoryview(data), to_round=to_round, message_type_to_read=MessageType.FMT_MESSAGE):
                pass
        fmt_messages = self.reader.fmt_messages
        chunks: dict = self.chunk_splitter.split(file_path, data, num_workers, fmt_messages)
        combine = [(num_chunk, chunk_data, to_round, fmt_messages, wanted_type) for num_chunk, chunk_data in chunks.items()]
        print(time.time() - a ,"sec, to read FMT, and split to chunks.")
        with Pool(num_workers) as pool:
            a = time.time()
            results = pool.starmap(self.read_chunk_messages, combine)
            b = time.time()
            print(b - a, "sec, only calc")
            results.sort(key=lambda x: x[0])
            all_messages = []
            for result in results:
                list_msg = result[1]
                all_messages.extend(list_msg)
            c = time.time()
            print(c - b, "sec, to sort")
        print(len(all_messages))
        return all_messages