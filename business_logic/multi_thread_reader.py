import time
from concurrent.futures.thread import ThreadPoolExecutor
from business_logic.reader import Reader
from utils.enums import MessageType
from utils.chunk_splitter import ChunkSplitter




class ThreadReader:
    GLOBAL_FMT = None

    def __init__(self):
        self.reader = Reader()
        self.chunk_splitter = ChunkSplitter()
        # self.logger = Logger(__class__.__name__)

    # def read_only_fmt(self, data: memoryview) -> None:
    #     num: int = 0
    #     length_data = len(data)
    #     counter = 0
    #     while num < length_data:
    #         header = data[num: num + 2]
    #         is_new_massage = header[0] == 0xA3 and header[1] == 0x95
    #
    #         if is_new_massage:
    #             counter += 1
    #             type_msg = data[num + 2]
    #             is_fmt = type_msg == 0x80
    #             if is_fmt:
    #                 self.read_fmt_massage(data, num)
    #                 num += 89
    #             elif not is_fmt:
    #                 exist_msg_config = self.fmt_massages[int(type_msg)]
    #                 length = exist_msg_config["length"]
    #                 num += length
    #         # print(countr)
    #
    #         elif not is_new_massage:
    #             # print(f"{header[0]:02x}")
    #             next_head = data[num:].find(self.HEADER)
    #             num += next_head

    @staticmethod
    def _read_chunk_messages(num_chunk: int, data: memoryview, to_round: bool, fmt_messages: dict):
        reader = Reader()
        for type_msg, msg_config in fmt_messages.items():
            reader._compile_processing(type_msg, msg_config["Format"], msg_config["cols"])
        print(f"Process num: {num_chunk} start to work.")
        messages = []
        for msg in reader.read_messages(data, to_round, MessageType.ALL_MESSAGES, fmt_messages):
            messages.append(msg)
        return num_chunk, messages

    def process_in_parallel(self, file_path: str, num_workers: int, to_round : bool):
        a = time.time()
        with open(file_path, "rb") as file:
            import mmap
            data = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
            for _ in self.reader.read_messages(data=memoryview(data), to_round=to_round, message_type_to_read=MessageType.FMT_MESSAGE):
                pass
        fmt_messages = self.reader.fmt_messages
        chunks: dict = self.chunk_splitter.split(file_path, data, num_workers, fmt_messages)
        combine = [(num_chunk, chunk_data, to_round, fmt_messages) for num_chunk, chunk_data in chunks.items()]
        print(time.time() - a ,"sec, to read FMT, and split to chunks.")

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            a = time.time()
            results = list(executor.map(lambda args: self._read_chunk_messages(*args), combine))
            b = time.time()
            print(b - a, "sec, only calc")
            results.sort(key=lambda x: x[0])
            combined = []
            for _, messages in results:
                combined.extend(messages)
            c = time.time()
            print(c - b, "sec, to sort")
        return combined
