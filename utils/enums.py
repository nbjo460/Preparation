from enum import Enum

class MessageType(Enum):
    FMT_MESSAGE = "fmt_message"
    DATA_MESSAGE = "data_message"
    ALL_MESSAGES = "all_messages"

class RunMode(Enum):
    NORMAL = "Normal Run"
    THREADS = "Threaded Run"
    MULTIPROCESS = "Multiprocess Run"