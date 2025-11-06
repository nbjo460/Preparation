"""Enums used throughout the application."""

from enum import Enum


class MessageType(Enum):
    """Types of messages that can be processed."""

    FMT_MESSAGE = "fmt_message"
    DATA_MESSAGE = "data_message"
    ALL_MESSAGES = "all_messages"


class RunMode(Enum):
    """Available execution modes."""

    NORMAL = "Normal Run"
    THREADS = "Threaded Run"
    MULTIPROCESS = "Multiprocess Run"