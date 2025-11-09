from typing import Generator, Any, NamedTuple


class Reader:
    class MessageInfo(NamedTuple):
        data: dict
        size: int

    def __init__(self):
        self.messages_configs: dict[Any, Any] = {}

    def receive_messages(self, data : memoryview, ) -> Generator[dict[str, Any], None, None]:
        yield from self._stream_data(data)

    def _stream_data(self, data : memoryview) -> Generator[dict[str, Any], None, None]:
        start_pos: int = 0
        length_of_data : int= len(data)
        while start_pos < length_of_data:
            message_data, message_size = self.extract_message(data, start_pos)
            if message_data:
                yield message_data
                start_pos += message_size
            elif not message_data:
                start_pos += self.find_next_header(data, start_pos)

    def extract_message(self, data: memoryview, start_pos: int) -> MessageInfo:
        info = MessageInfo(data = data, size = 4)
        return info