"""Module for splitting binary files into chunks."""


class ChunkSplitter:
    """Splits binary files into chunks at message boundaries."""

    @staticmethod
    def _find_chunk_boundaries(
        filepath: str, num_chunks: int, fmt_messages: dict[int, dict], header: bytes = b"\xA3\x95"
    ) -> list[int]:
        """Find message boundaries for splitting file into chunks.

        Args:
            filepath: Path to binary file
            num_chunks: Number of chunks to split into
            fmt_messages: Format messages dictionary
            header: Message header bytes

        Returns:
            List of chunk boundary positions
        """
        import os

        size = os.path.getsize(filepath)
        boundaries = [0]

        with open(filepath, "rb") as f:
            for i in range(1, num_chunks):
                f.seek(size // num_chunks * i)
                buffer = f.read(255)

                found = False
                pos = 0
                while pos < len(buffer) - 2:
                    header_pos = buffer.find(header, pos)
                    if header_pos == -1:
                        break

                    type_msg = buffer[header_pos + 2]
                    if type_msg in fmt_messages:
                        expected_length = fmt_messages[type_msg]["Length"]
                        if header_pos + expected_length < len(buffer):
                            if buffer[header_pos + expected_length : header_pos + expected_length + 2] == header:
                                boundaries.append(size // num_chunks * i + header_pos)
                                found = True
                                break

                    pos = header_pos + 1
                if not found:
                    boundaries.append(size // num_chunks * i)

            boundaries.append(size)

        return boundaries

    @staticmethod
    def split(
        file_path: str, data: bytes, num_chunk: int, fmt_messages: dict[int, dict]
    ) -> dict[int, bytes]:
        """Split binary data into chunks at message boundaries.

        Args:
            file_path: Path to binary file
            data: Binary data to split
            num_chunk: Number of chunks
            fmt_messages: Format messages dictionary

        Returns:
            Dictionary mapping chunk number to chunk data
        """
        chunks = {}
        chunks_pos: list[int] = ChunkSplitter()._find_chunk_boundaries(file_path, num_chunk, fmt_messages)
        for pos in range(len(chunks_pos) - 1):
            chunks[pos] = data[chunks_pos[pos] : chunks_pos[pos + 1]]
        return chunks
