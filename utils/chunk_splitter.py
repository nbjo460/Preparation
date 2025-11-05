class ChunkSplitter:

    @staticmethod
    def _find_chunk_boundaries(filepath: str, num_chunks: int, fmt_messages : dict[int, object],header: bytes = b'\xA3\x95') -> list[int]:
        import os
        size = os.path.getsize(filepath)
        boundaries = [0]

        with open(filepath, 'rb') as f:
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
                        expected_length = fmt_messages[type_msg]['Length']
                        if header_pos + expected_length < len(buffer):
                            if buffer[header_pos + expected_length:header_pos + expected_length + 2] == header:
                                boundaries.append(size // num_chunks * i + header_pos)
                                found = True
                                break

                    pos = header_pos + 1
                if not found:
                    boundaries.append(size // num_chunks * i)

            boundaries.append(size)

        return boundaries

    @staticmethod
    def split(file_path : str, data : bytes, num_chunk: int, fmt_messages : dict[int, object]) -> dict:
        chunks = {}
        chunks_pos: list[int] = ChunkSplitter()._find_chunk_boundaries(file_path, num_chunk, fmt_messages)
        for pos in range(len(chunks_pos) - 1):
            chunks[pos] = data[chunks_pos[pos]: chunks_pos[pos + 1]]
        return chunks
