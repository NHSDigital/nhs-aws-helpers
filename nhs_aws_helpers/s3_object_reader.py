import sys
from io import SEEK_CUR, SEEK_END, SEEK_SET
from typing import IO, Any, Final, Iterable, Optional, Union, cast

from botocore.response import StreamingBody
from mypy_boto3_s3.service_resource import Object

from nhs_aws_helpers.common import MiB


class S3ObjectReader(IO):
    """
    provides in-memory buffer to write to S3 objects
    NOTE: ... only use this if you want to buffer the entire stream into memory, to make it seekable
    """

    DEFAULT_BUFFER_SIZE: Final[int] = 8 * MiB

    def __init__(
        self,
        s3_obj: Object,
        encoding: Optional[str] = None,
        line_ending: str = "\n",
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        name: str = "",
    ) -> None:
        self._s3_obj = s3_obj
        self._closed = False
        self._encoding = encoding
        self._buffer: Union[str, bytes] = "" if encoding else b""
        self._bytes_read, self._position = (0, 0)
        self._fully_read = False
        self._body: Optional[StreamingBody] = None
        self._line_ending = line_ending
        self._buffer_size = buffer_size
        self._name = name

    @property
    def s3_obj(self) -> Object:
        return self._s3_obj

    def __iter__(self):
        return self

    def __next__(self):
        line = self.readline()
        if line is None:
            raise StopIteration

        return line.strip()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            return False

        self.close()
        return True

    def write(self, s: Any) -> int:
        raise NotImplementedError

    def writelines(self, lines: Iterable[Any]) -> None:
        raise NotImplementedError

    @property
    def mode(self) -> str:
        return "r" if self._encoding else "rb"

    @property
    def name(self) -> str:
        return self._name

    @property
    def raw(self):
        return self.body

    @property
    def closed(self):
        return self._closed

    @property
    def body(self):
        if self._body is None:
            obj = self._s3_obj.get()
            self._body = obj["Body"]
        return self._body

    def fileno(self):
        return self._s3_obj.key

    def close(self):
        if self._closed:
            return
        if self._body:
            self._body.close()
        self._closed = True

    def flush(self):
        raise NotImplementedError

    def readable(self):
        return self.body and (self._fully_read or not self._closed)

    def readinto(self, byte_array: bytearray) -> int:
        to_read = len(byte_array)
        chunk = self.read(to_read)
        bytes_read = len(chunk)
        for i, char in enumerate(chunk):
            byte_array[i] = char  # type: ignore[call-overload]
        return bytes_read

    def peek(self, num: Optional[int] = None) -> Union[str, bytes]:
        current_pos = self._position
        bytes_read = self.read(num)
        self._position = current_pos
        return bytes_read

    def read(self, num: Optional[int] = None) -> Union[str, bytes]:
        if self._fully_read:
            end = self._bytes_read if num is None else max(min(self._position + num, self._bytes_read), 0)
            pos = min(max(self._position, 0), self._bytes_read)

            chunk = self._buffer[min(pos, end) : max(pos, end)]
            self._position += end - pos
            return chunk

        if num is None:
            chunk = self.body.read()
            if self._encoding:
                chunk = self.ensure_valid_chunk(chunk)
            else:
                self._bytes_read = len(self._buffer) + len(chunk)
            self._buffer = self._buffer + chunk  # type: ignore[operator]
            self._fully_read = True
            self._position = len(self._buffer)
            return chunk

        if num == 0:
            return "" if self._encoding else b""

        end = max(0, self._position + num)

        if end > self._bytes_read:
            bytes_to_read = max(end - self._bytes_read, self._buffer_size)

            chunk = self.body.read(bytes_to_read)
            byte_count = len(chunk)

            if self._encoding:
                chunk = self.ensure_valid_chunk(chunk)
            else:
                self._bytes_read += byte_count

            self._buffer = self._buffer + chunk  # type: ignore[operator]

            # TODO This should really check that you're not at the end of the file
            if byte_count < bytes_to_read:
                self._fully_read = True

        end = min(end, self._bytes_read)

        read_from = min(self._position, end)
        read_to = max(self._position, end)

        self._position = end
        return self._buffer[read_from:read_to]

    def readline(self, limit: int = -1) -> Union[Optional[str], Any]:
        if self._encoding is None:
            raise ValueError("readline only works if _encoding was defined")

        max_read = limit if limit > 0 else sys.maxsize
        chunk_size = min(max_read, 8192)
        bytes_read = 0
        index = -1
        local_buffer = ""
        while index < 0 and bytes_read < max_read:
            chunk = self.read(chunk_size)
            if not chunk:
                if not local_buffer:
                    return None
                break

            local_buffer += chunk  # type: ignore[operator]
            index = local_buffer.find(self._line_ending, bytes_read)
            bytes_read += len(chunk)

        if index < 0:
            return local_buffer

        self.seek((bytes_read - index - 1) * -1, SEEK_CUR)
        return local_buffer[0 : index + 1]

    def readlines(self, size=None):
        if self._encoding is None:
            raise ValueError("readlines only works if _encoding was defined")

        local_buffer = cast(str, self.read(size))

        position = 0
        index = local_buffer.find(self._line_ending, position)
        while index > 0:
            yield local_buffer[position : index + 1]
            next_index = local_buffer.find(self._line_ending, index + 1)
            position = index + 1
            index = next_index

        if position < len(local_buffer):
            yield local_buffer[position:]

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        if whence == SEEK_END:
            self.read()  # read full stream
            offset = offset * -1 if offset > 0 else offset
            self._position = max(0, self._bytes_read + offset)
            return self._position

        cur_pos = 0 if whence == SEEK_SET else self._position

        new_pos = max(0, cur_pos + offset)
        if new_pos > self._bytes_read:
            self.read(new_pos - self._position)
            return self._position

        self._position = new_pos
        return self._position

    def seekable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def tell(self):
        return self._position

    def truncate(self, size=None):
        raise NotImplementedError

    def writable(self) -> bool:
        return False

    def ensure_valid_chunk(self, chunk, run_count: int = 0):
        """We need to check that the chunk is decode-able.
        This is because, with multibyte chars, when we create our chunks, we are just ticking
        through each byte. A multibyte char is between 2 and 4 bytes, so we need to check that
        we have not chopped off a char midway through it's bytes, so will loop through 3 times,
        removing last char and attempting to decode."""

        try:
            chunk = chunk.decode(self._encoding)
            self._bytes_read += len(chunk)
        except UnicodeDecodeError as err:
            if run_count > 2:
                raise err
            run_count += 1
            chunk = chunk[:-1]
            chunk = self.ensure_valid_chunk(chunk, run_count)

        return chunk
