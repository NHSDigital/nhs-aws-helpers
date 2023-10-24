from concurrent.futures import Future, ThreadPoolExecutor
from typing import (
    IO,
    Any,
    AnyStr,
    Dict,
    Final,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Union,
)

from mypy_boto3_s3.service_resource import MultipartUpload, Object

from nhs_aws_helpers.common import MiB


class S3ObjectWriter(IO):
    """provides in-memory buffer to write to S3 objects"""

    MIN_PART_SIZE: Final[int] = 5 * MiB
    DEFAULT_BUFFER_SIZE: Final[int] = 8 * MiB

    def __init__(
        self,
        s3_obj: Object,
        encoding: Optional[str] = None,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        name: str = "",
        create_object_args: Optional[Mapping[str, Any]] = None,
        max_multipart_concurrency: int = 12,
    ) -> None:
        buffer_size = max(buffer_size, self.MIN_PART_SIZE)
        self._s3_obj = s3_obj
        self._closed = False
        self._encoding = encoding
        self._buffer = b""
        self._bytes_written, self._position = (0, 0)
        self._parts: List[Dict[str, int]] = []
        self._upload_tasks: List[Future] = []
        self._thread_pool: Optional[ThreadPoolExecutor] = None
        self._upload_errors: List[Exception] = []
        self._buffer_size = buffer_size
        self._multipart_upload: Optional[MultipartUpload] = None
        self._max_multipart_concurrency = max_multipart_concurrency
        self._name = name
        self._create_object_args = dict(kv for kv in create_object_args.items()) if create_object_args else {}

    @property
    def s3_obj(self) -> Object:
        return self._s3_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        failed = exc_type is not None
        self._close(failed)
        return not failed

    def read(self, n: int = ...) -> Union[str, bytes]:
        raise NotImplementedError

    def readline(self, limit: int = ...) -> Union[str, bytes]:
        raise NotImplementedError

    def readlines(self, hint: int = ...) -> List[Union[str, bytes]]:
        raise NotImplementedError

    def seek(self, offset: int, whence: int = ...) -> int:
        raise NotImplementedError

    def __next__(self) -> Union[str, bytes]:
        raise NotImplementedError

    def __iter__(self) -> Iterator[Union[str, bytes]]:
        raise NotImplementedError

    @property
    def mode(self) -> str:
        return "w" if self._encoding else "wb"

    @property
    def name(self) -> str:
        return self._name

    @property
    def closed(self):
        return self._closed

    def fileno(self):
        return self._s3_obj.key

    def flush(self):
        self._maybe_write()

    def readable(self):
        return False

    def readinto(self, byte_array: bytearray) -> int:
        raise NotImplementedError

    def peek(self, num: Optional[int] = None) -> Union[str, bytes]:
        raise NotImplementedError

    def seekable(self) -> bool:
        return False

    def isatty(self) -> bool:
        return False

    def tell(self):
        return self._position

    def truncate(self, size=None):
        raise NotImplementedError

    def writable(self) -> bool:
        return True

    def writelines(self, lines: Iterable[Any]) -> None:
        for arg in lines:
            self.write(arg)

    def write(self, data: Any) -> int:
        """Write the given bytes (binary string) to the S3 file.
        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""

        encoded = self._maybe_encode(data)
        self._buffer = b"".join([self._buffer, encoded]) if self._buffer else encoded  # type: ignore[list-item]

        self._position += len(encoded)

        while self.tell() >= self._buffer_size:
            self._start_next_part_upload()

        return len(encoded)

    def _raise_if_errored(self):
        if len(self._upload_errors) < 1:
            return

        raise RuntimeError from self._upload_errors[0]

    def _start_next_part_upload(self):
        self._raise_if_errored()

        if self._multipart_upload is None:
            args = self._create_object_args or {}
            self._multipart_upload = self._s3_obj.initiate_multipart_upload(**args)
            self._thread_pool = ThreadPoolExecutor(self._max_multipart_concurrency)

        part_data = self._buffer[: self._buffer_size]
        part_num = len(self._upload_tasks) + 1

        assert self._thread_pool
        self._upload_tasks.append(self._thread_pool.submit(self._upload_part, part_num, part_data))

        self._buffer = self._buffer[self._buffer_size :]
        self._position = len(self._buffer)

    def _upload_part(self, part_num, part_data: bytes):
        try:
            assert self._multipart_upload is not None
            self._multipart_upload.Part(part_num).upload(Body=part_data)
            self._bytes_written += len(part_data)
        except Exception as err:
            self._upload_errors.append(err)

    def _maybe_encode(self, data: AnyStr) -> bytes:
        if isinstance(data, (bytes, bytearray)):
            return data
        if self._encoding is None:
            return data.encode("utf-8")
        return data.encode(self._encoding)

    def _maybe_write(self):
        if not self._multipart_upload:
            if not self.tell():
                return
            args = self._create_object_args or {}
            args["Body"] = self._buffer
            self._s3_obj.put(**args)
            self._buffer = b""
            self._position = 0
            return

        if self.tell():
            self._start_next_part_upload()

        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
            self._thread_pool = None

        multipart_upload = self._multipart_upload
        self._multipart_upload = None

        if self._upload_errors or self._bytes_written < 1:
            multipart_upload.abort()
            self._raise_if_errored()
            return

        multipart_upload.complete(
            MultipartUpload={
                "Parts": [
                    {"ETag": part.e_tag, "PartNumber": int(part.part_number)} for part in multipart_upload.parts.all()
                ]
            }
        )

    def close(self):
        self._close()

    def _close(self, failed: bool = False):
        if self._closed:
            return

        if failed:
            if self._multipart_upload:
                self._multipart_upload.abort()
                if self._thread_pool is not None:
                    self._thread_pool.shutdown(wait=True)
            self._closed = True
            return

        self._maybe_write()

        self._closed = True
