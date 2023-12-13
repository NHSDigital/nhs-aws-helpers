import asyncio
from asyncio import Task
from typing import Any, Final, Iterator, List, Mapping, Optional, Union

from mypy_boto3_s3.service_resource import MultipartUpload, Object

from nhs_aws_helpers.common import MiB, run_in_executor


class AsyncS3ObjectWriter:
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
        self._upload_tasks: List[Task] = []
        self._upload_errors: List[Exception] = []
        self._buffer_size = buffer_size
        self._multipart_upload: Optional[MultipartUpload] = None
        self._name = name
        self._create_object_args = dict(kv for kv in create_object_args.items()) if create_object_args else {}
        self._max_multipart_concurrency = max_multipart_concurrency

    @property
    def s3_obj(self) -> Object:
        return self._s3_obj

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        failed = exc_type is not None
        await self._close(failed=failed)
        if failed:
            return False

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
        raise NotImplementedError

    async def flush(self):
        await self._maybe_write()

    @staticmethod
    def seekable() -> bool:
        return False

    @staticmethod
    def readable() -> bool:
        return False

    @staticmethod
    def isatty() -> bool:
        return False

    def tell(self):
        return self._position

    def truncate(self, size=None):
        raise NotImplementedError

    @staticmethod
    def writable() -> bool:
        return True

    def read(self, num_bytes: int = ...) -> Union[str, bytes]:
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

    async def writelines(self, lines: List[Union[str, bytes]]) -> None:
        for arg in lines:
            await self.write(arg)

    async def write(self, data: Union[bytes, str]):
        """Write the given bytes (binary string) to the S3 file.

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""

        data = self._maybe_encode(data)
        self._buffer = b"".join([self._buffer, data])  # type: ignore[list-item]

        self._bytes_written += len(data)
        self._position += len(data)

        while self.tell() >= self._buffer_size:
            await self._start_next_part_upload()

        return len(data)

    def _raise_if_errored(self):
        if len(self._upload_errors) < 1:
            return

        raise RuntimeError from self._upload_errors[0]

    async def _start_next_part_upload(self):
        self._raise_if_errored()

        if self._multipart_upload is None:
            args = self._create_object_args or {}
            self._multipart_upload = await run_in_executor(self._s3_obj.initiate_multipart_upload, **args)

        part_data = self._buffer[: self._buffer_size]
        part_num = len(self._upload_tasks) + 1

        self._upload_tasks.append(asyncio.create_task(run_in_executor(self._upload_part, part_num, part_data)))

        self._buffer = self._buffer[self._buffer_size :]
        self._position = len(self._buffer)

    def _upload_part(self, part_num: int, part_data: bytes):
        try:
            assert self._multipart_upload is not None
            self._multipart_upload.Part(part_num).upload(Body=part_data)  # type: ignore[arg-type]
            self._bytes_written += len(part_data)
        except Exception as err:
            self._upload_errors.append(err)

    def _maybe_encode(self, data: Union[str, bytes]) -> bytes:
        if isinstance(data, (bytes, bytearray)):
            return data
        if self._encoding is None:
            return data.encode("utf-8")
        return data.encode(self._encoding)

    async def _maybe_write(self):
        if not self._multipart_upload:
            if not self.tell():
                return

            args = self._create_object_args or {}
            args["Body"] = self._buffer

            await run_in_executor(self._s3_obj.put, **args)
            self._buffer = b""
            self._position = 0
            return

        if self.tell():
            await self._start_next_part_upload()

        if self._upload_tasks:
            await asyncio.gather(*(task for task in self._upload_tasks if not task.done()))

        self._thread_pool = None
        multipart_upload = self._multipart_upload
        self._multipart_upload = None
        if self._upload_errors or self._bytes_written < 1:
            await run_in_executor(multipart_upload.abort)
            self._raise_if_errored()
            return

        result = await run_in_executor(
            multipart_upload.complete,
            MultipartUpload={
                "Parts": [{"ETag": part.e_tag, "PartNumber": part.part_number} for part in multipart_upload.parts.all()]
            },
        )
        assert result

    async def abort(self):
        if self._multipart_upload:
            await run_in_executor(self._multipart_upload.abort)
            for task in self._upload_tasks:
                if task.done():
                    continue
                task.cancel()
        self._closed = True

    async def close(self):
        await self._close()

    async def _close(self, failed: bool = False):
        if self._closed:
            return

        if failed:
            await self.abort()
            return

        await self._maybe_write()

        self._closed = True
