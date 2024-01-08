import sys
from io import SEEK_CUR, SEEK_END, BytesIO
from time import time
from typing import Any, List, cast
from uuid import uuid4

import pytest
from mypy_boto3_s3.service_resource import Bucket, Object

from nhs_aws_helpers import s3_object
from nhs_aws_helpers.async_s3_object_reader import AsyncS3ObjectReader
from nhs_aws_helpers.async_s3_object_writer import AsyncS3ObjectWriter
from nhs_aws_helpers.common import MiB

_DEFAULT_BUFFER_SIZE = 200

_TEXT = [
    "It bégan with the forging of the Great Rings.\r\n",
    "Threé were given to the Elves, immortal, wisest and fairest of all beings.\n",
    "Seven to the Dwarf-Lords, great miners and craftsmen of the mountain halls.\r\n",
    "And nine, nine rings were gifted to the race of Men, who above all else desire power.\r\n",
    "For within these rings was bound the strength and the will to govern each race.\r\n",
    "But théy were all of them deceived, for another ring was made.\r\n",
    "Déép in the land of Mordor, in thé Fires of Mount Doom, thé Dark Lord Sauron forged a master "
    "ring in secret, and into this ring he poured his cruelty, his malice and his will to dominaté "
    "all life.\n",
    "Oné ring to rule them all.",
]
_S3_TEXT_BODY = "".join(_TEXT)
_TEXT_BODY_LEN = len(_S3_TEXT_BODY)

_ASCII_TEXT = [
    "It began with the forging of the Great Rings.\r\n",
    "Three were given to the Elves, immortal, wisest and fairest of all beings.\n",
    "Seven to the Dwarf-Lords, great miners and craftsmen of the mountain halls.\r\n",
    "And nine, nine rings were gifted to the race of Men, who above all else desire power.\r\n",
    "For within these rings was bound the strength and the will to govern each race.\r\n",
    "But they were all of them deceived, for another ring was made.\r\n",
    "Deep in the land of Mordor, in the Fires of Mount Doom, the Dark Lord Sauron forged a master "
    "ring in secret, and into this ring he poured his cruelty, his malice and his will to dominate "
    "all life.\n",
    "One ring to rule them all.",
]
_ASCII_S3_TEXT_BODY = "".join(_ASCII_TEXT)
_ASCII_TEXT_BODY_LEN = len(_ASCII_S3_TEXT_BODY)

_NON_ASCII_TEXT = [
    "It begдn with the forging of the Great Rings.\r\n",
    "Three were given to the Elves, immortдl, wisest and fairest of дll beings.\n",
    "Seven to the Dwarf-Lords, great miners and craftsmen of the mountaiи halls.\r\n",
    "And nine, nine rings were gifted to the race of Men, who above all else desiлe power.\r\n",
    "For witлin these rings was bound the strength and the will to govern each race.\r\n",
    "But they were all of them deceived, for another ring was made.\r\n",
    "Deep in the land of Mordor, in the Fires of Mount Doom, the Dark Lord Sauлon forged a master "
    "ring in secret, and into this ring he poured his cruelty, his malice and his will to dominate "
    "all life.\n",
    "One ring to rule them all. Владимир Путин",
]
_NON_ASCII_S3_TEXT_BODY = "".join(_NON_ASCII_TEXT)
_NON_ASCII_TEXT_BODY_LEN = len(_NON_ASCII_S3_TEXT_BODY)
_NON_ASCII_TEXT_BODY_LEN_ENCODED = len(_NON_ASCII_S3_TEXT_BODY.encode("utf-8"))

_BINARY = b"Example bytes"


class MockS3Object:
    def __init__(self, bytes_io):
        self.bytes_io = bytes_io

    def get(self):
        return {"Body": self.bytes_io}

    def shh(self):
        return self.__dict__


def s3_reader_text():
    bytes_io = BytesIO(_S3_TEXT_BODY.encode("utf-8"))

    s3_io = AsyncS3ObjectReader(
        s3_obj=cast(Object, MockS3Object(bytes_io)), buffer_size=_DEFAULT_BUFFER_SIZE, encoding="utf-8"
    )

    return s3_io


def ascii_s3_reader_text():
    bytes_io = BytesIO(_ASCII_S3_TEXT_BODY.encode("utf-8"))

    s3_io = AsyncS3ObjectReader(
        s3_obj=cast(Object, MockS3Object(bytes_io)), buffer_size=_DEFAULT_BUFFER_SIZE, encoding="utf-8"
    )

    return s3_io


def non_ascii_s3_reader_text():
    bytes_io = BytesIO(_NON_ASCII_S3_TEXT_BODY.encode("utf-8"))

    s3_io = AsyncS3ObjectReader(
        s3_obj=cast(Object, MockS3Object(bytes_io)), buffer_size=_DEFAULT_BUFFER_SIZE, encoding="utf-8"
    )

    return s3_io


def s3_reader_binary():
    bytes_io = BytesIO(_BINARY)

    s3_io = AsyncS3ObjectReader(s3_obj=cast(Object, MockS3Object(bytes_io)), buffer_size=_DEFAULT_BUFFER_SIZE)

    return s3_io


def s3_writer(bucket: Bucket, key: str):
    return AsyncS3ObjectWriter(s3_object(bucket.name, key), encoding="utf-8")


def s3_writer_binary(bucket: Bucket, key: str):
    return AsyncS3ObjectWriter(s3_object(bucket.name, key))


async def test_read_all() -> None:
    async with s3_reader_text() as reader:
        body = await reader.read()

    assert body == _S3_TEXT_BODY


async def test_read_n() -> None:
    size = 25

    chunks = []

    async with s3_reader_text() as reader:
        chunk = await reader.read(size)

        pos = 0

        assert len(reader._buffer.encode("utf-8")) == _DEFAULT_BUFFER_SIZE

        while chunk:
            chunks.append(chunk)
            actual = _S3_TEXT_BODY[pos : pos + len(chunk)]
            assert chunk == actual
            pos += len(chunk)
            chunk = await reader.read(size)

    assert "".join(chunks) == _S3_TEXT_BODY


async def test_read_negative() -> None:
    async with s3_reader_text() as reader:
        first_chunk = await reader.read(300)

        neg_chunk = await reader.read(-33)

        assert _S3_TEXT_BODY.encode("utf-8")[0:300].decode("utf-8") == first_chunk

        assert first_chunk[-33:] == neg_chunk

        assert reader._bytes_read == 298

        assert reader._position == 265


async def test_read_negative_all() -> None:
    read_size = -57

    async with s3_reader_text() as reader:
        _ = await reader.read()
        chunks: List[str] = []
        chunk = await reader.read(read_size)
        while chunk:
            chunks.insert(0, chunk)
            chunk = await reader.read(read_size)
        joined = "".join(chunks)
        assert joined == _S3_TEXT_BODY


async def test_seek_back_and_forth() -> None:
    async with non_ascii_s3_reader_text() as reader:
        pos = await reader.seek(57)

        assert pos == 57
        assert reader._position == 57
        assert reader._bytes_read <= _DEFAULT_BUFFER_SIZE

        pos = await reader.seek(57)

        assert pos == 57
        assert reader._position == 57
        assert reader._bytes_read <= _DEFAULT_BUFFER_SIZE

        new_pos = _DEFAULT_BUFFER_SIZE + 57
        pos = await reader.seek(new_pos)
        assert new_pos == pos
        assert new_pos == reader._position
        assert reader._bytes_read <= 2 * _DEFAULT_BUFFER_SIZE

        pos = await reader.seek(_NON_ASCII_TEXT_BODY_LEN_ENCODED + 1)

        assert pos == _NON_ASCII_TEXT_BODY_LEN

        pos = await reader.seek(100, SEEK_END)

        assert pos == _NON_ASCII_TEXT_BODY_LEN - 100


async def test_seek_relative() -> None:
    pos = 0

    async with non_ascii_s3_reader_text() as reader:
        for i in range(7):
            pos = await reader.seek(100, SEEK_CUR)

            assert pos == min((i + 1) * 100, _NON_ASCII_TEXT_BODY_LEN)

        assert pos == _NON_ASCII_TEXT_BODY_LEN

        for i in range(7):
            pos = await reader.seek(-100, SEEK_CUR)

            assert pos == max(0, _NON_ASCII_TEXT_BODY_LEN - ((i + 1) * 100))

        assert pos == 0

        read_all = await reader.read()

        assert read_all == _NON_ASCII_S3_TEXT_BODY


async def test_read_line() -> None:
    async with s3_reader_text() as reader:
        line = await reader.readline()

        assert line == _TEXT[0]

        line = await reader.readline()

        assert line == _TEXT[1]

        for _ in range(len(_TEXT) - 2):
            line = await reader.readline()

        assert line == _TEXT[-1]


async def test_read_line_on_binary_object():
    with pytest.raises(ValueError, match="readline only works if _encoding was defined"):
        async with s3_reader_binary() as reader:
            await reader.readline()


async def test_read_lines() -> None:
    async with s3_reader_text() as reader:
        line = await reader.readline()

        assert (_TEXT[0]) == line

        lines = [line async for line in reader.readlines()]

        assert (_TEXT[1:]) == lines


if sys.version_info < (3, 10):

    async def anext(gen, default: Any = None):  # noqa: A001
        async for el in gen:
            return el
        return default


async def test_read_lines_on_binary_object():
    with pytest.raises(ValueError, match="readlines only works if _encoding was defined"):
        async with s3_reader_binary() as reader:
            await anext(reader.readlines())  # type: ignore[name-defined]


async def aenumerate(asequence, start=0):
    """Asynchronously enumerate an async iterator from a given start value"""
    num = start
    async for elem in asequence:
        yield num, elem
        num += 1


async def test_iter() -> None:
    async with s3_reader_text() as reader:
        async for idx, a_line in aenumerate(reader.readlines()):
            assert a_line.strip() == _TEXT[idx].strip()


async def test_write_binary(temp_s3_bucket) -> None:
    key = f"testkey/{uuid4().hex}"
    async with s3_writer_binary(temp_s3_bucket, key) as writer:
        await writer.write(_BINARY)
        await writer.close()

    read = temp_s3_bucket.Object(key).get()["Body"].read()

    assert read == _BINARY


async def test_write_binary_then_nothing(temp_s3_bucket) -> None:
    key = f"testkey/{uuid4().hex}"
    async with s3_writer_binary(temp_s3_bucket, key) as writer:
        await writer.write(_BINARY)
        await writer.write(b"")
        await writer.close()

    read = temp_s3_bucket.Object(key).get()["Body"].read()

    assert read == _BINARY


async def test_write_string(temp_s3_bucket) -> None:
    key = f"testkey/{uuid4().hex}"
    async with s3_writer(temp_s3_bucket, key) as writer:
        await writer.writelines(_S3_TEXT_BODY)
        await writer.close()

    read = temp_s3_bucket.Object(key).get()["Body"].read()
    decoded = read.decode("utf-8")
    assert decoded == _S3_TEXT_BODY


async def test_write_lines_string(temp_s3_bucket) -> None:
    key = f"testkey/{uuid4().hex}"
    async with s3_writer(temp_s3_bucket, key) as writer:
        await writer.writelines(_TEXT)
        await writer.close()

    read = temp_s3_bucket.Object(key).get()["Body"].read()
    decoded = read.decode("utf-8")
    assert decoded == _S3_TEXT_BODY


async def test_upload_large_multipart_file(temp_s3_bucket) -> None:
    bytes_to_write = 20 * MiB
    block = b"A" * (4 * MiB)
    key = f"testkey/{uuid4().hex}"
    start = time()
    async with s3_writer_binary(temp_s3_bucket, key) as writer:
        bytes_written = 0
        while bytes_written < bytes_to_write:
            bytes_written += await writer.write(block)
        await writer.flush()
        etag = writer.s3_obj.e_tag
        assert etag.endswith('-3"')
    elapsed = time() - start
    print(elapsed)
    assert elapsed < 2

    async with AsyncS3ObjectReader(s3_object(temp_s3_bucket.name, key)) as reader:
        bytes_read = await reader.read()
        assert len(bytes_read) == bytes_to_write
