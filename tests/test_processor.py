import asyncio
import datetime
from unittest.mock import MagicMock

import pytest

from app.parser import Command
from app.processor import Processor
from app.storage import Storage, Value


@pytest.fixture(scope="function")
def writer():
    class Writer:
        def __init__(self):
            self.response = []

        def write(self, current_response: bytes) -> None:
            self.response.append(current_response)

        async def drain(self):
            pass

    return Writer()


@pytest.fixture(scope="function")
def storage_stub():
    return Storage()


@pytest.fixture(scope="function")
def processor_stub(writer, storage_stub):
    return Processor(writer, storage_stub)


@pytest.fixture()
def mock_datetime_now(monkeypatch):
    datetime_mock = MagicMock(wraps=datetime.datetime)
    datetime_mock.now.return_value = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    monkeypatch.setattr(datetime, "datetime", datetime_mock)


@pytest.mark.asyncio
class TestProcessor:
    async def test_echo(self, processor_stub):
        await processor_stub.process_command((Command.ECHO, "banana"))
        assert processor_stub.writer.response[0].decode() == "$6\r\nbanana\r\n"

    async def test_set_simple(self, processor_stub):
        await processor_stub.process_command((Command.SET, "foo", "bar"))
        assert processor_stub.writer.response[0].decode() == "+OK\r\n"
        assert processor_stub.storage.data == {"foo": Value(item="bar", expire=None)}

    async def test_set_with_expiration_seconds(self, mock_datetime_now, processor_stub):
        await processor_stub.process_command((Command.SET, "foo", "bar", "ex", 50))
        assert processor_stub.writer.response[0].decode() == "+OK\r\n"
        assert len(processor_stub.storage.data) == 1
        assert processor_stub.storage.data["foo"].item == "bar"
        assert processor_stub.storage.data["foo"].expire == datetime.datetime(
            2020, 1, 1, 0, 0, 50, tzinfo=datetime.UTC
        )

    async def test_set_with_expiration_milliseconds(
        self, mock_datetime_now, processor_stub
    ):
        await processor_stub.process_command((Command.SET, "foo", "bar", "Px", 123))
        assert processor_stub.writer.response[0].decode() == "+OK\r\n"
        assert len(processor_stub.storage.data) == 1
        assert processor_stub.storage.data["foo"].item == "bar"
        assert processor_stub.storage.data["foo"].expire == datetime.datetime(
            2020, 1, 1, 0, 0, 0, 123000, tzinfo=datetime.UTC
        )

    async def test_get(self, mock_datetime_now, processor_stub):
        await processor_stub.process_command((Command.GET, "foo"))
        assert processor_stub.writer.response[0].decode() == "$-1\r\n"
        await processor_stub.process_command((Command.SET, "foo", "bar"))
        assert processor_stub.writer.response[1].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.GET, "foo"))
        assert processor_stub.writer.response[2].decode() == "$3\r\nbar\r\n"
        await processor_stub.process_command((Command.SET, "foo", "bar", "ex", 45))
        assert processor_stub.writer.response[3].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.GET, "foo"))
        assert processor_stub.writer.response[4].decode() == "$3\r\nbar\r\n"
        await processor_stub.process_command((Command.SET, "foo", "bar", "ex", -5))
        assert processor_stub.writer.response[5].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.GET, "foo"))
        assert processor_stub.writer.response[6].decode() == "$-1\r\n"

    async def test_ping(self, processor_stub):
        await processor_stub.process_command((Command.PING,))
        assert processor_stub.writer.response[0].decode() == "+PONG\r\n"

    async def test_rpush(self, processor_stub):
        assert processor_stub.storage.data == {}
        await processor_stub.process_command((Command.RPUSH, "key", "value1", "value2"))
        assert processor_stub.writer.response[0].decode() == ":2\r\n"
        assert processor_stub.storage.data["key"] == [
            Value(item="value1", expire=None),
            Value(item="value2", expire=None),
        ]
        await processor_stub.process_command((Command.RPUSH, "key", "value3"))
        assert processor_stub.writer.response[1].decode() == ":3\r\n"
        assert processor_stub.storage.data["key"] == [
            Value(item="value1", expire=None),
            Value(item="value2", expire=None),
            Value(item="value3", expire=None),
        ]

    async def test_lpush(self, processor_stub):
        assert processor_stub.storage.data == {}
        await processor_stub.process_command((Command.LPUSH, "key", "value1", "value2"))
        assert processor_stub.writer.response[0].decode() == ":2\r\n"
        assert processor_stub.storage.data["key"] == [
            Value(item="value2", expire=None),
            Value(item="value1", expire=None),
        ]
        await processor_stub.process_command((Command.LPUSH, "key", "value3"))
        assert processor_stub.writer.response[1].decode() == ":3\r\n"
        assert processor_stub.storage.data["key"] == [
            Value(item="value3", expire=None),
            Value(item="value2", expire=None),
            Value(item="value1", expire=None),
        ]
        await processor_stub.process_command((Command.RPUSH, "key", "value4"))
        assert processor_stub.writer.response[2].decode() == ":4\r\n"
        assert processor_stub.storage.data["key"] == [
            Value(item="value3", expire=None),
            Value(item="value2", expire=None),
            Value(item="value1", expire=None),
            Value(item="value4", expire=None),
        ]

    async def test_range(self, processor_stub):
        assert processor_stub.storage.data == {}
        await processor_stub.process_command(
            (Command.RPUSH, "key", "value1", "value2", "value3", "value4", "value5")
        )
        assert processor_stub.writer.response[0].decode() == ":5\r\n"
        await processor_stub.process_command((Command.LRANGE, "non_existent", "0", "1"))
        assert processor_stub.writer.response[1].decode() == "*0\r\n"
        await processor_stub.process_command((Command.LRANGE, "key", "0", "1"))
        assert (
            processor_stub.writer.response[2].decode()
            == "*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
        )
        await processor_stub.process_command((Command.LRANGE, "key", "3", "10"))
        assert (
            processor_stub.writer.response[3].decode()
            == "*2\r\n$6\r\nvalue4\r\n$6\r\nvalue5\r\n"
        )
        await processor_stub.process_command((Command.LRANGE, "key", "-3", "10"))
        assert (
            processor_stub.writer.response[4].decode()
            == "*3\r\n$6\r\nvalue3\r\n$6\r\nvalue4\r\n$6\r\nvalue5\r\n"
        )

    async def test_len(self, processor_stub):
        await processor_stub.process_command(
            (Command.RPUSH, "key", "value1", "value2", "value3", "value4", "value5")
        )
        await processor_stub.process_command((Command.LLEN, "key"))
        assert processor_stub.writer.response[0].decode() == ":5\r\n"

    async def test_lpop(self, processor_stub):
        await processor_stub.process_command((Command.LPOP, "key"))
        assert processor_stub.writer.response[0].decode() == "$-1\r\n"
        await processor_stub.process_command(
            (Command.RPUSH, "key", "value1", "value2", "value3")
        )
        await processor_stub.process_command((Command.LPOP, "key"))
        assert processor_stub.writer.response[2].decode() == "$6\r\nvalue1\r\n"
        assert processor_stub.storage.data["key"] == [
            Value(item="value2", expire=None),
            Value(item="value3", expire=None),
        ]

    async def test_lpop_multiple(self, processor_stub):
        await processor_stub.process_command(
            (Command.RPUSH, "key", "value1", "value2", "value3")
        )
        await processor_stub.process_command((Command.LPOP, "key", "2"))
        assert (
            processor_stub.writer.response[1].decode()
            == "*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
        )
        assert processor_stub.storage.data["key"] == [
            Value(item="value3", expire=None),
        ]
        await processor_stub.process_command((Command.LPOP, "key", "2"))
        assert processor_stub.writer.response[2].decode() == "*1\r\n$6\r\nvalue3\r\n"
        assert processor_stub.storage.data["key"] == []

    async def test_blpop_one_value(self, processor_stub):
        await processor_stub.process_command((Command.SET, "foo", "bar"))
        await processor_stub.process_command((Command.BLPOP, "foo"))
        assert processor_stub.writer.response[1].decode() == "$-1\r\n"

    async def test_blpop_one_value_zero_timeout(self, processor_stub):
        await processor_stub.process_command((Command.SET, "foo", "bar"))
        await processor_stub.process_command((Command.BLPOP, "foo", "0"))
        assert processor_stub.writer.response[1].decode() == "$-1\r\n"

    async def test_blpop_list(self, processor_stub):
        async def set_after_delay():
            await asyncio.sleep(0.01)
            await processor_stub.process_command((Command.RPUSH, "key", "value1", "value2"))

        await asyncio.gather(
            processor_stub.process_command((Command.BLPOP, "key")),
            set_after_delay()
        )

        assert processor_stub.writer.response[1].decode() == "*2\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n"

    async def test_blpop_list_timeout(self, processor_stub):
        async def set_after_delay():
            await asyncio.sleep(0.01)
            await processor_stub.process_command((Command.RPUSH, "key", "value1", "value2"))

        await asyncio.gather(
            processor_stub.process_command((Command.BLPOP, "key", "1")),
            set_after_delay()
        )

        assert processor_stub.writer.response[1].decode() == "*2\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n"

    async def test_blpop_list_timeout_exceeded(self, processor_stub):
        async def set_after_delay():
            await asyncio.sleep(0.51)
            await processor_stub.process_command((Command.RPUSH, "key", "value1", "value2"))

        await asyncio.gather(
            processor_stub.process_command((Command.BLPOP, "key", "0.5")),
            set_after_delay()
        )

        assert processor_stub.writer.response[0].decode() == "*-1\r\n"
        assert processor_stub.writer.response[1].decode() == ":2\r\n"

    async def test_get_type(self, processor_stub):
        await processor_stub.process_command((Command.TYPE, "key1"))
        assert processor_stub.writer.response[0].decode() == "+none\r\n"
        await processor_stub.process_command((Command.SET, "key2", "bar"))
        await processor_stub.process_command((Command.TYPE, "key2"))
        assert processor_stub.writer.response[2].decode() == "+string\r\n"
        await processor_stub.process_command((Command.RPUSH, "key", "value1", "value2"))
        await processor_stub.process_command((Command.TYPE, "key"))
        assert processor_stub.writer.response[4].decode() == "+list\r\n"

    async def test_get_type_timeout(self, processor_stub):
        await processor_stub.process_command((Command.XADD,  "key1", "0-1", "foo", "bar", "baz", "qux"))
        assert processor_stub.writer.response[0].decode() == "$3\r\n0-1\r\n"
        await processor_stub.process_command((Command.TYPE, "key1"))
        assert processor_stub.writer.response[1].decode() == "+stream\r\n"
