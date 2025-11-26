import asyncio
import datetime
from unittest.mock import MagicMock

import pytest

from app.replicaclient import Client
from app.parser import Command
from app.processor import Processor
from app.status import Status
from app.storage.storage_facade import Storage, Value


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
def reader():
    return None

@pytest.fixture(scope="function")
def storage_stub():
    return Storage()


@pytest.fixture(scope="function")
def status_stub():
    return Status("primary")


@pytest.fixture(scope="function")
def processor_stub(writer, storage_stub, status_stub):
    return Processor(storage_stub, status_stub)


@pytest.fixture()
def mock_datetime_now(monkeypatch):
    datetime_mock = MagicMock(wraps=datetime.datetime)
    datetime_mock.now.return_value = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    monkeypatch.setattr(datetime, "datetime", datetime_mock)


@pytest.mark.asyncio
class TestProcessor:
    async def test_echo(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.ECHO, "banana"), reader, writer)
        assert writer.response[0].decode() == "$6\r\nbanana\r\n"

    async def test_set_simple(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.SET, "foo", "bar"), reader, writer)
        assert writer.response[0].decode() == "+OK\r\n"
        assert processor_stub.storage.string_storage.data == {
            "foo": Value(item="bar", expire=None)
        }

    async def test_set_with_expiration_seconds(self, mock_datetime_now, processor_stub, reader, writer):
        await processor_stub.process_command((Command.SET, "foo", "bar", "ex", 50), reader, writer)
        assert writer.response[0].decode() == "+OK\r\n"
        assert len(processor_stub.storage.string_storage.data) == 1
        assert processor_stub.storage.string_storage.data["foo"].item == "bar"
        assert processor_stub.storage.string_storage.data[
            "foo"
        ].expire == datetime.datetime(2020, 1, 1, 0, 0, 50, tzinfo=datetime.UTC)

    async def test_set_with_expiration_milliseconds(
        self, mock_datetime_now, processor_stub, reader, writer
    ):
        await processor_stub.process_command((Command.SET, "foo", "bar", "Px", 123), reader, writer)
        assert writer.response[0].decode() == "+OK\r\n"
        assert len(processor_stub.storage.string_storage.data) == 1
        assert processor_stub.storage.string_storage.data["foo"].item == "bar"
        assert processor_stub.storage.string_storage.data[
            "foo"
        ].expire == datetime.datetime(2020, 1, 1, 0, 0, 0, 123000, tzinfo=datetime.UTC)

    async def test_get(self, mock_datetime_now, processor_stub, reader, writer):
        await processor_stub.process_command((Command.GET, "foo"), reader, writer)
        assert writer.response[0].decode() == "$-1\r\n"
        await processor_stub.process_command((Command.SET, "foo", "bar"), reader, writer)
        assert writer.response[1].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.GET, "foo"), reader, writer)
        assert writer.response[2].decode() == "$3\r\nbar\r\n"
        await processor_stub.process_command((Command.SET, "foo", "bar", "ex", 45), reader, writer)
        assert writer.response[3].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.GET, "foo"), reader, writer)
        assert writer.response[4].decode() == "$3\r\nbar\r\n"
        await processor_stub.process_command((Command.SET, "foo", "bar", "ex", -5), reader, writer)
        assert writer.response[5].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.GET, "foo"), reader, writer)
        assert writer.response[6].decode() == "$-1\r\n"

    async def test_ping(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.PING,), reader, writer)
        assert writer.response[0].decode() == "+PONG\r\n"

    async def test_rpush(self, processor_stub, reader, writer):
        assert processor_stub.storage.list_storage.data == {}
        await processor_stub.process_command((Command.RPUSH, "key", "value1", "value2"), reader, writer)
        assert writer.response[0].decode() == ":2\r\n"
        assert processor_stub.storage.list_storage.data["key"] == [
            Value(item="value1", expire=None),
            Value(item="value2", expire=None),
        ]
        await processor_stub.process_command((Command.RPUSH, "key", "value3"), reader, writer)
        assert writer.response[1].decode() == ":3\r\n"
        assert processor_stub.storage.list_storage.data["key"] == [
            Value(item="value1", expire=None),
            Value(item="value2", expire=None),
            Value(item="value3", expire=None),
        ]

    async def test_lpush(self, processor_stub, reader, writer):
        assert processor_stub.storage.list_storage.data == {}
        await processor_stub.process_command((Command.LPUSH, "key", "value1", "value2"), reader, writer)
        assert writer.response[0].decode() == ":2\r\n"
        assert processor_stub.storage.list_storage.data["key"] == [
            Value(item="value2", expire=None),
            Value(item="value1", expire=None),
        ]
        await processor_stub.process_command((Command.LPUSH, "key", "value3"), reader, writer)
        assert writer.response[1].decode() == ":3\r\n"
        assert processor_stub.storage.list_storage.data["key"] == [
            Value(item="value3", expire=None),
            Value(item="value2", expire=None),
            Value(item="value1", expire=None),
        ]
        await processor_stub.process_command((Command.RPUSH, "key", "value4"), reader, writer)
        assert writer.response[2].decode() == ":4\r\n"
        assert processor_stub.storage.list_storage.data["key"] == [
            Value(item="value3", expire=None),
            Value(item="value2", expire=None),
            Value(item="value1", expire=None),
            Value(item="value4", expire=None),
        ]

    async def test_range(self, processor_stub, reader, writer):
        assert processor_stub.storage.list_storage.data == {}
        await processor_stub.process_command(
            (Command.RPUSH, "key", "value1", "value2", "value3", "value4", "value5"), reader, writer
        )
        assert writer.response[0].decode() == ":5\r\n"
        await processor_stub.process_command((Command.LRANGE, "non_existent", "0", "1"), reader, writer)
        assert writer.response[1].decode() == "*0\r\n"
        await processor_stub.process_command((Command.LRANGE, "key", "0", "1"), reader, writer)
        assert (
            writer.response[2].decode()
            == "*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
        )
        await processor_stub.process_command((Command.LRANGE, "key", "3", "10"), reader, writer)
        assert (
            writer.response[3].decode()
            == "*2\r\n$6\r\nvalue4\r\n$6\r\nvalue5\r\n"
        )
        await processor_stub.process_command((Command.LRANGE, "key", "-3", "10"), reader, writer)
        assert (
            writer.response[4].decode()
            == "*3\r\n$6\r\nvalue3\r\n$6\r\nvalue4\r\n$6\r\nvalue5\r\n"
        )

    async def test_len(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.RPUSH, "key", "value1", "value2", "value3", "value4", "value5"), reader, writer
        )
        await processor_stub.process_command((Command.LLEN, "key"), reader, writer)
        assert writer.response[0].decode() == ":5\r\n"

    async def test_lpop(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.LPOP, "key"), reader, writer)
        assert writer.response[0].decode() == "$-1\r\n"
        await processor_stub.process_command(
            (Command.RPUSH, "key", "value1", "value2", "value3"), reader, writer
        )
        await processor_stub.process_command((Command.LPOP, "key"), reader, writer)
        assert writer.response[2].decode() == "$6\r\nvalue1\r\n"
        assert processor_stub.storage.list_storage.data["key"] == [
            Value(item="value2", expire=None),
            Value(item="value3", expire=None),
        ]

    async def test_lpop_multiple(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.RPUSH, "key", "value1", "value2", "value3"), reader, writer
        )
        await processor_stub.process_command((Command.LPOP, "key", "2"), reader, writer)
        assert (
            writer.response[1].decode()
            == "*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n"
        )
        assert processor_stub.storage.list_storage.data["key"] == [
            Value(item="value3", expire=None),
        ]
        await processor_stub.process_command((Command.LPOP, "key", "2"), reader, writer)
        assert writer.response[2].decode() == "*1\r\n$6\r\nvalue3\r\n"
        assert processor_stub.storage.list_storage.data["key"] == []

    async def test_blpop_list(self, processor_stub, reader, writer):
        async def set_after_delay():
            await asyncio.sleep(0.01)
            await processor_stub.process_command(
                (Command.RPUSH, "key", "value1", "value2"), reader, writer
            )

        await asyncio.gather(
            processor_stub.process_command((Command.BLPOP, "key"), reader, writer), set_after_delay()
        )

        assert (
            writer.response[1].decode()
            == "*2\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n"
        )

    async def test_blpop_list_timeout(self, processor_stub, reader, writer):
        async def set_after_delay():
            await asyncio.sleep(0.01)
            await processor_stub.process_command(
                (Command.RPUSH, "key", "value1", "value2"), reader, writer
            )

        await asyncio.gather(
            processor_stub.process_command((Command.BLPOP, "key", "1"), reader, writer),
            set_after_delay(),
        )

        assert (
            writer.response[1].decode()
            == "*2\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n"
        )

    async def test_blpop_list_timeout_exceeded(self, processor_stub, reader, writer):
        async def set_after_delay():
            await asyncio.sleep(0.51)
            await processor_stub.process_command(
                (Command.RPUSH, "key", "value1", "value2"), reader, writer
            )

        await asyncio.gather(
            processor_stub.process_command((Command.BLPOP, "key", "0.5"), reader, writer),
            set_after_delay(),
        )

        assert writer.response[0].decode() == "*-1\r\n"
        assert writer.response[1].decode() == ":2\r\n"

    async def test_get_type(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.TYPE, "key1"), reader, writer)
        assert writer.response[0].decode() == "+none\r\n"
        await processor_stub.process_command((Command.SET, "key2", "bar"), reader, writer)
        await processor_stub.process_command((Command.TYPE, "key2"), reader, writer)
        assert writer.response[2].decode() == "+string\r\n"
        await processor_stub.process_command((Command.RPUSH, "key", "value1", "value2"), reader, writer)
        await processor_stub.process_command((Command.TYPE, "key"), reader, writer)
        assert writer.response[4].decode() == "+list\r\n"

    async def test_xadd(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "key1", "0-1", "foo", "bar", "baz", "qux"), reader, writer
        )
        assert writer.response[0].decode() == "$3\r\n0-1\r\n"
        await processor_stub.process_command((Command.TYPE, "key1"), reader, writer)
        assert writer.response[1].decode() == "+stream\r\n"
        await processor_stub.process_command(
            (Command.XADD, "key1", "0-1", "foo", "bar", "baz", "qux"), reader, writer
        )
        assert (
            writer.response[2].decode()
            == "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        )

    async def test_xadd_autogenerated(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "key1", "1-*", "foo", "bar", "baz", "qux"), reader, writer
        )
        assert writer.response[0].decode() == "$3\r\n1-1\r\n"
        await processor_stub.process_command((Command.TYPE, "key1"), reader, writer)
        assert writer.response[1].decode() == "+stream\r\n"
        await processor_stub.process_command(
            (Command.XADD, "key1", "1-*", "foo", "bar", "baz", "qux"), reader, writer
        )
        assert writer.response[2].decode() == "$3\r\n1-2\r\n"
        await processor_stub.process_command(
            (Command.XADD, "key1", "0-*", "foo", "bar", "baz", "qux"), reader, writer
        )
        assert (
            writer.response[3].decode()
            == "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        )

    async def test_xadd_full_autogenerated(self, processor_stub, reader, writer, mock_datetime_now):
        await processor_stub.process_command(
            (Command.XADD, "key1", "*", "foo", "bar", "baz", "qux"), reader, writer
        )
        assert (
            writer.response[0].decode() == "$15\r\n1577836800000-0\r\n"
        )
        await processor_stub.process_command(
            (Command.XADD, "key1", "*", "foo", "bar", "baz", "qux"), reader, writer
        )
        assert (
            writer.response[1].decode() == "$15\r\n1577836800000-1\r\n"
        )

    async def test_xrange1(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command((Command.XRANGE, "banana", "0-2", "0-3"), reader, writer)
        assert (
            writer.response[3].decode()
            == "*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$9\r\nblueberry\r\n$6\r\nbanana\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
        )

    async def test_xrange2(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command((Command.XRANGE, "banana", "0-2", "0"), reader, writer)
        assert (
            writer.response[3].decode()
            == "*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$9\r\nblueberry\r\n$6\r\nbanana\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
        )

    async def test_xrange3(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "1-1", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command((Command.XRANGE, "banana", "0", "0"), reader, writer)
        assert (
            writer.response[4].decode()
            == "*3\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$5\r\ngrape\r\n$9\r\nraspberry\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$9\r\nblueberry\r\n$6\r\nbanana\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
        )

    async def test_xrange_query_minus(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-4", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command((Command.XRANGE, "banana", "-", "0-2"), reader, writer)
        assert (
            writer.response[4].decode()
            == "*2\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$5\r\ngrape\r\n$9\r\nraspberry\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$9\r\nblueberry\r\n$6\r\nbanana\r\n"
        )

    async def test_xrange_query_plus(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-4", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command((Command.XRANGE, "banana", "0-2", "+"), reader, writer)
        assert (
            writer.response[4].decode()
            == "*3\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$9\r\nblueberry\r\n$6\r\nbanana\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n*2\r\n$3\r\n0-4\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
        )

    async def test_xread_query(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-4", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XREAD, "STREAMS", "banana", "0-3"), reader, writer
        )
        assert (
            writer.response[4].decode()
            == "*1\r\n*2\r\n$6\r\nbanana\r\n*1\r\n*2\r\n$3\r\n0-4\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
        )

    async def test_xread_query_multiple(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-4", "orange", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "tomato", "0-3", "beetroot", "potato"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "tomato", "0-4", "redis", "cabbage"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XREAD, "STREAMS", "banana", "tomato", "0-3", "0-4"), reader, writer
        )
        assert (
            writer.response[6].decode()
            == "*1\r\n*2\r\n$6\r\nbanana\r\n*1\r\n*2\r\n$3\r\n0-4\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
        )

    async def test_xread_query_blocking(self, processor_stub, reader, writer):
        async def set_after_delay():
            await asyncio.sleep(0.4)
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
            )
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
            )
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
            )
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-4", "orange", "raspberry"), reader, writer
            )

        await asyncio.gather(
            processor_stub.process_command(
                (Command.XREAD, "block", "500", "STREAM", "banana", "0-1"), reader, writer
            ),
            set_after_delay(),
        )

        assert (
            writer.response[4].decode()
            == "*1\r\n*2\r\n$6\r\nbanana\r\n*3\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$9\r\nblueberry\r\n$6\r\nbanana\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n*2\r\n$3\r\n0-4\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
        )

    async def test_xread_query_blocking_timeout(self, processor_stub, reader, writer):
        async def set_after_delay():
            await asyncio.sleep(0.4)
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
            )
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
            )
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
            )
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-4", "orange", "raspberry"), reader, writer
            )

        await asyncio.gather(
            processor_stub.process_command(
                (Command.XREAD, "block", "300", "STREAM", "banana", "0-1"), reader, writer
            ),
            set_after_delay(),
        )

        assert writer.response[0].decode() == "*-1\r\n"

    async def test_xread_query_blocking_with_starting_id_empty(self, processor_stub, reader, writer):
        async def set_after_delay():
            await asyncio.sleep(0.01)
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
            )
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
            )

        await asyncio.gather(
            processor_stub.process_command(
                (Command.XREAD, "block", "500", "STREAM", "banana", "$"), reader, writer
            ),
            set_after_delay(),
        )

        assert writer.response[0].decode() == "$3\r\n0-1\r\n"
        assert writer.response[1].decode() == "$3\r\n0-2\r\n"
        assert (
            writer.response[2].decode()
            == "*1\r\n*2\r\n$6\r\nbanana\r\n*2\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$5\r\ngrape\r\n$9\r\nraspberry\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$9\r\nblueberry\r\n$6\r\nbanana\r\n"
        )

    async def test_xread_query_blocking_with_starting_id_existing(self, processor_stub, reader, writer):
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-1", "grape", "raspberry"), reader, writer
        )
        await processor_stub.process_command(
            (Command.XADD, "banana", "0-2", "blueberry", "banana"), reader, writer
        )

        async def set_after_delay():
            await asyncio.sleep(0.01)
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-3", "orange", "raspberry"), reader, writer
            )
            await processor_stub.process_command(
                (Command.XADD, "banana", "0-4", "orange", "raspberry"), reader, writer
            )

        await asyncio.gather(
            processor_stub.process_command(
                (Command.XREAD, "block", "500", "STREAM", "banana", "$"), reader, writer
            ),
            set_after_delay(),
        )

        assert writer.response[0].decode() == "$3\r\n0-1\r\n"
        assert writer.response[1].decode() == "$3\r\n0-2\r\n"
        assert writer.response[2].decode() == "$3\r\n0-3\r\n"
        assert writer.response[3].decode() == "$3\r\n0-4\r\n"
        assert (
            writer.response[4].decode()
            == "*1\r\n*2\r\n$6\r\nbanana\r\n*2\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n*2\r\n$3\r\n0-4\r\n*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
        )

    async def test_increment(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.INCR, "banana"), reader, writer)
        assert writer.response[0].decode() == ":1\r\n"
        await processor_stub.process_command((Command.INCR, "banana"), reader, writer)
        assert writer.response[1].decode() == ":2\r\n"
        await processor_stub.process_command((Command.SET, "mango", "pear"), reader, writer)
        await processor_stub.process_command((Command.INCR, "mango"), reader, writer)
        assert (
            writer.response[3].decode()
            == "-ERR value is not an integer or out of range\r\n"
        )

    async def test_transactions(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.MULTI,), reader, writer)
        assert writer.response[0].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.SET, "foo", "bar"), reader, writer)
        assert writer.response[1].decode() == "+QUEUED\r\n"
        await processor_stub.process_command((Command.SET, "baz", "1"), reader, writer)
        assert writer.response[2].decode() == "+QUEUED\r\n"
        await processor_stub.process_command((Command.INCR, "baz"), reader, writer)
        assert writer.response[3].decode() == "+QUEUED\r\n"
        await processor_stub.process_command((Command.EXEC,), reader, writer)
        assert (
            writer.response[4].decode() == "*3\r\n+OK\r\n+OK\r\n:2\r\n"
        )

    async def test_transactions_empty(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.EXEC,), reader, writer)
        assert (
            writer.response[0].decode() == "-ERR EXEC without MULTI\r\n"
        )
        await processor_stub.process_command((Command.MULTI,), reader, writer)
        assert writer.response[1].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.EXEC,), reader, writer)
        assert writer.response[2].decode() == "*0\r\n"

    async def test_discard(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.DISCARD,), reader, writer)
        assert (
            writer.response[0].decode()
            == "-ERR DISCARD without MULTI\r\n"
        )
        await processor_stub.process_command((Command.MULTI,), reader, writer)
        assert writer.response[1].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.SET, "foo", "bar"), reader, writer)
        assert writer.response[2].decode() == "+QUEUED\r\n"
        await processor_stub.process_command((Command.DISCARD,), reader, writer)
        assert writer.response[3].decode() == "+OK\r\n"
        await processor_stub.process_command((Command.EXEC,), reader, writer)
        assert (
            writer.response[4].decode() == "-ERR EXEC without MULTI\r\n"
        )

    async def test_info(self, processor_stub, reader, writer):
        await processor_stub.process_command((Command.INFO, "replication"), reader, writer)
        assert writer.response[0].decode() == "$50\r\nrole:primary\r\nmaster_replid:\r\nmaster_repl_offset:0\r\n"
