import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.parser import Command
from app.processor import process_command
from app.storage import Storage, Value


@pytest.fixture(scope="module")
def writer():
    class Writer:
        def __init__(self):
            self.response = None

        def write(self, response: bytes) -> None:
            self.response = response

        async def drain(self):
            pass

    return Writer()


@pytest.fixture(scope="function")
def storage_stub():
    return Storage()


@pytest.fixture()
def mock_datetime_now(monkeypatch):
    datetime_mock = MagicMock(wraps=datetime.datetime)
    datetime_mock.now.return_value = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    monkeypatch.setattr(datetime, "datetime", datetime_mock)


@pytest.mark.asyncio
class TestProcessor:
    async def test_echo(self, writer: Any, storage_stub):
        await process_command((Command.ECHO, "banana"), writer, storage_stub)
        assert writer.response.decode() == "$6\r\nbanana\r\n"

    async def test_set_simple(self, writer: Any, storage_stub):
        await process_command((Command.SET, "foo", "bar"), writer, storage_stub)
        assert writer.response.decode() == "+OK\r\n"
        assert storage_stub.data == {"foo": Value(item="bar", expire=None)}

    async def test_set_with_expiration_seconds(
        self, mock_datetime_now, writer: Any, storage_stub
    ):
        await process_command(
            (Command.SET, "foo", "bar", "ex", 50), writer, storage_stub
        )
        assert writer.response.decode() == "+OK\r\n"
        assert len(storage_stub.data) == 1
        assert storage_stub.data["foo"].item == "bar"
        assert datetime.datetime(2020, 1, 1, 0, 0, 50, tzinfo=datetime.UTC)

    async def test_set_with_expiration_milliseconds(
        self, mock_datetime_now, writer: Any, storage_stub
    ):
        await process_command(
            (Command.SET, "foo", "bar", "Px", 123), writer, storage_stub
        )
        assert writer.response.decode() == "+OK\r\n"
        assert len(storage_stub.data) == 1
        assert storage_stub.data["foo"].item == "bar"
        assert datetime.datetime(2020, 1, 1, 0, 0, 0, 123000, tzinfo=datetime.UTC)
