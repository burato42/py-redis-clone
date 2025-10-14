from typing import Any

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


@pytest.mark.asyncio
class TestProcessor:

    async def test_echo(self, writer: Any):
        await process_command((Command.ECHO, "banana"), writer, storage_stub)
        assert writer.response.decode() == "$6\r\nbanana\r\n"

    async def test_set_simple(self, writer: Any, storage_stub):
        await process_command((Command.SET, "foo", "bar"), writer, storage_stub)
        assert writer.response.decode() == "+OK\r\n"
        assert storage_stub.data == {"foo": Value(item='bar', expire=None)}
