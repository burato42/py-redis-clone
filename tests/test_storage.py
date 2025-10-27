import asyncio
from collections import deque
from datetime import datetime, timedelta

import pytest

from app.storage import Storage, Value, ValueType


@pytest.fixture(scope="function")
def storage():
    return Storage()


class TestStorage:
    def test_init(self, storage):
        assert storage.data == {}

    @pytest.mark.asyncio
    async def test_set(self, storage):
        await storage.set("key", Value("value1"))
        assert storage.get("key") == Value("value1")
        await storage.set("key", Value("value2"))
        assert storage.get("key") == Value("value2")

    @pytest.mark.asyncio
    async def test_get(self, storage):
        assert not storage.get("key")
        future_expiration = datetime.now() + timedelta(seconds=3)
        past_expiration = datetime.now() - timedelta(seconds=2)
        await storage.set("key1", Value("value1", expire=future_expiration))
        assert storage.get("key1") == Value("value1", expire=future_expiration)
        await storage.set("key2", Value("value2", expire=past_expiration))
        assert not storage.get("key2")

    @pytest.mark.asyncio
    async def test_rpush(self, storage):
        await storage.set("key1", Value("value1"))
        with pytest.raises(
            RuntimeError, match="Key key1 already exists and it's not a list"
        ):
            await storage.rpush("key1", [Value("value2")])
        await storage.rpush("key2", [Value("value1")])
        assert storage.get("key2") == [Value("value1")]
        await storage.rpush("key2", [Value("value2")])
        assert storage.get("key2") == [Value("value1"), Value("value2")]

    @pytest.mark.asyncio
    async def test_lpush(self, storage):
        await storage.set("key1", Value("value1"))
        with pytest.raises(
            RuntimeError, match="Key key1 already exists and it's not a list"
        ):
            await storage.lpush("key1", [Value("value2")])
        await storage.lpush("key2", [Value("value1")])
        assert storage.get("key2") == [Value("value1")]
        await storage.lpush("key2", [Value("value2")])
        assert storage.get("key2") == [Value("value2"), Value("value1")]

    @pytest.mark.asyncio
    async def test_get_blocking(self, storage):
        await storage.set("key1", Value("value1"))
        assert await storage.get_blocking("key1") == Value("value1")

    @pytest.mark.asyncio
    async def test_get_blocking_wait(self, storage):
        async def set_after_delay():
            await asyncio.sleep(0.01)  # Small delay
            await storage.set("key1", Value("value1"))

        value, _ = await asyncio.gather(storage.get_blocking("key1"), set_after_delay())

        assert value == Value("value1")

    @pytest.mark.asyncio
    async def test_get_blocking_timeout(self, storage):
        async def set_after_delay():
            await asyncio.sleep(0.01)  # Small delay
            await storage.set("key1", Value("value1"))

        value, _ = await asyncio.gather(
            storage.get_blocking("key1", 1), set_after_delay()
        )

        assert value == Value("value1")

    @pytest.mark.asyncio
    async def test_get_blocking_timeout_exceeded(self, storage):
        async def set_after_delay():
            await asyncio.sleep(1.01)
            await storage.set("key1", Value("value1"))

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.gather(storage.get_blocking("key1", 1), set_after_delay())

    @pytest.mark.asyncio
    async def test_type(self, storage):
        assert storage.get_type("key1") == ValueType.NONE
        await storage.set("key1", Value("value1"))
        assert storage.get_type("key1") == ValueType.STRING
        await storage.rpush("key2", [Value("value1"), Value("value2")])
        assert storage.get_type("key2") == ValueType.LIST

    def test_stream_xadd(self, storage):
        with pytest.raises(
            ValueError, match="The ID specified in XADD must be greater than 0-0"
        ):
            storage.set_stream("key1", Value({"id": "0-0", "foo": "bar", "baz": "qux"}))

        storage.set_stream("key1", Value({"id": "0-1", "foo": "bar", "baz": "qux"}))
        assert storage.data.get("key1") == deque(
            [Value({"id": "0-1", "foo": "bar", "baz": "qux"})]
        )

        with pytest.raises(
            ValueError,
            match="The ID specified in XADD is equal or smaller than the target stream top item",
        ):
            storage.set_stream("key1", Value({"id": "0-1", "bar": "foo", "baz": "qux"}))

        storage.set_stream("key1", Value({"id": "1-1", "bar": "foo", "baz": "qux"}))
        assert storage.data.get("key1") == deque(
            [
                Value({"id": "0-1", "foo": "bar", "baz": "qux"}),
                Value({"id": "1-1", "bar": "foo", "baz": "qux"}),
            ]
        )

        with pytest.raises(
            ValueError,
            match="The ID specified in XADD is equal or smaller than the target stream top item",
        ):
            storage.set_stream("key1", Value({"id": "0-1", "bar": "foo", "baz": "qux"}))
