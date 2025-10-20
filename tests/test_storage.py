import asyncio
from datetime import datetime, timedelta

import pytest

from app.storage import Storage, Value


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

        value, _ = await asyncio.gather(
            storage.get_blocking("key1"),
            set_after_delay()
        )

        assert value == Value("value1")

    @pytest.mark.asyncio
    async def test_get_blocking_timeout(self, storage):
        async def set_after_delay():
            await asyncio.sleep(0.01)  # Small delay
            await storage.set("key1", Value("value1"))

        value, _ = await asyncio.gather(
            storage.get_blocking("key1", 1),
            set_after_delay()
        )

        assert value == Value("value1")

    @pytest.mark.asyncio
    async def test_get_blocking_timeout_exceeded(self, storage):
        async def set_after_delay():
            await asyncio.sleep(1.01)
            await storage.set("key1", Value("value1"))

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.gather(
                storage.get_blocking("key1", 1),
                set_after_delay()
            )

