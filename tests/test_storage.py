from datetime import datetime, timedelta

import pytest

from app.storage import Storage, Value


@pytest.fixture(scope="function")
def storage():
    return Storage()


class TestStorage:

    def test_init(self, storage):
        assert storage.data == {}

    def test_set(self, storage):
        storage.set("key", Value("value1"))
        assert storage.get("key") == Value("value1")
        storage.set("key", Value("value2"))
        assert storage.get("key") == Value("value2")

    def test_get(self, storage):
        assert not storage.get("key")
        future_expiration = datetime.now() + timedelta(seconds=3)
        past_expiration = datetime.now() - timedelta(seconds=2)
        storage.set("key1", Value("value1", expire=future_expiration))
        assert storage.get("key1") == Value("value1", expire=future_expiration)
        storage.set("key2", Value("value2", expire=past_expiration))
        assert not storage.get("key2")

    def test_rpush(self, storage):
        storage.set("key1", Value("value1"))
        with pytest.raises(RuntimeError, match="Key key1 already exists and it's not a list"):
            storage.rpush("key1", Value("value2"))
        storage.rpush("key2", Value("value1"))
        assert storage.get("key2") == [Value("value1")]
        storage.rpush("key2", Value("value2"))
        assert storage.get("key2") == [Value("value1"), Value("value2")]