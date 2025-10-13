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
        # TODO Add tests for expiration
        assert not storage.get("key")