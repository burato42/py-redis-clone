from typing import Optional, Union

from app.storage.list_storage import ListStorage
from app.storage.stream_storage import StreamStorage
from app.storage.string_storage import StringStorage
from app.storage.values import ValueType, Value


# TODO Dedicate a separate component to work with blocking operations
class Storage:
    def __init__(self):
        self.string_storage = StringStorage()
        self.list_storage = ListStorage()
        self.stream_storage = StreamStorage()

    def get_type(self, key: str) -> ValueType:
        if key in self.string_storage.data:
            return ValueType.STRING
        elif key in self.list_storage.data:
            return ValueType.LIST
        elif key in self.stream_storage.data:
            return ValueType.STREAM

        return ValueType.NONE

    def get(self, key: str) -> Optional[Value] | Optional[list[Value]]:
        data_type = self.get_type(key)
        if data_type == ValueType.STRING:
            return self.string_storage.get(key)
        elif data_type == ValueType.LIST:
            return self.list_storage.get(key)
        return None

    async def set(self, key: str, value: Value) -> None:
        if (data_type := self.get_type(key)) not in (ValueType.NONE, ValueType.STRING):
            raise KeyError(
                f"Key {key} is already used for another data type: {data_type}"
            )
        await self.string_storage.set(key, value)

    async def get_blocking(
        self, key: str, timeout: Optional[float | int] = None
    ) -> Optional[Value]:
        if self.get_type(key) not in (ValueType.NONE, ValueType.LIST):
            raise KeyError("Blocking get operations is available for LIST only")
        return await self.list_storage.get_blocking(key, timeout)

    async def set_stream(self, key: str, value: Value) -> str:
        if (data_type := self.get_type(key)) not in (ValueType.NONE, ValueType.STREAM):
            raise KeyError(
                f"Key {key} is already used for another data type: {data_type}"
            )
        return await self.stream_storage.set_stream(key, value)

    async def rpush(self, key: str, values: list[Value]) -> list[Value]:
        if (data_type := self.get_type(key)) not in (ValueType.NONE, ValueType.LIST):
            raise KeyError(
                f"Key {key} is already used for another data type: {data_type}"
            )
        return await self.list_storage.rpush(key, values)

    async def lpush(self, key: str, values: list[Value]) -> list[Value]:
        if (data_type := self.get_type(key)) not in (ValueType.NONE, ValueType.LIST):
            raise KeyError(
                f"Key {key} is already used for another data type: {data_type}"
            )
        return await self.list_storage.lpush(key, values)

    async def get_stream_range(
        self,
        key: str,
        start: tuple[int, int],
        end: tuple[int | float, int | float],
        is_inclusive: bool = True,
        blocking_period: Optional[int] = None,
    ):
        if (data_type := self.get_type(key)) not in (ValueType.NONE, ValueType.STREAM):
            raise KeyError(
                f"Key {key} is already used for another data type: {data_type}"
            )
        return await self.stream_storage.get_stream_range(
            key, start, end, is_inclusive, blocking_period
        )


storage = Storage()
