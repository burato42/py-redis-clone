import asyncio
from dataclasses import dataclass
import datetime
from typing import Any, Optional


@dataclass
class Value:
    item: Any
    expire: Optional[datetime] = None


class Storage:
    def __init__(self):
        self.data: dict[Any, Any] = {}
        self.conditions: dict[Any, asyncio.Condition] = {}

    async def get_blocking(self, key, timeout=None):
        if key in self.data:
            return self.data.get(key)

        if key not in self.conditions:
            self.conditions[key] = asyncio.Condition()

        async with self.conditions[key]:
            await asyncio.wait_for(
                self.conditions[key].wait_for(lambda: key in self.data),
                timeout
            )
            del self.conditions[key] # Is this necessary?
            return self.data.get(key)

    async def set(self, key, value) -> None:
        self.data[key] = value

        if key in self.conditions:
            async with self.conditions[key]:
                self.conditions[key].notify_all()

    # def set(self, key: str, value: Value) -> None:
    #     self.data[key] = value

    async def rpush(self, key: str, values: list[Value]) -> list[Value]:
        if key in self.data and isinstance(self.data[key], list):
            self.data[key].extend(values)
        elif key not in self.data:
            self.data[key] = values
        else:
            raise RuntimeError(f"Key {key} already exists and it's not a list")
        if key in self.conditions:
            async with self.conditions[key]:
                self.conditions[key].notify_all()
        return self.data[key]

    async def lpush(self, key: str, values: list[Value]) -> list[Value]:
        if key in self.data and isinstance(self.data[key], list):
            self.data[key] = values + self.data[key]
        elif key not in self.data:
            self.data[key] = values
        else:
            raise RuntimeError(f"Key {key} already exists and it's not a list")
        if key in self.conditions:
            async with self.conditions[key]:
                self.conditions[key].notify_all()
        return self.data[key]

    def get(self, key: str) -> Any:
        if (
            key in self.data
            and not isinstance(
                self.data[key], list
            )  # Not sure that this approach is correct, will check in future
            and self.data[key].expire
            and self.data[key].expire <= datetime.datetime.now()
        ):
            return None
        return self.data.get(key)


storage = Storage()
