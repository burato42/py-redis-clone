import asyncio
from typing import Optional

from app.storage.values import Value


class ListStorage:
    def __init__(self):
        self.data: dict[str, list[Value]] = {}
        self.conditions: dict[str, asyncio.Condition] = {}

    def get(self, key: str) -> Optional[Value]:
        return self.data.get(key)

    async def get_blocking(self, key: str, timeout=None):
        if key in self.data:
            return self.data.get(key)

        if key not in self.conditions:
            self.conditions[key] = asyncio.Condition()

        async with self.conditions[key]:
            await asyncio.wait_for(
                self.conditions[key].wait_for(lambda: key in self.data), timeout
            )
            del self.conditions[key]  # Is this necessary?
            return self.data.get(key)

    async def rpush(self, key: str, values: list[Value]) -> list[Value]:
        if key in self.data and isinstance(self.data[key], list):
            self.data[key].extend(values)
        elif key not in self.data:
            self.data[key] = values

        if key in self.conditions:
            async with self.conditions[key]:
                self.conditions[key].notify_all()
        return self.data[key]

    async def lpush(self, key: str, values: list[Value]) -> list[Value]:
        if key in self.data and isinstance(self.data[key], list):
            self.data[key] = values + self.data[key]
        elif key not in self.data:
            self.data[key] = values

        if key in self.conditions:
            async with self.conditions[key]:
                self.conditions[key].notify_all()
        return self.data[key]
