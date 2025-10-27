import asyncio
from collections import deque
from dataclasses import dataclass
import datetime
from enum import Enum
from typing import Any, Optional


class ValueType(Enum):
    NONE = 0
    STRING = 1
    LIST = 2
    SET = 3
    ZSET = 4
    HASH = 5
    STREAM = 6
    VECTORSET = 7


@dataclass
class Value:
    item: Any
    expire: Optional[datetime.datetime] = None


class Storage:
    def __init__(self):
        self.data: dict[Any, Any] = {}
        self.conditions: dict[Any, asyncio.Condition] = {}

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

    async def set(self, key: str, value: Value) -> None:
        self.data[key] = value

        if key in self.conditions:
            async with self.conditions[key]:
                self.conditions[key].notify_all()

    def _autogenerate_and_set_stream_id(self, key: str, value: Value) -> Optional[str]:
        rec_id = value.item["id"]
        if rec_id == "*":
            auto_timestmp = datetime.datetime.now(datetime.UTC).microsecond // 1000
            auto_version = 1
            value.item["id"] = f"{auto_timestmp}-{auto_version}"
            time_versioned = auto_timestmp, auto_version
        else:
            time_versioned = rec_id.split("-")
            if time_versioned[1] == "*" and key not in self.data:
                value.item["id"] = f"{time_versioned[0]}-1"

                self.data[key] = deque([value])
                return value.item["id"]
            elif time_versioned[1] == "*" and key in self.data:
                cur_timestmp = int(time_versioned[0])
                last_timestmp, last_version = [
                    int(x) for x in self.data[key][-1].item["id"].split("-")
                ]
                if cur_timestmp == last_timestmp:
                    value.item["id"] = f"{time_versioned[0]}-{last_version + 1}"
                    self.data[key].append(value)
                    return value.item["id"]
                elif cur_timestmp < last_timestmp:
                    raise ValueError(
                        "The ID specified in XADD is equal or smaller than the target stream top item"
                    )
                else:
                    value.item["id"] = f"{cur_timestmp}-0"
                    self.data[key].append(value)
                    return value.item["id"]
            return None

    def _set_stream_id(self, key: str, value: Value) -> str:
        time_versioned = value.item["id"].split("-")
        timestmp, version = [int(x) for x in time_versioned]
        if timestmp < 0 or version < 1:
            raise ValueError("The ID specified in XADD must be greater than 0-0")

        if key not in self.data:
            self.data[key] = deque([value])
            return value.item["id"]

        last_timestmp, last_version = [
            int(x) for x in self.data[key][-1].item["id"].split("-")
        ]
        if timestmp < last_timestmp or (
            timestmp == last_timestmp and version <= last_version
        ):
            raise ValueError(
                "The ID specified in XADD is equal or smaller than the target stream top item"
            )
        self.data[key].append(value)
        return value.item["id"]

    def set_stream(self, key: str, value: Value) -> str:
        rec_id = value.item["id"]
        # When the format "*", "0-*" or "3-1" is violated we throw and exception
        if rec_id != "*" and len(rec_id.split("-")) != 2:
            raise ValueError("Invalid stream id")

        stream_id = self._autogenerate_and_set_stream_id(key, value)
        if stream_id is not None:
            return stream_id

        return self._set_stream_id(key, value)

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

    def get_type(self, key: str) -> ValueType:
        if key not in self.data:
            return ValueType.NONE
        match self.data[key]:
            case Value():
                return ValueType.STRING
            case list():
                return ValueType.LIST
            case set():
                return ValueType.SET
            case deque():
                return ValueType.STREAM
            case _:
                return ValueType.NONE


storage = Storage()
