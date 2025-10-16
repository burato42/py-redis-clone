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

    def set(self, key: str, value: Value) -> None:
        self.data[key] = value

    def rpush(self, key: str, value: Value) -> list[Value]:
        if key in self.data and isinstance(self.data[key], list):
            self.data[key].append(value)
        elif key not in self.data:
            self.data[key] = [value]
        else:
            raise RuntimeError(f"Key {key} already exists and it's not a list")
        return self.data[key]

    def lpush(self, key: str, value: Value) -> list[Value]:
        if key in self.data and isinstance(self.data[key], list):
            self.data[key].insert(0, value)
        elif key not in self.data:
            self.data[key] = [value]
        else:
            raise RuntimeError(f"Key {key} already exists and it's not a list")
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
