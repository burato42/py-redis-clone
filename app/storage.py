from dataclasses import dataclass
from datetime import datetime
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
        if key in self.data and isinstance(list, self.data[key]):
            self.data[key].append(value)
        elif key not in self.data:
            self.data[key] = [value]
        else:
            raise RuntimeError(f"Key {key} already exists and it's not a list")
        return self.data[key]

    def get(self, key: str) -> Any:
        if (
            key in self.data
            and self.data[key].expire
            and self.data[key].expire <= datetime.now()
        ):
            return None
        return self.data.get(key)


storage = Storage()
