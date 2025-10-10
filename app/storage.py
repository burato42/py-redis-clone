from typing import Any


class Storage:
    def __init__(self):
        self.data: dict[Any, Any] = {}

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def get(self, key: str) -> Any:
        return self.data.get(key)


storage = Storage()
