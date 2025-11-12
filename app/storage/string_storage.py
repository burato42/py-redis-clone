import datetime
from typing import Optional

from app.storage.values import Value


class StringStorage:
    def __init__(self):
        self.data: dict[str, Value] = {}

    def get(self, key: str) -> Optional[Value]:
        if key in self.data:
            expire = self.data[key].expire
            if expire is not None and expire <= datetime.datetime.now():
                return None
        return self.data.get(key)

    def set(self, key: str, value: Value) -> None:
        self.data[key] = value

    def increment(self, key: str) -> Value:
        if key not in self.data:
            self.data[key] = Value("1")
            return self.data[key]
        current = self.data[key]
        self.data[key].item = str(int(current.item) + 1)
        return self.data[key]
