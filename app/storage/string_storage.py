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

    async def set(self, key: str, value: Value) -> None:
        self.data[key] = value
