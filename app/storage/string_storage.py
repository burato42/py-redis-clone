import datetime
from typing import Optional

from app.storage.values import Value


class StringStorage:
    def __init__(self):
        self.data: dict[str, Value] = {}

    def get(self, key: str) -> Optional[Value]:
        if (
            key in self.data
            and self.data[key].expire
            and self.data[key].expire <= datetime.datetime.now()
        ):
            return None
        return self.data.get(key)

    async def set(self, key: str, value: Value) -> None:
        self.data[key] = value
