import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any


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
