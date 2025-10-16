import datetime  # use this way to keep tests working

from typing import Any

from app.formatter import formatter
from app.parser import Command
from app.storage import Storage, Value


class Processor:
    def __init__(self, writer: Any, storage: Storage):
        self.writer = writer
        self.storage = storage

    async def process_command(self, command: tuple[Command, str, ...]) -> None:
        """Process a command and return the result into the writer."""
        match command:
            case Command.ECHO, *statement:
                # Command example: (Command.ECHO, "banana")
                self.writer.write(formatter.format_echo_expression(statement[0]))
            case Command.SET, *statement:
                # Command example: (Command.SET, "foo", "bar", "PX", 100)
                # TODO Add check that only optional either EX or PX are possible
                record_key = statement[0]
                record_value = statement[1]
                if len(statement) > 2:
                    expiration = (
                        datetime.datetime.now()
                        + datetime.timedelta(seconds=int(statement[3]))
                        if statement[2].upper() == "EX"
                        else datetime.datetime.now()
                        + datetime.timedelta(milliseconds=int(statement[3]))
                    )
                else:
                    expiration = None
                self.storage.set(record_key, Value(record_value, expiration))
                self.writer.write(formatter.format_ok_expression())
            case Command.GET, *statement:
                # Command example: (Command.GET, "foo")
                value = self.storage.get(statement[0])
                self.writer.write(formatter.format_get_response(value))
            case Command.PING, *_:
                # Command example: (Command.PING,)
                self.writer.write(b"+PONG\r\n")
            case Command.RPUSH, *statement:
                # Command example: (Command.RPUSH, "key", "value1", "value2")
                record_key = statement[0]
                values = None
                for i in range(1, len(statement)):
                    values = self.storage.rpush(record_key, Value(statement[i]))
                if not values:
                    raise RuntimeError(f"No values for {record_key}")
                self.writer.write(formatter.format_len_response(values))
            case Command.LPUSH, *statement:
                # Command example: (Command.LPUSH, "key", "value1", "value2")
                record_key = statement[0]
                values = None
                for i in range(1, len(statement)):
                    values = self.storage.lpush(record_key, Value(statement[i]))
                if not values:
                    raise RuntimeError(f"No values for {record_key}")
                self.writer.write(formatter.format_len_response(values))
            case Command.LRANGE, *statement:
                # Command example: (Command.LRANGE, "list_key", "0", "1")
                record_key = statement[0]
                all_values = self.storage.get(record_key)
                if not all_values:
                    self.writer.write(formatter.format_lrange_response(None))
                else:
                    values = all_values[
                        int(statement[1]) : int(statement[2]) + 1 or len(all_values)
                    ]
                    self.writer.write(formatter.format_lrange_response(values))
            case Command.LLEN, *statement:
                # Command example: (Command.LLEN, "list_key")
                record_key = statement[0]
                all_values = self.storage.get(record_key)
                if not all_values or not isinstance(all_values, list):
                    self.writer.write(formatter.format_len_response([]))
                else:
                    self.writer.write(formatter.format_len_response(all_values))
            case _:
                raise RuntimeError(f"Unknown command: {command}")
        await self.writer.drain()
