from datetime import datetime, timedelta
from typing import Any

from app.formatter import formatter
from app.parser import Command
from app.storage import Storage, Value


async def process_command(command: tuple[Command, str, ...], writer: Any, storage: Storage) -> None:
    """Process a command and return the result into the writer."""
    match command:
        case Command.ECHO, *statement:
            # Command example: (Command.ECHO, "banana")
            writer.write(formatter.format_echo_expression(statement[0]))
        case Command.SET, *statement:
            # Command example: (Command.SET, "foo", "bar", "PX", 100)
            # TODO Add check that only optional either EX or PX are possible
            record_key = statement[0]
            record_value = statement[1]
            if len(statement) > 2:
                expiration = (
                    datetime.now() + timedelta(seconds=int(statement[3]))
                    if statement[2].upper() == "EX"
                    else datetime.now() + timedelta(milliseconds=int(statement[3]))
                )
            else:
                expiration = None
            storage.set(record_key, Value(record_value, expiration))
            writer.write(formatter.format_ok_expression())
        case Command.GET, *statement:
            # Command example: (Command.GET, "foo")
            value = storage.get(statement[0])
            writer.write(formatter.format_get_response(value))
        case Command.PING, *_:
            # Command example: (Command.PING,)
            writer.write(b"+PONG\r\n")
        case Command.RPUSH, *statement:
            # Command example: (Command.RPUSH, "key", "value1", "value2")
            record_key = statement[0]
            values = None
            for i in range(1, len(statement)):
                values = storage.rpush(record_key, Value(statement[i]))
            if not values:
                raise RuntimeError(f"No values for {record_key}")
            writer.write(formatter.format_rpush_response(values))
        case Command.LRANGE, *statement:
            # Command example: (Command.LRANGE, "list_key", "0", "1")
            record_key = statement[0]
            all_values = storage.get(record_key)
            if not all_values:
                writer.write(formatter.format_lrange_response(None))
            else:
                values = all_values[int(statement[1]): int(statement[2]) + 1]
                writer.write(formatter.format_lrange_response(values))
        case _:
            raise RuntimeError(f"Unknown command: {command}")
    await writer.drain()
