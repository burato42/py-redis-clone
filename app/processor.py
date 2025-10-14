from datetime import datetime, timedelta
from typing import Any

from app.formatter import formatter
from app.parser import Command
from app.storage import storage, Value


async def process_command(command: tuple[Command, str, ...], writer: Any) -> None:
    """Process a command and return the result into the writer."""
    match command:
        case Command.ECHO, *statement:
            # Command example: (Command.ECHO, "ECHO", "banana")
            writer.write(formatter.format_echo_expression(statement))
        case Command.SET, *statement:
            # Command example: (Command.SET, "SET", "foo", "bar", "PX", 100)
            # TODO Add check that only optional either EX or PX are possible
            request = statement[1:]
            record_key = request[0]
            record_value = request[1]
            if len(request) > 3:
                expiration = (
                    datetime.now() + timedelta(seconds=int(request[4]))
                    if request[2].upper() == "EX"
                    else datetime.now() + timedelta(milliseconds=int(request[4]))
                )
            else:
                expiration = None
            storage.set(record_key, Value(record_value, expiration))
            writer.write(formatter.format_ok_expression())

        case Command.GET, *statement:
            # Command example: (Command.GET, "GET", "foo")
            record_key = statement[1]
            value = storage.get(record_key)
            writer.write(formatter.format_get_response(value))
        case Command.PING, *statement:
            # Command example: (Command.PING, "PING")
            writer.write(b"+PONG\r\n")
        case Command.RPUSH, *statement:
            # Command example: (Command.RPUSH, "RPUSH", "key", "value")
            values = storage.rpush(statement[1], Value(statement[2]))
            writer.write(formatter.format_rpush_response(values))
        case _:
            raise RuntimeError(f"Unknown command: {command}")
    await writer.drain()
