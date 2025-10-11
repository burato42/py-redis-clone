from datetime import datetime, timedelta
from typing import Any

from app.formatter import formatter
from app.parser import Command
from app.storage import storage, Value


async def process_command(command: tuple[Command, str, ...], writer: Any) -> None:
    """Process a command and return the result into the writer."""
    if command[0] == Command.ECHO:
        # Command example: (Command.ECHO, "ECHO", "banana")
        writer.write(formatter.format_echo_expression(command[1:]))
    elif command[0] == Command.SET:
        # Command example: (Command.SET, "SET", "foo", "bar", "PX", 100)
        # TODO Add check that only optional either EX or PX are possible
        request = command[1:]
        record_key = request[1]
        record_value = request[2]
        if len(request) > 3:
            expiration = datetime.now() + timedelta(seconds=int(request[4])) \
                if request[3].upper() == "EX" else datetime.now() + timedelta(milliseconds=int(request[4]))
        else:
            expiration = None
        storage.set(record_key, Value(record_value, expiration))
        writer.write(formatter.format_ok_expression())
    elif command[0] == Command.GET:
        # Command example: (Command.GET, "GET", "foo")
        record_key = command[2]
        value = storage.get(record_key)
        writer.write(formatter.format_get_response(value))
    elif command[0] == Command.PING:
        # Command example: (Command.PING, "PING")
        writer.write(b"+PONG\r\n")
    await writer.drain()
