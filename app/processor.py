import asyncio
import datetime  # use this way to keep tests working
from enum import Enum
from typing import Any, Callable

from app.formatter import formatter
from app.parser import Command
from app.storage import Storage, Value


class Push(Enum):
    RIGHT = 1
    LEFT = 2


class CommandHandlerRegistry:
    """Registry for command handlers"""

    def __init__(self):
        self._handlers = {}

    def register(self, command: Command):
        """Decorator to register a command handler"""

        def decorator(handler_func: Callable):
            self._handlers[command] = handler_func
            return handler_func

        return decorator

    def get_handler(self, command: Command):
        """Get handler for a command"""
        return self._handlers.get(command)

    def list_commands(self):
        """List all registered commands"""
        return list(self._handlers.keys())


class Processor:
    def __init__(self, writer: Any, storage: Storage):
        self.writer = writer
        self.storage = storage
        self.registry = CommandHandlerRegistry()
        self._register_handlers()

    def _register_handlers(self):
        """Register all command handlers"""

        @self.registry.register(Command.ECHO)
        async def handle_echo(args: list[str]) -> None:
            # Command example: (Command.ECHO, "banana")
            self.writer.write(formatter.format_string_expression(args[0]))

        @self.registry.register(Command.SET)
        async def handle_set(args: list[str]) -> None:
            # Command example: (Command.SET, "foo", "bar", "PX", 100)
            # TODO Add check that only optional either EX or PX are possible
            record_key = args[0]
            record_value = args[1]
            if len(args) > 2:
                expiration = (
                    datetime.datetime.now() + datetime.timedelta(seconds=int(args[3]))
                    if args[2].upper() == "EX"
                    else datetime.datetime.now()
                    + datetime.timedelta(milliseconds=int(args[3]))
                )
            else:
                expiration = None
            await self.storage.set(record_key, Value(record_value, expiration))
            self.writer.write(formatter.format_ok_expression())

        @self.registry.register(Command.GET)
        async def handle_get(args: list[str]) -> None:
            # Command example: (Command.GET, "foo")
            value = self.storage.get(args[0])
            self.writer.write(formatter.format_get_response(value))

        @self.registry.register(Command.PING)
        async def handle_ping(_: list[str]) -> None:
            # Command example: (Command.PING,)
            self.writer.write(b"+PONG\r\n")

        @self.registry.register(Command.RPUSH)
        async def handle_rpush(args: list[str]) -> None:
            # Command example: (Command.RPUSH, "key", "value1", "value2")
            await self._process_push_command(Push.RIGHT, args)

        @self.registry.register(Command.LPUSH)
        async def handle_lpush(args: list[str]) -> None:
            # Command example: (Command.LPUSH, "key", "value1", "value2")
            await self._process_push_command(Push.LEFT, args)

        @self.registry.register(Command.LRANGE)
        async def handle_lrange(args: list[str]) -> None:
            # Command example: (Command.LRANGE, "list_key", "0", "1")
            record_key = args[0]
            all_values = self.storage.get(record_key)
            if not all_values:
                self.writer.write(formatter.format_lrange_response(None))
            else:
                values = all_values[int(args[1]) : int(args[2]) + 1 or len(all_values)]
                self.writer.write(formatter.format_lrange_response(values))

        @self.registry.register(Command.LLEN)
        async def handle_llen(args: list[str]) -> None:
            # Command example: (Command.LLEN, "list_key")
            record_key = args[0]
            all_values = self.storage.get(record_key)
            if not all_values or not isinstance(all_values, list):
                self.writer.write(formatter.format_len_response([]))
            else:
                self.writer.write(formatter.format_len_response(all_values))

        @self.registry.register(Command.BLPOP)
        async def handle_blpop(args: list[str]) -> None:
            # Command example: (Command.BLPOP, "mango", "0")
            record_key = args[0]
            if len(args) >= 2 and args[1] != "0":
                timeout = float(args[1])
            else:
                timeout = None

            try:
                all_values = await self.storage.get_blocking(record_key, timeout)
                if not all_values or not isinstance(all_values, list):
                    self.writer.write(formatter.format_get_response(None))
                else:
                    key_and_value = [Value(record_key), all_values.pop(0)]
                    self.writer.write(formatter.format_lrange_response(key_and_value))
            except asyncio.TimeoutError:
                self.writer.write(formatter.format_null_array_response())

        @self.registry.register(Command.LPOP)
        async def handle_lpop(args: list[str]) -> None:
            # Command example: (Command.LPOP, "mango")
            record_key = args[0]
            all_values = self.storage.get(record_key)
            if not all_values or not isinstance(all_values, list):
                self.writer.write(formatter.format_get_response(None))
            elif len(args) == 2:
                queried = []
                for _ in range(int(args[1])):
                    if not all_values:
                        break
                    queried.append(all_values.pop(0))
                self.writer.write(formatter.format_lrange_response(queried))
            else:
                self.writer.write(formatter.format_get_response(all_values.pop(0)))

        @self.registry.register(Command.TYPE)
        async def handle_type(args: list[str]) -> None:
            # Command example: (Command.TYPE, "foo")
            record_key = args[0]
            record_type = self.storage.get_type(record_key)
            self.writer.write(formatter.format_type_response(record_type))

        @self.registry.register(Command.XADD)
        async def handle_xadd(args: list[str]) -> None:
            # Command example: (Command.XADD,  "key1", "0-1", "foo", "bar", "baz", "qux")
            record_key = args[0]
            stream_key = args[1]
            obj = dict(id=stream_key)
            idx = 2
            while idx < len(args):
                obj[args[idx]] = args[idx + 1]
                idx += 2

            try:
                stream_id = self.storage.set_stream(record_key, Value(obj))
                self.writer.write(formatter.format_string_expression(stream_id))
            except ValueError as err:
                self.writer.write(formatter.format_simple_error(err))

        @self.registry.register(Command.XRANGE)
        async def handle_xrange(args: list[str]) -> None:
            # Command example:(Command.XRANGE, "some_key", "1526985054069-0", "1526985054079")
            record_key = args[0]

            start, end = args[1], args[2]
            if start == "-":
                start_params = 0, 1
            elif len(start_params := tuple([int(x) for x in start.split("-")])) == 1:
                start_params = start_params[0], 0

            if end == "+":
                end_params = float("inf"), float("inf")
            elif len(end_params := tuple([int(x) for x in end.split("-")])) == 1:
                end_params = end_params[0], float("inf")

            records = self.storage.get_stream_range(
                record_key, start_params, end_params
            )
            self.writer.write(formatter.format_xrange_response(records))

    async def process_command(self, command: tuple[Command, *tuple[str]]) -> None:
        """Process a command and return the result into the writer."""
        if not command:
            raise RuntimeError("Empty command")

        cmd_type = command[0]
        args = list(command[1:])

        handler = self.registry.get_handler(cmd_type)

        if handler is None:
            raise RuntimeError(f"Unknown command: {cmd_type}")

        await handler(args)
        await self.writer.drain()

    async def _process_push_command(self, push: Push, args: list[str]) -> None:
        record_key = args[0]
        values = None
        match push:
            case Push.RIGHT:
                values = await self.storage.rpush(
                    record_key, [Value(val) for val in args[1:]]
                )
            case Push.LEFT:
                values = await self.storage.lpush(
                    record_key, [Value(val) for val in args[-1:0:-1]]
                )
        if not values:
            raise RuntimeError(f"No values for {record_key}")
        self.writer.write(formatter.format_len_response(values))
