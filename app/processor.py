import asyncio
import datetime  # use this way to keep tests working
from collections import deque
from enum import Enum
from typing import Any, Callable

from app.formatter import formatter
from app.parser import Command
from app.storage.storage_facade import Storage, Value


class Push(Enum):
    RIGHT = 1
    LEFT = 2


class StreamParams(Enum):
    BLOCK = 1


CommandType = tuple[Command, *tuple[str]]


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
    def __init__(
        self,
        writer: Any,
        storage: Storage,
    ):
        self.writer = writer
        self.storage = storage
        self.is_queued = False
        self.command_queue: deque[CommandType] = deque()
        self.registry = CommandHandlerRegistry()
        self._register_handlers()

    def _register_handlers(self):
        """Register all command handlers"""

        @self.registry.register(Command.ECHO)
        async def handle_echo(args: list[str]) -> bytes:
            # Command example: (Command.ECHO, "banana")
            return formatter.format_string_expression(args[0])

        @self.registry.register(Command.SET)
        async def handle_set(args: list[str]) -> bytes:
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
            self.storage.set(record_key, Value(record_value, expiration))
            return formatter.format_ok_expression()

        @self.registry.register(Command.GET)
        async def handle_get(args: list[str]) -> bytes:
            # Command example: (Command.GET, "foo")
            value = self.storage.get(args[0])
            return formatter.format_get_response(value)

        @self.registry.register(Command.PING)
        async def handle_ping(_: list[str]) -> bytes:
            # Command example: (Command.PING,)
            return b"+PONG\r\n"

        @self.registry.register(Command.RPUSH)
        async def handle_rpush(args: list[str]) -> bytes:
            # Command example: (Command.RPUSH, "key", "value1", "value2")
            return await self._process_push_command(Push.RIGHT, args)

        @self.registry.register(Command.LPUSH)
        async def handle_lpush(args: list[str]) -> bytes:
            # Command example: (Command.LPUSH, "key", "value1", "value2")
            return await self._process_push_command(Push.LEFT, args)

        @self.registry.register(Command.LRANGE)
        async def handle_lrange(args: list[str]) -> bytes:
            # Command example: (Command.LRANGE, "list_key", "0", "1")
            record_key = args[0]
            all_values = self.storage.get(record_key)
            if not all_values:
                return formatter.format_lrange_response(None)

            values = all_values[int(args[1]) : int(args[2]) + 1 or len(all_values)]
            return formatter.format_lrange_response(values)

        @self.registry.register(Command.LLEN)
        async def handle_llen(args: list[str]) -> bytes:
            # Command example: (Command.LLEN, "list_key")
            record_key = args[0]
            all_values = self.storage.get(record_key)
            if not all_values or not isinstance(all_values, list):
                return formatter.format_len_response([])

            return formatter.format_len_response(all_values)

        @self.registry.register(Command.BLPOP)
        async def handle_blpop(args: list[str]) -> bytes:
            # Command example: (Command.BLPOP, "mango", "0")
            record_key = args[0]
            if len(args) >= 2 and args[1] != "0":
                timeout = float(args[1])
            else:
                timeout = None

            try:
                all_values = await self.storage.get_blocking(record_key, timeout)
                if not all_values or not isinstance(all_values, list):
                    return formatter.format_get_response(None)
                else:
                    key_and_value = [Value(record_key), all_values.pop(0)]
                    return formatter.format_lrange_response(key_and_value)
            except asyncio.TimeoutError:
                return formatter.format_null_array_response()

        @self.registry.register(Command.LPOP)
        async def handle_lpop(args: list[str]) -> bytes:
            # Command example: (Command.LPOP, "mango")
            record_key = args[0]
            all_values = self.storage.get(record_key)
            if not all_values or not isinstance(all_values, list):
                return formatter.format_get_response(None)
            elif len(args) == 2:
                queried = []
                for _ in range(int(args[1])):
                    if not all_values:
                        break
                    queried.append(all_values.pop(0))
                return formatter.format_lrange_response(queried)
            else:
                return formatter.format_get_response(all_values.pop(0))

        @self.registry.register(Command.TYPE)
        async def handle_type(args: list[str]) -> bytes:
            # Command example: (Command.TYPE, "foo")
            record_key = args[0]
            record_type = self.storage.get_type(record_key)
            return formatter.format_type_response(record_type)

        @self.registry.register(Command.XADD)
        async def handle_xadd(args: list[str]) -> bytes:
            # Command example: (Command.XADD,  "key1", "0-1", "foo", "bar", "baz", "qux")
            record_key = args[0]
            stream_key = args[1]
            obj = dict(id=stream_key)
            idx = 2
            while idx < len(args):
                obj[args[idx]] = args[idx + 1]
                idx += 2

            try:
                stream_id = await self.storage.set_stream(record_key, Value(obj))
                return formatter.format_string_expression(stream_id)
            except ValueError as err:
                return formatter.format_simple_error(err)

        @self.registry.register(Command.XRANGE)
        async def handle_xrange(args: list[str]) -> bytes:
            # Command example:(Command.XRANGE, "some_key", "1526985054069-0", "1526985054079")
            record_key = args[0]

            start, end = args[1], args[2]
            start_params = ProcessingUtils.prepare_start_params(start)

            if end == "+":
                end_params = float("inf"), float("inf")
            elif len(end_input := tuple([int(x) for x in end.split("-")])) == 1:
                end_params = end_input[0], float("inf")
            else:
                end_params = end_input[0], end_input[1]

            records = await self.storage.get_stream_range(
                record_key, start_params, end_params, is_inclusive=True
            )
            return formatter.format_xrange_response(records)

        @self.registry.register(Command.XREAD)
        async def handle_xread(args: list[str]) -> bytes:
            # Command example:(Command.XREAD, "STREAMS", "some_key", "1526985054069-0")
            record_list: list[
                tuple[str, list[Value]]
            ] = []  # list containing the stream key and list of values for every key

            blocking_period = None  # milliseconds, if None then non-blocking
            if args[0].upper() == StreamParams.BLOCK.name:
                blocking_period = int(args[1])
                parameters = args[3:]
            else:
                parameters = args[1:]

            parameter_size = len(parameters)

            if parameter_size % 2 != 0:
                raise RuntimeError("Incorrect number of parameters")

            key_parameters = parameters[: parameter_size // 2]
            id_parameters = parameters[parameter_size // 2 :]

            for record_key, start in zip(key_parameters, id_parameters):
                if (
                    start == "$"
                    and self.storage.stream_storage.data.get(record_key) is None
                ):
                    start_params = 0, 0
                elif start == "$" and self.storage.stream_storage.data.get(record_key):
                    start_params = ProcessingUtils.prepare_start_params(
                        self.storage.stream_storage.data.get(record_key)[-1].item["id"]
                    )
                else:
                    start_params = ProcessingUtils.prepare_start_params(start)

                end_params = float("inf"), float("inf")
                records = await self.storage.get_stream_range(
                    record_key,
                    start_params,
                    end_params,
                    is_inclusive=False,
                    blocking_period=blocking_period,
                )
                if records:
                    record_list.append((record_key, records))

            return formatter.format_xread_response(record_list)

        @self.registry.register(Command.INCR)
        async def handle_incr(args: list[str]) -> bytes:
            # Command example:(Command.INCR, "some_key")
            response = self.storage.increment(args[0])
            if response:
                return formatter.format_integer_response(response)
            return formatter.format_simple_error(
                Exception("value is not an integer or out of range")
            )

        @self.registry.register(Command.MULTI)
        async def handle_multi(_: list[str]) -> bytes:
            self.is_queued = True
            return formatter.format_ok_expression()

        @self.registry.register(Command.EXEC)
        async def handle_exec(_: list[str]) -> bytes:
            if not self.is_queued:
                return formatter.format_simple_error(Exception("EXEC without MULTI"))

            if not self.command_queue:
                self.is_queued = False
                return formatter.format_lrange_response(None)

            self.is_queued = False
            responses = []
            for command in self.command_queue:
                responses.append(await self._execute_command(command))
            self.command_queue.clear()
            return formatter.format_multiple_responses(responses)

    async def _execute_command(self, command: CommandType) -> bytes:
        """Execute a command and return the formatted result"""
        if not command:
            raise RuntimeError("Empty command")

        cmd_type = command[0]

        if self.is_queued and cmd_type != Command.EXEC:
            self.command_queue.append(command)
            response = formatter.format_queued_response()
        else:
            args = list(command[1:])

            handler = self.registry.get_handler(cmd_type)

            if handler is None:
                raise RuntimeError(f"Unknown command: {cmd_type}")

            response = await handler(args)
        return response

    async def process_command(self, command: CommandType) -> None:
        """Run the command execution and writes the result into writer"""
        response = await self._execute_command(command)
        self.writer.write(response)
        await self.writer.drain()

    async def _process_push_command(self, push: Push, args: list[str]) -> bytes:
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
        return formatter.format_len_response(values)


class ProcessingUtils:
    @staticmethod
    def prepare_start_params(start: str) -> tuple[int, int]:
        if start == "-":
            start_params = 0, 1
        elif len(start_input := tuple([int(x) for x in start.split("-")])) == 1:
            start_params = start_input[0], 0
        else:
            start_params = start_input[0], start_input[1]
        return start_params
