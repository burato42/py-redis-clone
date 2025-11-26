import asyncio
import datetime
from asyncio import StreamReader, StreamWriter
from collections import deque
from enum import Enum
from typing import Any, Callable, Optional

from app.formatter import formatter
from app.parser import Command
from app.replicaclient import Client
from app.status import Status
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
        self._writing = {}

    def register(self, command: Command, writing: bool = False):
        """Decorator to register a command handler"""

        def decorator(handler_func: Callable):
            self._handlers[command] = handler_func
            self._writing[command] = writing
            return handler_func

        return decorator

    def get_handler(self, command: Command):
        """Get handler for a command"""
        return self._handlers.get(command)

    def list_commands(self):
        """List all registered commands"""
        return list(self._handlers.keys())

    def is_writing(self, command: Command):
        """Check if a command is writing"""
        return self._writing.get(command, False)


class Processor:
    def __init__(
            self,
            storage: Storage,
            status: Status,
            connections: Optional[list[Client]] = None,
    ):
        self.storage = storage
        self.status = status
        self.is_queued = False
        self.command_queue: deque[CommandType] = deque()
        self.registry = CommandHandlerRegistry()
        self._register_handlers()
        self.client: Optional[Client] = None
        self.connections = connections if connections is not None else []

    def _register_handlers(self):
        """Register all command handlers"""

        @self.registry.register(Command.ECHO)
        async def handle_echo(args: list[str]) -> bytes:
            return formatter.format_bulk_string(args[0])

        @self.registry.register(Command.OK)
        async def handle_ok(args: list[str]) -> bytes:
            return b""

        @self.registry.register(Command.SET, True)
        async def handle_set(args: list[str]) -> bytes:
            print(args)
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
            value = self.storage.get(args[0])
            return formatter.format_get_response(value)

        @self.registry.register(Command.PING)
        async def handle_ping(_: list[str]) -> bytes:
            return b"+PONG\r\n"

        @self.registry.register(Command.PONG)
        async def handle_pong(_: list[str]) -> bytes:
            return b""

        @self.registry.register(Command.RPUSH)
        async def handle_rpush(args: list[str]) -> bytes:
            return await self._process_push_command(Push.RIGHT, args)

        @self.registry.register(Command.LPUSH)
        async def handle_lpush(args: list[str]) -> bytes:
            return await self._process_push_command(Push.LEFT, args)

        @self.registry.register(Command.LRANGE)
        async def handle_lrange(args: list[str]) -> bytes:
            record_key = args[0]
            all_values = self.storage.get(record_key)
            if not all_values:
                return formatter.format_lrange_response(None)

            values = all_values[int(args[1]): int(args[2]) + 1 or len(all_values)]
            return formatter.format_lrange_response(values)

        @self.registry.register(Command.LLEN)
        async def handle_llen(args: list[str]) -> bytes:
            record_key = args[0]
            all_values = self.storage.get(record_key)
            if not all_values or not isinstance(all_values, list):
                return formatter.format_len_response([])

            return formatter.format_len_response(all_values)

        @self.registry.register(Command.BLPOP)
        async def handle_blpop(args: list[str]) -> bytes:
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
            record_key = args[0]
            record_type = self.storage.get_type(record_key)
            return formatter.format_type_response(record_type)

        @self.registry.register(Command.XADD)
        async def handle_xadd(args: list[str]) -> bytes:
            record_key = args[0]
            stream_key = args[1]
            obj = dict(id=stream_key)
            idx = 2
            while idx < len(args):
                obj[args[idx]] = args[idx + 1]
                idx += 2

            try:
                stream_id = await self.storage.set_stream(record_key, Value(obj))
                return formatter.format_bulk_string(stream_id)
            except ValueError as err:
                return formatter.format_simple_error(err)

        @self.registry.register(Command.XRANGE)
        async def handle_xrange(args: list[str]) -> bytes:
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
            record_list: list[tuple[str, list[Value]]] = []

            blocking_period = None
            if args[0].upper() == StreamParams.BLOCK.name:
                blocking_period = int(args[1])
                parameters = args[3:]
            else:
                parameters = args[1:]

            parameter_size = len(parameters)

            if parameter_size % 2 != 0:
                raise RuntimeError("Incorrect number of parameters")

            key_parameters = parameters[: parameter_size // 2]
            id_parameters = parameters[parameter_size // 2:]

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
                responses.append(await self._execute_command(command, None, None))
            self.command_queue.clear()
            return formatter.format_multiple_responses(responses)

        @self.registry.register(Command.DISCARD)
        async def handle_discard(_: list[str]) -> bytes:
            if not self.is_queued:
                return formatter.format_simple_error(Exception("DISCARD without MULTI"))

            self.is_queued = False
            self.command_queue.clear()
            return formatter.format_ok_expression()

        @self.registry.register(Command.INFO)
        async def handle_info(_: list[str]) -> bytes:
            response_text = f"role:{self.status.role}"
            response_text += f"\r\nmaster_replid:{self.status.master_replid}"
            response_text += f"\r\nmaster_repl_offset:{self.status.master_repl_offset}"
            return formatter.format_bulk_string(response_text)

        @self.registry.register(Command.REPLCONF)
        async def handle_replconf(args: list[str]) -> bytes:
            if args[0].upper() == "LISTENING-PORT" and args[1].isdigit():
                print("listening_port", args[1])
            elif args[0].upper() == "CAPA" and args[1].upper() == Command.PSYNC2.name:
                print("capa", args[1])
            else:
                return formatter.format_simple_error("Unexpected command")
            return formatter.format_ok_expression()

    async def _execute_command(self, command: CommandType, reader, writer) -> bytes:
        """Execute a command and return the formatted result"""
        if not command:
            raise RuntimeError("Empty command")

        cmd_type = command[0]

        if cmd_type == Command.PSYNC:
            args = list(command[1:])
            if args[0].upper() == "?" and args[1] == "-1":
                psync_response = formatter.format_simple_string(
                    f"{Command.FULLRESYNC.name} {self.status.master_replid} 0"
                )
                with open("app/files/empty.rdb", "br") as f:
                    empty_rdb = f.read()
                    file_response = formatter.format_file_response(empty_rdb)

                # Add this replica connection to the list
                if reader and writer:
                    self.connections.append(Client(reader, writer))
                    print(f"Replica connected. Total replicas: {len(self.connections)}")

                return psync_response + file_response
            return formatter.format_simple_error("Unexpected command")

        if cmd_type == Command.DISCARD or not (
                self.is_queued and cmd_type != Command.EXEC
        ):
            args = list(command[1:])
            handler = self.registry.get_handler(cmd_type)
            if handler is None:
                return formatter.format_simple_error("Unknown command")

            # Execute the command
            response = await handler(args)

            # Propagate write commands to replicas (only if we are a master)
            if (self.registry.is_writing(cmd_type) and
                    self.connections and
                    len(self.connections) > 0 and
                    self.status.role == "master"):
                print(f"Propagating {cmd_type.name} to {len(self.connections)} replicas")
                # Create tasks for all replicas to avoid blocking
                tasks = []
                for connection in self.connections:
                    task = asyncio.create_task(
                        connection.send_command(cmd_type.name, *args)
                    )
                    tasks.append(task)

                # Wait for all propagations to complete (optional)
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            return response
        else:
            # For open transaction
            self.command_queue.append(command)
            return formatter.format_queued_response()

    async def process_command(self, command: CommandType, reader, writer) -> None:
        """Run the command execution and writes the result into writer"""
        response = await self._execute_command(command, reader, writer)
        writer.write(response)
        await writer.drain()

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