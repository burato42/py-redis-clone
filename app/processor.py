import asyncio
import datetime  # use this way to keep tests working
from enum import Enum

from typing import Any

from app.formatter import formatter
from app.parser import Command
from app.storage import Storage, Value


class Push(Enum):
    RIGHT = 1
    LEFT = 2


class Processor:
    def __init__(self, writer: Any, storage: Storage):
        self.writer = writer
        self.storage = storage

    async def process_command(self, command: tuple[Command, str, ...]) -> None:
        """Process a command and return the result into the writer."""
        match command:
            case Command.ECHO, *statement:
                # Command example: (Command.ECHO, "banana")
                self._process_echo_command(statement)
            case Command.SET, *statement:
                # Command example: (Command.SET, "foo", "bar", "PX", 100)
                await self._process_set_command(statement)
            case Command.GET, *statement:
                # Command example: (Command.GET, "foo")
                self._process_get_command(statement)
            case Command.PING, *_:
                # Command example: (Command.PING,)
                self._process_ping_command()
            case Command.RPUSH, *statement:
                # Command example: (Command.RPUSH, "key", "value1", "value2")
                await self._process_push_command(Push.RIGHT, statement)
            case Command.LPUSH, *statement:
                # Command example: (Command.LPUSH, "key", "value1", "value2")
                await self._process_push_command(Push.LEFT, statement)
            case Command.LRANGE, *statement:
                # Command example: (Command.LRANGE, "list_key", "0", "1")
                self._process_range_command(statement)
            case Command.LLEN, *statement:
                # Command example: (Command.LLEN, "list_key")
                self._process_len_command(statement)
            case Command.BLPOP, *statement:
                # Command example: (Command.BLPOP, "mango", "0")
                await self._process_blpop_command(statement)
            case Command.LPOP, *statement:
                # Command example: (Command.LPOP, "mango")
                self._process_lpop_command(statement)
            case _:
                raise RuntimeError(f"Unknown command: {command}")
        await self.writer.drain()

    def _process_echo_command(self, args: list[str]) -> None:
        self.writer.write(formatter.format_echo_expression(args[0]))

    def _process_ping_command(self):
        self.writer.write(b"+PONG\r\n")

    async def _process_set_command(self, args: list[str]) -> None:
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

    def _process_get_command(self, args: list[str]) -> None:
        value = self.storage.get(args[0])
        self.writer.write(formatter.format_get_response(value))

    async def _process_push_command(self, push: Push, args: list[str]) -> None:
        record_key = args[0]
        values = None
        match push:
            case Push.RIGHT:
                values = await self.storage.rpush(record_key, [Value(val) for val in args[1:]])
            case Push.LEFT:
                values = await self.storage.lpush(record_key, [Value(val) for val in args[-1: 0: -1]])
        if not values:
            raise RuntimeError(f"No values for {record_key}")
        self.writer.write(formatter.format_len_response(values))

    def _process_range_command(self, args: list[str]) -> None:
        record_key = args[0]
        all_values = self.storage.get(record_key)
        if not all_values:
            self.writer.write(formatter.format_lrange_response(None))
        else:
            values = all_values[int(args[1]) : int(args[2]) + 1 or len(all_values)]
            self.writer.write(formatter.format_lrange_response(values))

    def _process_len_command(self, args: list[str]) -> None:
        record_key = args[0]
        all_values = self.storage.get(record_key)
        if not all_values or not isinstance(all_values, list):
            self.writer.write(formatter.format_len_response([]))
        else:
            self.writer.write(formatter.format_len_response(all_values))

    def _process_lpop_command(self, args: list[str]) -> None:
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

    async def _process_blpop_command(self, args: list[str]) -> None:
        record_key = args[0]
        if len(args) >= 2 and args[1] != "0":
            timeout = int(args[1])
        else:
            timeout = None

        try:
            all_values = await self.storage.get_blocking(record_key, timeout)
            if not all_values or not isinstance(all_values, list):
                self.writer.write(formatter.format_get_response(None))
            else:
                self.writer.write(formatter.format_lrange_response([Value(record_key)] + [all_values.pop(0)]))
        except asyncio.TimeoutError:
            self.writer.write(formatter.format_get_response(None))