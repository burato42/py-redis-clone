import asyncio
from asyncio import StreamReader, StreamWriter
from typing import Optional, Any

from app.parser import Command


class Client:
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self.reader = reader
        self.writer = writer
        self._lock = asyncio.Lock()

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def send_command(self, *args) -> Any:
        async with self._lock:
            print("This should be propageted", args)
            command = self._encode_command(*args)

            self.writer.write(command)
            await self.writer.drain()

            response = await self._read_response()
            return response

    def _encode_command(self, *args) -> bytes:
        parts = [f"${len(str(arg))}\r\n{arg}\r\n" for arg in args]
        command = f"*{len(args)}\r\n{''.join(parts)}"
        return command.encode()

    async def _read_response(self) -> Any:
        line = await self.reader.readline()
        return self._parse_response(line)

    def _parse_response(self, data: bytes) -> Any:
        return data.decode().strip()

    async def ping(self) -> Optional[str]:
        """PING command."""
        return await self.send_command(Command.PING.name)

    async def replconf_port(self, port: str) -> Optional[str]:
        """REPLCONF command for listening on port number."""
        return await self.send_command(Command.REPLCONF.name, "listening-port", port)

    async def replicaconf_capabilities(self, protocol: str) -> Optional[str]:
        """REPLCONF command for setting relication protocol."""
        return await self.send_command(Command.REPLCONF.name, "capa", protocol)

    async def psync(self, replication_id: str, offset: int) -> Optional[str]:
        """PSYNC command."""
        return await self.send_command(Command.PSYNC.name, replication_id, str(offset))

    async def get(self, key: str) -> Optional[str]:
        """GET command."""
        return await self.send_command(Command.GET.name, key)

    async def set(self, key: str, value: str) -> str:
        """SET command."""
        return await self.send_command(Command.SET.name, key, value)

    # # Context manager support
    # async def __aenter__(self):
    #     await self.connect()
    #     return self
    #
    # async def __aexit__(self, exc_type, exc_val, exc_tb):
    #     await self.close()


class ConnectionFactory:
    def __init__(self, host: str = "localhost", port: int = 6379, is_connected: bool = False):
        self.host = host
        self.port = port

    async def connect(self) -> Client:
        return Client(*await asyncio.open_connection(self.host, self.port))
