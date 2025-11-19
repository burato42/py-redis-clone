import asyncio
from typing import Optional, Any


class Client:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def _send_command(self, *args) -> Any:
        async with self._lock:
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
        return await self._send_command("PING")

    async def replconf_port(self, port: str) -> Optional[str]:
        """REPLCONF command for listening on port number."""
        return await self._send_command("REPLCONF", "listening-port", port)

    async def replicaconf_capabilities(self, protocol: str) -> Optional[str]:
        """REPLCONF command for setting relication protocol."""
        return await self._send_command("REPLCONF", "capa", protocol)

    async def get(self, key: str) -> Optional[str]:
        """GET command."""
        return await self._send_command("GET", key)

    async def set(self, key: str, value: str) -> str:
        """SET command."""
        return await self._send_command("SET", key, value)

    async def delete(self, key: str) -> int:
        """DELETE command."""
        return await self._send_command("DEL", key)

    # Context manager support
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
