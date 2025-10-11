import asyncio
from typing import Any

from app.parser import parser
from app.formatter import formatter
from app.storage import storage


async def process_command(command: str, writer: Any) -> None:
    if "ECHO" in command:
        # Example: *2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n
        request = parser.parse(command)
        writer.write(formatter.format_echo_expression(request))
    elif "SET" in command:
        # Example: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        request = parser.parse(command)
        record_key = request[1]
        record_value = request[2]
        storage.set(record_key, record_value)
        writer.write(formatter.format_ok_expression())
    elif "GET" in command:
        # Example: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
        request = parser.parse(command)
        record_key = request[1]
        value = storage.get(record_key)
        writer.write(formatter.format_get_response(value))
    elif "PING" in command:
        writer.write(b"+PONG\r\n")
    await writer.drain()


async def handle_client(reader, writer):
    """Handle a single client connection."""

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            command = data.decode()
            await process_command(command, writer)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(
        handle_client,
        "localhost",
        6379
    )

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
