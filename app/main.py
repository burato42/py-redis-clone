import asyncio
from datetime import datetime, timedelta
from typing import Any

from app.parser import parser
from app.formatter import formatter
from app.storage import storage, Value



async def process_command(command: str, writer: Any) -> None:
    """Process a command and return the result into the writer."""
    if "ECHO" in command.upper():
        # Example: *2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n
        request = parser.parse(command)
        writer.write(formatter.format_echo_expression(request))
    elif "SET" in command.upper():
        # Example: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        request = parser.parse(command)
        record_key = request[1]
        record_value = request[2]
        if len(request) > 3:
            expiration = datetime.now() + timedelta(seconds=int(request[4])) \
                if request[3].upper() == "EX" else datetime.now() + timedelta(milliseconds=int(request[4]))
        else:
            expiration = None
        storage.set(record_key, Value(record_value, expiration))
        writer.write(formatter.format_ok_expression())
    elif "GET" in command.upper():
        # Example: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
        request = parser.parse(command)
        record_key = request[1]
        value = storage.get(record_key)
        writer.write(formatter.format_get_response(value))
    elif "PING" in command.upper():
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
