import asyncio
from app.parser import parser
from app.formatter import formatter
from app.storage import storage

async def handle_client(reader, writer):
    """Handle a single client connection."""

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break

            message = data.decode()
            if "ECHO" in message:
                request = parser.parse(message)
                writer.write(formatter.format_echo_expression(request))
                await writer.drain()
            elif "SET" in message:
                request = parser.parse(message)
                storage.set(request[1], request[2])
                writer.write(formatter.format_ok_expression())
                await writer.drain()
            elif "GET" in message:
                request = parser.parse(message)
                value = storage.get(request[1])
                writer.write(formatter.format_get_response(value))
                await writer.drain()
            elif "PING" in message:
                writer.write(b"+PONG\r\n")
                await writer.drain()
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
