import asyncio


async def handle_client(reader, writer):
    """Handle a single client connection."""

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break

            message = data.decode()
            if "PING" in message:
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
