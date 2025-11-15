import argparse
import asyncio

from app.parser import parser
from app.processor import Processor
from app.storage.storage_facade import storage


async def handle_client(reader, writer):
    """Handle a single client connection."""

    try:
        processor = Processor(writer, storage)
        while True:
            data = await reader.read(1024)
            if not data:
                break
            cmd = parser.parse_command(data)
            await processor.process_command(cmd)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main(port: int):
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(handle_client, "localhost", port)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-p", "--port", help="port number", type=int, default=6379)
    args = arg_parser.parse_args()
    asyncio.run(main(args.port))
