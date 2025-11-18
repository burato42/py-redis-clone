import argparse
import asyncio

from app.parser import parser
from app.processor import Processor
from app.status import Status
from app.storage.storage_facade import storage


async def main(port: int, replicaof: str):
    print("Logs from your program will appear here!")

    if replicaof is None:
        status = Status("master")
    else:
        host, replica_port = replicaof.split(" ")
        status = Status("slave")

    async def handle_client(reader, writer):
        """Handle a single client connection."""
        nonlocal status

        try:
            processor = Processor(writer, storage, status)
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

    server = await asyncio.start_server(handle_client, "localhost", port)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-p", "--port", help="port number", type=int, default=6379)
    arg_parser.add_argument("--replicaof", type=str, help="instance to replicate")
    args = arg_parser.parse_args()
    asyncio.run(main(args.port, args.replicaof))
