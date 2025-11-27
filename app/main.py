import argparse
import asyncio
from asyncio import StreamReader, StreamWriter

from app.parser import parser
from app.processor import Processor
from app.replicaclient import Client
from app.status import Status
from app.storage.storage_facade import storage, Storage
from app.logging import logger


class Server(Processor):
    def __init__(self, host: str, port: int, storage: Storage, status: Status):
        super().__init__(storage, status, connections=[])
        self.host = host
        self.port = port

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port,
        )
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader: StreamReader, writer: StreamWriter) -> None:
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                cmd = parser.parse_command(data)
                logger.debug(
                    "Command to master: {cmd}, parameters: {reader}, {writer}",
                    cmd=cmd,
                    reader=reader,
                    writer=writer)
                await self.process_command(cmd, reader, writer)
        except Exception as e:
            logger.warning(e)
        finally:
            logger.info(f"Closing connection to {self.host}:{self.port}")
            writer.close()
            await writer.wait_closed()


class Replica(Processor):
    def __init__(self, master_host: str, master_port: int, replica_port: int, storage: Storage, status: Status):
        super().__init__(storage, status)
        self.master_host = master_host
        self.master_port = master_port
        self.replica_port = replica_port
        self.master_reader = None
        self.master_writer = None

    async def start(self):
        await asyncio.gather(
            self.start_replica_server(),
            self.connect_to_master()
        )

    async def start_replica_server(self):
        server = await asyncio.start_server(
            self.handle_connection,
            "localhost",
            self.replica_port
        )
        async with server:
            await server.serve_forever()

    async def handle_connection(self, reader: StreamReader, writer: StreamWriter):
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                cmd = parser.parse_command(data)
                await self.process_command(cmd, reader, writer)
        except Exception as e:
            logger.warning(e)
        finally:
            logger.info("Closing connection to replica {self.replica_port}")
            writer.close()
            await writer.wait_closed()

    async def connect_to_master(self):
        self.master_reader, self.master_writer = await asyncio.open_connection(
            self.master_host,
            self.master_port
        )
        client = Client(self.master_reader, self.master_writer)
        response = await client.ping()

        if "PONG" in response:
            replica_port_response = await client.replconf_port(str(self.replica_port))
            if "OK" not in replica_port_response:
                raise ValueError(
                    f"Wrong configuration for primary {self.master_host, self.master_port}: {replica_port_response}"
                )
            replica_capa_response = await client.replicaconf_capabilities("psync2")
            if "OK" not in replica_capa_response:
                raise ValueError(
                    f"Wrong configuration for primary {self.master_host, self.master_port}: {replica_capa_response}"
                )

            # Send PSYNC and handle the response
            psync_response = await client.psync("?", -1)
            logger.debug("PSYNC response: {psync_response}", psync_response=psync_response)

            # Read and discard the RDB file that follows
            await self._read_rdb_file()

            # Now start the replication loop
            await self.replication_loop()

    async def _read_rdb_file(self):
        """Read and discard the RDB file sent after PSYNC"""
        rdb_header = await self.master_reader.readline()
        logger.debug("RDB header: {rdb_header}", rdb_header=rdb_header)

        if rdb_header.startswith(b'$'):
            file_size = int(rdb_header[1:].strip())
            logger.debug("RDB file size: {file_size}", file_size=file_size)

            rdb_content = await self.master_reader.readexactly(file_size)
            logger.debug("Read RDB file of {len(rdb_content)} bytes")

    async def replication_loop(self):
        logger.debug("Starting replication loop - will run forever...")

        while True:
            try:
                # Read commands from master
                data = await self.master_reader.read(1024)
                if not data:
                    logger.debug("No data received from master, breaking loop")
                    break

                logger.debug("Received from master: {data}", data=data)
                cmd = parser.parse_command(data)
                logger.debug("Parsed command: {cmd}", cmd=cmd)

                # Process the command (no need to send response back to master)
                await self.process_command(cmd, self.master_reader, self.master_writer)

            except asyncio.CancelledError:
                logger.warning("Replication loop cancelled")
                break
            except Exception as e:
                logger.warning("Error in replication loop: {e}", e=e)
                break


async def main(port: int, replicaof: str):
    logger.debug("Logs from your program will appear here!")

    if replicaof is None:
        status = Status("master", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0)
        await Server("localhost", port, storage, status).start()
    else:
        primary_host, primary_port = replicaof.split(" ")
        status = Status("slave", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0)
        await Replica(primary_host, int(primary_port), port, storage, status).start()


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-p", "--port", help="port number", type=int, default=6379)
    arg_parser.add_argument("--replicaof", type=str, help="instance to replicate")
    args = arg_parser.parse_args()
    asyncio.run(main(args.port, args.replicaof))