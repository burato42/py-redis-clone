from enum import Enum


class Command(Enum):
    ECHO = 1
    SET = 2
    GET = 3
    PING = 4
    RPUSH = 5
    LRANGE = 6
    LPUSH = 7
    LLEN = 8
    LPOP = 9
    BLPOP = 10
    TYPE = 11


class Parser:

    COMMAND_MAP = {
        "ECHO": Command.ECHO,
        "SET": Command.SET,
        "GET": Command.GET,
        "PING": Command.PING,
        "RPUSH": Command.RPUSH,
        "LRANGE": Command.LRANGE,
        "LPUSH": Command.LPUSH,
        "LLEN": Command.LLEN,
        "LPOP": Command.LPOP,
        "BLPOP": Command.BLPOP,
        "TYPE": Command.TYPE,
    }

    def parse_command(self, payload: bytes) -> tuple[Command, ...]:
        """Parse command from payload"""
        decoded = payload.decode()

        command_name = self._extract_command_name(decoded)
        command_enum = self.COMMAND_MAP.get(command_name)

        if command_enum is None:
            raise RuntimeError(
                f"Unknown command {decoded}".encode("unicode_escape").decode("utf-8")
            )

        if command_enum == Command.PING:
            return (command_enum,)

        args = self._parse(decoded)
        return command_enum, *args

    def _extract_command_name(self, message: str) -> str:
        """Extract the command name from the Redis protocol message"""
        elements = message.split("\r\n")

        if elements[0].startswith("+"):
            return elements[0][1:].upper()

        if not elements or not elements[0].startswith("*"):
            raise Exception(f"Invalid message format: {message}")

        # Skip array size and first bulk string size
        # The command name is at index 2
        if len(elements) > 2:
            return elements[2].upper()

        raise Exception(f"Cannot extract command from: {message}")

    @staticmethod
    def _parse(message: str) -> list[str]:
        """Parse Redis protocol message into arguments"""
        expression = []
        elements = message.split("\r\n")

        if not elements:
            raise Exception(f"Empty message: {message}")

        if elements[0].startswith("*"):
            size = int(elements[0][1:])
            elements.pop(0)
            for i in range(size):
                elements.pop(0)  # Skip size markers
                expression.append(elements.pop(0))

        # Drop the command name itself
        return expression[1:]


parser = Parser()
