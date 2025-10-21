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


class CommandRegistry:
    """Registry for command parsers"""

    def __init__(self):
        self._parsers = {}

    def register(self, command_name: str, command_enum: Command):
        """Decorator to register a command parser"""

        def decorator(parser_func):
            self._parsers[command_name.upper()] = (command_enum, parser_func)
            return parser_func

        return decorator

    def get_parser(self, command_name: str):
        """Get parser for a command name"""
        return self._parsers.get(command_name.upper())

    def list_commands(self):
        """List all registered commands"""
        return list(self._parsers.keys())


class Parser:
    def __init__(self):
        self.registry = CommandRegistry()
        self._register_commands()

    def _register_commands(self):
        """Register all command parsers"""

        @self.registry.register("ECHO", Command.ECHO)
        def parse_echo(payload: str):
            # Example: *2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n
            return self._parse(payload)

        @self.registry.register("SET", Command.SET)
        def parse_set(payload: str):
            # Example: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
            return self._parse(payload)

        @self.registry.register("GET", Command.GET)
        def parse_get(payload: str):
            # Example: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
            return self._parse(payload)

        @self.registry.register("PING", Command.PING)
        def parse_ping(payload: str):
            return []

        @self.registry.register("RPUSH", Command.RPUSH)
        def parse_rpush(payload: str):
            # Example: *4\r\n$5\r\nRPUSH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n
            return self._parse(payload)

        @self.registry.register("LRANGE", Command.LRANGE)
        def parse_lrange(payload: str):
            # Example: *4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n0\r\n$1\r\n1\r\n
            return self._parse(payload)

        @self.registry.register("LPUSH", Command.LPUSH)
        def parse_lpush(payload: str):
            # Example: *4\r\n$5\r\nLPUSH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n
            return self._parse(payload)

        @self.registry.register("LLEN", Command.LLEN)
        def parse_llen(payload: str):
            # Example: *2\r\n$4\r\nLLEN\r\n$3\r\nfoo\r\n
            return self._parse(payload)

        @self.registry.register("BLPOP", Command.BLPOP)
        def parse_blpop(payload: str):
            # Example: *2\r\n$5\r\nBLPOP\r\n$3\r\nfoo\r\n
            return self._parse(payload)

        @self.registry.register("LPOP", Command.LPOP)
        def parse_lpop(payload: str):
            # Example: *2\r\n$4\r\nLPOP\r\n$3\r\nfoo\r\n
            return self._parse(payload)

    def parse_command(self, payload: bytes) -> tuple[Command, ...]:
        """Parse command from payload using registry"""
        decoded = payload.decode()

        # Extract command name from payload
        command_name = self._extract_command_name(decoded)

        # Look up parser in registry
        parser_info = self.registry.get_parser(command_name)

        if parser_info is None:
            raise RuntimeError(
                f"Unknown command {decoded}".encode("unicode_escape").decode("utf-8")
            )

        command_enum, parser_func = parser_info

        # Parse arguments using the registered parser
        args = parser_func(decoded)

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
