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


class Parser:
    def parse_command(self, payload: bytes) -> tuple[Command, ...]:
        # TODO Use registry pattern here
        if b"ECHO" in payload.upper():
            # Example: *2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n
            return Command.ECHO, *self._parse(payload.decode())
        elif b"SET" in payload.upper():
            # Example: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
            return Command.SET, *self._parse(payload.decode())
        elif b"GET" in payload.upper():
            # Example: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
            return Command.GET, *self._parse(payload.decode())
        elif b"PING" in payload.upper():
            return (Command.PING,)
        elif b"RPUSH" in payload.upper():
            # Example: *4\r\n$5\r\nRPUSH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n
            return Command.RPUSH, *self._parse(payload.decode())
        elif b"LRANGE" in payload.upper():
            # Example: *4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n0\r\n$1\r\n1\r\n
            return Command.LRANGE, *self._parse(payload.decode())
        elif b"LPUSH" in payload.upper():
            # Example: *4\r\n$5\r\nLPUSH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n
            return Command.LPUSH, *self._parse(payload.decode())
        elif b"LLEN" in payload.upper():
            # Example: *2\r\n$4\r\nLLEN\r\n$3\r\nfoo\r\n
            return Command.LLEN, *self._parse(payload.decode())
        elif b"BLPOP" in payload.upper():
            # Example: *2\r\n$5\r\nBLPOP\r\n$3\r\nfoo\r\n
            return Command.BLPOP, *self._parse(payload.decode())
        elif b"LPOP" in payload.upper():
            # Example: *2\r\n$4\r\nLPOP\r\n$3\r\nfoo\r\n
            return Command.LPOP, *self._parse(payload.decode())
        else:
            raise RuntimeError(
                f"Unknown command {payload.decode()}".encode("unicode_escape").decode(
                    "utf-8"
                )
            )

    @staticmethod
    def _parse(message: str) -> list[str]:
        # Very basic parser, it could be improved later
        expression = []
        elements = message.split("\r\n")
        if not elements:
            raise Exception(f"Empty message: {message}")

        if elements[0].startswith("*"):
            size = int(elements[0][1:])
            elements.pop(0)
            for i in range(size):
                elements.pop(
                    0
                )  # this is the size of the message, it's not important now
                expression.append(elements.pop(0))

        # Drop the command name itself
        return expression[1:]


parser = Parser()
