from enum import Enum


class Command(Enum):
    ECHO = 1
    SET = 2
    GET = 3
    PING = 4


class Parser:

    def parse_command(self, payload: bytes) -> tuple[Command, ...]:
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
        else:
            raise RuntimeError("Unknown command")


    @staticmethod
    def _parse(message: str) -> list[str]:
        expression = []
        elements = message.split("\r\n")
        if not elements:
            raise Exception(f"Empty message: {message}")

        if elements[0].startswith("*"):
            size = int(elements[0][1:])
            elements.pop(0)
            for i in range(size):
                elements.pop(0) # this is the size of the message, doesn't important now
                expression.append(elements.pop(0))

        return expression


parser = Parser()
