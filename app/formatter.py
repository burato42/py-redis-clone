class Formatter:

    def format_echo_expression(self, expression: list[str]) -> bytes:
        response = f"${len(expression[1])}\r\n{expression[1]}\r\n"
        return response.encode("utf-8")

    def format_ok_expression(self) -> bytes:
        return b"+OK\r\n"

    def format_get_response(self, value: str) -> bytes:
        if not value:
            return b"$-1\r\n"
        response = f"${len(value)}\r\n{value}\r\n"
        return response.encode("utf-8")


formatter = Formatter()
