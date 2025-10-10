class Formatter:
    def format_echo_expression(self, expression: list[str]) -> bytes:
        response = f"${len(expression[1])}\r\n{expression[1]}\r\n"
        return response.encode("utf-8")


formatter = Formatter()
