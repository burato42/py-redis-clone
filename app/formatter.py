from app.storage import Value


class Formatter:
    def format_echo_expression(self, argument: str) -> bytes:
        response = f"${len(argument)}\r\n{argument}\r\n"
        return response.encode("utf-8")

    def format_ok_expression(self) -> bytes:
        return b"+OK\r\n"

    def format_get_response(self, value: Value) -> bytes:
        if not value:
            return b"$-1\r\n"
        response = f"${len(value.item)}\r\n{value.item}\r\n"
        return response.encode("utf-8")

    def format_len_response(self, values: list[Value]) -> bytes:
        return f":{len(values)}\r\n".encode("utf-8")

    def format_lrange_response(self, values: list[Value]) -> bytes:
        if not values:
            return b"*0\r\n"
        return (
            f"*{len(values)}\r\n"
            + "\r\n".join([f"${len(str(v.item))}\r\n{str(v.item)}" for v in values])
            + "\r\n"
        ).encode("utf-8")


formatter = Formatter()
