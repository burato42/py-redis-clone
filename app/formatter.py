from typing import Optional

from app.storage import Value, ValueType


class Formatter:
    def format_string_expression(self, argument: str) -> bytes:
        response = f"${len(argument)}\r\n{argument}\r\n"
        return response.encode("utf-8")

    def format_ok_expression(self) -> bytes:
        return b"+OK\r\n"

    def format_get_response(self, value: Optional[Value]) -> bytes:
        if not value:
            return b"$-1\r\n"
        response = f"${len(value.item)}\r\n{value.item}\r\n"
        return response.encode("utf-8")

    def format_len_response(self, values: list[Value]) -> bytes:
        return f":{len(values)}\r\n".encode("utf-8")

    def format_lrange_response(self, values: Optional[list[Value]]) -> bytes:
        if not values:
            return b"*0\r\n"
        return (
            f"*{len(values)}\r\n"
            + "\r\n".join([f"${len(str(v.item))}\r\n{str(v.item)}" for v in values])
            + "\r\n"
        ).encode("utf-8")

    def format_null_array_response(self) -> bytes:
        return b"*-1\r\n"

    def format_type_response(self, record_type: ValueType) -> bytes:
        return f"+{record_type.name.lower()}\r\n".encode("utf-8")

    def format_simple_error(self, error: Exception) -> bytes:
        return f"-ERR {str(error)}\r\n".encode("utf-8")

    def format_xrange_response(self, values: Optional[list[Value]]) -> bytes:
        if not values:
            return b"*0\r\n"
        items = ""
        for value in values:
            fields = ""
            for k, v in value.item.items():
                if k == "id":
                    continue
                fields += f"${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n"

            items += f"*2\r\n${len(value.item['id'])}\r\n{value.item['id']}\r\n*{(len(value.item) - 1) * 2}\r\n{fields}"

        return f"*{len(values)}\r\n{items}".encode("utf-8")

    def format_xread_response(
        self, record_list: list[tuple[str, list[Value]]]
    ) -> bytes:
        # TODO The functionality of format_xrange_response and format_xread_response almost identical
        # Will be fixed as there might be more requirements for the format_xread_response
        if not record_list:
            return b"*0\r\n"
        streams = ""
        for record_key, values in record_list:
            items = ""
            for value in values:
                fields = ""
                for k, v in value.item.items():
                    if k == "id":
                        continue
                    fields += f"${len(k)}\r\n{k}\r\n${len(v)}\r\n{v}\r\n"

                items += f"*2\r\n${len(value.item['id'])}\r\n{value.item['id']}\r\n*{(len(value.item) - 1) * 2}\r\n{fields}"

            streams += (
                f"*2\r\n${len(record_key)}\r\n{record_key}\r\n*{len(values)}\r\n{items}"
            )

        return f"*{len(record_list)}\r\n{streams}".encode("utf-8")


formatter = Formatter()
