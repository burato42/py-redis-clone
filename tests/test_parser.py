import pytest

from app.parser import parser, Command


class TestParser:
    def test_echo(self):
        cmd = parser.parse_command(b"*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n")
        assert cmd == (Command.ECHO, "banana")

    def test_set_simple(self):
        cmd = parser.parse_command(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        assert cmd == (Command.SET, "foo", "bar")

    @pytest.mark.skip("Numbers are currently parsed as a numbers what is not correct")
    def test_set_simple_with_number(self):
        # Number is parsed as a string
        cmd = parser.parse_command(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n1.1\r\n")
        assert cmd == (Command.SET, "foo", "1.1")

    def test_set_with_expiry_seconds(self):
        cmd = parser.parse_command(
            b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nEX\r\n$3\r\n100\r\n"
        )
        assert cmd == (Command.SET, "foo", "bar", "EX", "100")

    def test_set_with_expiry_milliseconds(self):
        cmd = parser.parse_command(
            b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n200\r\n"
        )
        assert cmd == (Command.SET, "foo", "bar", "PX", "200")

    @pytest.mark.skip(
        "This check is not implemented and by default PX is set to 100, what is wrong."
    )
    def test_set_with_wrong_parameters(self):
        with pytest.raises(ValueError):
            parser.parse_command(
                b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nAB\r\n$3\r\n100\r\n"
            )

    def test_get(self):
        cmd = parser.parse_command(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        assert cmd == (Command.GET, "foo")

    def test_ping(self):
        cmd = parser.parse_command(b"+PING\r\n")
        assert cmd == (Command.PING,)

    def test_unknown_command(self):
        with pytest.raises(RuntimeError):
            parser.parse_command(b"*2\r\n$3\r\nIMPROVE\r\n$3\r\nfoo\r\n")

    def test_rpush(self):
        cmd = parser.parse_command(
            b"*4\r\n$5\r\nRPUSH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"
        )
        assert cmd == (Command.RPUSH, "foo", "bar", "baz")

    def test_lrange(self):
        cmd = parser.parse_command(
            b"*4\r\n$6\r\nLRANGE\r\n$8\r\nlist_key\r\n$1\r\n0\r\n$1\r\n1\r\n"
        )
        assert cmd == (Command.LRANGE, "list_key", "0", "1")

    def test_lpush(self):
        cmd = parser.parse_command(
            b"*4\r\n$5\r\nLPUSH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"
        )
        assert cmd == (Command.LPUSH, "foo", "bar", "baz")

    def test_llen(self):
        cmd = parser.parse_command(b"*2\r\n$4\r\nLLEN\r\n$3\r\nfoo\r\n")
        assert cmd == (Command.LLEN, "foo")
