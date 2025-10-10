class Parser:

    def parse(self, message: str) -> list[bytes]:
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
