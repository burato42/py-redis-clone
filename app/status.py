# Should it be a dataclass?
class Status:
    def __init__(self, role, master_replid: str = "", master_repl_offset: int = 0):
        self.role = role
        self.master_replid = master_replid
        self.master_repl_offset = master_repl_offset
