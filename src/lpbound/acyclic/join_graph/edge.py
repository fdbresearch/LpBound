class Edge:
    def __init__(self, alias_left: str, alias_right: str, join_var_left: str, join_var_right: str):
        """
        join_var_left and join_var_right are the variables that are joined on.
        """
        self.alias_left: str = alias_left.upper()
        self.alias_right: str = alias_right.upper()
        self.join_var_left: str = join_var_left.upper()
        self.join_var_right: str = join_var_right.upper()

    def __repr__(self) -> str:
        return (
            f"Edge({self.alias_left} <-> {self.alias_right}, "
            f"join_var_left={self.join_var_left}, join_var_right={self.join_var_right})"
        )
