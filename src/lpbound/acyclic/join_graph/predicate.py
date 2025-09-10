class EqualityPredicate:
    """
    Represents an equality predicate on a single column,
    e.g., col_id = value.
    """

    def __init__(self, col_id: str, value: int | float | str, value_type: str):
        self.col_id: str = col_id.upper()
        self.value: int | float | str = value
        self.value_type: str = value_type  # e.g. "int", "string", "date"
        self.mcv_id: int | None = None

    def set_mcv_id(self, mcv_id: int):
        self.mcv_id = mcv_id

    def __repr__(self):
        return f"EqualityPredicate(col_id={self.col_id}, value={self.value}, type={self.value_type})"


class InequalityPredicate:
    """
    Represents an inequality or range predicate on a single column,
    e.g., left_value <= col_id <= right_value (with inclusive flags).
    """

    def __init__(
        self,
        col_id: str,
        value_type: str,
        left_value: int | float | str | None = None,
        right_value: int | float | str | None = None,
        left_inclusive: bool = True,
        right_inclusive: bool = True,
    ):
        self.col_id: str = col_id.upper()
        self.value_type: str = value_type
        self.left_value: int | float | str | None = left_value
        self.right_value: int | float | str | None = right_value
        self.left_inclusive: bool = left_inclusive
        self.right_inclusive: bool = right_inclusive
        self.range_id: int | None = None

    def set_range_id(self, range_id: int):
        self.range_id = range_id

    def __repr__(self):
        return (
            f"InequalityPredicate(col_id={self.col_id}, type={self.value_type}, "
            f"left_value={self.left_value}, right_value={self.right_value}, "
            f"left_incl={self.left_inclusive}, right_incl={self.right_inclusive})"
        )
