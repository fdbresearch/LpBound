from lpbound.acyclic.join_graph.predicate import EqualityPredicate, InequalityPredicate


class Vertex:
    def __init__(self, alias: str, relation_id: int, relation_name: str, groupby_vars: list[str] | None = None):
        self.alias: str = alias.upper()
        self.relation_id: int = relation_id
        self.relation_name: str = relation_name.upper()
        self.groupby_vars: list[str] = groupby_vars or []

        # Which columns in this table are used in join conditions?
        self.join_columns: set[str] = set()  # set of column names

        # Local predicates that apply directly to this table
        self.equalities: list[EqualityPredicate] = []
        self.inequalities: list[InequalityPredicate] = []

        self.pk_fk_info: list[tuple[str, str, str]] = []  # type: list[("pk_relation": str, "fk": str, "pk": str)]

    def add_join_column(self, col_name: str):
        self.join_columns.add(col_name)

    def add_equality_predicate(self, pred: EqualityPredicate):
        self.equalities.append(pred)

    def add_inequality_predicate(self, new_pred: InequalityPredicate):
        """
        If there's already an inequality predicate on the same col_id,
        merge them into a single combined range.
        Otherwise, just append this new_pred.
        """
        merged = False
        for existing_pred in self.inequalities:
            # Same column => attempt a merge
            if existing_pred.col_id == new_pred.col_id:
                # Merge logic
                _merge_inequality(existing_pred, new_pred)
                merged = True
                break

        if not merged:
            # No existing predicate for col_id, so add it directly
            self.inequalities.append(new_pred)

    def add_pk_fk_info(self, pk_fk_info: tuple[str, str, str]):
        self.pk_fk_info.append(pk_fk_info)

    def __repr__(self):
        return (
            f"Vertex(alias={self.alias}, id={self.relation_id}, "
            f"groupby_vars={self.groupby_vars}, "
            f"equalities={self.equalities}, "
            f"inequalities={self.inequalities}, "
            f"pk_fk_info={self.pk_fk_info})"
        )


def _merge_inequality(base: InequalityPredicate, newp: InequalityPredicate) -> None:
    """
    Merge newp into base, updating base in place so that it captures
    the intersection of the two ranges on the same col_id.
    Example:
    base:  col_id=x, left_value=2005, right_value=None, ...
    newp:  col_id=x, left_value=None, right_value=2010, ...
    => result: left_value=2005, right_value=2010
    Also merges inclusive flags appropriately.
    """
    # 1) Merge left boundaries (the max of the two lefts):
    if newp.left_value is not None:
        if base.left_value is None:
            # base had no left bound, so adopt newp's left bound
            base.left_value = newp.left_value
            base.left_inclusive = newp.left_inclusive
        else:
            # both have left bounds => take the larger
            if newp.left_value > base.left_value:
                base.left_value = newp.left_value
                base.left_inclusive = newp.left_inclusive
            elif newp.left_value == base.left_value:
                # If they share the same boundary, tighten inclusivity to 'both must be inclusive'
                # i.e., if either is non-inclusive => overall is non-inclusive
                base.left_inclusive = base.left_inclusive and newp.left_inclusive

    # 2) Merge right boundaries (the min of the two rights):
    if newp.right_value is not None:
        if base.right_value is None:
            # base had no right bound, so adopt newp's
            base.right_value = newp.right_value
            base.right_inclusive = newp.right_inclusive
        else:
            # both have right bounds => take the smaller
            if newp.right_value < base.right_value:
                base.right_value = newp.right_value
                base.right_inclusive = newp.right_inclusive
            elif newp.right_value == base.right_value:
                # If same boundary => tighten inclusivity similarly
                base.right_inclusive = base.right_inclusive and newp.right_inclusive
