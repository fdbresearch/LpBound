AliasColPair = tuple[str, str]  # (alias, column)
Stats = dict[AliasColPair, list[float]]  # [(alias, join_column)] -> [lp_norm]
DomainSizeStats = dict[str, int]  # [alias] -> domain_size
