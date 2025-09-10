"""
sql_utils.py

Utility functions for SQL generation.
"""

from typing import TypedDict

from lpbound.config.lpbound_config import LpBoundConfig


class SqlCommand(TypedDict):
    """Type for SQL commands with tags."""

    sql: str
    tag: str


def maybe_drop_temp_table_sql(table_name: str) -> list[SqlCommand]:
    """
    If drop_temp_tables is True, return a command to drop the table. Otherwise empty.
    We'll return a list of command dicts so it's consistent with the rest of the code.
    """
    return [{"sql": f"DROP TABLE IF EXISTS {table_name};", "tag": "DROP_TEMP_TABLE"}]


def lp_select_expressions(p_list: list[int], cfg: LpBoundConfig) -> list[str]:
    """
    Returns a list of SELECT expressions for computing Lp norms over 'deg'.
    For example: ["COUNT(*) AS l0", "SUM(POWER(deg,1)) AS l1", "MAX(deg) AS l_inf"]
    but only includes l0 if cfg.include_l0 is True, and l_inf if cfg.include_l_inf.
    """
    select_exprs: list[str] = []

    # l0 => COUNT(*)
    # only if include_l0 is True
    if cfg.include_l0 and 0 in p_list:
        # if p_list has 0, we'll do l0 here
        select_exprs.append("COUNT(*) AS l0")

    # Summation of deg^p for p>0
    for p in p_list:
        if p == 0:
            # skip it if we already included l0 above
            # or if p=0 is not in p_list (some code includes it by default).
            continue
        select_exprs.append(f"SUM(POWER(deg, {p})) AS l{p}")

    # l_inf => MAX(deg)
    if cfg.include_l_inf:
        select_exprs.append("MAX(deg) AS l_inf")

    return select_exprs


def lp_insert_column_list(p_list: list[int], cfg: LpBoundConfig) -> str:
    """
    Return the CSV string for the columns we will INSERT into norms(...),
    matching the order of _lp_select_expressions().
    e.g. "l0, l1, l2, ..., l10, l_inf" (conditionally including l0, l_inf).
    """
    # We'll use the same logic as above, but just want the names, not the expressions
    col_names: list[str] = []
    if cfg.include_l0 and 0 in p_list:
        col_names.append("l0")
    for p in p_list:
        if p == 0:
            continue
        col_names.append(f"l{p}")
    if cfg.include_l_inf:
        col_names.append("l_inf")
    return ", ".join(col_names)


def ordered_non_dup_vars(vars: list[str]) -> list[str]:
    """
    Return a list of variables with duplicates removed, ordered by lex order.
    """
    return sorted(list(set(vars)))
