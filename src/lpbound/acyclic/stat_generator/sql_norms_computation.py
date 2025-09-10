"""
sql_norms_computation.py

Functions for generating SQL queries for Lp-norm computations.

Function generate_aggregator_sql creates the temporary aggregator table. Other functions
compute the Lp norms based on the aggregator table (for different types of predicates).
"""

from duckdb import DuckDBPyConnection
from lpbound.config.lpbound_config import LpBoundConfig
from .sql_utils import (
    SqlCommand,
    lp_select_expressions,
    lp_insert_column_list,
    maybe_drop_temp_table_sql,
    ordered_non_dup_vars,
)


def generate_aggregator_sql(
    table_name: str,  # joined table name
    groupby_cols: list[str],
    pk_table: str | None,
    join_var: str,
    pred_col: str | None = None,
    pred_table: str | None = None,
    is_mcv: bool = True,
    is_range: bool = False,
    is_fk_pk_join: bool = False,
) -> list[SqlCommand]:
    """
    Create a temp aggregator table grouping by (pred_col, join_var) => deg=COUNT(*).
    We'll call this "Degree" table. A degree sequence.

    - If `pred_table` is given and `is_mcv=True`, we do an INNER JOIN to keep only rows that match MCV.
    - If `pred_table` is given and not is_mcv, we do a LEFT JOIN and filter out matches (non-MCV).
    - If `pred_col` is None, we only group by the join_var.

    Returns a list of command dicts like:
    [
      { "sql": "DROP TABLE IF EXISTS ...", "tag": "DROP" },
      { "sql": "CREATE TEMP TABLE ...",    "tag": "EQUALITY_MCV" or "EQUALITY_NONMCV" or "NOPRED" }
    ]
    """
    # 1) Construct the aggregator table name
    agg_suffix: list[str] = []
    if pred_col:
        agg_suffix.append(pred_col)
    agg_suffix.append(join_var)
    agg_table_name = f"Degree_{table_name}_{'_'.join(agg_suffix)}"

    # 2) Possibly drop the table if configured
    commands: list[SqlCommand] = []
    commands.extend(maybe_drop_temp_table_sql(agg_table_name))

    # 3) Decide on JOIN style + SQL tag
    join_clause: str = ""
    where_clause: str = f"WHERE {table_name}.{join_var} IS NOT NULL"
    # where_clause = ""
    tag_val: str = "PREFIX"  # default if no MCV table and we need to do prefixes

    # in joined tables, the predicate column are renamed to be pk_table_pred_col
    pk_pred_col = pred_col if not is_fk_pk_join else f"{pk_table}_{pred_col}"

    if not is_range:
        if pred_table and is_mcv:
            # MCV => inner join
            join_clause = f"JOIN {pred_table} mcvt ON {table_name}.{pk_pred_col} = mcvt.{pred_col}"
            tag_val = "MCV" if not is_fk_pk_join else "FKPK_MCV"
        elif pred_table and not is_mcv:
            # Non-MCV => left join + exclude matches
            join_clause = f"LEFT JOIN {pred_table} mcvt ON {table_name}.{pk_pred_col} = mcvt.{pred_col}"
            where_clause += " AND mcvt.mcv_id IS NULL "
            tag_val = "MCV" if not is_fk_pk_join else "FKPK_MCV"
    else:
        tag_val = "RANGE" if not is_fk_pk_join else "FKPK_RANGE"

    # to count the number of distinct values (e.g. COUNT(DISTINCT(col1, col2, ...) or COUNT(*))
    count_sql = "COUNT(*)"
    if "*" in groupby_cols:
        count_sql = "COUNT(*)"
    else:
        groupby_cols_str = ", ".join([f"{table_name}.{col}" for col in groupby_cols])
        count_sql = f"COUNT(DISTINCT({groupby_cols_str}))"

    # 4) Build SELECT statement
    if pred_col and not is_range and is_mcv:
        # If we have a predicate column
        create_sql = f"""
-- Create aggregator table {agg_table_name} for {'MCV' if is_mcv else 'Histogram'} {table_name}.{pk_pred_col} -- {tag_val}
CREATE TEMP TABLE {agg_table_name} AS
SELECT
    mcvt.mcv_id AS mcv_id,
    {table_name}.{join_var} AS {join_var},
    {count_sql} AS deg
FROM {table_name}
{join_clause}
{where_clause}
GROUP BY
    mcvt.mcv_id, {table_name}.{join_var}
;
""".strip()
    elif pred_col and not is_range and not is_mcv:  # NONMCV
        # If we have a predicate column
        create_sql = f"""
-- Create aggregator table {agg_table_name} for NONMCV {table_name}.{pk_pred_col} -- {tag_val}
CREATE TEMP TABLE {agg_table_name} AS
SELECT
    {table_name}.{pk_pred_col} AS {pred_col},
    {table_name}.{join_var} AS {join_var},
    {count_sql} AS deg
FROM {table_name}
{join_clause}
{where_clause}
GROUP BY
    {table_name}.{pk_pred_col}, {table_name}.{join_var}
;
""".strip()
    elif pred_col and is_range:
        # If we have a predicate column
        create_sql = f"""
-- Create aggregator table {agg_table_name} for Histogram {table_name}.{pk_pred_col} -- {tag_val}
CREATE TEMP TABLE {agg_table_name} AS
SELECT
    {table_name}.{pk_pred_col} AS {pred_col},
    {table_name}.{join_var} AS {join_var},
    {count_sql} AS deg
FROM {table_name}
{join_clause}
{where_clause}
GROUP BY
    {table_name}.{pk_pred_col}, {table_name}.{join_var}
;
""".strip()
    else:
        # No predicate column; only group by the join_var
        create_sql = f"""
-- Create aggregator table {agg_table_name}
CREATE TEMP TABLE {agg_table_name} AS
SELECT
    {table_name}.{join_var} AS {join_var},
    {count_sql} AS deg
FROM {table_name}
{join_clause}
{where_clause}
GROUP BY
    {table_name}.{join_var}
HAVING {count_sql} > 0
ORDER BY deg DESC
;
""".strip()

    commands.append({"sql": create_sql, "tag": tag_val})
    return commands


def generate_lp_sql_no_pred(
    con: DuckDBPyConnection,
    agg_table_name: str,
    groupby_cols: list[str],
    p_list: list[int],
    rel_name: str,
    jv: str,
    aggregator_type: str,
    cfg: LpBoundConfig,
    is_groupby_query: bool,
) -> list[SqlCommand]:
    """
    The prefix optimization is applied here.

    Returns a list of commands that do an INSERT INTO norms(...) for prefix sizes
    of the aggregator table (like partial sums).
    We compute any of l0..lX..l_inf if included, using the helper functions.

    Because we do multiple prefixes, we'll have multiple SELECT statements appended.
    """
    # figure out how many distinct rows in the base relation for that jv
    row_count: int = con.execute(f"SELECT COUNT(DISTINCT({jv})) FROM {rel_name}").fetchone()[0]

    # figure out the prefix sizes
    prefixes: list[int] = []
    val: int = 1
    while val <= row_count:
        prefixes.append(val)
        val *= 2
    prefixes.append(row_count)  # always add the entire set

    # build the SELECT expressions e.g. COUNT(*) AS l0, SUM(POWER(deg,1)) as l1, ...
    select_exprs = lp_select_expressions(p_list, cfg)
    select_clause = ",\n  ".join(select_exprs)

    # build the column list for the final insert
    lp_cols = lp_insert_column_list(p_list, cfg)

    # We'll produce multiple INSERT SELECT statements (one per prefix)
    result_cmds: list[SqlCommand] = []

    # related to groupby cols
    norm_table_name = "norms_groupby" if is_groupby_query else "norms"

    insert_name_sql = """
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    """
    if is_groupby_query:
        insert_name_sql += "groupby_vars_name,"

    select_name_sql = f"""
    '{rel_name}' AS relation_name,
    '{jv}' AS join_var_name,
    '{aggregator_type}' AS aggregator_name,
    NULL AS pred_col_name,
    """
    if is_groupby_query:
        gby_cols_str = "|".join(ordered_non_dup_vars(groupby_cols))
        select_name_sql += f"'{gby_cols_str}' AS groupby_vars_name,"

    for i, prefix in enumerate(prefixes):
        prefix_select = f"""
INSERT INTO {norm_table_name}(
    {insert_name_sql}
    pred_value_id,
  {lp_cols}
)
SELECT
    {select_name_sql}
    {prefix} AS pred_value_id,
  {select_clause}
FROM (
  SELECT * FROM {agg_table_name}
  LIMIT {prefix}
);
""".strip()

        # last one might have a separate tag
        tag = "BASE" if i == (len(prefixes) - 1) else "PREFIX"
        result_cmds.append({"sql": prefix_select, "tag": tag})

    return result_cmds


def generate_lp_sql(
    agg_table_name: str,
    groupby_cols: list[str],
    p_list: list[int],
    rel_name: str,
    jv: str,
    aggregator_type: str,
    pred_col: str,
    cfg: LpBoundConfig,
    is_groupby_query: bool = False,
    is_fk_pk_join: bool = False,
    fk_rel: str | None = None,
    pk_rel: str | None = None,
    fk_col: str | None = None,
    pk_col: str | None = None,
) -> SqlCommand:
    """
    Returns a single command that does an INSERT INTO norms(...) statement
    grouping by the predicate value.  (e.g. for MCV aggregator).
    """
    # build the SELECT expression for Lp terms
    select_exprs = lp_select_expressions(p_list, cfg)
    select_clause = ",\n    ".join(select_exprs)

    # related to groupby cols
    norm_table_name = "norms_groupby" if is_groupby_query else "norms"

    # group by pred_col
    group_by_clause = "GROUP BY mcv_id" if pred_col else ""

    tag_val = "MCV" if not is_fk_pk_join else "FKPK_MCV"

    relation_name: str = fk_rel if fk_rel is not None else rel_name
    fk_join_var_name: str = "NULL" if fk_col is None else f"'{fk_col}'"
    pk_join_var_name: str = "NULL" if pk_col is None else f"'{pk_col}'"
    pk_relation_name: str = "NULL" if pk_rel is None else f"'{pk_rel}'"

    pred_value: str = "mcv_id"

    insert_name_sql = """
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    """
    if is_groupby_query:
        insert_name_sql += "groupby_vars_name,"

    select_name_sql = f"""
    '{relation_name}' AS relation_name,
    '{jv}' AS join_var_name,
    '{aggregator_type}' AS aggregator_name,
    '{pred_col}' AS pred_col_name,
    {fk_join_var_name} AS fk_join_var_name,
    {pk_relation_name} AS pk_relation_name,
    {pk_join_var_name} AS pk_join_var_name,
    """
    if is_groupby_query:
        gby_cols_str = "|".join(ordered_non_dup_vars(groupby_cols))
        select_name_sql += f"'{gby_cols_str}' AS groupby_vars_name,"

    final_select = f"""
SELECT
    {select_name_sql}
    {pred_value} AS pred_value,
    {select_clause}
FROM {agg_table_name}
{group_by_clause}
""".strip()

    # build the column list for final insert
    lp_cols = lp_insert_column_list(p_list, cfg)

    insert_sql = f"""
INSERT INTO {norm_table_name}(
    {insert_name_sql}
  pred_value_id,
  {lp_cols}
)
{final_select}
;
""".strip()

    return {"sql": insert_sql, "tag": tag_val}


def generate_nonmcv_max_sql(
    agg_table_name: str,
    groupby_cols: list[str],
    pred_col: str,
    p_list: list[int],
    rel_name: str,
    jv: str,
    aggregator_type: str,
    cfg: LpBoundConfig,
    is_fk_pk_join: bool = False,
    fk_rel: str | None = None,
    pk_rel: str | None = None,
    fk_col: str | None = None,
    pk_col: str | None = None,
    is_groupby_query: bool = False,
) -> SqlCommand:
    """
    Summation of deg^p grouped by pred_col, then take max over them.
    (Used to capture the "worst-case" among non-MCV values.)
    """
    # Build subselect columns
    sub_exprs: list[str] = []
    # Only do l0 if included
    if cfg.include_l0 and 0 in p_list:
        sub_exprs.append("COUNT(*) AS l0")
    for p in p_list:
        if p == 0:
            continue
        sub_exprs.append(f"SUM(POWER(deg, {p})) AS l{p}")
    if cfg.include_l_inf:
        sub_exprs.append("MAX(deg) AS l_inf")

    subselect_clause = ", ".join(sub_exprs)

    # Now we do the outer select => MAX(lX)
    out_cols: list[str] = []
    if cfg.include_l0 and 0 in p_list:
        out_cols.append("MAX(l0) AS l0")
    for p in p_list:
        if p == 0:
            continue
        out_cols.append(f"MAX(l{p}) AS l{p}")
    if cfg.include_l_inf:
        out_cols.append("MAX(l_inf) AS l_inf")

    final_clause = ", ".join(out_cols)

    relation_name = fk_rel if fk_rel is not None else rel_name
    fk_join_var_name = "NULL" if fk_col is None else f"'{fk_col}'"
    pk_join_var_name = "NULL" if pk_col is None else f"'{pk_col}'"
    pk_relation_name = "NULL" if pk_rel is None else f"'{pk_rel}'"

    # related to groupby cols
    norm_table_name = "norms_groupby" if is_groupby_query else "norms"

    insert_name_sql = """
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    """
    if is_groupby_query:
        insert_name_sql += "groupby_vars_name,"

    select_name_sql = f"""
    '{relation_name}' AS relation_name,
    '{jv}' AS join_var_name,
    '{aggregator_type}' AS aggregator_name,
    '{pred_col}' AS pred_col_name,
    {fk_join_var_name} AS fk_join_var_name,
    {pk_relation_name} AS pk_relation_name,
    {pk_join_var_name} AS pk_join_var_name,
    """
    if is_groupby_query:
        gby_cols_str = "|".join(ordered_non_dup_vars(groupby_cols))
        select_name_sql += f"'{gby_cols_str}' AS groupby_vars_name,"

    per_value_sql = f"""
WITH perValue AS (
  SELECT
    {pred_col},
    {subselect_clause}
  FROM {agg_table_name}
  GROUP BY {pred_col}
  HAVING {pred_col} IS NOT NULL
)
""".strip()

    tag_val = "MCV" if not is_fk_pk_join else "FKPK_MCV"
    final_select = f"""
SELECT
    {select_name_sql}
  NULL              AS pred_value_id,
  {final_clause}
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
""".strip()

    # build final column list
    lp_cols = lp_insert_column_list(p_list, cfg)

    insert_sql = f"""
-- Insert into norms for Non-MCV {rel_name} {pred_col} {jv} {aggregator_type}
-- Among all non-MCV values in {agg_table_name}, find max of each Lp
{per_value_sql}
INSERT INTO {norm_table_name}(
    {insert_name_sql}
  pred_value_id,
  {lp_cols}
)
{final_select}
;
""".strip()

    return {"sql": insert_sql, "tag": tag_val}


def generate_range_agg_sql(
    agg_table_name: str,
    histogram_table_name: str,
    groupby_cols: list[str],
    p_list: list[int],
    rel_name: str,
    jv: str,
    aggregator_type: str,
    pred_col: str,
    cfg: LpBoundConfig,
    is_groupby_query: bool = False,
    is_fk_pk_join: bool = False,
    fk_rel: str | None = None,
    pk_rel: str | None = None,
    fk_col: str | None = None,
    pk_col: str | None = None,
) -> SqlCommand:
    """
    Create aggregator table grouping by (range bucket, join_var),
    and then do an INSERT INTO norms(...) with the final Lp sums.
    """

    # build select expressions
    select_exprs = lp_select_expressions(p_list, cfg)
    select_clause = ",\n    ".join(select_exprs)

    # We'll group by h.bucket_id
    tag_val = "RANGE" if not is_fk_pk_join else "FKPK_RANGE"

    relation_name = fk_rel if fk_rel is not None else rel_name
    fk_join_var_name = "NULL" if fk_col is None else f"'{fk_col}'"
    pk_join_var_name = "NULL" if pk_col is None else f"'{pk_col}'"
    pk_relation_name = "NULL" if pk_rel is None else f"'{pk_rel}'"

    # related to groupby cols
    norm_table_name = "norms_groupby" if is_groupby_query else "norms"

    insert_name_sql = """
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    """
    if is_groupby_query:
        insert_name_sql += "groupby_vars_name,"

    select_name_sql = f"""
    '{relation_name}' AS relation_name,
    '{jv}' AS join_var_name,
    '{aggregator_type}' AS aggregator_name,
    '{pred_col}' AS pred_col_name,
    {fk_join_var_name} AS fk_join_var_name,
    {pk_relation_name} AS pk_relation_name,
    {pk_join_var_name} AS pk_join_var_name,
    """
    if is_groupby_query:
        gby_cols_str = "|".join(ordered_non_dup_vars(groupby_cols))
        select_name_sql += f"'{gby_cols_str}' AS groupby_vars_name,"

    inner_select = f"""
    SELECT
        {jv}, bucket_id, SUM(deg) as deg
        FROM {agg_table_name} d
        JOIN {histogram_table_name} h
        ON (h.lower_bound IS NULL OR d.{pred_col} >= h.lower_bound)
        AND (h.upper_bound IS NULL OR d.{pred_col} <= h.upper_bound)
    WHERE {jv} IS NOT NULL AND bucket_id IS NOT NULL
    GROUP BY {jv}, bucket_id
    """.strip()

    # DuckDB v0.10 does not support the above select well (it freezes) for a certain query.
    # So we use the following alternative.
    # This issue does not exist in newer versions of DuckDB.
    if (
        histogram_table_name == "POSTHISTORY_CREATIONDATE_histogram"
        or histogram_table_name == "VOTES_CREATIONDATE_histogram"
    ):
        inner_select = f"""
        WITH
        joined_data AS MATERIALIZED (
            SELECT
            d.{jv} AS {jv}, h.bucket_id AS bucket_id, d.deg AS deg
            FROM {agg_table_name} d
            JOIN {histogram_table_name} h
            ON (h.lower_bound IS NULL OR d.{pred_col} >= h.lower_bound)
            AND (h.upper_bound IS NULL OR d.{pred_col} <= h.upper_bound)
        )
        SELECT {jv}, bucket_id, SUM(deg) as deg
        FROM joined_data
        GROUP BY {jv}, bucket_id
        """.strip()

    final_select = f"""
SELECT
    {select_name_sql}
    bucket_id     AS pred_value,
    {select_clause}
FROM 
(
  {inner_select}
)
GROUP BY bucket_id
ORDER BY bucket_id
""".strip()

    # build column list for norms
    lp_cols = lp_insert_column_list(p_list, cfg)

    insert_sql = f"""
INSERT INTO {norm_table_name}(
{insert_name_sql}
pred_value_id,
{lp_cols}
)
{final_select}
;
""".strip()

    return {"sql": insert_sql, "tag": tag_val}
