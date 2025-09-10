"""
sql_tables.py

Functions for creating and populating database tables.
"""

from duckdb import DuckDBPyConnection
from lpbound.config.lpbound_config import LpBoundConfig
from .sql_utils import SqlCommand, ordered_non_dup_vars
from lpbound.config.benchmark_schema import BenchmarkSchema


def create_groupby_domain_sizes_table_if_not_exists(cfg: LpBoundConfig) -> SqlCommand:
    """
    Create a table 'norms_groupby' that includes: relation_name, groupby_vars_name, aggregator_name, pred_col_name, pk_relation_name, fk_join_var_name, pred_value_id, domain_size
    """

    table_name = "groupby_domain_sizes"

    create_sql = f"""
DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
    relation_name TEXT NOT NULL,
    aggregator_name TEXT NOT NULL,
    groupby_vars_name TEXT NOT NULL,
    pred_col_name TEXT,
    pred_value_id INTEGER,
    pk_relation_name TEXT,
    fk_join_var_name TEXT,
    pk_join_var_name TEXT,
    domain_size INTEGER,
);
    """

    return {"sql": create_sql, "tag": "CREATE_GROUPBY_DOMAIN_SIZES_TABLE"}


def no_pred_groupby_domain_sizes_sql(
    joined_table_name: str,  # if is_fk_pk_join, this is the joined table name, else this is the rel_name
    rel_name: str,
    groupby_cols: list[str],
    aggregator_type: str,
    cfg: LpBoundConfig,
    fk_rel: str | None = None,
    pk_rel: str | None = None,
    fk_col: str | None = None,
    pk_col: str | None = None,
) -> SqlCommand:
    """
    Returns a single command that does an INSERT INTO norms(...) statement
    """

    groupby_vars_str = "|".join(ordered_non_dup_vars(groupby_cols))
    select_domain_size_clause = f"COUNT(DISTINCT({','.join(groupby_cols)}))"

    tag_val = aggregator_type

    relation_name: str = fk_rel if fk_rel is not None else rel_name
    fk_join_var_name: str = "NULL" if fk_col is None else f"'{fk_col}'"
    pk_join_var_name: str = "NULL" if pk_col is None else f"'{pk_col}'"
    pk_relation_name: str = "NULL" if pk_rel is None else f"'{pk_rel}'"

    placeholders_select_sql = f"""
    '{relation_name}' AS relation_name,
    '{groupby_vars_str}' AS groupby_vars_name,
    '{aggregator_type}' AS aggregator_name,
    NULL AS pred_col_name,
    NULL AS pred_value_id,
    {fk_join_var_name} AS fk_join_var_name,
    {pk_relation_name} AS pk_relation_name,
    {pk_join_var_name} AS pk_join_var_name,
    {select_domain_size_clause} AS domain_size,
    """.strip()

    compute_norms_sql = f"""
        SELECT
            {placeholders_select_sql}
        FROM
            {joined_table_name} AS R
    """.strip()

    # insert_name_sql = """
    # relation_name,
    # groupby_vars_name,
    # aggregator_name,
    # pred_col_name,
    # pred_value_id,
    # fk_join_var_name,
    # pk_relation_name,
    # pk_join_var_name,
    # """.strip()

    insert_sql = f"""
INSERT INTO groupby_domain_sizes BY NAME 
    {compute_norms_sql}
;
""".strip()

    return {"sql": insert_sql, "tag": tag_val}


def mcv_groupby_domain_sizes_sql(
    joined_table_name: str,  # if is_fk_pk_join, this is the joined table name, else this is the rel_name
    rel_name: str,
    groupby_cols: list[str],
    aggregator_type: str,
    cfg: LpBoundConfig,
    mcv_table_name: str,
    pk_pred_col: str,
    is_fk_pk_join: bool = False,
    fk_rel: str | None = None,
    pk_rel: str | None = None,
    fk_col: str | None = None,
    pk_col: str | None = None,
) -> SqlCommand:
    """
    Generate the SQL commands to compute the domain sizes of the groupby vars.
    """

    groupby_vars_str = "|".join(ordered_non_dup_vars(groupby_cols))
    select_domain_size_clause = f"COUNT(DISTINCT({','.join(groupby_cols)}))"
    groupby_vars_select_clause = ", ".join([f"{joined_table_name}.{col} AS {col}" for col in groupby_cols])

    tag_val = aggregator_type

    relation_name: str = fk_rel if fk_rel is not None else rel_name
    fk_join_var_name: str = "NULL" if fk_col is None else f"'{fk_col}'"
    pk_join_var_name: str = "NULL" if pk_col is None else f"'{pk_col}'"
    pk_relation_name: str = "NULL" if pk_rel is None else f"'{pk_rel}'"

    join_table_pk_pred_col = f"{pk_rel}_{pk_pred_col}" if is_fk_pk_join else pk_pred_col

    placeholders_select_sql = f"""
    '{relation_name}' AS relation_name,
    '{groupby_vars_str}' AS groupby_vars_name,
    '{aggregator_type}' AS aggregator_name,
    '{pk_pred_col}' AS pred_col_name,
    {fk_join_var_name} AS fk_join_var_name,
    {pk_relation_name} AS pk_relation_name,
    {pk_join_var_name} AS pk_join_var_name,
    {select_domain_size_clause} AS domain_size,
    """.strip()

    # first filter the table by MCV
    compute_norms_sql = f"""
    WITH
        filtered_table AS (
            SELECT
                mcvt.mcv_id AS mcv_id,
                {groupby_vars_select_clause}
            FROM {joined_table_name}
            JOIN {mcv_table_name} mcvt ON {joined_table_name}.{join_table_pk_pred_col} = mcvt.{pk_pred_col}
        )
        SELECT
            {placeholders_select_sql}
            R.mcv_id AS pred_value_id,
        FROM
            filtered_table AS R
        GROUP BY
            R.mcv_id
    """.strip()

    # insert_name_sql = """
    # relation_name,
    # groupby_vars_name,
    # aggregator_name,
    # pred_col_name,
    # fk_join_var_name,
    # pk_relation_name,
    # pk_join_var_name,
    # pred_value_id,
    # domain_size
    # """.strip()

    insert_sql = f"""
INSERT INTO groupby_domain_sizes BY NAME 
    {compute_norms_sql}
;
""".strip()

    return {"sql": insert_sql, "tag": tag_val}


def non_mcv_groupby_domain_sizes_sql(
    joined_table_name: str,  # if is_fk_pk_join, this is the joined table name, else this is the rel_name
    rel_name: str,
    groupby_cols: list[str],
    aggregator_type: str,
    cfg: LpBoundConfig,
    mcv_table_name: str,
    pk_pred_col: str,
    is_fk_pk_join: bool = False,
    fk_rel: str | None = None,
    pk_rel: str | None = None,
    fk_col: str | None = None,
    pk_col: str | None = None,
) -> SqlCommand:
    """
    Generate the SQL commands to compute the domain sizes of the groupby vars.
    """

    groupby_vars_str = "|".join(ordered_non_dup_vars(groupby_cols))
    select_domain_size_clause = f"COUNT(DISTINCT({','.join(groupby_cols)}))"
    groupby_vars_select_clause = ", ".join([f"{joined_table_name}.{col} AS {col}" for col in groupby_cols])

    tag_val = aggregator_type

    relation_name: str = fk_rel if fk_rel is not None else rel_name
    fk_join_var_name: str = "NULL" if fk_col is None else f"'{fk_col}'"
    pk_join_var_name: str = "NULL" if pk_col is None else f"'{pk_col}'"
    pk_relation_name: str = "NULL" if pk_rel is None else f"'{pk_rel}'"

    join_table_pk_pred_col = f"{pk_rel}_{pk_pred_col}" if is_fk_pk_join else pk_pred_col

    placeholders_select_sql = f"""
    '{relation_name}' AS relation_name,
    '{groupby_vars_str}' AS groupby_vars_name,
    '{aggregator_type}' AS aggregator_name,
    '{pk_pred_col}' AS pred_col_name,
    {fk_join_var_name} AS fk_join_var_name,
    {pk_relation_name} AS pk_relation_name,
    {pk_join_var_name} AS pk_join_var_name,
    {select_domain_size_clause} AS domain_size,
    """.strip()

    # first filter the table by MCV
    compute_norms_sql = f"""
    WITH
        non_mcv_table AS (
            SELECT
                {joined_table_name}.{join_table_pk_pred_col} AS non_mcv_value,
                {groupby_vars_select_clause}
            FROM {joined_table_name}
            LEFT JOIN {mcv_table_name} mcvt ON {joined_table_name}.{join_table_pk_pred_col} = mcvt.{pk_pred_col}
            WHERE mcvt.mcv_id IS NULL
        )
        SELECT
            {placeholders_select_sql}
            NULL AS pred_value_id,
        FROM
            non_mcv_table AS R
    """.strip()

    insert_sql = f"""
INSERT INTO groupby_domain_sizes BY NAME 
    {compute_norms_sql}
;
""".strip()

    return {"sql": insert_sql, "tag": tag_val}


def range_groupby_domain_sizes_sql(
    joined_table_name: str,  # if is_fk_pk_join, this is the joined table name, else this is the rel_name
    rel_name: str,
    groupby_cols: list[str],
    aggregator_type: str,
    cfg: LpBoundConfig,
    histogram_table_name: str,
    pk_pred_col: str,
    is_fk_pk_join: bool = False,
    fk_rel: str | None = None,
    pk_rel: str | None = None,
    fk_col: str | None = None,
    pk_col: str | None = None,
) -> SqlCommand:
    """
    Generate the SQL commands to compute the domain sizes of the groupby vars.
    """

    groupby_vars_str = "|".join(ordered_non_dup_vars(groupby_cols))
    select_domain_size_clause = f"COUNT(DISTINCT({','.join(groupby_cols)}))"
    groupby_vars_select_clause = ", ".join([f"{joined_table_name}.{col} AS {col}" for col in groupby_cols])

    tag_val = aggregator_type

    relation_name: str = fk_rel if fk_rel is not None else rel_name
    fk_join_var_name: str = "NULL" if fk_col is None else f"'{fk_col}'"
    pk_join_var_name: str = "NULL" if pk_col is None else f"'{pk_col}'"
    pk_relation_name: str = "NULL" if pk_rel is None else f"'{pk_rel}'"

    join_table_pk_pred_col = f"{pk_rel}_{pk_pred_col}" if is_fk_pk_join else pk_pred_col

    placeholders_select_sql = f"""
    '{relation_name}' AS relation_name,
    '{groupby_vars_str}' AS groupby_vars_name,
    '{aggregator_type}' AS aggregator_name,
    '{pk_pred_col}' AS pred_col_name,
    {fk_join_var_name} AS fk_join_var_name,
    {pk_relation_name} AS pk_relation_name,
    {pk_join_var_name} AS pk_join_var_name,
    {select_domain_size_clause} AS domain_size,
    """.strip()

    # TODO: I add "MATERIALIZED" as a hack to avoid the issue in DuckDB v0.10.0
    # first filter the table by MCV
    materialized_str = (
        "MATERIALIZED"
        if histogram_table_name == "USERS_DOWNVOTES_histogram" or histogram_table_name == "POSTS_SCORE_histogram"
        else ""
    )
    compute_norms_sql = f"""
    WITH
        table_with_buckets AS {materialized_str} (
            SELECT
                H.bucket_id AS bucket_id,
                {groupby_vars_select_clause}
            FROM {joined_table_name}
            JOIN {histogram_table_name} H ON 
                (H.lower_bound IS NULL OR {joined_table_name}.{join_table_pk_pred_col} >= H.lower_bound)
                AND (H.upper_bound IS NULL OR {joined_table_name}.{join_table_pk_pred_col} <= H.upper_bound)
        )
        SELECT
            {placeholders_select_sql}
            R.bucket_id AS pred_value_id,
        FROM
            table_with_buckets AS R
        GROUP BY
            R.bucket_id
    """.strip()

    insert_sql = f"""
INSERT INTO groupby_domain_sizes BY NAME 
    {compute_norms_sql}
;
""".strip()

    return {"sql": insert_sql, "tag": tag_val}
