"""
sql_queries.py

Functions for generating SQL queries for Lp-norm computations.
"""

from typing import Any

from duckdb import DuckDBPyConnection

from lpbound.config.lpbound_config import LpBoundConfig
from .sql_utils import SqlCommand, ordered_non_dup_vars
from .hierarchical_bucket_generator import HierarchicalBucketGenerator


def generate_joined_table_sql(
    fk_table: str,
    pk_table: str,
    fk_col: str,
    pk_col: str,
    fk_join_cols: list[str],
    pk_pred_cols: list[str],
    is_groupby_query: bool,
    groupby_vars_list: list[list[str]],
) -> list[SqlCommand]:
    """
    This function is for creating the fk-pk join tables.
    Returns structured commands for:
      1) Possibly dropping the existing joined table
      2) Creating the joined table by left-joining FK->PK
    """
    joined_table_name = f"{fk_table}_{fk_col}_{pk_col}_{pk_table}"
    commands: list[SqlCommand] = []

    # Possibly drop the table if it exists
    drop_sql = f"DROP TABLE IF EXISTS {joined_table_name};"
    commands.append({"sql": drop_sql, "tag": "DROP_TEMP_TABLE"})

    # Format column selections
    fk_relation_columns = ", ".join([f"{fk_table}.{col} AS {col}" for col in fk_join_cols])
    pk_relation_columns = ", ".join([f"{pk_table}.{col} AS {pk_table}_{col}" for col in pk_pred_cols])

    # in fk, also keep the groupby vars
    if is_groupby_query:
        # concat all groupby vars together
        all_groupby_vars = [col for groupby_vars in groupby_vars_list for col in groupby_vars if col != "*"]
        all_groupby_vars = ordered_non_dup_vars(all_groupby_vars)
        all_groupby_vars_str = ", ".join([f"{fk_table}.{col} AS {col}" for col in all_groupby_vars])
        fk_relation_columns += f", {all_groupby_vars_str}"

    # Create the joined table
    create_sql = f"""
-- Create temp table {joined_table_name} (FK->PK join)
CREATE TEMP TABLE {joined_table_name} AS
SELECT
    {fk_relation_columns},
    {pk_relation_columns}
FROM {fk_table}
JOIN {pk_table}
  ON {fk_table}.{fk_col} = {pk_table}.{pk_col}
;
""".strip()

    commands.append({"sql": create_sql, "tag": "JOIN_FKPK"})
    return commands


def generate_mcv_table_sql(table_name: str, pred_col: str, cfg: LpBoundConfig) -> list[SqlCommand]:
    """
    Create a temp table for top MCVs of (table_name.pred_col).
    Includes an auto-generated mcv_id column.
    """
    mcv_table_name = f"{table_name}_{pred_col}_mcv"
    commands: list[SqlCommand] = []

    # Possibly drop the table if it exists
    drop_sql = f"DROP TABLE IF EXISTS {mcv_table_name};"
    commands.append({"sql": drop_sql, "tag": "DROP_TEMP_TABLE"})

    # Create the MCV table with a unique mcv_id
    create_sql = f"""
-- Create MCV table for {table_name}.{pred_col}, with a unique mcv_id
CREATE TABLE {mcv_table_name} AS
WITH base AS (
    SELECT {pred_col}, COUNT(*) AS freq
    FROM {table_name}
    WHERE {pred_col} IS NOT NULL
    GROUP BY {pred_col}
    ORDER BY freq DESC
    LIMIT {cfg.num_mcvs}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    {pred_col},
    freq
FROM base;
""".strip()

    commands.append({"sql": create_sql, "tag": "CREATE_MCV_TABLE"})
    return commands


def generate_histogram_table_sql(
    con: DuckDBPyConnection, table_name: str, pred_col: str, data_type: str, cfg: LpBoundConfig
) -> list[SqlCommand]:
    """
    Generate a 'histogram' table with hierarchical bucket boundaries
    for range queries.
    """
    if data_type == "int":
        data_type = "INT"
    elif data_type == "float":
        data_type = "FLOAT"
    elif data_type == "str":
        data_type = "TEXT"
    elif data_type == "date":
        data_type = "DATETIME"
    else:
        raise ValueError(f"Unknown data type: {data_type}")

    # load data
    if data_type == "DATETIME":
        data = con.execute(
            f"SELECT CAST(epoch({pred_col}) AS INT) FROM {table_name} WHERE {pred_col} IS NOT NULL ORDER BY {pred_col}"
        ).fetchall()
    else:
        data = con.execute(
            f"SELECT DISTINCT {pred_col} FROM {table_name} WHERE {pred_col} IS NOT NULL ORDER BY {pred_col}"
        ).fetchall()
    data = [row[0] for row in data]

    print(f"Histogram for {table_name}.{pred_col}")
    # print(data)

    hbg = HierarchicalBucketGenerator(data, num_bins=cfg.num_buckets)
    hierarchical_buckets = hbg.generate_buckets()

    histogram_table_name = f"{table_name}_{pred_col}_histogram"
    create_histogram_table_sql = f"""
DROP TABLE IF EXISTS {histogram_table_name};
CREATE TABLE {histogram_table_name} (
    bucket_id   INT NOT NULL,
    layer       INT NOT NULL,
    lower_bound {data_type},
    upper_bound {data_type},
    bucket_type TEXT
);
""".strip()

    processed_hierarchical_buckets: list[tuple[int | str | float | None, int | str | float | None, int, str]] = []
    # lower bound or upper bound can be None
    # if they are None, we need to convert them to NULL
    # if they are not None, we need to convert them to the data type
    for lower_bound, upper_bound, layer, bucket_type in hierarchical_buckets:
        # print(f"lower_bound: {lower_bound}, upper_bound: {upper_bound}, layer: {layer}, bucket_type: {bucket_type}")

        if lower_bound is None:
            lower_bound = "NULL"
        else:
            if data_type == "DATETIME":
                lower_bound = f"to_timestamp({lower_bound})"
            elif data_type == "TEXT":
                lower_bound = f"'{lower_bound}'"

        if upper_bound is None:
            upper_bound = "NULL"
        else:
            if data_type == "DATETIME":
                upper_bound = f"to_timestamp({upper_bound})"
            elif data_type == "TEXT":
                upper_bound = f"'{upper_bound}'"

        processed_hierarchical_buckets.append((lower_bound, upper_bound, layer, bucket_type))

    insert_histogram_table_sql = f"""
INSERT INTO {histogram_table_name} (bucket_id, layer, lower_bound, upper_bound, bucket_type)
VALUES {", ".join([f"({i}, {layer}, {lower_bound}, {upper_bound}, '{bucket_type}')" for i, (lower_bound, upper_bound, layer, bucket_type) in enumerate(processed_hierarchical_buckets)])}
;
    """

    return [
        {"sql": create_histogram_table_sql, "tag": "CREATE_HISTOGRAM_TABLE"},
        {"sql": insert_histogram_table_sql, "tag": "CREATE_HISTOGRAM_TABLE"},
    ]
