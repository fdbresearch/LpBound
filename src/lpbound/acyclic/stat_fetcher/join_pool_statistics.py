from duckdb import DuckDBPyConnection

from lpbound.acyclic.join_graph.join_graph import JoinGraph
from lpbound.acyclic.join_graph.vertex import Vertex
from lpbound.utils.sql_execution import execute_fetchone_sql
from lpbound.utils.types import Stats


def produce_domain_size_sql(vertex: Vertex, join_column: str, is_groupby: bool) -> str:
    """
    Generate SQL to get domain size for a join column.
    Returns the SQL string.
    """
    norm_table_name = "norms" if not is_groupby else "norms_groupby"
    return f"""
    SELECT l0 FROM {norm_table_name}
    WHERE relation_name = '{vertex.relation_name}'
    AND join_var_name = '{join_column}'
    AND aggregator_name = 'NOPRED'
    ORDER BY pred_value_id DESC
    LIMIT 1
    ;"""


def compute_join_pool_domain_size(
    con: DuckDBPyConnection, join_graph: JoinGraph, pool_id: int, statistics: Stats
) -> None:
    """
    Compute domain size for a specific join pool.
    This function modifies join_graph.join_pool_domain_size.
    """
    # Get the alias-column pairs in the pool
    alias_col_pairs = [pair for pair in join_graph.join_pool_map if join_graph.join_pool_map[pair] == pool_id]

    num_distinct_values: list[int] = []

    # Get l0 norms from existing statistics
    for alias, col in alias_col_pairs:
        if (alias, col) in statistics:
            num_distinct_values.append(int(statistics[(alias, col)][0]))  # l0 norm

    # Also get domain sizes from base tables
    for v in join_graph.vertices.values():
        for join_column in v.join_columns:
            if join_graph.join_pool_map[(v.alias, join_column)] != pool_id:
                continue

            sql = produce_domain_size_sql(v, join_column, join_graph.is_groupby)
            domain_size = execute_fetchone_sql(con, sql)
            assert domain_size is not None and len(domain_size) > 0
            num_distinct_values.append(int(domain_size[0]))

    min_value = min(num_distinct_values) if num_distinct_values else -1
    # print(f"pool_id: {pool_id}, num_distinct_values: {num_distinct_values} min: {min_value}")

    # Update the join graph
    join_graph.join_pool_domain_size[pool_id] = min_value


def compute_join_pool_domain_sizes(con: DuckDBPyConnection, join_graph: JoinGraph, statistics: Stats) -> None:
    """
    Compute domain sizes for all join pools.
    This function modifies join_graph.join_pool_domain_size.
    """
    pool_ids = set(join_graph.join_pool_map.values())
    for pool_id in pool_ids:
        compute_join_pool_domain_size(con, join_graph, pool_id, statistics)
