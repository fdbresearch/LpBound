from duckdb import DuckDBPyConnection

from lpbound.acyclic.join_graph.join_graph import JoinGraph
from lpbound.acyclic.join_graph.vertex import Vertex
from lpbound.utils.sql_execution import execute_fetchone_sql


def _produce_base_norms_sql(vertex: Vertex, join_column: str, prefix_value: int, max_p: int, is_groupby: bool) -> str:
    """
    Generate SQL to get base norms for a join column.
    Returns the SQL string.
    """
    select_clauses = ["l0", "l1"] + [f"POWER(l{p}, 1.0/{p})" for p in range(2, max_p + 1)] + ["l_inf"]
    select_clause_str = ", ".join(select_clauses)

    pred_value_id = f"pred_value_id >= {prefix_value}" if prefix_value != -1 else "1 = 1"

    norm_table_name = "norms" if not is_groupby else "norms_groupby"

    return f"""-- base (prefix) norms for {vertex.relation_name}.{join_column}
             SELECT {select_clause_str} FROM {norm_table_name} 
             WHERE relation_name = '{vertex.relation_name}' 
             AND join_var_name = '{join_column}'
             AND aggregator_name = 'NOPRED'
             AND {pred_value_id}
             ORDER BY pred_value_id ASC
             LIMIT 1
             ;"""


def fetch_base_norms(
    con: DuckDBPyConnection, join_graph: JoinGraph, statistics: dict[tuple[str, str], list[float]], max_p: int
) -> dict[tuple[str, str], list[float]]:
    """
    Fetch base norms for all join columns.
    This function modifies the provided statistics dictionary and returns it.
    """
    for v in join_graph.vertices.values():
        for join_column in v.join_columns:
            pool_id = join_graph.join_pool_map[(v.alias, join_column)]
            prefix_value = join_graph.join_pool_domain_size[pool_id]

            sql = _produce_base_norms_sql(v, join_column, prefix_value, max_p, join_graph.is_groupby)
            # print(sql)
            norms = execute_fetchone_sql(con, sql)
            assert norms is not None and len(norms) > 0

            # Take the MIN of these norms and existing norms
            result_norms: list[float] = []
            pred_norms = statistics.get((v.alias, join_column), None)

            for i in range(len(norms)):
                if pred_norms is not None and i < len(pred_norms):
                    result_norms.append(min(norms[i], pred_norms[i]))
                else:
                    result_norms.append(norms[i])

            statistics[(v.alias, join_column)] = result_norms

    return statistics
