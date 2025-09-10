from duckdb import DuckDBPyConnection

from lpbound.acyclic.join_graph.join_graph import JoinGraph
from lpbound.acyclic.join_graph.vertex import Vertex
from lpbound.utils.sql_execution import execute_fetchone_sql
from lpbound.utils.types import DomainSizeStats, Stats
from lpbound.acyclic.stat_generator.sql_utils import ordered_non_dup_vars


def _produce_predicate_norms_conditions(vertex: Vertex, join_graph: JoinGraph) -> list[str]:
    """
    Generate SQL conditions for predicate norms.
    Returns a list of SQL condition strings.
    """
    predicate_conditions: list[str] = []

    # Handle equality predicates
    for predicate in vertex.equalities:
        if predicate.mcv_id is not None:
            predicate_conditions.append(
                f" (pk_relation_name IS NULL AND aggregator_name = 'MCV' AND pred_col_name = '{predicate.col_id}' AND pred_value_id = {predicate.mcv_id}) "
            )
        else:  # NON-MCV
            predicate_conditions.append(
                f" (pk_relation_name IS NULL AND aggregator_name = 'NONMCV' AND pred_col_name = '{predicate.col_id}') "
            )

    # Handle range predicates
    for predicate in vertex.inequalities:
        if predicate.range_id is not None:
            predicate_conditions.append(
                f" (pk_relation_name IS NULL AND aggregator_name = 'RANGE' AND pred_col_name = '{predicate.col_id}' AND pred_value_id = {predicate.range_id}) "
            )

    # Handle FKPK joins
    for pk_alias, fk, pk in vertex.pk_fk_info:
        pk_vertex: Vertex = join_graph.vertices[pk_alias]
        pk_relation: str = pk_vertex.relation_name

        for predicate in pk_vertex.equalities:
            if predicate.mcv_id is not None:
                predicate_conditions.append(
                    f" (pk_relation_name = '{pk_relation}' AND fk_join_var_name = '{fk}' AND pk_join_var_name = '{pk}' AND aggregator_name = 'MCV' AND pred_col_name = '{predicate.col_id}' AND pred_value_id = {predicate.mcv_id}) "
                )
            else:  # NON-MCV
                predicate_conditions.append(
                    f" (pk_relation_name = '{pk_relation}' AND fk_join_var_name = '{fk}' AND pk_join_var_name = '{pk}' AND aggregator_name = 'NONMCV' AND pred_col_name = '{predicate.col_id}') "
                )

        for predicate in pk_vertex.inequalities:
            if predicate.range_id is not None:
                predicate_conditions.append(
                    f" (pk_relation_name = '{pk_relation}' AND fk_join_var_name = '{fk}' AND pk_join_var_name = '{pk}' AND aggregator_name = 'RANGE' AND pred_col_name = '{predicate.col_id}' AND pred_value_id = {predicate.range_id}) "
                )

    return predicate_conditions


def _produce_predicate_norms_sql(vertex: Vertex, join_column: str, max_p: int, join_graph: JoinGraph) -> str | None:
    """
    Generate SQL to fetch predicate norms for a vertex's join column.
    Returns the SQL string or None if there are no predicates.
    """
    predicate_conditions = _produce_predicate_norms_conditions(vertex, join_graph)

    select_clauses = (
        ["MIN(l0)", "MIN(l1)"] + [f"POWER(MIN(l{p}), 1.0/{p})" for p in range(2, max_p + 1)] + ["MIN(l_inf)"]
    )
    select_clause_str = ", ".join(select_clauses)

    # handle groupby
    norm_table_name = "norms" if not join_graph.is_groupby else "norms_groupby"
    groupby_sql = ""
    if join_graph.is_groupby:
        if len(vertex.groupby_vars) > 0:  # not *
            groupby_vars_str = "|".join(ordered_non_dup_vars(vertex.groupby_vars))
            groupby_sql = f"AND groupby_vars_name = '{groupby_vars_str}'"
        else:  # *
            groupby_sql = "AND groupby_vars_name = '*'"

    sql = None
    if len(predicate_conditions) > 0:
        predicate_conditions_str = " OR ".join(predicate_conditions)
        sql = f"""-- equality norms for {vertex.relation_name}.{join_column}
                SELECT {select_clause_str} FROM {norm_table_name} 
                WHERE relation_name = '{vertex.relation_name}'
                AND join_var_name = '{join_column}'
                {groupby_sql}
                AND ({predicate_conditions_str})
                ;"""
    return sql


def fetch_predicate_norms(
    con: DuckDBPyConnection, join_graph: JoinGraph, max_p: int
) -> dict[tuple[str, str], list[float]]:
    """
    Fetch predicate norms for all join columns.
    Returns a dictionary mapping (alias, column) to norm values.
    """
    statistics: Stats = {}
    for v in join_graph.vertices.values():
        for join_column in v.join_columns:
            sql = _produce_predicate_norms_sql(v, join_column, max_p, join_graph)
            if sql is not None:
                norms = execute_fetchone_sql(con, sql)
                statistics[(v.alias, join_column)] = norms
    return statistics


def _produce_no_pred_conditions(vertex: Vertex, join_graph: JoinGraph) -> str:
    """
    Generate SQL conditions for no predicates.
    Returns a SQL condition string.
    """

    return " (aggregator_name = 'NOPRED') "


def _produce_groupby_domain_sizes_sql(vertex: Vertex, join_graph: JoinGraph) -> str:
    """
    Generate SQL to fetch predicate norms for a vertex's join column.
    Returns the SQL string or None if there are no predicates.
    """
    predicate_conditions = _produce_predicate_norms_conditions(vertex, join_graph)
    no_pred_condition = _produce_no_pred_conditions(vertex, join_graph)
    predicate_conditions_str = " OR ".join(predicate_conditions + [no_pred_condition])

    groupby_vars_str = "|".join(ordered_non_dup_vars(vertex.groupby_vars))

    sql = f"""-- groupby domain sizes for {vertex.relation_name}
            SELECT MIN(domain_size)
            FROM groupby_domain_sizes
            WHERE relation_name = '{vertex.relation_name}'
            AND groupby_vars_name = '{groupby_vars_str}'
            AND ({predicate_conditions_str})
            ;"""
    return sql


def fetch_groupby_domain_sizes(con: DuckDBPyConnection, join_graph: JoinGraph) -> DomainSizeStats:
    """
    Fetch groupby domain sizes for all vertices.
    Returns a dictionary mapping [alias] -> domain_size
    """
    statistics: DomainSizeStats = {}
    for v in join_graph.vertices.values():
        if len(v.groupby_vars) > 0:
            sql = _produce_groupby_domain_sizes_sql(v, join_graph)
            # print(sql)
            domain_size: int = execute_fetchone_sql(con, sql)[0]
            statistics[v.alias] = domain_size
    return statistics
