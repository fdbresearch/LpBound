from duckdb import DuckDBPyConnection

from lpbound.acyclic.join_graph.vertex import Vertex
from lpbound.acyclic.join_graph.join_graph import JoinGraph


def compute_predicate_ids(con: DuckDBPyConnection, join_graph: JoinGraph) -> None:
    """
    Compute MCV and range IDs for all vertices in the join graph.
    This function modifies the vertices by setting MCVs and range IDs.
    """
    for v in join_graph.vertices.values():
        _compute_mcv_ids(con, v)
        _compute_range_ids(con, v)


def _compute_mcv_ids(con: DuckDBPyConnection, vertex: Vertex) -> None:
    """
    Compute MCV IDs for equality predicates in a vertex.
    This function modifies the vertex's predicates by setting their mcv_id.
    """
    for predicate in vertex.equalities:
        mcv_table_name = f"{vertex.relation_name}_{predicate.col_id}_mcv"

        # Handle different types
        if predicate.value_type == "int" or predicate.value_type == "date":
            sql = f"SELECT mcv_id FROM {mcv_table_name} WHERE {predicate.col_id} = {predicate.value}"
        elif predicate.value_type == "str":
            sql = f"SELECT mcv_id FROM {mcv_table_name} WHERE {predicate.col_id} = '{predicate.value}'"
        else:
            raise ValueError(f"Unsupported value type: {predicate.value_type}")

        # There might be no MCV for this predicate
        res: tuple[int] | None = con.execute(sql).fetchone()
        if res is not None:
            predicate_id = res[0]
            predicate.set_mcv_id(predicate_id)


def _compute_range_ids(con: DuckDBPyConnection, vertex: Vertex) -> None:
    """
    Compute range IDs for inequality predicates in a vertex.
    This function modifies the vertex's predicates by setting their range_id.
    """
    for predicate in vertex.inequalities:
        range_table_name = f"{vertex.relation_name}_{predicate.col_id}_histogram"

        # Find the bucket that covers the predicate's range
        left_value: str | int | float | None = predicate.left_value
        right_value: str | int | float | None = predicate.right_value

        if predicate.value_type == "int":
            left_value = f"{predicate.left_value}"
            right_value = f"{predicate.right_value}"
        elif predicate.value_type == "date":
            left_value = f"to_timestamp({predicate.left_value})::TIMESTAMP"  # this is required for DuckDB v0.10
            right_value = f"to_timestamp({predicate.right_value})::TIMESTAMP"  # this is required for DuckDB v0.10
        elif predicate.value_type == "str":
            left_value = f"'{predicate.left_value}'"
            right_value = f"'{predicate.right_value}'"
        else:
            raise ValueError(f"Unsupported value type: {predicate.value_type}")

        conditions: list[str] = []
        if predicate.left_value is not None:
            conditions.append(f"(lower_bound IS NULL OR lower_bound <= {left_value})")
        else:
            conditions.append("lower_bound IS NULL")

        if predicate.right_value is not None:
            conditions.append(f"(upper_bound IS NULL OR upper_bound >= {right_value})")
        else:
            conditions.append("upper_bound IS NULL")

        conditions_str = " AND ".join(conditions)

        sql = f"""SELECT bucket_id FROM {range_table_name} 
        WHERE {conditions_str}
        ORDER BY bucket_id ASC
        LIMIT 1
        """

        res: tuple[int] | None = con.execute(sql).fetchone()
        if res is not None:
            bucket_id = res[0]
            predicate.set_range_id(bucket_id)
            # print(f"Found bucket {bucket_id} for predicate {predicate}")
        else:
            print(f"Warning: No bucket found for predicate {predicate}!!!")
