from duckdb import DuckDBPyConnection

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.duckdb_adapter.duckdb_manager import DatabaseManager
from lpbound.config.benchmark_schema import load_benchmark_schema, BenchmarkSchema

from lpbound.acyclic.join_graph.sql_parser import parse_sql_query_to_join_graph
from lpbound.acyclic.join_graph.join_graph import JoinGraph
from lpbound.acyclic.stat_fetcher.join_pool_statistics import compute_join_pool_domain_sizes
from lpbound.acyclic.stat_fetcher.jg_pred_stats import fetch_groupby_domain_sizes, fetch_predicate_norms
from lpbound.acyclic.stat_fetcher.vertex_base_stats import fetch_base_norms
from lpbound.acyclic.stat_fetcher.vertex_local_pred_stats import compute_predicate_ids

from lpbound.utils.types import DomainSizeStats, Stats


def fetch_statistics_for_query(query: str, config: LpBoundConfig) -> tuple[Stats, DomainSizeStats, JoinGraph]:
    """
    Fetch statistics for a given query.
    """
    schema_data: BenchmarkSchema = load_benchmark_schema(config)
    con: DuckDBPyConnection = DatabaseManager(schema_data).create_or_load_db(read_only=True)

    jg = parse_sql_query_to_join_graph(query, schema_data)

    # Use the standalone fetch_statistics function
    statistics = _fetch_statistics(con, jg, config.p_max)

    domain_size_statistics: DomainSizeStats = {}
    if jg.is_groupby:
        domain_size_statistics = fetch_groupby_domain_sizes(con, jg)

    # print("----> domain_size_statistics: ", domain_size_statistics)
    for key, value in statistics.items():
        # print(key, value)
        assert len(value) > 0

    return statistics, domain_size_statistics, jg


def _fetch_statistics(con: DuckDBPyConnection, join_graph: JoinGraph, max_p: int) -> dict[tuple[str, str], list[float]]:
    """
    Main function to fetch all statistics for a join graph.
    Returns a dictionary mapping (alias, column) to statistics.
    """
    # # Print all vertices for debugging
    # for v in join_graph.vertices.values():
    #     print(v)

    # 1. Compute MCV and range IDs for all vertices
    compute_predicate_ids(con, join_graph)

    # 2. Fetch predicate norms for each join column
    statistics: dict[tuple[str, str], list[float]] = fetch_predicate_norms(con, join_graph, max_p)

    # 3. Compute join pool domain sizes
    compute_join_pool_domain_sizes(con, join_graph, statistics)

    # 4. Compute base norms
    statistics = fetch_base_norms(con, join_graph, statistics, max_p)

    return statistics
