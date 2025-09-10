import re


from lpbound.config.lpbound_config import LpBoundConfig

# from lpbound.solver.lp_solver import run_solver
from lpbound.acyclic.stat_fetcher.main import fetch_statistics_for_query
from lpbound.acyclic.stat_generator.main import create_lpbound_statistics
from lpbound.solver.main import run_solver


def _add_count_to_distinct(sql_query: str) -> str:
    # Regular expression to match SELECT DISTINCT(...) pattern
    pattern = r"SELECT\s+DISTINCT\s*\((.*?)\)"

    # Function to replace the matched pattern
    def replace_with_count(match: re.Match[str]) -> str:
        distinct_content = match.group(1)
        return f"SELECT COUNT(DISTINCT({distinct_content}))"

    # Use re.sub to replace the pattern in the SQL query
    modified_query = re.sub(pattern, replace_with_count, sql_query, flags=re.IGNORECASE)

    return modified_query


def estimate(
    input_query_sql: str,
    config: LpBoundConfig,
    dump_lp_program_file: str | None = None,
    verbose: bool = False,
) -> float:
    # handle groupby queries
    query_sql = input_query_sql
    if "*" in input_query_sql:  # convert the * to COUNT(*)
        if "COUNT(*)" not in input_query_sql and "count(*)" not in input_query_sql:
            query_sql = input_query_sql.replace("*", "COUNT(*)")
    else:
        assert "DISTINCT" in input_query_sql  # the query must have DISTINCT
    # is_groupby_query = "DISTINCT" in query_sql
    # if is_groupby_query:
    #     # replace the DISTINCT(...) with COUNT(DISTINCT(...))
    #     query_sql = _add_count_to_distinct(query_sql)

    statistics, domain_size_statistics, jg = fetch_statistics_for_query(query_sql, config)

    method = "base" if jg.is_groupby else "berge"
    estimation, _ = run_solver(
        jg,
        statistics,
        domain_size_statistics,
        method=method,
        demo_mode=False,
        dump_lp_program_file=dump_lp_program_file,
        verbose=verbose,
    )

    return estimation


def build_lpbound_statistics(lpbound_config: LpBoundConfig) -> dict[str, float]:
    """Build LPBound statistics for the given benchmark."""

    times_dict = create_lpbound_statistics(lpbound_config)
    return times_dict
