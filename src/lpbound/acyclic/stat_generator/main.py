#!/usr/bin/env python3
"""
execute_sqls.py

1) Builds/loads a DuckDB database connection via DatabaseManager.
2) Generates the Lp-norm SQL commands from sql_generation.py for the chosen benchmark.
3) Executes them one by one, measuring time per 'tag'.
4) Prints a time breakdown.

Usage:
  python execute_sqls.py
"""

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.acyclic.stat_generator.sql_statistics_generator import generate_all_sql_for_benchmark

# from lpbound.acyclic.stat_generator.sm_experiments import generate_sm_commands
from lpbound.config.paths import LpBoundPaths
from lpbound.config.benchmark_schema import load_benchmark_schema
from lpbound.acyclic.stat_generator.sql_utils import SqlCommand
from lpbound.duckdb_adapter.duckdb_manager import DatabaseManager

from lpbound.utils.sql_execution import execute_with_timing, get_time_breakdown_string, write_commands_to_file


def create_lpbound_statistics(lpbound_config: LpBoundConfig) -> dict[str, float]:

    schema_data = load_benchmark_schema(lpbound_config)

    # 2) Build or load the DB connection
    #    The DatabaseManager references config.DATA_DIR, etc.
    db_manager = DatabaseManager(benchmark_schema=schema_data)
    con = db_manager.create_or_load_db(read_only=False)

    # 3) Generate the Lp-norm SQL commands
    commands: list[SqlCommand] = generate_all_sql_for_benchmark(con, lpbound_config)

    # 3a) write the commands to a file for inspection
    groupby_suffix = "_groupby" if lpbound_config.enable_groupby else ""
    sql_file = LpBoundPaths.GENERATED_SQL_DIR / f"{lpbound_config.benchmark_name}{groupby_suffix}_lpnorm_queries.sql"
    write_commands_to_file(commands, str(sql_file))
    print(f"Done. See {LpBoundPaths.GENERATED_SQL_DIR} for all generated SQL statements.")

    # 4) Execute with timing
    times_dict = execute_with_timing(commands, con)

    # 5) Print a breakdown
    print(get_time_breakdown_string(times_dict))

    con.close()

    return times_dict
