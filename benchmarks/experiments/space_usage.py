import os
from duckdb import DuckDBPyConnection
from lpbound.config.benchmark_schema import BenchmarkSchema, load_benchmark_schema
from lpbound.config.paths import LpBoundPaths
from lpbound.duckdb_adapter.duckdb_manager import DatabaseManager
import sys

# add parent directory to the sys path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.acyclic.lpbound import build_lpbound_statistics


def build_benchmarks():

    for benchmark in ["stats", "joblight", "jobrange", "jobjoin"]:
        lpbound_config = LpBoundConfig(
            benchmark_name=benchmark,
            num_mcvs=5000,
            num_buckets=128,
            p_max=10,
        )
        _ = build_lpbound_statistics(lpbound_config)


def compute_space_usage(benchmark_names: list[str]):

    space_usage_dir = LpBoundPaths.RESULTS_DIR / "space_usage"
    space_usage_dir.mkdir(exist_ok=True)

    print("Space usage for each benchmark:")
    res: str = ""
    res += "Space usage for each benchmark:\n"

    # get the space usage for each benchmark
    for benchmark in benchmark_names:
        config = LpBoundConfig(
            benchmark_name=benchmark,
            p_max=10,
        )
        num_bytes = _compute_space_usage(config)
        print(f"{benchmark}: {num_bytes / 1000 / 1000} MB")
        res += f"{benchmark}: {num_bytes / 1000 / 1000} MB\n"

    with open(space_usage_dir / "lpbound.txt", "w") as f:
        f.write(res)


def _compute_space_usage(config: LpBoundConfig) -> int:
    """
    Get the number of lp-norms from the norms table in the database
    """
    schema_data: BenchmarkSchema = load_benchmark_schema(config)
    con: DuckDBPyConnection = DatabaseManager(schema_data).create_or_load_db(read_only=True)

    # get the number of rows in the norms table
    sql = "SELECT COUNT(*) FROM norms;"
    num_rows = con.execute(sql).fetchone()[0]

    # each row stores the lp-norms for an MCV or a bucket for a join variable
    # also the indices of relation, join variable, and the MCVs or buckets
    # each row has 10 + 1 + 1 lp-norms, i.e., l0, l1, ..., l10, linf
    # and three indices
    num_lp_norms = num_rows * (10 + 1 + 1)

    # each lp-norm is a float64, which takes 8 bytes
    num_bytes = num_lp_norms * 8

    return num_bytes


if __name__ == "__main__":

    # if the number of MCVs or buckets is changed, we need to rebuild the statistics
    # build_benchmarks()
    compute_space_usage(["joblight", "stats", "jobrange", "jobjoin"])
