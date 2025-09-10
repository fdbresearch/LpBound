import os
from duckdb import DuckDBPyConnection
from tqdm import tqdm
import sys

import pandas as pd

# add parent directory to the sys path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.config.paths import LpBoundPaths
from lpbound.acyclic.lpbound import build_lpbound_statistics, estimate

from benchmarks.approaches.dbx.dbx import MSSQLHelper
from benchmarks.approaches.postgres.postgres import DatabaseConnection, get_dbConn
from benchmarks.approaches.duckdb_helper.duckdb_helper import get_connection, get_duckdb_estimate


def estimation_experiment(config: LpBoundConfig, variant="small", verbose=False):
    """variant = [small, large, mixed]"""
    benchmark = config.benchmark_name

    dump_lp_program_path = f"{LpBoundPaths.OUTPUT_DIR}/lp_programs/{benchmark}_gby/"
    estimation_result_path = f"{LpBoundPaths.RESULTS_DIR}/accuracy_groupby/{benchmark}/"
    os.makedirs(dump_lp_program_path, exist_ok=True)
    os.makedirs(estimation_result_path, exist_ok=True)

    # NOTE: workload file
    workload_file = f"{LpBoundPaths.WORKLOADS_DIR}/{benchmark}_gby/{benchmark}_groupby_{variant}.sql"
    # read the query from the workload file
    with open(workload_file, "r") as f:
        queries = f.readlines()
    query_ids = range(1, len(queries) + 1)

    estimates = []
    this_query_ids = []

    for i in tqdm(range(len(query_ids))):
        query_id = query_ids[i]

        this_query_ids.append(query_id)
        query_sql = queries[query_id - 1]

        # estimation
        overall_estimation = estimate(
            input_query_sql=query_sql,
            config=config,
            dump_lp_program_file=f"{dump_lp_program_path}/{benchmark}_groupby_{variant}_{query_id}.lp",
            verbose=verbose,
        )

        estimates.append(overall_estimation)

    result = pd.DataFrame(
        {
            "QueryID": this_query_ids,
            "lpbound_Estimate": estimates,
        }
    )
    result.to_csv(f"{estimation_result_path}/lpbound_{benchmark}_groupby_{variant}_estimations.csv", index=False)


def build_benchmarks():
    for benchmark in ["joblight", "jobrange", "stats"]:
        lpbound_config = LpBoundConfig(
            benchmark_name=benchmark,
            num_mcvs=5000,
            num_buckets=128,
            p_max=10,
            enable_groupby=True,
        )
        build_lpbound_statistics(lpbound_config)


def estimation_benchmarks():
    for variant in ["small", "large", "mixed"]:
        for benchmark in ["joblight", "jobrange", "stats"]:
            config = LpBoundConfig(
                benchmark_name=benchmark,
                p_max=10,
                enable_groupby=True,
            )
            estimation_experiment(config, variant, verbose=False)


if __name__ == "__main__":

    build_benchmarks()
    estimation_benchmarks()
