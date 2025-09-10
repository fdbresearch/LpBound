import os
from tqdm import tqdm
import sys

import pandas as pd

# add parent directory to the sys path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.config.paths import LpBoundPaths
from lpbound.acyclic.lpbound import estimate

from benchmarks.approaches.postgres.postgres import get_dbConn, reset_cache, JoinHint


def compute_subquery_cardinalities(config: LpBoundConfig):
    """
    Compute the lattics for the given queries and method
    Store the results in the statistics folder
    """

    benchmark = config.benchmark_name
    assert benchmark.lower() in ["joblight", "jobrange", "stats"]

    dump_lp_program_path = f"{LpBoundPaths.OUTPUT_DIR}/lp_programs/{benchmark}_subqueries/"
    os.makedirs(dump_lp_program_path, exist_ok=True)

    # read the subqueries for the benchmark
    subqueries_file = f"{LpBoundPaths.WORKLOADS_DIR}/{benchmark}/{benchmark}_subqueries.csv"
    df = pd.read_csv(subqueries_file, sep="|")

    this_query_ids = []
    tables_list = []
    estimates = []

    for i in tqdm(range(len(df))):
        row = df.iloc[i]
        query_id = row["QueryID"]
        tables = row["Tables"].split()
        subquery_sql = row["SQL"]

        this_estimate = estimate(
            input_query_sql=subquery_sql,
            config=config,
            dump_lp_program_file=f"{dump_lp_program_path}/{benchmark}_subquery_estimations_{i+1}.lp",
            verbose=False,
        )

        # record the statistics
        this_query_ids.append(query_id)
        tables_list.append(" ".join(sorted(tables)))
        estimates.append(this_estimate)

        # output statistics
        subquery_estimate_results = pd.DataFrame()
        subquery_estimate_results["QueryID"] = this_query_ids
        subquery_estimate_results["Tables"] = tables_list
        subquery_estimate_results["lpbound_Estimate"] = estimates

        # write the statistics
        output_dir = f"{LpBoundPaths.RESULTS_DIR}/evaluation_time/subquery_estimations/{benchmark}"
        os.makedirs(
            output_dir,
            exist_ok=True,
        )
        subquery_estimate_results.to_csv(
            f"{output_dir}/lpbound_subquery_estimations.csv",
            index=False,
        )


def get_evaluation_time_using_psql(
    benchmark: str,
    method: str,  # lpbound, safebound, postgres, truecardinality, factorjoin, dbx
    num_runs: int = 5,
    dbStatisticsTarget: int = 100,
):
    """
    Make sure queryJGs and query_ids are consistent
    query_ids are 1-indexed
    """

    this_query_ids: list[str] = []
    run_labels: list[int] = []
    runtimes: list[float] = []

    # NOTE: get the dbConn
    dbConn = get_dbConn(benchmark, groupby=False)
    dbConn.changeStatisticsTarget(dbStatisticsTarget)

    # read the subquery statistics
    # check if the subquery statistics exist
    if not os.path.exists(
        f"{LpBoundPaths.RESULTS_DIR}/evaluation_time/subquery_estimations/{benchmark}/{method}_subquery_estimations.csv"
    ):
        print(f"Subquery statistics for {benchmark} and {method} do not exist. Computing subquery statistics...")
        compute_all_subquery_cardinalities()

    subquery_estimation_df = pd.read_csv(
        f"{LpBoundPaths.RESULTS_DIR}/evaluation_time/subquery_estimations/{benchmark}/{method}_subquery_estimations.csv"
    )
    sqls_file = f"{LpBoundPaths.WORKLOADS_DIR}/{benchmark}/{benchmark}Queries.sql"
    with open(sqls_file, "r") as f:
        sqls = f.readlines()

    # unique query ids
    query_ids = subquery_estimation_df["QueryID"].unique()

    assert len(sqls) == len(query_ids)

    # progress bar
    pbar = tqdm(total=len(query_ids) * num_runs)

    for run_i in range(num_runs):
        print("=" * 40)
        print(f"Run {run_i + 1}...")
        reset_cache()
        dbConn.reset()  # this loads pg_hint_plan again

        # NOTE: construct the queryHints
        for query_id, sql in zip(query_ids, sqls):

            df_query_id = subquery_estimation_df[subquery_estimation_df["QueryID"] == query_id]

            queryHints = []
            if method != "postgres":
                for i in range(len(df_query_id)):  # for every subquery
                    row = df_query_id.iloc[i]
                    tables = row["Tables"].split()
                    estimate = row[f"{method}_Estimate"]

                    hint = JoinHint(tables, estimate)
                    queryHints.append(hint)

            # execute the query; SELECT * FROM -> SELECT COUNT(*) FROM
            runtime = dbConn.executeQueryWithHints(sql, queryHints, countStar=True)

            runtimes.append(runtime)
            this_query_ids.append(query_id)
            run_labels.append(run_i + 1)

            # record the runtime
            runtime_results = pd.DataFrame()
            runtime_results["QueryID"] = this_query_ids
            runtime_results["Run"] = run_labels
            runtime_results["Runtime"] = runtimes

            save_runtime(benchmark, method, runtime_results)

            pbar.update(1)

    dbConn.close()


def save_runtime(benchmark, method, runtime):

    # compute the mean runtime over the runs
    mean_runtime_results = runtime.groupby("QueryID").mean().reset_index()
    del mean_runtime_results["Run"]

    os.makedirs(
        f"{LpBoundPaths.RESULTS_DIR}/evaluation_time/{benchmark}",
        exist_ok=True,
    )
    mean_runtime_results.to_csv(
        f"{LpBoundPaths.RESULTS_DIR}/evaluation_time/{benchmark}/{method}_evaluation_time.csv",
        index=False,
    )


def run_evaluation_time_experiments():
    methods = ["lpbound", "safebound", "postgres", "truecardinality", "factorjoin", "dbx", "bayescard"]
    benchmarks = ["joblight", "jobrange", "stats"]

    for benchmark in benchmarks:
        for method in methods:
            if method == "bayescard" and benchmark == "jobrange":  # we do not have bayescard for jobrange
                continue
            print(f"Running runtime experiments for {benchmark} and {method}...")
            get_evaluation_time_using_psql(benchmark, method, num_runs=5)


def compute_all_subquery_cardinalities():
    benchmarks = ["joblight", "jobrange", "stats"]
    for benchmark in benchmarks:
        config = LpBoundConfig(
            benchmark_name=benchmark,
            p_max=10,
        )
        compute_subquery_cardinalities(config)


if __name__ == "__main__":

    compute_all_subquery_cardinalities()
    run_evaluation_time_experiments()
