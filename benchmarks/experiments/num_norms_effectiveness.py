import os
from tqdm import tqdm
import sys

import pandas as pd

# add parent directory to the sys path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.config.paths import LpBoundPaths
from lpbound.acyclic.lpbound import build_lpbound_statistics, estimate


def estimation_experiment(config: LpBoundConfig, verbose=False):

    benchmark = config.benchmark_name

    estimation_result_path = f"{LpBoundPaths.RESULTS_DIR}/num_norms_effectiveness/{benchmark}/"
    os.makedirs(estimation_result_path, exist_ok=True)

    # NOTE: workload file
    workload_file = f"{LpBoundPaths.WORKLOADS_DIR}/{benchmark}/{benchmark}Queries.sql"
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
            verbose=verbose,
        )

        estimates.append(overall_estimation)

    result = pd.DataFrame(
        {
            "QueryID": this_query_ids,
            "lpbound_Estimate": estimates,
        }
    )
    result.to_csv(f"{estimation_result_path}/lpbound_{benchmark}_num_norms_{config.p_max}.csv", index=False)


if __name__ == "__main__":

    config = LpBoundConfig(
        benchmark_name="joblight",
        p_max=31,
    )
    build_lpbound_statistics(config)

    num_norms = list([i + 1 for i in range(31)])
    for num_norm in num_norms:
        config = LpBoundConfig(
            benchmark_name="joblight",
            p_max=num_norm,
        )
        estimation_experiment(config)
