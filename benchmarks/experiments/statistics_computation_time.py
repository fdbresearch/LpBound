import os
import sys

from lpbound.config.paths import LpBoundPaths
from lpbound.utils.sql_execution import get_overall_time

# add parent directory to the sys path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.acyclic.lpbound import build_lpbound_statistics
from lpbound.cyclic.stats_generator_by_duckdb import build_subgraph_matching_statistics


def build_benchmarks(benchmark_names: list[str]):

    time_dicts_dir = LpBoundPaths.RESULTS_DIR / "statistics_computation_time"
    time_dicts_dir.mkdir(exist_ok=True)

    print("--- LpBound only l1 ---")
    res = ""
    res += "LpBound-l1:\n"
    for benchmark in benchmark_names:
        lpbound_config = LpBoundConfig(
            benchmark_name=benchmark,
            num_mcvs=5000,
            num_buckets=128,
            p_max=1,
            include_l0=False,
            include_l_inf=False,
        )
        time_dict = None
        if benchmark == "subgraph_matching":
            time_dict = build_subgraph_matching_statistics(lpbound_config)
        else:
            time_dict = build_lpbound_statistics(lpbound_config)
        res += f"{benchmark}: {get_overall_time(time_dict):.1f} seconds\n"
        # write the time dict to disk
        # with open(time_dicts_dir / f"stats_comp_time_{benchmark}_l1.txt", "w") as f:
        #     # f.write(get_time_breakdown_string(time_dict))
        #     f.write(f"{benchmark}: {get_overall_time(time_dict)} seconds")

    print("-" * 80)
    print("--- LpBound all lp-norms ---")
    res += "\nLpBound:\n"
    for benchmark in benchmark_names:
        lpbound_config = LpBoundConfig(
            benchmark_name=benchmark,
            num_mcvs=5000,
            num_buckets=128,
            p_max=10,
        )
        if benchmark == "subgraph_matching":
            time_dict = build_subgraph_matching_statistics(lpbound_config)
        else:
            time_dict = build_lpbound_statistics(lpbound_config)
        res += f"{benchmark}: {get_overall_time(time_dict):.1f} seconds\n"
        # with open(time_dicts_dir / f"stats_comp_time_{benchmark}_all_lpnorms.txt", "w") as f:
        #     f.write(get_time_breakdown_string(time_dict))

    with open(time_dicts_dir / "lpbound.txt", "w") as f:
        f.write(res)


if __name__ == "__main__":

    build_benchmarks(["stats", "joblight", "jobrange", "jobjoin", "subgraph_matching"])
