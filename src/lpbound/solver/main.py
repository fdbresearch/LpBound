from lpbound.acyclic.join_graph.join_graph import JoinGraph

from lpbound.solver.berge_lp_solver import run_berge_lp_solver
from lpbound.solver.basic_lp_solver import run_base_lp_solver
from lpbound.utils.types import DomainSizeStats, Stats


def run_solver(
    join_graph: JoinGraph,
    statistics_dict: Stats,
    domain_size_statistics: DomainSizeStats,
    method: str = "berge",
    demo_mode: bool = False,
    dump_lp_program_file: str | None = None,
    verbose: bool = False,
):

    join_pool_map = join_graph.join_pool_map
    join_pool_alias_map = join_graph.join_pool_alias_map
    aliases = list(join_graph.vertices.keys())

    if method == "berge":
        return run_berge_lp_solver(
            join_pool_map,
            join_pool_alias_map,
            aliases,
            statistics_dict,
            domain_size_statistics,
            demo_mode=demo_mode,
            dump_lp_program_file=dump_lp_program_file,
            verbose=verbose,
        )
    elif method == "base":
        return run_base_lp_solver(
            join_pool_map,
            join_pool_alias_map,
            aliases,
            statistics_dict,
            domain_size_statistics,
            demo_mode=demo_mode,
            dump_lp_program_file=dump_lp_program_file,
            verbose=verbose,
        )
    else:
        raise ValueError(f"Method {method} not supported")
