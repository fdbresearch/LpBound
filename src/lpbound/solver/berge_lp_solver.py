from ortools.linear_solver import pywraplp
import numpy as np

from lpbound.solver.solver_utils import print_objective
from lpbound.solver.additivity_lp_variables import create_additivity_lp_variables
from lpbound.solver.statistics_inequalities import add_domain_size_inequalities, add_statistics_inequalities
from lpbound.utils.types import DomainSizeStats, Stats, AliasColPair


def run_berge_lp_solver(
    join_pool_map: dict[AliasColPair, int],
    join_pool_alias_map: dict[str, list[int]],
    aliases: list[str],
    statistics_dict: Stats,
    domain_size_statistics: DomainSizeStats,
    demo_mode: bool = False,
    dump_lp_program_file: str | None = None,
    verbose: bool = False,
) -> tuple[float, list[tuple[str, float]]]:
    # initialize solver and create variables for the lp program
    solver = pywraplp.Solver.CreateSolver("GLOP")

    # join_pool_map = join_graph.join_pool_map
    # join_pool_alias_map = join_graph.join_pool_alias_map
    # aliases = join_graph.vertices.keys()

    if verbose:
        print("\n\n")
        print("----> join_pool_map: ", join_pool_map)
        print("----> join_pool_alias_map: ", join_pool_alias_map)
        print("----> aliases: ", aliases)

    # add constraints
    lp_variables, objective_entropy = create_additivity_lp_variables(
        solver,
        join_pool_map,
        join_pool_alias_map,
        aliases,
        verbose=verbose,
    )

    # add statistics inequalities
    add_statistics_inequalities(
        solver,
        lp_variables,
        statistics_dict,
        join_pool_map,
        join_pool_alias_map,
        verbose=verbose,
    )

    if domain_size_statistics:
        add_domain_size_inequalities(
            solver,
            lp_variables,
            domain_size_statistics,
            verbose=verbose,
        )

    if verbose:
        # print the constraints
        for constraint in solver.constraints():
            print(f"{constraint.name()}: {constraint.ub()}")

        print("----> objective entropy: ")
        print_objective(objective_entropy, lp_variables)

    if dump_lp_program_file:
        with open(dump_lp_program_file, "w") as f:
            lp_object = solver.ExportModelAsLpFormat(False)
            lp_string = str(lp_object)  # Convert SwigPyObject to string
            with open(dump_lp_program_file, "w") as f:
                f.write(lp_string)

    status = solver.Solve()

    if status == pywraplp.Solver.OPTIMAL:
        solution = solver.Objective().Value()
        if verbose:
            print("\nConstraints used in the optimal solution:")
            for constraint in solver.constraints():
                dual_value = constraint.dual_value()
                if dual_value > 0:
                    print(f"{constraint.name()}: dual value = {dual_value}")
            print(f"solution: {2 ** solution}")
    else:
        solution = np.inf

    if demo_mode:
        # return the active constraints
        active_constraints = []
        for constraint in solver.constraints():
            dual_value = constraint.dual_value()
            if dual_value > 0:
                active_constraints.append((constraint.name(), dual_value))
        return 2**solution, active_constraints

    return 2**solution, []
