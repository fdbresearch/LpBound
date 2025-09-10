import numpy as np
from ortools.linear_solver import pywraplp

from lpbound.solver.all_combination_lp_variables import create_and_get_lp_variables
from lpbound.solver.basic_shannon_inequalities import (
    add_basic_shannon_monotonicity_inequalities,
    add_basic_shannon_submodularity_inequalities,
)
from lpbound.solver.statistics_inequalities import (
    add_domain_size_inequalities,
    add_statistics_inequalities,
    get_groupby_objective_entropy,
)
from lpbound.utils.types import DomainSizeStats, Stats, AliasColPair


def run_base_lp_solver(
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

    # add constraints
    lp_variables, objective_entropy, entropy_variables_combinations = create_and_get_lp_variables(
        solver,
        set(join_pool_map.values()),
        aliases,
        verbose=False,
    )

    add_basic_shannon_monotonicity_inequalities(
        solver,
        lp_variables,
        entropy_variables_combinations,
        verbose=False,
    )
    add_basic_shannon_submodularity_inequalities(
        solver,
        lp_variables,
        entropy_variables_combinations,
        verbose=False,
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
        # update the objective entropy to be only the groupby variables
        objective_entropy = get_groupby_objective_entropy(domain_size_statistics)

    if verbose:
        print("----> objective entropy: ", objective_entropy)

    solver.Maximize(lp_variables[objective_entropy])

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
