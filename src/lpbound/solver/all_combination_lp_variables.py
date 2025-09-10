import multiprocessing
from ortools.linear_solver import pywraplp
from itertools import combinations

from lpbound.solver.solver_utils import entropy, sorted_set


def generate_combinations(i: int, objective_entropy_variables: list[str]):
    return list(combinations(objective_entropy_variables, i))


def create_and_get_lp_variables(
    solver: pywraplp.Solver, join_pool_ids: set[int], aliases: list[str], verbose: bool = False
) -> tuple[dict[str, pywraplp.Variable], str, list[list[str]]]:
    """
    join_pools: list of join pools; we actually just need their ids
    relations: list of relations
    create the following variables:
        - for each join pool, we create a variable with the name {pool_index}, such as 1, 2, ...
        - for each alias, we create a variable with the name {0_alias}, such as 0_T, 0_MC, ...
    Then, we create the combinations of these variables, such as {0_T, 0_MC}, {0_T, 0_MC, 1}, ...
    """

    lp_variables: dict[str, pywraplp.Variable] = {}  # to store the variables used in the LP program
    # join_pool_ids = set(join_pool_map.values())

    base_variables: list[str] = []
    # create the variables for the join pools
    for pool_id in join_pool_ids:
        base_variables.append(str(pool_id))
    # create the variables for the aliases
    for alias in aliases:
        base_variables.append(f"0{alias}")

    objective_entropy_variables = sorted_set(base_variables)
    objective_entropy = entropy(objective_entropy_variables)

    entropy_variables_combinations = []
    # create the combinations of the variables in parallel
    with multiprocessing.Pool() as pool:
        combination_lists = pool.starmap(
            generate_combinations,
            [(i, base_variables) for i in range(1, len(base_variables) + 1)],
        )
        entropy_variables_combinations = [list(combo) for sublist in combination_lists for combo in sublist]

    # once we have all possible combinations, we can create the LP variables
    for entropy_variables_combination in entropy_variables_combinations:
        lp_variables[entropy(entropy_variables_combination)] = solver.NumVar(
            0, solver.infinity(), entropy(entropy_variables_combination)
        )

    if verbose:
        print("---> lp_variables:")
        print("Objective entropy: ", objective_entropy)
        print("Entropy variables combinations: ", entropy_variables_combinations)
        for key, value in lp_variables.items():
            print(f"Variable {key}: {value}")

    return lp_variables, objective_entropy, entropy_variables_combinations
