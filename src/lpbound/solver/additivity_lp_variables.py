from collections import defaultdict
from ortools.linear_solver import pywraplp

from lpbound.solver.solver_utils import entropy, sorted_set
from lpbound.utils.types import AliasColPair


def create_additivity_lp_variables(
    solver: pywraplp.Solver,
    join_pool_map: dict[AliasColPair, int],
    join_pool_alias_map: dict[str, list[int]],
    aliases: list[str],
    verbose: bool = False,
) -> tuple[dict[str, pywraplp.Variable], pywraplp.Objective]:
    """
    join_pools: list of join pools; we actually just need their ids
    join_pool_alias_map: map of aliases to the pool_ids they are in
    relations: list of relations
    create the following variables:
        - for each join pool, we create a variable with the name {pool_index}, such as 1, 2, ...
        - for each alias, we create a variable with the name {0_alias}, such as 0_T, 0_MC, ...
    Then, we create some combinations:
        - for each relation, we create the union of variables in the relation
    """

    lp_variables = {}  # to store the variables used in the LP program
    join_pool_ids = set(join_pool_map.values())

    alias_variables_list: list[pywraplp.Variable] = []
    join_variables_map: defaultdict[str, int] = defaultdict(
        int
    )  # pool_id -> number of times it appears in the relations

    # create the variables for the join pools
    for pool_id in join_pool_ids:
        str_pool_id = str(pool_id)
        lp_variables[str_pool_id] = solver.NumVar(0, solver.infinity(), str_pool_id)
        if verbose:
            print(f"Variable {str_pool_id}: {lp_variables[str_pool_id]}")

    for pool_id in join_pool_map.values():
        join_variables_map[str(pool_id)] += 1

    # for each relation,
    # - we create the non-join variables: 0T, 0MC, ...
    # - we create the union of the variables in the relation: 0T_1_2 (if 1 and 2 are in the relation), 0MC_1 (if 1 is in the relation)
    for alias in aliases:
        non_join_variable = f"0{alias}"
        non_join_variable_lp_var = solver.NumVar(0, solver.infinity(), non_join_variable)
        if verbose:
            print(f"Variable {non_join_variable}: {non_join_variable_lp_var}")
        lp_variables[non_join_variable] = non_join_variable_lp_var

        # for all join pools that contain the alias, we create the union of the variables in the relation
        pool_ids = join_pool_alias_map[alias]
        alias_variables = entropy(sorted_set([non_join_variable] + [str(pool_id) for pool_id in pool_ids]))
        alias_variables_lp_var = solver.NumVar(0, solver.infinity(), alias_variables)
        if verbose:
            print(f"Variable {alias_variables}: {alias_variables_lp_var}")
        lp_variables[alias_variables] = alias_variables_lp_var

        alias_variables_list.append(alias_variables_lp_var)

        # add the local monotonicity inequality
        # h(R-vars) >= h(non_join_variable/join_variable)
        # e.g., h(0T_1_2) >= h(0T), h(0MC_1) >= h(0MC), h(0T_1) >= h(1)
        constraint, contraint_id = (
            lp_variables[alias_variables] >= lp_variables[non_join_variable],
            f"Mono Ineq.: H({alias_variables}) >= H({non_join_variable})",
        )
        if verbose:
            print(f"Constraint {contraint_id}")
        solver.Add(constraint, contraint_id)
        for pool_id in pool_ids:
            constraint, contraint_id = (
                lp_variables[alias_variables] >= lp_variables[str(pool_id)],
                f"Mono Ineq.: H({alias_variables}) >= H({str(pool_id)})",
            )
            if verbose:
                print(f"Constraint {contraint_id}")
            solver.Add(constraint, contraint_id)

        # add the additivity inequality
        # h(R-vars) <= sum_{X in R} h(X)
        # e.g., h(0T_1_2) <= h(0T) + h(1) + h(2)
        lp_variables_in_relation = [non_join_variable_lp_var] + [lp_variables[str(pool_id)] for pool_id in pool_ids]
        constraint, contraint_id = (
            lp_variables[alias_variables] <= solver.Sum(lp_variables_in_relation),
            f"Additivity Ineq.: H({alias_variables}) <= H({non_join_variable}) + "
            + " + ".join([f"H({str(pool_id)})" for pool_id in pool_ids]),
        )
        if verbose:
            print(f"Constraint {contraint_id}")
        solver.Add(constraint, contraint_id)

    # create the objective:
    #  sum_{R in relations}h(R-vars) - (# appearances of join_vars)h(join_vars)
    objective_entropy = solver.Objective()
    for alias_variables in alias_variables_list:
        objective_entropy.SetCoefficient(alias_variables, 1)

    for pool_id in join_variables_map:
        lp_var = lp_variables[pool_id]
        num_appearances = join_variables_map[pool_id]
        if num_appearances > 1:
            objective_entropy.SetCoefficient(lp_var, -(num_appearances - 1))
        else:
            print(f"Warning: pool_id {pool_id} appears only once")
    objective_entropy.SetMaximization()

    if verbose:
        print("---> lp_variables:")
        for key, value in lp_variables.items():
            print(f"Variable {key}: {value}")

    return lp_variables, objective_entropy
