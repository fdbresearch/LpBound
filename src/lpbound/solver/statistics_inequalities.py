# pyright: reportMissingTypeStubs=false, reportOperatorIssue=false
# pyright: reportUnknownVariableType=information, reportUnknownMemberType=information, reportUnknownArgumentType=information

from ortools.linear_solver import pywraplp
import numpy as np

from lpbound.solver.solver_utils import entropy
from lpbound.utils.types import DomainSizeStats, Stats, AliasColPair


def add_statistics_inequalities(
    solver: pywraplp.Solver,
    lp_variables: dict[str, pywraplp.Variable],
    statistics_dict: Stats,  # [(alias, join_column)] -> [lp_norm]
    join_pool_map: dict[AliasColPair, int],
    join_pool_alias_map: dict[str, list[int]],
    verbose: bool = False,
):

    counter = 1

    for key, norms in statistics_dict.items():
        alias, join_column = key
        pool_id = join_pool_map[(alias, join_column)]
        alias_pool_ids = join_pool_alias_map[alias]

        alias_star_entropy = entropy([str(pool_id) for pool_id in alias_pool_ids] + [f"0{alias}"])
        join_variable_entropy = str(pool_id)

        # print(f"alias_star_entropy: {alias_star_entropy}, join_variable_entropy: {join_variable_entropy}")

        for p, norm in enumerate(norms):
            if p == 0:  # skip l0 norm
                continue

            if p == len(norms) - 1:  # linf norm
                constraint, contraint_id = (
                    lp_variables[alias_star_entropy] - lp_variables[join_variable_entropy] <= np.log2(float(norm)),
                    f"Statistics Ineq. {counter}: h({alias_star_entropy}) - h({join_variable_entropy}) <= log2({norm}) | inf | {alias}.{join_column}",
                )
                if verbose:
                    print(
                        f"Statistics Ineq. {counter}: h({alias_star_entropy}) - h({join_variable_entropy}) <= log2({norm})"
                    )
                solver.Add(constraint, contraint_id)
            else:  # l1, l2, ...
                constraint, contraint_id = (
                    p * lp_variables[alias_star_entropy] - (p - 1) * lp_variables[join_variable_entropy]
                    <= p * np.log2(float(norm)),
                    f"Statistics Ineq. {counter}: {p} * h({alias_star_entropy}) - {p - 1} * h({join_variable_entropy}) <= {p} * log2({norm}) | {p} | {alias}.{join_column}",
                )
                if verbose:
                    print(
                        f"Statistics Ineq. {counter}: {p} * h({alias_star_entropy}) - {p - 1} * h({join_variable_entropy}) <= {p} * log2({norm})"
                    )
                solver.Add(constraint, contraint_id)

            counter += 1


def add_domain_size_inequalities(
    solver: pywraplp.Solver,
    lp_variables: dict[str, pywraplp.Variable],
    domain_size_statistics: DomainSizeStats,
    verbose: bool = False,
):
    """
    Add domain size inequalities to the solver.
    h(groupby_vars) <= log2(domain_size)
    """
    counter = 1
    for alias, domain_size in domain_size_statistics.items():
        groupby_vars_entropy = entropy([f"0{alias}"])
        constraint, contraint_id = (
            lp_variables[groupby_vars_entropy] <= np.log2(float(domain_size)),
            f"Domain Size Ineq. {counter}: h({groupby_vars_entropy}) <= log2({domain_size}) | inf | {alias}.*",
        )
        if verbose:
            print(f"Domain Size Ineq. {counter}: h({groupby_vars_entropy}) <= log2({domain_size})")

        solver.Add(constraint, contraint_id)
        counter += 1


def get_groupby_objective_entropy(
    domain_size_statistics: DomainSizeStats,
) -> str:
    """
    Get the groupby objective entropy.
    """
    groupby_vars = [f"0{alias}" for alias in domain_size_statistics.keys()]
    return entropy(groupby_vars)
