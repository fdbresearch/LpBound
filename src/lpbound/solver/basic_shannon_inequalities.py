from ortools.linear_solver import pywraplp
from itertools import combinations

from lpbound.solver.solver_utils import entropy, sorted_set


def add_basic_shannon_monotonicity_inequalities(
    solver: pywraplp.Solver,
    lp_variables: dict[str, pywraplp.Variable],
    entropy_variables_combinations: list[list[str]],
    verbose: bool = False,
):

    counter = 1
    maximum_number_of_elements = len(max(entropy_variables_combinations, key=len))

    # h(U v V) >= h(U) ; h(U v V) >= h(V)
    for i in range(maximum_number_of_elements, 2, -1):
        for long_element in [combination for combination in entropy_variables_combinations if len(combination) == i]:
            for short_element in [
                combination for combination in entropy_variables_combinations if len(combination) == i - 1
            ]:
                # one element has to be contained in the other
                if set(short_element) <= set(long_element):
                    constraint, contraint_id = (
                        lp_variables[entropy(long_element)] >= lp_variables[entropy(short_element)],
                        f"Monotonicity Ineq. {counter}: H({long_element}) >= H({short_element})",
                    )
                    if verbose:
                        print(f"Monotonicity Ineq. {counter}: H({long_element}) >= H({short_element})")
                    solver.Add(constraint, contraint_id)
                    counter += 1


def add_basic_shannon_submodularity_inequalities(
    solver: pywraplp.Solver,
    lp_variables: dict[str, pywraplp.Variable],
    entropy_variables_combinations: list[list[str]],
    verbose: bool = False,
):

    # print("----> Entropy variables combinations: ", entropy_variables_combinations)

    # we create the combinations to generate the basic shannon inequalities
    # we take all (U, V) possible combinations from the input entropy combinations
    inequalities_combinations = combinations(entropy_variables_combinations, 2)
    counter = 1

    for combination in inequalities_combinations:

        entropy_elements = combination

        U = sorted_set(entropy_elements[0])
        V = sorted_set(entropy_elements[1])
        U_union_V = sorted_set(sum(combination, []))
        # U_intersection_V = get_intersection(sum(combination, []))
        U_intersection_V = sorted_set(set(U).intersection(set(V)))

        # print(f"Counter: {counter} - Combination: {combination}")
        # print(f"U: {U}, V: {V}, U_union_V: {U_union_V}, U_intersection_V: {U_intersection_V}")

        if set(U) <= set(V) or set(V) <= set(U):
            continue  # if one variable is contained in the other, it creates a trivial inequality

        if len(U_intersection_V) == 0:
            # h(U) + h(V) >= h(U v V)
            constraint, contraint_id = (
                lp_variables[entropy(U)] + lp_variables[entropy(V)] >= lp_variables[entropy(U_union_V)],
                f"Submodularity Ineq. {counter}: H({U}) + H({V}) >= H({U_union_V})",
            )
            if verbose:
                print(f"Submodularity Ineq. {counter}: H({U}) + H({V}) >= H({U_union_V})")
            solver.Add(constraint, contraint_id)
        else:
            # h(U) + h(V) >= h(U v V) + h(U ^ V)
            constraint, contraint_id = (
                lp_variables[entropy(U)] + lp_variables[entropy(V)]
                >= lp_variables[entropy(U_union_V)] + lp_variables[entropy(U_intersection_V)],
                f"Submodularity Ineq. {counter}: H({U}) + H({V}) >= H({U_union_V}) + H({U_intersection_V})",
            )
            if verbose:
                print(f"Submodularity Ineq. {counter}: H({U}) + H({V}) >= H({U_union_V}) + H({U_intersection_V})")
            solver.Add(constraint, contraint_id)
        counter += 1
