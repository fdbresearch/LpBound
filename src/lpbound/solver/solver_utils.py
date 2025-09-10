from ortools.linear_solver import pywraplp


def entropy(variables: list[str]) -> str:
    sorted_variables = sorted(variables)
    return "_".join((sorted_variables))


def sorted_set(variables: list[str]) -> list[str]:
    return sorted(set(variables))


def print_objective(objective: pywraplp.Objective, variables: dict[str, pywraplp.Variable]):
    # Build a readable representation of the objective
    terms: list[str] = []
    for var in variables.values():
        coeff = objective.GetCoefficient(var)
        if coeff != 0:  # Only include non-zero coefficients
            term = f"{coeff} * {var.name()}"
            terms.append(term)
    obj_str: str = " + ".join(terms)
    direction: str = "Maximize"
    print(f"{direction}: {obj_str}")
