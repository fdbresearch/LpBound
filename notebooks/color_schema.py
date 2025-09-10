import matplotlib.pyplot as plt
import numpy as np


# ColorBrewer qualitative 12-class palette
cb_palette = [
    "#a6cee3",
    "#1f78b4",
    "#b2df8a",
    "#33a02c",
    "#fb9a99",
    "#e31a1c",
    "#fdbf6f",
    "#ff7f00",
    "#cab2d6",
    "#6a3d9a",
    "#ffff99",
    "#b15928",
]

# Additional colors from Tableau 20 palette
t20_palette = [
    "#4e79a7",
    "#f28e2b",
    "#e15759",
    "#76b7b2",
    "#59a14f",
    "#edc948",
    "#b07aa1",
    "#ff9da7",
    "#9c755f",
    "#bab0ac",
]

# Nature color palette
nature_palette = {
    "blue": "#0072B2",
    "orange": "#E69F00",
    "green": "#009E73",
    "red": "#D55E00",
    "purple": "#CC79A7",
    "yellow": "#F0E442",
    "sky_blue": "#56B4E9",
    "pink": "#FFB3BA",
}


color_palette = plt.cm.get_cmap("Set2")(np.linspace(0, 1, 8))

ESTIMATOR_COLORS = {
    # Pessimistic estimators (cold colors)
    "LPBound": color_palette[1],
    "TrueCardinality": nature_palette["blue"],
    "SafeBound": nature_palette["green"],
    "PANDA": nature_palette["purple"],
    # Traditional estimators
    "DuckDB": nature_palette["sky_blue"],
    "DBX": nature_palette["pink"],
    "Postgres": nature_palette["yellow"],
    "PostgresFull": "#44AA99",  # Muted teal (additional color)
    "PostgresExactStats": "#117733",  # Dark green (additional color)
    "PostgresFullExactStats": "#999933",  # Olive (additional color)
    # ML-based estimators
    "FactorJoin": nature_palette["orange"],
    "NeuroCard*": nature_palette["red"],
    "BayesCard": nature_palette["purple"],
    "DeepDB": "#DDCC77",  # Pale yellow (additional color)
    "Flat*": "#88CCEE",  # Light blue (additional color)
}

benchmark_names = {"jobjoin": "JOBjoin", "joblight": "JOBlight", "jobrange": "JOBrange", "stats": "STATS"}

method_names = {
    "truecardinality": "TrueCardinality",
    "duckdb": "DuckDB",
    "panda": "PANDA",
    "lpbound": "LPBound",
    "safebound": "SafeBound",
    "dbx": "DBX",
    "factorjoin": "FactorJoin",
    "bayescard": "BayesCard",
    "deepdb": "DeepDB",
    "flat": "Flat*",
    "neurocard": "NeuroCard*",
    "postgres": "Postgres",
    "Postgres": "Postgres",
    "Postgres-Full": "PostgresFull",
    "postgresfull": "PostgresFull",
    "Postgres-Accurate": "PostgresExactStats",
    "Postgres-Full-Accurate": "PostgresFullExactStats",
}

# Group methods by category
categories = {
    "Pessimistic": ["lpbound", "safebound", "panda"],
    "Traditional": ["dbx", "duckdb", "postgres", "Postgres-Full", "Postgres-Accurate", "Postgres-Full-Accurate"],
    "PGM-based": ["bayescard", "deepdb", "factorjoin"],
    "ML-based": ["neurocard", "flat"],
}
# Adjust category offsets to spread out the boxes within each group
category_offsets = {"Pessimistic": -0.05, "Traditional": 0, "PGM-based": 0.05, "ML-based": 0.05}
category_patterns = {"Pessimistic": "", "Traditional": "///", "PGM-based": "...", "ML-based": "xxx"}
category_colors = {
    "Pessimistic": "#FFA07A",  # Light salmon
    "Traditional": "#98FB98",  # Pale green
    "PGM-based": "#ADD8E6",  # Light blue
    "ML-based": "#FFFFC5",  # Light yellow
}
