from pathlib import Path
import sys


class LpBoundPaths:

    # Base paths
    LPBOUND_DIR: Path = Path(__file__).resolve().parent
    PROJ_ROOT_DIR: Path = LPBOUND_DIR.parent.parent.parent

    DATA_DIR: Path = PROJ_ROOT_DIR / "data"
    DATASETS_DIR: Path = DATA_DIR / "datasets"

    BENCHMARKS_DIR: Path = PROJ_ROOT_DIR / "benchmarks"
    WORKLOADS_DIR: Path = BENCHMARKS_DIR / "workloads"
    SCHEMAS_DIR: Path = BENCHMARKS_DIR / "schemas"

    OUTPUT_DIR: Path = PROJ_ROOT_DIR / "output"
    RESULTS_DIR: Path = PROJ_ROOT_DIR / "results"

    GENERATED_SQL_DIR: Path = OUTPUT_DIR / "statistics_sql"

    # used for duckdb to find the csv data directory
    CSV_DATA_DIR_MAP: dict[str, str] = {
        "imdb": "imdb",
        "joblight": "imdb",
        "jobrange": "imdb",
        "stats": "stats",
        "subgraph_matching": "dblp",
    }

    # Used for duckdb to find the database name
    WORKLOAD_TO_DB_MAP: dict[str, str] = {
        "jobjoin": "imdb",
        "joblight": "joblight",
        "jobrange": "jobrange",
        "stats": "stats",
        "subgraph_matching": "subgraph_matching",
    }

    @classmethod
    def ensure_directories(cls):
        """Ensure all necessary directories exist."""
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.DATASETS_DIR.mkdir(exist_ok=True)
        cls.BENCHMARKS_DIR.mkdir(exist_ok=True)
        cls.WORKLOADS_DIR.mkdir(exist_ok=True)
        cls.OUTPUT_DIR.mkdir(exist_ok=True)
        cls.RESULTS_DIR.mkdir(exist_ok=True)
        cls.GENERATED_SQL_DIR.mkdir(exist_ok=True)

    @classmethod
    def setup_python_path(cls):
        """Add the project root to Python path if it's not already there."""

        proj_paths = [str(p) for p in [cls.PROJ_ROOT_DIR, cls.LPBOUND_DIR]]
        for proj_path in proj_paths:
            if proj_path not in sys.path:
                sys.path.insert(0, proj_path)


# Ensure directories exist when the module is imported
LpBoundPaths.ensure_directories()

# Set up Python path
LpBoundPaths.setup_python_path()
