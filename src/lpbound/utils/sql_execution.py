import time
from typing import Any
from tqdm import tqdm
from collections import defaultdict

from duckdb import DuckDBPyConnection

from lpbound.acyclic.stat_generator.sql_utils import SqlCommand


def execute_with_timing(commands: list[SqlCommand], con: DuckDBPyConnection) -> dict[str, float]:
    """
    Execute each command in 'commands', measure how long each takes.
    Returns dict { tag -> total_time_in_seconds }.
    """
    times_per_tag: defaultdict[str, float] = defaultdict(float)

    with tqdm(total=len(commands), desc="Executing queries") as pbar:
        for cmd in commands:
            sql_text: str = cmd["sql"]
            tag: str = cmd["tag"]
            # print(f"Executing query: {tag}")
            # print(sql_text)
            # print("-" * 100)

            pbar.set_postfix_str(f"Current: {tag}")

            start = time.perf_counter()

            con.execute(sql_text)

            end = time.perf_counter()

            times_per_tag[tag] += end - start
            pbar.update(1)

    return times_per_tag


def write_commands_to_file(commands: list[SqlCommand], output_file: str) -> None:
    """
    Write the commands to a file as raw SQL (plus comments).
    """
    with open(output_file, "w") as f_out:
        for cmd in commands:
            f_out.write(cmd["sql"] + "\n\n")
    print(f"[INFO] Wrote {len(commands)} commands to {output_file}")


def execute_fetchone_sql(con: DuckDBPyConnection, sql: str) -> list[Any]:
    """Execute SQL and return the result."""
    return con.execute(sql).fetchone()


def get_time_breakdown_string(times_dict: dict[str, float]) -> str:
    """Get a string with the time breakdown."""
    s = "=== Execution Time Breakdown by Tag ===\n"
    overall_time = sum(times_dict.values())
    # sort by the values
    items = [item for item in sorted(times_dict.items(), key=lambda x: x[1], reverse=True)]
    for tag, total_time in items:
        s += f"{tag}: {total_time:.3f} seconds -- {total_time * 100 / overall_time:.3f}%\n"
    s += f"Total time: {overall_time:.3f} seconds\n"
    return s


def get_overall_time(times_dict: dict[str, float]) -> float:
    """Get the overall time."""
    return sum(times_dict.values())
