from lpbound.acyclic.stat_generator.sql_utils import SqlCommand
from lpbound.config.benchmark_schema import load_benchmark_schema
from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.config.paths import LpBoundPaths
from lpbound.duckdb_adapter.duckdb_manager import DatabaseManager
from lpbound.utils.sql_execution import execute_with_timing, write_commands_to_file


def build_subgraph_matching_statistics(config: LpBoundConfig) -> dict[str, float]:

    assert config.benchmark_name == "subgraph_matching"

    schema_data = load_benchmark_schema(config)

    # 2) Build or load the DB connection
    #    The DatabaseManager references config.DATA_DIR, etc.
    db_manager = DatabaseManager(benchmark_schema=schema_data)
    con = db_manager.create_or_load_db(read_only=False)

    # 3) Generate the Lp-norm SQL commands
    commands: list[SqlCommand] = _generate_subgraph_matching_sql(config)

    # 3a) write the commands to a file for inspection
    groupby_suffix = "_groupby" if config.enable_groupby else ""
    sql_file = LpBoundPaths.GENERATED_SQL_DIR / f"{config.benchmark_name}{groupby_suffix}_lpnorm_queries.sql"
    write_commands_to_file(commands, str(sql_file))
    print(f"Done. See {LpBoundPaths.GENERATED_SQL_DIR} for all generated SQL statements.")

    # 4) Execute with timing
    times_dict = execute_with_timing(commands, con)

    # 5) Print a breakdown
    print("=== Execution Time Breakdown by Tag ===")
    overall_time = sum(times_dict.values())
    # sort by the values
    items = [item for item in sorted(times_dict.items(), key=lambda x: x[1], reverse=True)]
    for tag, total_time in items:
        print(f"{tag}: {total_time:.3f} seconds -- {total_time * 100 / overall_time:.3f}%")
    print(f"Total time: {overall_time:.3f} seconds")

    con.close()

    return times_dict


def _create_norms_table_if_not_exists(cfg: LpBoundConfig) -> SqlCommand:
    """
    Create a table 'norms' that includes l0..l_p..l_inf columns
    depending on cfg.include_l0, cfg.include_l_inf, cfg.p_min, cfg.p_max, etc.
    """
    # For example, if you want columns from p = 0..p_max:
    # But only if cfg.include_l0 is True.
    # And also conditionally l_inf if cfg.include_l_inf is True.

    col_defs: list[str] = []
    for p in range(cfg.p_min, cfg.p_max + 1):
        col_defs.append(f"l{p} DOUBLE")
    if cfg.include_l_inf:
        col_defs.append("l_inf DOUBLE")

    lp_norm_cols = ",\n    ".join(col_defs)

    create_sql = f"""
DROP TABLE IF EXISTS norms;
CREATE TABLE norms (
    relation_id   INTEGER NOT NULL, 
    join_var_id        INTEGER NOT NULL,
    aggregator_id INTEGER NOT NULL,
    pred_col_id        INTEGER,
    pred_value_id      INTEGER,
    {lp_norm_cols}
);
    """.strip()

    return SqlCommand(sql=create_sql, tag="CREATE_NORM_TABLE")


def _generate_subgraph_matching_sql(config: LpBoundConfig) -> list[SqlCommand]:

    select_clause = [f"SUM(POWER(deg,{p})) AS l{p}" for p in range(1, config.p_max + 1)]
    if config.include_l_inf:
        select_clause.append("MAX(deg) AS l_inf")
    select_clause = ", ".join(select_clause)

    lp_cols = [f"l{p}" for p in range(1, config.p_max + 1)]
    if config.include_l_inf:
        lp_cols.append("l_inf")
    lp_cols = ", ".join(lp_cols)

    # put the constants in the SQL
    relation_id = 0
    join_var_id = 0
    aggregator_id = 0
    # pred_col_id = 0
    # pred_value_id = 0

    cmds: list[SqlCommand] = []

    cmds.append(_create_norms_table_if_not_exists(config))

    two_cols = ["s", "t"]
    for col in two_cols:

        # create the degree sequence table
        sql = f"""
    DROP TABLE IF EXISTS degree_sequence;
    CREATE TEMP TABLE degree_sequence AS 
    SELECT {col}, COUNT(*) AS deg
    FROM dblp.edge
    GROUP BY {col}"""
        cmds.append(SqlCommand(sql=sql, tag="BASE"))

        # compute the full relation norms
        sql = f"""
    INSERT INTO norms(relation_id, join_var_id, aggregator_id, pred_col_id, pred_value_id, {lp_cols})
    SELECT 
        {relation_id},
        {join_var_id},
        {aggregator_id},
        NULL,
        NULL,
        {select_clause} 
    FROM degree_sequence;
    """.strip()
        cmds.append(SqlCommand(sql=sql, tag="BASE"))

    # create the joined table
    sql = """
DROP TABLE IF EXISTS joined;
CREATE TEMP TABLE joined AS 
SELECT 
    e.s AS SOURCE, e.t AS TARGET, source_vertex.l AS SOURCE_LABEL, target_vertex.l AS TARGET_LABEL 
FROM dblp.edge AS e
JOIN dblp.vertex AS source_vertex ON e.s = source_vertex.i
JOIN dblp.vertex AS target_vertex ON e.t = target_vertex.i
"""
    cmds.append(SqlCommand(sql=sql, tag="MCV"))

    # create the degree sequence table with MCV rows
    sql = """
DROP TABLE IF EXISTS degree_sequence_mcv;
CREATE TEMP TABLE degree_sequence_mcv AS 
SELECT 
    joined.SOURCE,
    joined.TARGET,
    joined.SOURCE_LABEL,
    joined.TARGET_LABEL,
    COUNT(*) AS deg
FROM joined
GROUP BY joined.SOURCE, joined.TARGET, joined.SOURCE_LABEL, joined.TARGET_LABEL
;
""".strip()
    cmds.append(SqlCommand(sql=sql, tag="MCV"))

    # compute the MCV norms
    two_cols = ["SOURCE", "TARGET"]
    for col in two_cols:

        # compute the full relation norms
        sql = f"""
    INSERT INTO norms(relation_id, join_var_id, aggregator_id, pred_col_id, pred_value_id, {lp_cols})
    SELECT 
        {relation_id},
        {join_var_id},
        {aggregator_id},
        SOURCE_LABEL AS pred_col_id,
        TARGET_LABEL AS pred_value_id,
        {select_clause} 
    FROM degree_sequence_mcv
    GROUP BY {col}, SOURCE_LABEL, TARGET_LABEL
    """.strip()
        cmds.append(SqlCommand(sql=sql, tag="MCV"))

    return cmds
