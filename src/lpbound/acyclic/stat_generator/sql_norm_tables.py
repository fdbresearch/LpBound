"""
sql_tables.py

Functions for creating and populating database tables.
"""

from lpbound.config.lpbound_config import LpBoundConfig
from .sql_utils import SqlCommand
from lpbound.config.benchmark_schema import BenchmarkSchema


def create_norms_table_if_not_exists(cfg: LpBoundConfig) -> SqlCommand:
    """
    Create a table 'norms' that includes l0..l_p..l_inf columns
    depending on cfg.include_l0, cfg.include_l_inf, cfg.p_min, cfg.p_max, etc.
    """
    # For example, if you want columns from p = 0..p_max:
    # But only if cfg.include_l0 is True.
    # And also conditionally l_inf if cfg.include_l_inf is True.

    col_defs: list[str] = []
    if cfg.include_l0:
        col_defs.append("l0 DOUBLE")
    for p in range(cfg.p_min, cfg.p_max + 1):
        col_defs.append(f"l{p} DOUBLE")
    if cfg.include_l_inf:
        col_defs.append("l_inf DOUBLE")

    lp_norm_cols: str = ",\n    ".join(col_defs)

    name_sql: str = """
    relation_name TEXT NOT NULL,
    join_var_name TEXT NOT NULL,
    aggregator_name TEXT NOT NULL,
    pred_col_name TEXT,
    pk_relation_name TEXT,
    fk_join_var_name TEXT,
    pk_join_var_name TEXT,
    """
    if cfg.enable_groupby:
        name_sql += "groupby_vars_name TEXT NOT NULL,"

    table_name = "norms" if not cfg.enable_groupby else "norms_groupby"

    create_sql = f"""
DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
    {name_sql}
    pred_value_id      INTEGER,
    {lp_norm_cols}
);
    """.strip()

    return {"sql": create_sql, "tag": "CREATE_NORM_TABLE"}


# The code below is not used in the current implementation


def create_dimension_tables() -> list[SqlCommand]:
    """
    Returns the SQL commands to create dimension tables:
      - dim_relations: store relation_id + relation_name
    In a later step, we'll populate these tables from schema_data.
    """
    cmds: list[SqlCommand] = []

    # 1) dim_relations
    create_dim_relations_sql = """
    -- Dimension table for relations
    DROP TABLE IF EXISTS dim_relations;
    CREATE TABLE dim_relations (
        relation_id INTEGER PRIMARY KEY,
        relation_name TEXT NOT NULL,
        fk_relation_name TEXT,
        pk_relation_name TEXT,
        fk_join_var TEXT,
        pk_join_var TEXT
    );
    """
    cmds.append({"sql": create_dim_relations_sql.strip(), "tag": "CREATE_DIM"})

    # 2) dim_joinvars -- we use relation_id + joinvar_id to reference the joinvars
    create_dim_joinvars_sql = """
    -- Dimension table for join variables
    DROP TABLE IF EXISTS dim_joinvars;
    CREATE TABLE dim_joinvars (
        relation_id INTEGER NOT NULL,
        joinvar_id INTEGER NOT NULL,
        joinvar_name TEXT NOT NULL
    );
    """
    cmds.append({"sql": create_dim_joinvars_sql.strip(), "tag": "CREATE_DIM"})

    # 3) dim_aggregators
    create_dim_aggregators_sql = """
    -- Dimension table for aggregators
    DROP TABLE IF EXISTS dim_aggregators;
    CREATE TABLE dim_aggregators (
        aggregator_id INTEGER PRIMARY KEY,
        aggregator_name TEXT NOT NULL
    );
    """
    cmds.append({"sql": create_dim_aggregators_sql.strip(), "tag": "CREATE_DIM"})

    # 4) dim_predicate_vars
    create_dim_predicate_vars_sql = """
    -- Dimension table for predicate variables
    DROP TABLE IF EXISTS dim_predicate_vars;
    CREATE TABLE dim_predicate_vars (
        relation_id INTEGER NOT NULL,
        pred_var_id INTEGER NOT NULL,
        pred_var_name TEXT NOT NULL
    );
    """
    cmds.append({"sql": create_dim_predicate_vars_sql.strip(), "tag": "CREATE_DIM"})

    return cmds


# this function is not used in the current implementation
def populate_dimension_tables(
    schema_data: BenchmarkSchema, aggregator_types: dict[str, int]
) -> tuple[list[SqlCommand], list[str], list[list[str]], dict[int, int], dict[int, int]]:
    """
    Populate the dimension tables with data from schema_data.
    Returns:
        - SQL commands
        - relations list
        - relation_variables list
        - fkpk_id_to_fk_id mapping
        - fkpk_id_to_pk_id mapping
    """
    cmds: list[SqlCommand] = []
    relations: list[str] = list(schema_data["relations"].keys())
    relation_variables: list[list[str]] = [list(schema_data["relations"][rel_name].keys()) for rel_name in relations]

    # Maps for FK-PK relationships
    fkpk_id_to_fk_id = {}
    fkpk_id_to_pk_id = {}

    # 1) dim_relations
    sql = "INSERT INTO dim_relations (relation_id, relation_name) VALUES "
    for i, rel_name in enumerate(relations):
        sql += f"({i}, '{rel_name}'), "
    sql = sql.rstrip(", ") + ";"
    cmds.append({"sql": sql, "tag": "POPULATE_DIM"})

    # 2) dim_joinvars
    sql = "INSERT INTO dim_joinvars (relation_id, joinvar_id, joinvar_name) VALUES "
    for rel_name, join_vars in schema_data["join_variables"].items():
        relation_id = relations.index(rel_name)
        for i, jv in enumerate(join_vars):
            sql += f"({relation_id}, {i}, '{jv}'), "
    sql = sql.rstrip(", ") + ";"
    if "join_variables" in schema_data and schema_data["join_variables"]:
        cmds.append({"sql": sql, "tag": "POPULATE_DIM"})

    # 3) dim_aggregators
    sql = "INSERT INTO dim_aggregators (aggregator_id, aggregator_name) VALUES "
    for agg_name, agg_id in aggregator_types.items():
        sql += f"({agg_id}, '{agg_name}'), "
    sql = sql.rstrip(", ") + ";"
    cmds.append({"sql": sql, "tag": "POPULATE_DIM"})

    # 4) dim_predicate_vars
    sql = "INSERT INTO dim_predicate_vars (relation_id, pred_var_id, pred_var_name) VALUES "
    has_pred_vars = False

    for relation_name, pred_vars in schema_data.get("equality_predicate_variables", {}).items():
        if relation_name in relations and pred_vars:
            has_pred_vars = True
            relation_id = relations.index(relation_name)
            for i, pred_var in enumerate(pred_vars):
                sql += f"({relation_id}, {i}, '{pred_var}'), "

    for relation_name, pred_vars in schema_data.get("range_predicate_variables", {}).items():
        if relation_name in relations and pred_vars:
            has_pred_vars = True
            relation_id = relations.index(relation_name)
            for i, pred_var in enumerate(pred_vars):
                sql += f"({relation_id}, {i}, '{pred_var}'), "

    if has_pred_vars:
        sql = sql.rstrip(", ") + ";"
        cmds.append({"sql": sql, "tag": "POPULATE_DIM"})

    # Process FK-PK joins to populate the mappings
    if "fk_pk_joins_dict" in schema_data:
        fk_pk_joins = schema_data["fk_pk_joins_dict"]
        fkpk_id = 0

        for fk_rel, joins in fk_pk_joins.items():
            fk_id = relations.index(fk_rel)

            for join_info in joins:
                pk_rel = join_info["pk_relation"]
                pk_id = relations.index(pk_rel)

                fkpk_id_to_fk_id[fkpk_id] = fk_id
                fkpk_id_to_pk_id[fkpk_id] = pk_id
                fkpk_id += 1

    return cmds, relations, relation_variables, fkpk_id_to_fk_id, fkpk_id_to_pk_id
