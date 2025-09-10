#!/usr/bin/env python3
"""
statistics_generator.py

Generates statistics for a given benchmark.
It first generates all the SQL commands for generating the statistics,
then executes them.

The statistics are stored in the database.
"""

from duckdb import DuckDBPyConnection
from lpbound.acyclic.stat_generator.sql_groupby_domain_sizes import (
    create_groupby_domain_sizes_table_if_not_exists,
    no_pred_groupby_domain_sizes_sql,
    mcv_groupby_domain_sizes_sql,
    non_mcv_groupby_domain_sizes_sql,
    range_groupby_domain_sizes_sql,
)
from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.config.benchmark_schema import load_benchmark_schema

from .sql_utils import SqlCommand
from .sql_norm_tables import create_norms_table_if_not_exists
from .sql_predicate_tables import generate_mcv_table_sql, generate_histogram_table_sql, generate_joined_table_sql
from .sql_norms_computation import (
    generate_aggregator_sql,
    generate_lp_sql_no_pred,
    generate_lp_sql,
    generate_nonmcv_max_sql,
    generate_range_agg_sql,
)


### --------------------------------------------------------------------------
### Main driver: generate_all_sql_for_benchmark
### --------------------------------------------------------------------------


def generate_all_sql_for_benchmark(con: DuckDBPyConnection, lpnorm_config: LpBoundConfig) -> list[SqlCommand]:
    """
    Return a list of structured SQL commands for the given benchmark config.
    We'll parse the JSON from lpnorm_config.schema_path, then build aggregator queries.

    Steps:
      - Load the schema dict
      - For each relation:
          * Possibly do FK->PK join creation if specified
          * No-predicate aggregator => Lp
          * MCV aggregator => Lp
          * Non-MCV aggregator => Lp
          * Range aggregator => Lp
      - Return a combined list of {sql, tag} dicts
    """
    schema_data = load_benchmark_schema(lpnorm_config)

    relations = schema_data["relations"]
    join_vars = schema_data["join_variables"]
    eq_pred_vars = schema_data["equality_predicate_variables"]
    range_pred_vars = schema_data["range_predicate_variables"]
    fk_pk_joins = schema_data["fk_pk_joins_dict"]
    groupby_vars = schema_data["groupby_variables"]

    aggregator_types = {"NOPRED": 0, "MCV": 1, "NONMCV": 2, "RANGE": 3}

    # For p=0..p_max, to handle l0..l∞ (if included)
    # We'll still keep 0 in the list if we might do an l0
    # If user doesn't want l0, the aggregator just won't include it in the final SELECT.
    # (But if you prefer to skip p=0 entirely, you can condition here.)
    p_list = [0] + list(range(lpnorm_config.p_min, lpnorm_config.p_max + 1))

    all_cmds: list[SqlCommand] = []

    # 1) Create the norms table  and the dimension tables
    create_norms_table_cmd = create_norms_table_if_not_exists(lpnorm_config)
    all_cmds.append(create_norms_table_cmd)

    if lpnorm_config.enable_groupby:
        create_groupby_domain_sizes_table_cmd = create_groupby_domain_sizes_table_if_not_exists(lpnorm_config)
        all_cmds.append(create_groupby_domain_sizes_table_cmd)

    # create_dimension_tables_cmd = create_dimension_tables()
    # all_cmds.extend(create_dimension_tables_cmd)
    # populate_dimension_tables_cmd, fkpk_relations, relation_variables, fkpk_id_to_fk_id, fkpk_id_to_pk_id = (
    #     populate_dimension_tables(schema_data, aggregator_types)
    # )
    # all_cmds.extend(populate_dimension_tables_cmd)

    # 2) Generate MCV/histogram tables for each relation/pred_col
    all_cmds.append(
        {
            "sql": "----------------------------------\n-- START: generate all mcvs and histograms\n----------------------------------\n",
            "tag": "COMMENT",
        }
    )

    for rel_name in relations:
        # MCV
        if rel_name in eq_pred_vars:
            for pred_col in eq_pred_vars[rel_name]:
                mcv_cmds = generate_mcv_table_sql(table_name=rel_name, pred_col=pred_col, cfg=lpnorm_config)
                all_cmds.extend(mcv_cmds)

        # Histograms
        if rel_name in range_pred_vars:
            for rng_col in range_pred_vars[rel_name]:
                histogram_cmds = generate_histogram_table_sql(
                    con=con,
                    table_name=rel_name,
                    pred_col=rng_col,
                    data_type=relations[rel_name][rng_col],
                    cfg=lpnorm_config,
                )
                all_cmds.extend(histogram_cmds)

    all_cmds.append(
        {
            "sql": "----------------------------------\n-- END: generate all mcvs and histograms\n----------------------------------\n",
            "tag": "COMMENT",
        }
    )

    # 3) For each relation, build aggregator queries
    for rel_name in relations:
        all_cmds.append(
            {
                "sql": f"----------------------------------\n-- START: generate all lpnorm queries for {rel_name}\n----------------------------------\n",
                "tag": "COMMENT",
            }
        )

        # Groupby
        groupby_vars_rel = [["*"]] if not lpnorm_config.enable_groupby else groupby_vars[rel_name]

        # Possibly do FK->PK joins
        if rel_name in fk_pk_joins:
            for join_info in fk_pk_joins[rel_name]:
                pk_rel = join_info["pk_relation"]
                fk_col = join_info["fk"]
                pk_col = join_info["pk"]
                pk_pred_cols = eq_pred_vars.get(pk_rel, []) + range_pred_vars.get(pk_rel, [])
                pk_pred_cols = list(set(pk_pred_cols))
                join_cmds: list[SqlCommand] = generate_joined_table_sql(
                    fk_table=rel_name,
                    pk_table=pk_rel,
                    fk_col=fk_col,
                    pk_col=pk_col,
                    fk_join_cols=join_vars[rel_name],
                    pk_pred_cols=pk_pred_cols,
                    is_groupby_query=lpnorm_config.enable_groupby,
                    groupby_vars_list=groupby_vars_rel,
                )
                all_cmds.extend(join_cmds)

        for gby_cols in groupby_vars_rel:
            # gby_cols is a list of column names or ["*"]
            # the statistics computation will be counting distinct values of gby_cols

            if rel_name in join_vars:
                for jv in join_vars[rel_name]:
                    # No predicate aggregator
                    agg_no_pred_cmds = generate_aggregator_sql(
                        table_name=rel_name,
                        groupby_cols=gby_cols,
                        pk_table=None,
                        join_var=jv,
                        pred_col=None,
                        pred_table=None,
                        is_mcv=False,
                        is_fk_pk_join=False,
                    )
                    all_cmds.extend(agg_no_pred_cmds)

                    # Lp for no predicate aggregator
                    agg_table_name = f"Degree_{rel_name}_{jv}"
                    lp_cmds = generate_lp_sql_no_pred(
                        con=con,
                        agg_table_name=agg_table_name,
                        groupby_cols=gby_cols,
                        is_groupby_query=lpnorm_config.enable_groupby,
                        p_list=p_list,
                        rel_name=rel_name,
                        jv=jv,
                        aggregator_type="NOPRED",
                        cfg=lpnorm_config,
                    )
                    all_cmds.extend(lp_cmds)

                    if lpnorm_config.enable_groupby and gby_cols != ["*"]:
                        groupby_domain_sizes_cmds = no_pred_groupby_domain_sizes_sql(
                            joined_table_name=rel_name,
                            groupby_cols=gby_cols,
                            rel_name=rel_name,
                            aggregator_type="NOPRED",
                            cfg=lpnorm_config,
                        )
                        all_cmds.append(groupby_domain_sizes_cmds)

                    # EQUALITY predicates => MCV & Non-MCV
                    if rel_name in eq_pred_vars:
                        for pred_col in eq_pred_vars[rel_name]:
                            # aggregator for MCV
                            mcv_table_name = f"{rel_name}_{pred_col}_mcv"
                            agg_mcv_cmds = generate_aggregator_sql(
                                table_name=rel_name,
                                groupby_cols=gby_cols,
                                pk_table=None,
                                join_var=jv,
                                pred_col=pred_col,
                                pred_table=mcv_table_name,
                                is_mcv=True,
                                is_fk_pk_join=False,
                            )
                            all_cmds.extend(agg_mcv_cmds)

                            agg_mcv_table = f"Degree_{rel_name}_{pred_col}_{jv}"
                            lp_mcv_cmd = generate_lp_sql(
                                agg_table_name=agg_mcv_table,
                                groupby_cols=gby_cols,
                                is_groupby_query=lpnorm_config.enable_groupby,
                                jv=jv,
                                pred_col=pred_col,
                                p_list=p_list,
                                rel_name=rel_name,
                                aggregator_type="MCV",
                                cfg=lpnorm_config,
                                is_fk_pk_join=False,
                            )
                            all_cmds.append(lp_mcv_cmd)

                            if lpnorm_config.enable_groupby and gby_cols != ["*"]:
                                groupby_domain_sizes_cmds = mcv_groupby_domain_sizes_sql(
                                    joined_table_name=rel_name,
                                    groupby_cols=gby_cols,
                                    rel_name=rel_name,
                                    aggregator_type="MCV",
                                    cfg=lpnorm_config,
                                    mcv_table_name=mcv_table_name,
                                    pk_pred_col=pred_col,
                                    is_fk_pk_join=False,
                                )
                                all_cmds.append(groupby_domain_sizes_cmds)

                            # aggregator for Non-MCV
                            agg_nonmcv_cmds = generate_aggregator_sql(
                                table_name=rel_name,
                                groupby_cols=gby_cols,
                                pk_table=None,
                                join_var=jv,
                                pred_col=pred_col,
                                pred_table=mcv_table_name,
                                is_mcv=False,
                                is_fk_pk_join=False,
                            )
                            all_cmds.extend(agg_nonmcv_cmds)

                            agg_nonmcv_table = f"Degree_{rel_name}_{pred_col}_{jv}"
                            nonmcv_max_cmd = generate_nonmcv_max_sql(
                                agg_table_name=agg_nonmcv_table,
                                groupby_cols=gby_cols,
                                is_groupby_query=lpnorm_config.enable_groupby,
                                pred_col=pred_col,
                                p_list=p_list,
                                rel_name=rel_name,
                                jv=jv,
                                aggregator_type="NONMCV",
                                cfg=lpnorm_config,
                            )
                            all_cmds.append(nonmcv_max_cmd)

                            if lpnorm_config.enable_groupby and gby_cols != ["*"]:
                                groupby_domain_sizes_cmds = non_mcv_groupby_domain_sizes_sql(
                                    joined_table_name=rel_name,
                                    groupby_cols=gby_cols,
                                    rel_name=rel_name,
                                    aggregator_type="NONMCV",
                                    cfg=lpnorm_config,
                                    mcv_table_name=mcv_table_name,
                                    pk_pred_col=pred_col,
                                    is_fk_pk_join=False,
                                )
                                all_cmds.append(groupby_domain_sizes_cmds)

                    # RANGE predicates
                    if rel_name in range_pred_vars:
                        for pred_col in range_pred_vars[rel_name]:
                            # aggregator for range
                            range_table_name = f"{rel_name}_{pred_col}_histogram"
                            agg_range_cmds = generate_aggregator_sql(
                                table_name=rel_name,
                                groupby_cols=gby_cols,
                                pk_table=None,
                                join_var=jv,
                                pred_col=pred_col,
                                pred_table=range_table_name,
                                is_mcv=False,
                                is_range=True,
                                is_fk_pk_join=False,
                            )
                            all_cmds.extend(agg_range_cmds)

                            agg_range_table = f"Degree_{rel_name}_{pred_col}_{jv}"
                            range_norm_cmd = generate_range_agg_sql(
                                agg_table_name=agg_range_table,
                                groupby_cols=gby_cols,
                                is_groupby_query=lpnorm_config.enable_groupby,
                                histogram_table_name=range_table_name,
                                p_list=p_list,
                                rel_name=rel_name,
                                jv=jv,
                                aggregator_type="RANGE",
                                pred_col=pred_col,
                                cfg=lpnorm_config,
                            )
                            all_cmds.append(range_norm_cmd)

                            if lpnorm_config.enable_groupby and gby_cols != ["*"]:
                                groupby_domain_sizes_cmds = range_groupby_domain_sizes_sql(
                                    joined_table_name=rel_name,
                                    groupby_cols=gby_cols,
                                    rel_name=rel_name,
                                    aggregator_type="RANGE",
                                    cfg=lpnorm_config,
                                    histogram_table_name=range_table_name,
                                    pk_pred_col=pred_col,
                                    is_fk_pk_join=False,
                                )
                                all_cmds.append(groupby_domain_sizes_cmds)

                    # Now handle aggregator queries for any FK->PK join
                    if rel_name in fk_pk_joins:
                        for join_info in fk_pk_joins[rel_name]:
                            pk_rel = join_info["pk_relation"]
                            fk_col = join_info["fk"]
                            pk_col = join_info["pk"]

                            joined_table_name = f"{rel_name}_{fk_col}_{pk_col}_{pk_rel}"

                            # EQUALITY preds on the pk_rel
                            if pk_rel in eq_pred_vars:
                                for pred_col in eq_pred_vars[pk_rel]:
                                    mcv_table_name = f"{pk_rel}_{pred_col}_mcv"
                                    agg_mcv_cmds = generate_aggregator_sql(
                                        table_name=joined_table_name,
                                        groupby_cols=gby_cols,
                                        pk_table=pk_rel,
                                        join_var=jv,
                                        pred_col=pred_col,
                                        pred_table=mcv_table_name,
                                        is_mcv=True,
                                        is_fk_pk_join=True,
                                    )
                                    all_cmds.extend(agg_mcv_cmds)

                                    agg_mcv_table = f"Degree_{joined_table_name}_{pred_col}_{jv}"
                                    lp_mcv_cmd = generate_lp_sql(
                                        agg_table_name=agg_mcv_table,
                                        groupby_cols=gby_cols,
                                        is_groupby_query=lpnorm_config.enable_groupby,
                                        jv=jv,
                                        pred_col=pred_col,
                                        p_list=p_list,
                                        rel_name=joined_table_name,
                                        aggregator_type="MCV",
                                        cfg=lpnorm_config,
                                        is_fk_pk_join=True,
                                        fk_rel=rel_name,
                                        pk_rel=pk_rel,
                                        fk_col=fk_col,
                                        pk_col=pk_col,
                                    )
                                    all_cmds.append(lp_mcv_cmd)

                                    if lpnorm_config.enable_groupby and gby_cols != ["*"]:
                                        groupby_domain_sizes_cmds = mcv_groupby_domain_sizes_sql(
                                            joined_table_name=joined_table_name,
                                            groupby_cols=gby_cols,
                                            rel_name=joined_table_name,
                                            aggregator_type="MCV",
                                            cfg=lpnorm_config,
                                            mcv_table_name=mcv_table_name,
                                            pk_pred_col=pred_col,
                                            is_fk_pk_join=True,
                                            fk_rel=rel_name,
                                            pk_rel=pk_rel,
                                            fk_col=fk_col,
                                            pk_col=pk_col,
                                        )
                                        all_cmds.append(groupby_domain_sizes_cmds)

                                    agg_nonmcv_cmds = generate_aggregator_sql(
                                        table_name=joined_table_name,
                                        groupby_cols=gby_cols,
                                        pk_table=pk_rel,
                                        join_var=jv,
                                        pred_col=pred_col,
                                        pred_table=mcv_table_name,
                                        is_mcv=False,
                                        is_fk_pk_join=True,
                                    )
                                    all_cmds.extend(agg_nonmcv_cmds)

                                    agg_nonmcv_table = f"Degree_{joined_table_name}_{pred_col}_{jv}"
                                    nonmcv_max_cmd = generate_nonmcv_max_sql(
                                        agg_table_name=agg_nonmcv_table,
                                        groupby_cols=gby_cols,
                                        is_groupby_query=lpnorm_config.enable_groupby,
                                        pred_col=pred_col,
                                        p_list=p_list,
                                        rel_name=joined_table_name,
                                        jv=jv,
                                        aggregator_type="NONMCV",
                                        cfg=lpnorm_config,
                                        is_fk_pk_join=True,
                                        fk_rel=rel_name,
                                        pk_rel=pk_rel,
                                        fk_col=fk_col,
                                        pk_col=pk_col,
                                    )
                                    all_cmds.append(nonmcv_max_cmd)

                                    if lpnorm_config.enable_groupby and gby_cols != ["*"]:
                                        groupby_domain_sizes_cmds = non_mcv_groupby_domain_sizes_sql(
                                            joined_table_name=joined_table_name,
                                            groupby_cols=gby_cols,
                                            rel_name=joined_table_name,
                                            aggregator_type="NONMCV",
                                            cfg=lpnorm_config,
                                            mcv_table_name=mcv_table_name,
                                            pk_pred_col=pred_col,
                                            is_fk_pk_join=True,
                                            fk_rel=rel_name,
                                            pk_rel=pk_rel,
                                            fk_col=fk_col,
                                            pk_col=pk_col,
                                        )
                                        all_cmds.append(groupby_domain_sizes_cmds)

                            # RANGE preds on the pk_rel
                            if pk_rel in range_pred_vars:
                                for rng_col in range_pred_vars[pk_rel]:
                                    range_table_name = f"{pk_rel}_{rng_col}_histogram"
                                    agg_range_cmds = generate_aggregator_sql(
                                        table_name=joined_table_name,
                                        groupby_cols=gby_cols,
                                        pk_table=pk_rel,
                                        join_var=jv,
                                        pred_col=rng_col,
                                        pred_table=range_table_name,
                                        is_mcv=False,
                                        is_range=True,
                                        is_fk_pk_join=True,
                                    )
                                    all_cmds.extend(agg_range_cmds)

                                    agg_range_table = f"Degree_{joined_table_name}_{rng_col}_{jv}"
                                    range_norm_cmd = generate_range_agg_sql(
                                        agg_table_name=agg_range_table,
                                        groupby_cols=gby_cols,
                                        is_groupby_query=lpnorm_config.enable_groupby,
                                        histogram_table_name=range_table_name,
                                        p_list=p_list,
                                        rel_name=joined_table_name,
                                        jv=jv,
                                        aggregator_type="RANGE",
                                        pred_col=rng_col,
                                        cfg=lpnorm_config,
                                        is_fk_pk_join=True,
                                        fk_rel=rel_name,
                                        pk_rel=pk_rel,
                                        fk_col=fk_col,
                                        pk_col=pk_col,
                                    )
                                    all_cmds.append(range_norm_cmd)

                                    if lpnorm_config.enable_groupby and gby_cols != ["*"]:
                                        groupby_domain_sizes_cmds = range_groupby_domain_sizes_sql(
                                            joined_table_name=joined_table_name,
                                            groupby_cols=gby_cols,
                                            rel_name=joined_table_name,
                                            aggregator_type="RANGE",
                                            cfg=lpnorm_config,
                                            histogram_table_name=range_table_name,
                                            pk_pred_col=rng_col,
                                            is_fk_pk_join=True,
                                            fk_rel=rel_name,
                                            pk_rel=pk_rel,
                                            fk_col=fk_col,
                                            pk_col=pk_col,
                                        )
                                        all_cmds.append(groupby_domain_sizes_cmds)

    return all_cmds
