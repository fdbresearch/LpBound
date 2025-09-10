import duckdb
import sys
import os
import pandas as pd

sys.path.append("..")
sys.path.append("../..")
from .utility import schema, datasets
from ..config.paths import LpBoundPaths
import joblib
import numpy as np
import itertools


def safe_norm(x, p):

    if len(x) == 0:
        return 0

    if p == np.inf:
        norm = np.max(x)
    else:
        norm = np.linalg.norm(x, ord=p)

    return norm


class StatsGenerator:

    def __init__(self, schema, ps=list(range(1, 31)) + [np.inf], db_path: str = "db", data_path: str = None):

        self.con = duckdb.connect(db_path)
        self.cur = self.con.cursor()

        # Use LpBoundPaths for default statistics path
        if data_path:
            self.data_path = data_path
        else:
            self.data_path = LpBoundPaths.OUTPUT_DIR / "_statistics"
            # Ensure the statistics directory exists
            self.data_path.mkdir(parents=True, exist_ok=True)

        self.schema = schema
        self.ps = ps
        self.statistics = {}

    def get_stats(self, dataset):

        # check if stats already exist in data_path
        # if yes, return
        try:
            stats_file = self.data_path / f"{dataset}_stats.pkl"
            self.statistics = joblib.load(str(stats_file))
        except:
            self.compute_stats(dataset)

    def compute_stats(self, dataset: str):

        self.statistics = {}

        self.compute_unconstraint_stats(dataset)

        self.compute_single_constraint_stats(dataset)

        self.compute_double_constraint_stats(dataset)

        self.compute_constraint_vertex_stats(dataset)

        # Create the statistics directory if it doesn't exist
        self.data_path.mkdir(parents=True, exist_ok=True)

        stats_file = self.data_path / f"{dataset}_stats.pkl"
        joblib.dump(self.statistics, str(stats_file))

    def compute_unconstraint_stats(self, dataset):

        relations = self.schema[dataset]["relations"]

        for r in relations:
            for jv in r["join_vars"]:
                query = f"SELECT {jv}, COUNT(*) as degree FROM {dataset}_{r['name']} GROUP BY {jv} ORDER BY degree DESC"
                # print(query)
                res = self.con.execute(query).fetchall()
                df = pd.DataFrame(res, columns=["join_var", "degree"])

                # print(df)
                # df.columns = ["join_var", "degree"]

                degree_sequence = df["degree"].values

                for p in self.ps:

                    norm = safe_norm(degree_sequence, p)

                    try:
                        self.statistics[dataset]
                    except:
                        self.statistics[dataset] = {}

                    if r["name"] not in self.statistics[dataset]:
                        self.statistics[dataset][r["name"]] = {}

                    try:
                        self.statistics[dataset][r["name"]][(jv, "unconstraint")][p] = norm
                    except:
                        self.statistics[dataset][r["name"]][(jv, "unconstraint")] = {}
                        self.statistics[dataset][r["name"]][(jv, "unconstraint")][p] = norm

    def compute_single_constraint_stats(self, dataset):

        pk_fks = self.schema[dataset]["pk_fk"]

        for pk_fk in pk_fks:
            fk_rel = pk_fk["fk_rel"]
            pk_rel = pk_fk["pk_rel"]
            fk_vars = pk_fk["fk"]
            pk_var = pk_fk["pk"]

            query_get_data_vals = f"SELECT DISTINCT l FROM {dataset}_{pk_rel}"
            res = self.con.execute(query_get_data_vals).fetchall()
            df = pd.DataFrame(res, columns=["l"])
            data_vals = df["l"].values

            for fk in fk_vars:

                for data_val in data_vals:
                    query = """ 
                    SELECT {fk}, COUNT(*) as degree
                    FROM {dataset}_{fk_rel} JOIN {dataset}_{pk_rel} ON {dataset}_{fk_rel}.{fk} = {dataset}_{pk_rel}.{pk}
                    WHERE {dataset}_{pk_rel}.l = {data_val}
                    GROUP BY {fk}
                    ORDER BY degree DESC
                    """.format(
                        dataset=dataset, fk_rel=fk_rel, fk=fk, pk_rel=pk_rel, pk=pk_var, data_val=data_val
                    )

                    res = self.con.execute(query).fetchall()
                    df = pd.DataFrame(res, columns=["join_var", "degree"])

                    for p in self.ps:
                        norm = safe_norm(df["degree"].values, p)

                        try:
                            self.statistics[dataset]
                        except:
                            self.statistics[dataset] = {}

                        if fk_rel not in self.statistics[dataset]:
                            self.statistics[dataset][fk_rel] = {}

                        try:
                            self.statistics[dataset][fk_rel][(fk, "single_constraint")][data_val][p] = norm
                        except:
                            try:
                                self.statistics[dataset][fk_rel][(fk, "single_constraint")][data_val] = {}
                                self.statistics[dataset][fk_rel][(fk, "single_constraint")][data_val][p] = norm
                            except:
                                self.statistics[dataset][fk_rel][(fk, "single_constraint")] = {}
                                self.statistics[dataset][fk_rel][(fk, "single_constraint")][data_val] = {}
                                self.statistics[dataset][fk_rel][(fk, "single_constraint")][data_val][p] = norm

    def compute_constraint_vertex_stats(self, dataset):
        query_get_data_vals = f"SELECT DISTINCT l FROM {dataset}_vertex"

        res = self.con.execute(query_get_data_vals).fetchall()
        df = pd.DataFrame(res, columns=["l"])
        data_vals = df["l"].values

        for data_val in data_vals:
            query = f"SELECT i, COUNT(*) as degree FROM {dataset}_vertex WHERE l = {data_val} GROUP BY i ORDER BY degree DESC"
            res = self.con.execute(query).fetchall()
            df = pd.DataFrame(res, columns=["i", "degree"])

            for p in self.ps:
                norm = safe_norm(df["degree"].values, p)

                try:
                    self.statistics[dataset]
                except:
                    self.statistics[dataset] = {}

                try:
                    self.statistics[dataset]["vertex"][("i", "single_constraint")][data_val][p] = norm
                except:
                    try:
                        self.statistics[dataset]["vertex"][("i", "single_constraint")][data_val] = {}
                        self.statistics[dataset]["vertex"][("i", "single_constraint")][data_val][p] = norm
                    except:
                        self.statistics[dataset]["vertex"][("i", "single_constraint")] = {}
                        self.statistics[dataset]["vertex"][("i", "single_constraint")][data_val] = {}
                        self.statistics[dataset]["vertex"][("i", "single_constraint")][data_val][p] = norm

    def compute_double_constraint_stats(self, dataset):

        pk_fks = self.schema[dataset]["pk_fk"]

        for pk_fk in pk_fks:
            fk_rel = pk_fk["fk_rel"]
            pk_rel = pk_fk["pk_rel"]
            fk_vars = pk_fk["fk"]
            pk_var = pk_fk["pk"]

            query_get_data_vals = f"SELECT DISTINCT l FROM {dataset}_{pk_rel}"
            res = self.con.execute(query_get_data_vals).fetchall()
            df = pd.DataFrame(res, columns=["l"])
            data_vals = df["l"].values

            data_vals_combinations = list(itertools.product(data_vals, repeat=2))

            fk1 = fk_vars[0]
            fk2 = fk_vars[1]

            for fk in fk_vars:
                for combo in data_vals_combinations:
                    query = """
                    SELECT {fk}, COUNT(*) as degree
                    FROM {dataset}_{fk_rel} 
                    JOIN {dataset}_{pk_rel} as pk1 ON {dataset}_{fk_rel}.{fk1} = pk1.{pk}
                    JOIN {dataset}_{pk_rel} as pk2 ON {dataset}_{fk_rel}.{fk2} = pk2.{pk}
                    WHERE pk1.l = {combo[0]} AND pk2.l = {combo[1]}
                    GROUP BY {fk}
                    ORDER BY degree DESC
                    """.format(
                        dataset=dataset, fk_rel=fk_rel, fk=fk, pk_rel=pk_rel, pk=pk_var, combo=combo, fk1=fk1, fk2=fk2
                    )

                    res = self.con.execute(query).fetchall()
                    df = pd.DataFrame(res, columns=["join_var", "degree"])

                    for p in self.ps:
                        norm = safe_norm(df["degree"].values, p)

                        try:
                            self.statistics[dataset]
                        except:
                            self.statistics[dataset] = {}

                        if fk_rel not in self.statistics[dataset]:
                            self.statistics[dataset][fk_rel] = {}

                        try:
                            self.statistics[dataset][fk_rel][(fk, "double_constraint")][combo][p] = norm
                        except:
                            try:
                                self.statistics[dataset][fk_rel][(fk, "double_constraint")][combo] = {}
                                self.statistics[dataset][fk_rel][(fk, "double_constraint")][combo][p] = norm
                            except:
                                self.statistics[dataset][fk_rel][(fk, "double_constraint")] = {}
                                self.statistics[dataset][fk_rel][(fk, "double_constraint")][combo] = {}
                                self.statistics[dataset][fk_rel][(fk, "double_constraint")][combo][p] = norm

    def compute_constraints_path(self, dataset):

        # get label values
        query_get_data_vals = f"SELECT DISTINCT l FROM {dataset}_vertex"
