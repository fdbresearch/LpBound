import duckdb
import os

from duckdb import DuckDBPyConnection

from lpbound.config.benchmark_schema import BenchmarkSchema
from lpbound.config.paths import LpBoundPaths


class DatabaseManager:
    def __init__(self, benchmark_schema: BenchmarkSchema):
        """benchmark: str, e.g., "jobjoin", "joblight", "jobrange", "stats" subgraph_matching"""
        self.benchmark: str = benchmark_schema["name"]  # e.g., "jobjoin"
        self.db_name: str = LpBoundPaths.WORKLOAD_TO_DB_MAP[self.benchmark]
        self.duckdb_file: str = os.path.join(LpBoundPaths.DATA_DIR, "duckdb", f"{self.db_name}_duckdb.db")
        self.benchmark_schema: BenchmarkSchema = benchmark_schema

        self.csv_data_dir: str = LpBoundPaths.CSV_DATA_DIR_MAP[self.db_name]

    def create_or_load_db(self, read_only: bool = False) -> DuckDBPyConnection:
        if os.path.exists(self.duckdb_file):
            return duckdb.connect(database=self.duckdb_file, read_only=read_only)

        os.makedirs(os.path.dirname(self.duckdb_file), exist_ok=True)
        con: DuckDBPyConnection = duckdb.connect(database=self.duckdb_file, read_only=read_only)
        self.create_and_load_db_tables(con)
        self.create_db_indexes(con)
        return con

    def create_and_load_db_tables(self, con: DuckDBPyConnection):
        relations = self.benchmark_schema["relations"]
        relation_names = relations.keys()

        # create tables
        file_path = os.path.join(LpBoundPaths.DATA_DIR, "sql_scripts", "duckdb", f"create_queries_{self.db_name}.sql")
        # read the whole file and execute it
        with open(file_path, "r") as f:
            con.sql(f.read())
        print("All tables created.")

        # import data into tables
        delimiter = "," if self.benchmark != "subgraph_matching" else "|"

        # import data into tables
        for name in relation_names:
            file_name = name.lower()
            if self.benchmark == "subgraph_matching":  #
                if name == "dblp.edge":
                    file_name = "dblp_edge_undirected"
                elif name == "dblp.vertex":
                    file_name = "dblp_vertex"

            insert_query = f"COPY {name} FROM '{LpBoundPaths.DATA_DIR}/datasets/{self.csv_data_dir}/{file_name}.csv' WITH CSV QUOTE '\"' ESCAPE '\\' DELIMITER '{delimiter}';"
            con.execute(insert_query)
        print("All data inserted.")

    def create_db_indexes(self, con: DuckDBPyConnection):
        file_path = os.path.join(LpBoundPaths.DATA_DIR, "sql_scripts", "duckdb", f"fkindexes_{self.db_name}.sql")
        with open(file_path, "r") as f:
            con.sql(f.read())
        print("All indices created. \n")
