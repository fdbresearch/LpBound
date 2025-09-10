import duckdb
import re 
import sys
import os

# Add path to find lpbound module
sys.path.append("..")
sys.path.append("../..")
from ..config.paths import LpBoundPaths

datasets = [
    "dblp",
    "eu2005",
    "hprd",
    "human",
    "patents",
    "wordnet",
    "yeast",
    "youtube"
]


schema = {
    "dblp": {
        "relations": [
            {"name": "vertex",
             "join_vars": ["i"],
             "data_vars": ["l"],
             "other_vars": ["d"]},
            {"name": "edge",
                "join_vars": ["s", "t"],
                "data_vars": [],
                "other_vars": []}
        ],
        "pk_fk": [{
            "pk_rel": "vertex",
            "pk": "i",
            "fk_rel": "edge",
            "fk": ["s", "t"]
        }]
        },
    "eu2005": {
        "relations": [
            {"name": "vertex",
             "join_vars": ["i"],
             "data_vars": ["l"],
             "other_vars": ["d"]},
            {"name": "edge",
                "join_vars": ["s", "t"],
                "data_vars": [],
                "other_vars": []}
        ],
        "pk_fk": [{
            "pk_rel": "vertex",
            "pk": "i",
            "fk_rel": "edge",
            "fk": ["s", "t"]
        }]
        },
    "hprd": {
        "relations": [
            {"name": "vertex",
             "join_vars": ["i"],
             "data_vars": ["l"],
             "other_vars": ["d"]},
            {"name": "edge",
                "join_vars": ["s", "t"],
                "data_vars": [],
                "other_vars": []}
        ],
        "pk_fk": [{
            "pk_rel": "vertex",
            "pk": "i",
            "fk_rel": "edge",
            "fk": ["s", "t"]
        }]
        },
    "human": {
        "relations": [
            {"name": "vertex",
             "join_vars": ["i"],
             "data_vars": ["l"],
             "other_vars": ["d"]},
            {"name": "edge",
                "join_vars": ["s", "t"],
                "data_vars": [],
                "other_vars": []}
        ],
        "pk_fk": [{
            "pk_rel": "vertex",
            "pk": "i",
            "fk_rel": "edge",
            "fk": ["s", "t"]
        }]
        },
    "patents": {
        "relations": [
            {"name": "vertex",
             "join_vars": ["i"],
             "data_vars": ["l"],
             "other_vars": ["d"]},
            {"name": "edge",
                "join_vars": ["s", "t"],
                "data_vars": [],
                "other_vars": []}
        ],
        "pk_fk": [{
            "pk_rel": "vertex",
            "pk": "i",
            "fk_rel": "edge",
            "fk": ["s", "t"]
        }]
        },
    "wordnet": {
        "relations": [
            {"name": "vertex",
             "join_vars": ["i"],
             "data_vars": ["l"],
             "other_vars": ["d"]},
            {"name": "edge",
                "join_vars": ["s", "t"],
                "data_vars": [],
                "other_vars": []}
        ],
        "pk_fk": [{
            "pk_rel": "vertex",
            "pk": "i",
            "fk_rel": "edge",
            "fk": ["s", "t"]
        }]
        },
    "yeast": {
        "relations": [
            {"name": "vertex",
             "join_vars": ["i"],
             "data_vars": ["l"],
             "other_vars": ["d"]},
            {"name": "edge",
                "join_vars": ["s", "t"],
                "data_vars": [],
                "other_vars": []}
        ],
        "pk_fk": [{
            "pk_rel": "vertex",
            "pk": "i",
            "fk_rel": "edge",
            "fk": ["s", "t"]
        }]
        },
    "youtube": {
        "relations": [
            {"name": "vertex",
             "join_vars": ["i"],
             "data_vars": ["l"],
             "other_vars": ["d"]},
            {"name": "edge",
                "join_vars": ["s", "t"],
                "data_vars": [],
                "other_vars": []}
        ],
        "pk_fk": [{
            "pk_rel": "vertex",
            "pk": "i",
            "fk_rel": "edge",
            "fk": ["s", "t"]
        }]
        }
}

def get_fk_pk_map(schema):
    """ 
    A map fk_rel -> pk_rel -> (fk, pk)
    Args:
        schema: dict, schema of the dataset
    Returns:
        dict, fk_rel -> pk_rel -> (fk, pk)
    """
    
    fk_pk_map = {}
    for d in schema:
        fk_pk_map[d] = {}
        for fk in schema[d]["pk_fk"]:
            fk_rel = fk["fk_rel"]
            pk_rel = fk["pk_rel"]
            fk_vars = fk["fk"]
            pk_var = fk["pk"]
            if fk_rel not in fk_pk_map[d]:
                fk_pk_map[d][fk_rel] = {}
            if pk_rel not in fk_pk_map[d][fk_rel]:
                fk_pk_map[d][fk_rel][pk_rel] = []
            fk_pk_map[d][fk_rel][pk_rel].append((fk_vars, pk_var))
    return fk_pk_map

# create database with tables {dataset}_vertix and {dataset}_edge

def create_db(datasets, db_name='db', directed=True):
    # make sure that database is deleted
    # delete database db

    con = duckdb.connect(db_name)

    for dataset in datasets:
        
        con = duckdb.connect(db_name)
        
        try:
            con.execute(f"DROP TABLE IF EXISTS {dataset}_vertex")
            con.execute(f"DROP TABLE IF EXISTS {dataset}_edge")
        except:
            pass

        con.execute(f"CREATE TABLE {dataset}_vertex (i INTEGER, l INTEGER, d INTEGER)")
        
        con.execute(f"CREATE TABLE {dataset}_edge (s INTEGER, t INTEGER)")
        
        # set index PK-FK on edge table
        #con.execute(f"CREATE INDEX edge_s ON {dataset}_edge(s)")
        #con.execute(f"CREATE INDEX edge_t ON {dataset}_edge(t)")
        
        # set index on vertex table
        #con.execute(f"CREATE INDEX vertex_l ON {dataset}_vertex(l)")
        
       
        # Use LpBoundPaths for dataset directory
        dataset_dir = LpBoundPaths.DATASETS_DIR / dataset
        
        if directed:
            edge_file = dataset_dir / f"{dataset}_edge.csv"
        else:
            edge_file = dataset_dir / f"{dataset}_edge_undirected.csv"
            
        vertex_file = dataset_dir / f"{dataset}_vertex.csv"
        
        query_load_edge = f"COPY {dataset}_edge FROM '{edge_file}' (DELIMITER '|')"
        query_load_vertex = f"COPY {dataset}_vertex FROM '{vertex_file}' (DELIMITER '|')"
        
        print(query_load_edge)
        con.execute(query_load_edge)
        con.execute(query_load_vertex)
                
        
    con.close()
    
    
def create_db_distinct_rels(dataset, db_name, n_copies=8):
    
    con = duckdb.connect(db_name)
    for i in range(n_copies):
        try:
            con.execute(f"DROP TABLE IF EXISTS e" + str(i))
            con.execute(f"DROP TABLE IF EXISTS v" + str(i))
        except:
            pass
        
        for dataset in datasets:
            
            # delete tables if they exist
            con.execute(f"DROP TABLE IF EXISTS e" + str(i))
            con.execute(f"DROP TABLE IF EXISTS v" + str(i))
            
            con.execute(f"CREATE TABLE v{str(i)} (i INTEGER, l INTEGER, d INTEGER)")
            con.execute(f"CREATE TABLE e{str(i)} (s INTEGER, t INTEGER)")
            
            # set index PK-FK on edge table
            #con.execute(f"CREATE INDEX edge_s ON {dataset}_edge(s)")
            #con.execute(f"CREATE INDEX edge_t ON {dataset}_edge(t)")
            
            # set index on vertex table
            #con.execute(f"CREATE INDEX vertex_l ON {dataset}_vertex(l)")
            
        
            # Use LpBoundPaths for dataset directory
            dataset_dir = LpBoundPaths.DATASETS_DIR / dataset
            edge_file = dataset_dir / f"{dataset}_edge.csv"
            vertex_file = dataset_dir / f"{dataset}_vertex.csv"
            
            query_load_edge = f"COPY e{str(i)} FROM '{edge_file}' (DELIMITER '|')"
            query_load_vertex = f"COPY v{str(i)} FROM '{vertex_file}' (DELIMITER '|')"
            
            con.execute(query_load_edge)
            con.execute(query_load_vertex)
    
    con.close()            


def count_labels(dataset):
    con = duckdb.connect('db')
    query = f"SELECT COUNT(DISTINCT l) FROM {dataset}_vertex"
    result = con.execute(query)
    count = result.fetchone()[0]
    con.close()
    return count

def extract_number(string):
    """Extracts the number following "EC:" from a multi-line string, even if interrupted by "│ " or other characters.

    Args:
        string: The input string.

    Returns:
        The extracted number as a string, or None if no match is found.
    """

    pattern = r"│EC:\s*(\d+)\s*.*?\n│\s*(\d+)"
    match = re.search(pattern, string, re.MULTILINE)
    
    return match.group(1) + match.group(2)


def get_duckdb_estimate(query, db_name='db'):
    con = duckdb.connect(db_name)
    ugly_string = con.sql(query).explain()
    
    try:
        estimated_cardinalities = extract_number(ugly_string)
    except:
        # get first occurence of EC: in the string
        estimated_cardinalities = re.findall(r'EC:\s(\d+)', ugly_string)[0]
        
                
    if estimated_cardinalities is None:
        estimated_cardinalities = re.findall(r'EC:\s(\d+)', ugly_string)[0]
        
    if len(estimated_cardinalities) > 0:
        total_estimated_cardinality = int(estimated_cardinalities)
    else:
        total_estimated_cardinality = 0
        
    return total_estimated_cardinality

def get_duckdb_estimate_full(query, db_name='db'):
    con = duckdb.connect(db_name)
    ugly_string = con.sql(query).explain()
    return ugly_string