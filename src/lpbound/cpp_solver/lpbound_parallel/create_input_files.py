import pandas as pd
import json
import os
import itertools
from itertools import *
import sys
import importlib    
import lpbound.cpp_solver.lpbound_parallel.constraints as u
from lpbound.config.paths import LpBoundPaths

importlib.reload(u)

def process_subqueries(benchmark_name, n_queries, queries_to_skip):
    path_output = f"{LpBoundPaths.PROJ_ROOT_DIR}/src/lpbound/cpp_solver/lpbound_parallel/input_data/" + benchmark_name
    path_input = f"{LpBoundPaths.PROJ_ROOT_DIR}/src/lpbound/cpp_solver/lpbound_parallel/raw_input/"+ benchmark_name + "_subqueries/"
    
    # check if directory exists. if not, create
    os.makedirs(path_output, exist_ok=True)
    
    files = os.listdir(path_input)
    files = [f for f in files if f.endswith('.lp')]
    
    for q in range(1, n_queries + 1):
        if q in queries_to_skip:
            continue
        
        files_query = [f for f in files if f.find(f"_subquery_estimations_{q}") != -1]
         
        n_subqueries = len(files_query)
        # write number of subqueries to a file lattice_points_{q}.txt
        with open(f'{path_output}/lattice_points_{q}.txt', 'w') as f:
            f.write(str(n_subqueries))
            
        
        counter_subq = 0
        for file in files_query:
            
            # query_stats_ineq_{q}_subq_{counter_subq}.json
            # shannon_optimized_{q}_subq_{counter_subq}.json
            # query_vars_{q}_subq_{counter_subq}.json
            
            (
                dict_monotonicities_additivities_optim,
                dict_stats_ineq,
                dict_vars,
                dict_objective
                ) = u.lbound_berge_all_constraints(path_input, file)
            
            # save dicts to folder
            
            json.dump(
                dict_monotonicities_additivities_optim, 
                open(f"{path_output}/shannon_optimized_{q}_subq_{counter_subq}.json", 'w'), indent=4)
            
            json.dump(
                dict_stats_ineq, 
                open(f"{path_output}/query_stats_ineq_{q}_subq_{counter_subq}.json", 'w'), indent=4)
            
            dict_vars_inv = {v: k for k, v in dict_vars.items()}
            json.dump(
                dict_vars_inv, 
                open(f"{path_output}/query_vars_{q}_subq_{counter_subq}.json", 'w'), indent=4)
            
            json.dump(
                dict_objective, 
                open(f"{path_output}/objective_function_{q}_subq_{counter_subq}.json", 'w'), indent=4)
            #print(f"Processed query {q}, subquery {counter_subq}")
            counter_subq += 1
            
def process_full_queries(benchmark_name, n_queries, queries_to_skip):
    
    path_input = f"{LpBoundPaths.DATA_DIR}/lps/jobjoin_lpberge/"
    path_output = f"{LpBoundPaths.PROJ_ROOT_DIR}/src/lpbound/cpp_solver/lpbound_parallel/input_data_full_queries/jobjoin_lpberge"
    
    # Create output directory if it doesn't exist
    os.makedirs(path_output, exist_ok=True)
    
    for q in range(1, n_queries + 1):
        if q in queries_to_skip:
            continue
            
        file = f"jobjoin_full_estimations_{q}.lp"
        filepath = path_input + file
        
        if not os.path.exists(filepath):
            print(f"Warning: File {filepath} not found, skipping query {q}")
            continue
            
        print(f"Processing full query {q}...")
        
        (
            dict_monotonicities_additivities_optim,
            dict_stats_ineq,
            dict_vars,
            dict_objective
        ) = u.lbound_berge_all_constraints(path_input, file)
        
        # Save dicts to folder (without subquery suffix)
        json.dump(
            dict_monotonicities_additivities_optim,
            open(f"{path_output}/shannon_optimized_{q}.json", 'w'), indent=4)
        
        json.dump(
            dict_stats_ineq,
            open(f"{path_output}/query_stats_ineq_{q}.json", 'w'), indent=4)
        
        dict_vars_inv = {v: k for k, v in dict_vars.items()}
        json.dump(
            dict_vars_inv,
            open(f"{path_output}/query_vars_{q}.json", 'w'), indent=4)
        
        json.dump(
            dict_objective,
            open(f"{path_output}/objective_function_{q}.json", 'w'), indent=4)

def prepare_lpbase_files():
      
    queries_to_prepare = {1: 8,
                          2: 8,
                          3: 6,
                          4: 8,
                          5: 8,
                          6: 8,
                          7: 12,
                          8: 11,
                          9: 13,
                          10: 12,
                          11: 13,
                          12: 13,
                          14: 13,
                          16: 12,
                          17: 11,
                          18: 11,
                          30: 10
                          }
    
    for q in list(queries_to_prepare.keys()):
        u.lpbound_base_allconstraints(
            f"{LpBoundPaths.DATA_DIR}/lps/jobjoin_lpberge/",
            "jobjoin_full_estimations_" + str(q) + ".lp"
        )
        
    

def main():
    
    print("Preparing LpBase Files (JobJoin)")
    prepare_lpbase_files()
    
    print("Processing JOBjoin subqueries...")
    process_subqueries(
        "jobjoin", 33, [29, 31]
    )
    print(".. Finished.\n\n")
    
    print("Processing JOBjoin full queries...")
    process_full_queries(
        "jobjoin", 33, [29, 31]
    )
    print(".. Finished.\n\n")
    
    print("Processing STATS ...")
    process_subqueries(
        "stats", 146, []
    )
    print(".. Finished.\n\n")
    
    print("Processing JOBlight ...")
    process_subqueries(
        "joblight", 70, []
    )
    print(".. Finished.\n\n")