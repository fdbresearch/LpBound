import sys
import os
# add src directory to sys path to find the lpbound module
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from lpbound.config.paths import LpBoundPaths
from lpbound.cyclic import LPBuilder as LPB
from lpbound.cyclic import StatsGenerator as SG
from lpbound.cyclic.utility import schema
from lpbound.cyclic.utility import create_db

import time
import sys

RUN_UNDIRECTED = True
USE_FLOW_BOUND = False

# Use LpBoundPaths for consistent path management
query_folder = LpBoundPaths.WORKLOADS_DIR / "subgraph_matching"
result_log = LpBoundPaths.RESULTS_DIR / "accuracy_cyclic" / "dblp_undirected_8.log"
dataset = 'dblp'
query_sizes = [8]


def run_experiments(
    query_sizes:list=[8],
    datasets:list=['dblp'],
    result_log:str=None,
    use_flow_bound:bool=True):
    
    # Set default result_log if not provided
    if result_log is None:
        result_log = LpBoundPaths.RESULTS_DIR / "accuracy_cyclic" / "dblp_undirected_8.log"
    
    # Ensure results directory exists
    result_log_path = LpBoundPaths.RESULTS_DIR / "accuracy_cyclic"
    result_log_path.mkdir(parents=True, exist_ok=True)
        
    for dataset in datasets:
        if RUN_UNDIRECTED:
            # create db_undirected
            create_db([dataset], 
                        db_name="db_undirected",
                        directed=False)
            statsGen = SG.StatsGenerator(schema, db_path="db_undirected", data_path=LpBoundPaths.OUTPUT_DIR / "_statistics")
            
        else:
            create_db([dataset], 
                        db_name="db",
                        directed=True)
            statsGen = SG.StatsGenerator(schema, db_path="db", data_path=LpBoundPaths.OUTPUT_DIR / "_statistics")
        
        
        statsGen.get_stats(dataset)

        # Use LpBoundPaths for workload directory
        workload_dir = LpBoundPaths.WORKLOADS_DIR / "subgraph_matching" / dataset
        query_files = os.listdir(str(workload_dir))
        query_files = [query_file for query_file in query_files if query_file.endswith('.sql')]
        
        query_sizes_patter = [f'_{query_size}_' for query_size in query_sizes]
        # check if the query file contains the query size
        query_files = [query_file for query_file in query_files if any(query_size_pattern in query_file for query_size_pattern in query_sizes_patter)]
        
        number_of_queries = len(query_files)
        counter = 1
        for file in query_files:
            
            query_file_path = workload_dir / file
            query = open(str(query_file_path), 'r').read()
            
            lpBuilder = LPB.LPBuilder(
                dataset,
                query,
                statistics=statsGen.statistics)
            
            if use_flow_bound:
                start_time = time.time()
                estimate = lpBuilder.build_and_solve_flow_bound()
                end_time = time.time()
                # print query name and estimate to result_log
                estimate = 2**estimate
                with open(str(result_log), 'a') as f:
                    f.write(f'{file.replace(".sql", "")}, {estimate}, {end_time-start_time}\n')
            else:
                print("Start building LP")
                lpBuilder.build()
                print("Solving LP")
                objective_value = lpBuilder.solve()
                # export LP to file
                #lpBuilder.export_lp(f'_lp_files/undirected/{file.replace(".sql", ".lp")}')
                estimate = 2**objective_value
                # print query name and estimate to result_log
                with open(str(result_log), 'a') as f:
                    f.write(f'{file.replace(".sql", "")}, {estimate}\n')
                    
                print(f'{file} - {estimate} - {counter}/{number_of_queries}')
                
            counter += 1
                        

if __name__ == '__main__':
    run_experiments(
        query_sizes=query_sizes, 
        datasets=[dataset], 
        result_log=str(result_log), 
        use_flow_bound=USE_FLOW_BOUND)